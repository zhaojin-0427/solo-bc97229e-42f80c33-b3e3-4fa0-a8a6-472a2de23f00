"""Background recompute worker.

Guarantees:

* A single worker thread drains the queue in job-id order.
* Jobs whose meter sets overlap (directly or transitively) are coalesced into
  **one atomic publish**: every meter of every such job is recomputed and
  written in a single write transaction.  So the meters affected by one upload
  are always updated together, including when uploads overlap.
* Computation reads from a read-only snapshot and then publishes in a short
  write transaction guarded by the per-meter ``data_version``.  If an upload
  lands between the read and the publish, the publish is refused, the job
  re-reads fresh data and recomputes.  A result computed from older input can
  therefore never overwrite newer confirmed changes, regardless of timing.
* The publish transaction writes results, clears ``pending`` and marks the
  coalesced jobs done together.  Any failure rolls the whole unit back,
  leaving the previous published snapshot intact.
* All state is in SQLite, so ``start()`` recovers: ``running`` jobs (interrupted
  by a crash) go back to ``queued``, and meters still flagged pending with no
  live job get a fresh recovery job.
"""

from __future__ import annotations

import logging
import threading
import time

from . import compute
from .csv_ingest import now_iso
from .db import Database, get_db

log = logging.getLogger("carbon.worker")

MAX_CONFLICT_RETRIES = 8
MAX_JOB_ATTEMPTS = 10


class Worker:
    def __init__(self, db: Database, poll_interval: float = 0.2):
        self.db = db
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._current_job_id: int | None = None
        self._published = 0
        self._failed = 0
        self._conflicts = 0

    # -- lifecycle ------------------------------------------------------
    def start(self) -> None:
        self._stop.clear()
        self.recover()
        if self._thread is not None and self._thread.is_alive():
            self.wake()
            return
        self._thread = threading.Thread(target=self._run, name="recompute-worker", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def wake(self) -> None:
        self._wake.set()

    def recover(self) -> None:
        with self.db.write() as conn:
            interrupted = [
                row["id"]
                for row in conn.execute("SELECT id FROM jobs WHERE status = 'running'")
            ]
            for job_id in interrupted:
                conn.execute(
                    "UPDATE jobs SET status = 'queued', error = NULL WHERE id = ?",
                    (job_id,),
                )
            # A meter flagged pending needs at least one live (queued/running)
            # job that covers it.  Looking at job_meters rather than just
            # pending_job_id also recovers meters whose recorded job finished
            # without covering them (e.g. a coalesced job superseded by a
            # narrower one).
            orphaned = [
                row["meter_id"]
                for row in conn.execute(
                    "SELECT ms.meter_id FROM meter_state ms WHERE ms.pending = 1 AND "
                    "NOT EXISTS ("
                    "  SELECT 1 FROM job_meters jm JOIN jobs j ON j.id = jm.job_id "
                    "  WHERE jm.meter_id = ms.meter_id AND j.status IN ('queued', 'running'))"
                )
            ]
            if orphaned:
                job_id = conn.execute(
                    "INSERT INTO jobs(status, created_at, reason) "
                    "VALUES ('queued', ?, 'startup recovery')",
                    (now_iso(),),
                ).lastrowid
                for meter_id in orphaned:
                    conn.execute(
                        "INSERT INTO job_meters(job_id, meter_id) VALUES (?, ?)",
                        (job_id, meter_id),
                    )
                    conn.execute(
                        "UPDATE meter_state SET pending_job_id = ?, last_error = NULL "
                        "WHERE meter_id = ?",
                        (job_id, meter_id),
                    )
            if interrupted or orphaned:
                log.warning(
                    "recovery: re-queued %d interrupted job(s), %d orphaned meter(s)",
                    len(interrupted),
                    len(orphaned),
                )

    # -- main loop ------------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set():
            claim = None
            try:
                claim = self._claim()
            except Exception:  # pragma: no cover - defensive
                log.exception("failed to poll job queue")
            if claim is None:
                self._wake.wait(self.poll_interval)
                self._wake.clear()
                continue
            seed, component = claim
            try:
                self._process(seed, component)
            except Exception:
                self._failed += 1
                log.exception("unexpected error processing job %s", seed)
                # Nothing was published (the write transaction rolled back).
                # Return every job this claim touched to the queue so the next
                # pass retries; never leave a job stuck in 'running'.
                self._requeue_jobs(component, "requeued after worker error; see server logs")
            finally:
                self._current_job_id = None

    def _meter_sets(self, conn, statuses: tuple[str, ...]) -> dict[int, set[str]]:
        placeholders = ",".join("?" for _ in statuses)
        groups: dict[int, set[str]] = {}
        for row in conn.execute(
            f"SELECT jm.job_id, jm.meter_id FROM job_meters jm "
            f"JOIN jobs j ON j.id = jm.job_id WHERE j.status IN ({placeholders})",
            statuses,
        ):
            groups.setdefault(row["job_id"], set()).add(row["meter_id"])
        return groups

    @staticmethod
    def _closure(groups: dict[int, set[str]], seed: int) -> tuple[list[int], set[str]]:
        """Jobs transitively sharing a meter with ``seed`` (including seed)."""
        component = {seed}
        meters = set(groups.get(seed, set()))
        changed = True
        while changed:
            changed = False
            for job_id, job_meters in groups.items():
                if job_id not in component and job_meters & meters:
                    component.add(job_id)
                    meters |= job_meters
                    changed = True
        return sorted(component), meters

    def _claim(self) -> tuple[int, list[int]] | None:
        with self.db.write() as conn:
            seed_row = conn.execute(
                "SELECT id FROM jobs WHERE status = 'queued' ORDER BY id LIMIT 1"
            ).fetchone()
            if seed_row is None:
                return None
            seed = seed_row["id"]
            component, meters = self._closure(self._meter_sets(conn, ("queued",)), seed)
            if not meters:
                conn.execute(
                    "UPDATE jobs SET status = 'done', finished_at = ? WHERE id = ?",
                    (now_iso(), seed),
                )
                return None
            started = now_iso()
            for job_id in component:
                conn.execute(
                    "UPDATE jobs SET status = 'running', started_at = ?,"
                    " attempts = attempts + 1 WHERE id = ?",
                    (started, job_id),
                )
            self._current_job_id = seed
            return seed, component

    def _process(self, seed: int, claimed: list[int]) -> None:
        # Read-only snapshot -> compute with no transaction -> short guarded
        # write transaction.  The official database never holds a write
        # transaction while computation runs.
        for attempt in range(1, MAX_CONFLICT_RETRIES + 1):
            with self.db.read_snapshot() as conn:
                groups = self._meter_sets(conn, ("queued", "running"))
                component, meters = self._closure(groups, seed)
                plans = compute.load_plans(conn, sorted(meters))
            if not plans:
                with self.db.write() as conn:
                    self._finish(conn, component, seed)
                return

            results = compute.compute_plans(plans)
            created_at = now_iso()
            # Attribute each meter's published snapshot to the newest job in
            # the component that actually covers that meter, so audit queries
            # point at the job whose upload produced the numbers.
            attribution = {
                plan.meter_id: max(
                    (job_id for job_id in component
                     if plan.meter_id in groups.get(job_id, set())),
                    default=seed,
                )
                for plan in plans
            }
            with self.db.write() as conn:
                conflict = compute.publish(
                    conn, attribution, plans, results, created_at
                )
                if conflict is None:
                    self._finish(conn, component, seed)
                    self._published += 1
                    return
                self._conflicts += 1
                conn.execute(
                    "UPDATE jobs SET error = ? WHERE id = ?",
                    (f"conflict retry {attempt}: {conflict}", seed),
                )
            time.sleep(min(0.02 * attempt, 0.2))

        # Uploads arrived continuously during every attempt.  Put the job back
        # so a later pass recomputes from the (now newer) data; never publish
        # the stale result.
        self._requeue_jobs(component, "requeued: input changed during recompute")

    @staticmethod
    def _finish(conn, component: list[int], seed: int) -> None:
        finished = now_iso()
        for job_id in component:
            conn.execute(
                "UPDATE jobs SET status = 'done', finished_at = ?, error = NULL WHERE id = ?",
                (finished, job_id),
            )

    def _requeue_jobs(self, job_ids: list[int], message: str) -> None:
        """Return jobs to the queue, but fail them after too many attempts.

        A job that keeps erroring must not spin forever; it is marked failed
        and its meters stay pending (with ``last_error``), so startup recovery
        or a later upload retries it.
        """
        try:
            with self.db.write() as conn:
                for job_id in job_ids:
                    attempts = conn.execute(
                        "SELECT attempts FROM jobs WHERE id = ?", (job_id,)
                    ).fetchone()
                    if attempts is not None and attempts["attempts"] >= MAX_JOB_ATTEMPTS:
                        conn.execute(
                            "UPDATE jobs SET status = 'failed', finished_at = ?, error = ? "
                            "WHERE id = ?",
                            (now_iso(), message[:2000], job_id),
                        )
                        conn.execute(
                            "UPDATE meter_state SET last_error = ?, pending = 1,"
                            " pending_job_id = NULL WHERE pending_job_id = ?",
                            (message[:2000], job_id),
                        )
                    else:
                        conn.execute(
                            "UPDATE jobs SET status = 'queued', error = ? WHERE id = ?",
                            (message[:2000], job_id),
                        )
        except Exception:  # pragma: no cover - best effort
            log.exception("could not requeue jobs %s", job_ids)

    def status(self) -> dict:
        return {
            "running": self._thread is not None and self._thread.is_alive(),
            "current_job_id": self._current_job_id,
            "published": self._published,
            "conflicts_retried": self._conflicts,
            "failed": self._failed,
        }


_worker: Worker | None = None


def get_worker() -> Worker:
    global _worker
    if _worker is None:
        _worker = Worker(get_db())
    return _worker


def set_worker(worker: Worker | None) -> None:
    global _worker
    _worker = worker
