"""Invariant tests for atomic grouped publishing and version-guarded results.

These exercise interleavings directly against the service layer so the
ordering is deterministic (no reliance on thread scheduling luck).
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from app import compute, service
from app.csv_ingest import parse_batches_csv, parse_readings_csv
from app.db import Database, set_db
from app.worker import Worker, set_worker


@pytest.fixture()
def db(tmp_path):
    database = Database(tmp_path / "inv.db")
    set_db(database)
    set_worker(None)
    yield database
    set_worker(None)


def up_readings(db, text):
    return service.accept_readings(db, parse_readings_csv(text.encode(), "r.csv"))


def up_batches(db, text):
    return service.accept_batches(db, parse_batches_csv(text.encode(), "b.csv"))


def run_all(db):
    """Drain the queue synchronously with a real worker thread."""
    w = Worker(db, poll_interval=0.01)
    w.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        if service.get_progress(db)["idle"]:
            break
        time.sleep(0.01)
    w.stop()
    return w


BASE_READINGS = "meter_id,ts_utc,value_wh\nM,0,0\nM,1000,1000\nM,2000,2000\n"
BATCH = "batch_no,meter_id,start_utc,end_utc\nB,M,500,1500\n"


def test_late_old_job_cannot_overwrite_newer(db, monkeypatch):
    """A job that read old data must not publish after newer data landed."""
    up_readings(db, BASE_READINGS)
    up_batches(db, BATCH)
    run_all(db)
    assert service.get_batch(db, "B")["result"]["energy_wh"] == 1000

    # Capture the plans + results for an OLD job, then let a NEW upload land.
    with db.read_snapshot() as conn:
        meters = [r["meter_id"] for r in conn.execute("SELECT meter_id FROM job_meters ORDER BY job_id DESC LIMIT 1")]
        old_plans = compute.load_plans(conn, meters)
    old_results = compute.compute_plans(old_plans)

    up_readings(db, "meter_id,ts_utc,value_wh\nM,1000,1200\n")  # new slope
    run_all(db)  # newer job publishes
    assert service.get_batch(db, "B")["result"]["energy_wh"] == 1000

    # Now the old computation tries to publish: it must be refused and the
    # published result must still be the newer one.
    with db.write() as conn:
        conflict = compute.publish(conn, 999, old_plans, old_results, "2026-01-01T00:00:00Z")
    assert conflict is not None and "changed" in conflict
    r = service.get_batch(db, "B")["result"]
    assert r["energy_wh"] == 1000
    assert r["energy_wh_exact"] == pytest.approx(1000.0)


def test_overlapping_uploads_publish_grouped_meters(db):
    """Upload #2 touching 2 meters publishes both under one snapshot/job."""
    up_readings(db, BASE_READINGS)
    up_batches(db, BATCH)
    run_all(db)

    # New upload touching M and N; both must appear in the same job.
    res = up_readings(
        db,
        "meter_id,ts_utc,value_wh\nM,1000,1500\nN,0,0\nN,1000,100\n",
    )
    assert res["affected_meters"] == ["M", "N"]
    run_all(db)
    with db.reader() as conn:
        rows = conn.execute(
            "SELECT ms.meter_id, ms.job_id FROM meter_snapshots ms WHERE ms.is_current=1"
        ).fetchall()
    assert {r["meter_id"]: r["job_id"] for r in rows} == {"M": res["job_id"], "N": res["job_id"]}


def test_pending_cleared_only_on_publish(db):
    up_readings(db, BASE_READINGS)
    up_batches(db, BATCH)
    run_all(db)
    assert service.get_progress(db)["pending_meters"] == []
    up_readings(db, "meter_id,ts_utc,value_wh\nM,1000,1200\n")
    progress = service.get_progress(db)
    assert [m["meter_id"] for m in progress["pending_meters"]] == ["M"]
    run_all(db)
    assert service.get_progress(db)["pending_meters"] == []


def test_rejected_upload_does_not_bump_version_or_queue(db):
    up_readings(db, BASE_READINGS)
    up_batches(db, BATCH)
    run_all(db)
    with db.reader() as conn:
        before = conn.execute("SELECT data_version, applied_job_id FROM meter_state WHERE meter_id='M'").fetchone()
        jobs_before = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]

    from app.csv_ingest import UploadRejected

    with pytest.raises(UploadRejected):
        up_readings(db, "meter_id,ts_utc,value_wh\nM,500,999999\nM,1500,10\n")
    with db.reader() as conn:
        after = conn.execute("SELECT data_version, applied_job_id FROM meter_state WHERE meter_id='M'").fetchone()
        jobs_after = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    assert tuple(before) == tuple(after)
    assert jobs_before == jobs_after


def test_batch_move_between_meters_updates_both(db):
    up_readings(db, BASE_READINGS + "N,0,0\nN,1000,100\nN,2000,200\n")
    up_batches(db, BATCH)
    run_all(db)
    # Move batch B to meter N; both M and N are affected and republished.
    res = up_batches(db, "batch_no,meter_id,start_utc,end_utc\nB,N,500,1500\n")
    assert res["affected_meters"] == ["M", "N"]
    run_all(db)
    r = service.get_batch(db, "B")
    assert r["definition"]["meter_id"] == "N"
    assert r["result"]["energy_wh"] == 100


def test_all_meters_of_job_share_version_publication(db):
    """When several meters change at once they become current in one commit."""
    up_readings(db, "meter_id,ts_utc,value_wh\nA,0,0\nA,10,10\nB,0,0\nB,10,20\nC,0,0\nC,10,30\n")
    up_batches(db, "batch_no,meter_id,start_utc,end_utc\nBA,A,2,8\nBB,B,2,8\nBC,C,2,8\n")
    run_all(db)
    res = up_readings(db, "meter_id,ts_utc,value_wh\nA,10,100\nB,10,200\nC,10,300\n")
    run_all(db)
    with db.reader() as conn:
        jobs = [
            r["job_id"]
            for r in conn.execute("SELECT job_id FROM meter_snapshots WHERE is_current=1")
        ]
    assert jobs == [res["job_id"]] * 3
    # A: 0@0 -> 100@10; start t=2 => 20, end t=8 => 80, energy 60.
    assert service.get_batch(db, "BA")["result"]["energy_wh"] == 60


def test_overlapping_upload_chains_coalesce(db):
    """Upload1{A}, Upload2{A,B}: both publish together, atomically."""
    up_readings(db, "meter_id,ts_utc,value_wh\nA,0,0\nA,10,10\nB,0,0\nB,10,20\n")
    up_batches(db, "batch_no,meter_id,start_utc,end_utc\nBA,A,2,8\nBB,B,2,8\n")
    run_all(db)

    set_worker(None)
    r1 = up_readings(db, "meter_id,ts_utc,value_wh\nA,10,100\n")
    r2 = up_readings(db, "meter_id,ts_utc,value_wh\nA,10,200\nB,10,400\n")
    run_all(db)

    with db.reader() as conn:
        snaps = {
            r["meter_id"]: r["job_id"]
            for r in conn.execute("SELECT meter_id, job_id FROM meter_snapshots WHERE is_current=1")
        }
    # A and B must share one job id — never a mix of the two uploads.
    assert snaps["A"] == snaps["B"]
    assert snaps["A"] in (r1["job_id"], r2["job_id"])
    # And the numbers reflect the final data (A: 0->200, B: 0->400).
    assert service.get_batch(db, "BA")["result"]["energy_wh"] == 120
    assert service.get_batch(db, "BB")["result"]["energy_wh"] == 240
    assert service.get_progress(db)["pending_meters"] == []


def test_concurrent_uploads_converge_to_newest_data(db):
    """Hammer the worker with concurrent uploads from many threads."""
    import threading

    meters = [f"M{i}" for i in range(6)]
    readings = "meter_id,ts_utc,value_wh\n" + "".join(
        f"{m},0,0\n{m},10000,1000\n" for m in meters
    )
    up_readings(db, readings)
    up_batches(
        db,
        "batch_no,meter_id,start_utc,end_utc\n"
        + "".join(f"B{m}, {m}, 1000, 9000\n".replace(" ", "") for m in meters),
    )
    run_all(db)

    worker = Worker(db, poll_interval=0.005)
    worker.start()
    stop = threading.Event()
    errors = []

    def uploader(n):
        try:
            for i in range(1, 15):
                meter = meters[n % len(meters)]
                value = 1000 + n * 100 + i
                up_readings(db, f"meter_id,ts_utc,value_wh\n{meter},10000,{value}\n")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=uploader, args=(n,)) for n in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    deadline = time.time() + 30
    while time.time() < deadline:
        p = service.get_progress(db)
        if p["idle"] and not p["pending_meters"]:
            break
        time.sleep(0.01)
    worker.stop()
    assert not errors, errors
    assert service.get_progress(db)["pending_meters"] == []

    # Every published number must equal what the final readings imply.
    with db.reader() as conn:
        final = {
            (r["meter_id"], r["ts_utc"]): r["value_wh"]
            for r in conn.execute("SELECT meter_id, ts_utc, value_wh FROM readings")
        }
        snaps = [
            (r["meter_id"], r["job_id"], r["snapshot_id"])
            for r in conn.execute("SELECT * FROM meter_snapshots WHERE is_current=1")
        ]
    for meter, job_id, snapshot_id in snaps:
        end_value = final[(meter, 10000)]
        # readings 0@0 -> v@10000; batch 1000..9000 => v*0.9 - v*0.1 = 0.8*v
        expected = end_value * 0.8
        got = service.get_batch(db, f"B{meter}")
        assert got["result"]["energy_wh"] == round(expected), (meter, got["result"], expected)
        assert got["result"]["end_value_wh"] == pytest.approx(end_value * 0.9), (
            meter,
            "published snapshot must match the final readings",
        )
    # No snapshot row may reference a job that is not done.
    with db.reader() as conn:
        bad = conn.execute(
            "SELECT COUNT(*) FROM meter_snapshots ms JOIN jobs j ON j.id = ms.job_id "
            "WHERE ms.is_current = 1 AND j.status != 'done'"
        ).fetchone()[0]
    assert bad == 0


def test_concurrent_conflicting_uploads_never_both_apply(db):
    """Two uploads that are individually valid but jointly non-monotonic.

    Each row is valid against the base data, but together they would make the
    reading decrease between 4000 and 6000.  Validation runs inside the
    write transaction, so exactly one is rejected under any interleaving.
    """
    import threading

    up_readings(db, "meter_id,ts_utc,value_wh\nM,0,1000\nM,10000,2000\n")
    barrier = threading.Barrier(2)
    outcomes = []

    def attempt(ts, value):
        text = f"meter_id,ts_utc,value_wh\nM,{ts},{value}\n"
        barrier.wait()
        try:
            up_readings(db, text)
            outcomes.append(("ok", ts, value))
        except Exception as exc:  # noqa: BLE001
            outcomes.append(("rejected", ts, value, str(exc)))

    # (4000, 1800) and (6000, 1200): second applied after first is a decrease.
    threads = [
        threading.Thread(target=attempt, args=(4000, 1800)),
        threading.Thread(target=attempt, args=(6000, 1200)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [o for o in outcomes if o[0] == "ok"]
    rejected = [o for o in outcomes if o[0] == "rejected"]
    assert len(accepted) == 1 and len(rejected) == 1, outcomes
    # Either row may win the race; the invariant is that they can never both
    # be applied (both were valid alone, jointly non-monotonic).

    with db.reader() as conn:
        rows = [(r["ts_utc"], r["value_wh"]) for r in conn.execute(
            "SELECT ts_utc, value_wh FROM readings WHERE meter_id='M' ORDER BY ts_utc")]
    assert all(a[1] <= b[1] for a, b in zip(rows, rows[1:])), rows
    with db.reader() as conn:
        assert conn.execute("SELECT COUNT(*) FROM uploads").fetchone()[0] == 2


def test_recovery_requeues_pending_meter_whose_job_is_done(db):
    """A pending meter must be retried even if its recorded job is 'done'."""
    up_readings(db, "meter_id,ts_utc,value_wh\nM,0,0\nM,10,10\n")
    up_batches(db, "batch_no,meter_id,start_utc,end_utc\nB,M,2,8\n")
    run_all(db)

    # Simulate a crash-after-marking-pending / incomplete-job state: the meter
    # is pending but every job covering it is already done.
    with db.write() as conn:
        conn.execute("UPDATE meter_state SET pending = 1, pending_job_id = NULL WHERE meter_id='M'")
        conn.execute("UPDATE jobs SET status='done'")
    # Push the data version the published snapshot was based on out of date by
    # deleting the result rows, so recovery has real work to do.
    with db.write() as conn:
        conn.execute("UPDATE meter_state SET data_version = data_version + 1 WHERE meter_id='M'")

    w = Worker(db, poll_interval=0.01)
    w.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        p = service.get_progress(db)
        if p["idle"] and not p["pending_meters"]:
            break
        time.sleep(0.01)
    w.stop()
    result = service.get_batch(db, "B")
    assert result["result"] is not None
    assert result["result"]["energy_wh"] == 6
    assert service.get_progress(db)["pending_meters"] == []
