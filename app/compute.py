"""Batch energy computation.

For a batch [start, end] on one meter we linearly interpolate the cumulative
reading at each endpoint from the two adjacent observed readings and subtract.
If either endpoint is not enclosed by observed readings the batch is reported
as ``pending_readings`` instead of producing a number.

Computation only ever reads: all reads happen inside one read-only transaction
(``Database.read_snapshot``).  Results are written afterwards in a single short
write transaction together with a data-version guard, so nothing computed from
stale input can ever be published, and a failure never exposes partial rows.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field



@dataclass
class Interpolated:
    value: float
    refs: list[tuple[str, int, int]]  # (role, ts_utc, value_wh)


@dataclass
class BatchComputation:
    batch_no: str
    meter_id: str
    start_ts_utc: int
    end_ts_utc: int
    status: str
    energy_wh: int | None = None
    energy_wh_exact: float | None = None
    start_value_wh: float | None = None
    end_value_wh: float | None = None
    note: str | None = None
    start: Interpolated | None = None
    end: Interpolated | None = None


def interpolate(
    timestamps: list[int], values: list[int], t: int
) -> tuple[Interpolated | None, str | None]:
    """Linear interpolation at ``t``; returns (value, missing_reason)."""
    if not timestamps:
        return None, "meter has no readings"
    idx = bisect_left(timestamps, t)
    if idx < len(timestamps) and timestamps[idx] == t:
        return Interpolated(float(values[idx]), [("exact", timestamps[idx], values[idx])]), None
    if idx == 0:
        return None, f"no earlier reading at or before t={t}"
    if idx == len(timestamps):
        return None, f"no later reading at or after t={t}"
    t0, t1 = timestamps[idx - 1], timestamps[idx]
    v0, v1 = values[idx - 1], values[idx]
    value = v0 + (v1 - v0) * (t - t0) / (t1 - t0)
    return Interpolated(
        float(value),
        [("lower", t0, v0), ("upper", t1, v1)],
    ), None


def compute_meter(
    conn,
    meter_id: str,
    batches: list[dict],
    readings: list[tuple[int, int]],
) -> list[BatchComputation]:
    timestamps = [ts for ts, _ in readings]
    values = [v for _, v in readings]
    out: list[BatchComputation] = []
    for batch in batches:
        start, start_missing = interpolate(timestamps, values, batch["start_utc"])
        end, end_missing = interpolate(timestamps, values, batch["end_utc"])
        missing = [m for m in (start_missing, end_missing) if m]
        if start is None or end is None:
            out.append(
                BatchComputation(
                    batch_no=batch["batch_no"],
                    meter_id=meter_id,
                    start_ts_utc=batch["start_utc"],
                    end_ts_utc=batch["end_utc"],
                    status="pending_readings",
                    start_value_wh=start.value if start else None,
                    end_value_wh=end.value if end else None,
                    note="; ".join(missing),
                    start=start,
                    end=end,
                )
            )
            continue
        exact = end.value - start.value
        if exact < 0:
            # Defensive: monotonic readings make this impossible unless data
            # changed under us, in which case the version guard retries anyway.
            out.append(
                BatchComputation(
                    batch_no=batch["batch_no"],
                    meter_id=meter_id,
                    start_ts_utc=batch["start_utc"],
                    end_ts_utc=batch["end_utc"],
                    status="pending_readings",
                    start_value_wh=start.value,
                    end_value_wh=end.value,
                    note="interpolated end is below start; readings inconsistent",
                    start=start,
                    end=end,
                )
            )
            continue
        out.append(
            BatchComputation(
                batch_no=batch["batch_no"],
                meter_id=meter_id,
                start_ts_utc=batch["start_utc"],
                end_ts_utc=batch["end_utc"],
                status="ok",
                energy_wh=int(round(exact)),
                energy_wh_exact=exact,
                start_value_wh=start.value,
                end_value_wh=end.value,
                start=start,
                end=end,
            )
        )
    return out


@dataclass
class MeterPlan:
    meter_id: str
    data_version: int
    batches: list[dict] = field(default_factory=list)
    readings: list[tuple[int, int]] = field(default_factory=list)


def load_plans(conn, meter_ids: list[str]) -> list[MeterPlan]:
    plans = []
    for meter_id in meter_ids:
        state = conn.execute(
            "SELECT data_version FROM meter_state WHERE meter_id = ?", (meter_id,)
        ).fetchone()
        data_version = state["data_version"] if state else 0
        batches = [
            dict(row)
            for row in conn.execute(
                "SELECT batch_no, meter_id, start_utc, end_utc FROM batches "
                "WHERE meter_id = ? ORDER BY batch_no",
                (meter_id,),
            )
        ]
        readings = [
            (row["ts_utc"], row["value_wh"])
            for row in conn.execute(
                "SELECT ts_utc, value_wh FROM readings WHERE meter_id = ? ORDER BY ts_utc",
                (meter_id,),
            )
        ]
        plans.append(MeterPlan(meter_id, data_version, batches, readings))
    return plans


def compute_plans(plans: list[MeterPlan]) -> dict[str, list[BatchComputation]]:
    return {
        plan.meter_id: compute_meter(None, plan.meter_id, plan.batches, plan.readings)
        for plan in plans
    }


def publish(
    conn,
    job_id: int | dict[str, int],
    plans: list[MeterPlan],
    results: dict[str, list[BatchComputation]],
    created_at: str,
) -> str | None:
    """Write all meters of a job in one transaction.

    ``job_id`` is either one id for all meters or a mapping meter -> job id
    (used when several coalesced jobs are published together, so each meter's
    snapshot points at the job whose upload produced its numbers).

    Must be called inside ``Database.write`` (BEGIN IMMEDIATE), which holds the
    write lock, so the version check and the writes are indivisible.

    Returns None on success or a reason string when the input data changed
    after it was read (the caller then recomputes and retries).
    """
    attribution = job_id if isinstance(job_id, dict) else {p.meter_id: job_id for p in plans}
    for plan in plans:
        state = conn.execute(
            "SELECT data_version FROM meter_state WHERE meter_id = ?", (plan.meter_id,)
        ).fetchone()
        current = state["data_version"] if state else 0
        if current != plan.data_version:
            return (
                f"meter {plan.meter_id} changed (data_version {plan.data_version} -> "
                f"{current}); recompute required"
            )

    for plan in plans:
        version_row = conn.execute(
            "SELECT version FROM meter_state WHERE meter_id = ?", (plan.meter_id,)
        ).fetchone()
        version = (version_row["version"] if version_row else 0) + 1

        conn.execute(
            "UPDATE meter_snapshots SET is_current = 0 "
            "WHERE meter_id = ? AND is_current = 1",
            (plan.meter_id,),
        )
        cursor = conn.execute(
            "INSERT INTO meter_snapshots(meter_id, job_id, version, created_at, is_current) "
            "VALUES (?, ?, ?, ?, 1)",
            (plan.meter_id, attribution[plan.meter_id], version, created_at),
        )
        snapshot_id = cursor.lastrowid

        for result in results[plan.meter_id]:
            conn.execute(
                "INSERT INTO batch_results("
                "snapshot_id, batch_no, meter_id, status, energy_wh, energy_wh_exact,"
                " start_value_wh, end_value_wh, start_ts_utc, end_ts_utc, note) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    snapshot_id,
                    result.batch_no,
                    result.meter_id,
                    result.status,
                    result.energy_wh,
                    result.energy_wh_exact,
                    result.start_value_wh,
                    result.end_value_wh,
                    result.start_ts_utc,
                    result.end_ts_utc,
                    result.note,
                ),
            )
            for endpoint, interp in (("start", result.start), ("end", result.end)):
                if interp is None:
                    continue
                for role, ts, value in interp.refs:
                    conn.execute(
                        "INSERT INTO batch_reading_refs("
                        "snapshot_id, batch_no, endpoint, role, meter_id, ts_utc, value_wh) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (snapshot_id, result.batch_no, endpoint, role, plan.meter_id, ts, value),
                    )

        conn.execute(
            "INSERT INTO meter_state(meter_id, applied_job_id, version, data_version, "
            " pending, pending_job_id, updated_at) "
            "VALUES (?, ?, ?, ?, 0, NULL, ?) "
            "ON CONFLICT(meter_id) DO UPDATE SET "
            "  applied_job_id = excluded.applied_job_id,"
            "  version = excluded.version,"
            "  pending = 0,"
            "  pending_job_id = NULL,"
            "  updated_at = excluded.updated_at,"
            "  last_error = NULL",
            (
                plan.meter_id,
                attribution[plan.meter_id],
                version,
                plan.data_version,
                created_at,
            ),
        )
    return None
