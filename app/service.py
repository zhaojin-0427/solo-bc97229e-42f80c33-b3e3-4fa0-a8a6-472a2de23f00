"""Upload acceptance and query logic.

Accepting an upload is one short ``BEGIN IMMEDIATE`` write transaction that
**validates against current committed state and then writes**, so:

* a rejected upload leaves the database completely untouched (the transaction
  rolls back, nothing was stored, no job was queued);
* two concurrent uploads cannot each pass validation against a stale view and
  then jointly violate monotonicity/overlap — the write lock serialises them.

The transaction:

    validate merged state (existing + new rows, same-key overwrite)
    insert raw upload record
    upsert rows (same-key overwrite)
    bump data_version for affected meters
    mark those meters pending
    insert the recompute job + its meter set
"""

from __future__ import annotations

from .csv_ingest import (
    MAX_REPORTED_ERRORS,
    BatchRow,
    ParsedUpload,
    UploadRejected,
    now_iso,
    utc_iso,
    validate_monotonic,
    validate_no_overlap,
)
from .db import Database


def _existing_readings(conn, meter_ids: list[str]) -> dict[str, dict[int, int]]:
    out: dict[str, dict[int, int]] = {m: {} for m in meter_ids}
    for chunk_start in range(0, len(meter_ids), 500):
        chunk = meter_ids[chunk_start : chunk_start + 500]
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT meter_id, ts_utc, value_wh FROM readings WHERE meter_id IN ({placeholders})",
            chunk,
        ):
            out[row["meter_id"]][row["ts_utc"]] = row["value_wh"]
    return out


def _validate_readings_merged(conn, upload: ParsedUpload) -> None:
    meters = sorted({r.meter_id for r in upload.readings})
    existing = _existing_readings(conn, meters)

    merged: dict[str, dict[int, int]] = {m: dict(existing[m]) for m in meters}
    for row in upload.readings:
        merged[row.meter_id][row.ts_utc] = row.value_wh

    violations = []
    for meter_id in meters:
        points = sorted(merged[meter_id].items())
        violations.extend(validate_monotonic(meter_id, points))
        if len(violations) >= MAX_REPORTED_ERRORS:
            break
    if violations:
        raise UploadRejected(
            "reading_decreased",
            "Upload rejected: cumulative readings must never decrease. "
            "No rows from this upload were stored.",
            violations[:MAX_REPORTED_ERRORS],
        )


def _existing_batches(conn, meter_ids: list[str]) -> dict[str, dict[str, dict]]:
    out: dict[str, dict[str, dict]] = {m: {} for m in meter_ids}
    for chunk_start in range(0, len(meter_ids), 500):
        chunk = meter_ids[chunk_start : chunk_start + 500]
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT batch_no, meter_id, start_utc, end_utc FROM batches "
            f"WHERE meter_id IN ({placeholders})",
            chunk,
        ):
            out[row["meter_id"]][row["batch_no"]] = dict(row)
    return out


def _validate_batches_merged(conn, upload: ParsedUpload) -> list[str]:
    """Validate merged batch state and return all affected meters."""
    new_by_no = {b.batch_no: b for b in upload.batches}
    # Overwriting a batch can move it to another meter, so both the old and
    # the new meter of every touched batch_no are affected.
    previous = {
        row["batch_no"]: dict(row)
        for row in conn.execute(
            "SELECT batch_no, meter_id, start_utc, end_utc FROM batches "
            f"WHERE batch_no IN ({','.join('?' for _ in new_by_no)})",
            list(new_by_no),
        )
    } if new_by_no else {}

    affected = sorted(
        {b.meter_id for b in upload.batches}
        | {p["meter_id"] for p in previous.values()}
    )
    existing = _existing_batches(conn, affected)

    merged: dict[str, dict[str, BatchRow]] = {
        m: {no: BatchRow(b["batch_no"], m, b["start_utc"], b["end_utc"]) for no, b in rows.items()}
        for m, rows in existing.items()
    }
    # Overwrite semantics first: drop the rows this upload replaces, then add
    # the new ones.  This is exactly what the database transaction will do.
    for old in previous.values():
        merged.setdefault(old["meter_id"], {}).pop(old["batch_no"], None)
    for batch in upload.batches:
        merged.setdefault(batch.meter_id, {})[batch.batch_no] = batch

    violations = []
    for meter_id in sorted(merged):
        violations.extend(validate_no_overlap(meter_id, list(merged[meter_id].values())))
        if len(violations) >= MAX_REPORTED_ERRORS:
            break
    if violations:
        raise UploadRejected(
            "batch_overlap",
            "Upload rejected: batches on the same meter must not overlap. "
            "No rows from this upload were stored.",
            violations[:MAX_REPORTED_ERRORS],
        )
    return affected


def accept_readings(db: Database, upload: ParsedUpload) -> dict:
    meters = sorted({r.meter_id for r in upload.readings})
    received_at = now_iso()

    with db.write() as conn:
        _validate_readings_merged(conn, upload)
        upload_id = _insert_upload(conn, upload, received_at)
        for row in upload.readings:
            conn.execute(
                "INSERT INTO readings(meter_id, ts_utc, value_wh) VALUES (?, ?, ?) "
                "ON CONFLICT(meter_id, ts_utc) DO UPDATE SET value_wh = excluded.value_wh",
                (row.meter_id, row.ts_utc, row.value_wh),
            )
        job_id = _mark_and_queue(conn, meters, upload_id, received_at, "readings upload")
        conn.execute("UPDATE uploads SET job_id = ? WHERE id = ?", (job_id, upload_id))

    return {
        "upload_id": upload_id,
        "job_id": job_id,
        "status": "accepted",
        "affected_meters": meters,
        "stats": upload.stats,
    }


def accept_batches(db: Database, upload: ParsedUpload) -> dict:
    received_at = now_iso()

    with db.write() as conn:
        affected = _validate_batches_merged(conn, upload)
        upload_id = _insert_upload(conn, upload, received_at)
        for batch in upload.batches:
            conn.execute(
                "INSERT INTO batches(batch_no, meter_id, start_utc, end_utc) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(batch_no) DO UPDATE SET "
                "  meter_id = excluded.meter_id,"
                "  start_utc = excluded.start_utc,"
                "  end_utc = excluded.end_utc",
                (batch.batch_no, batch.meter_id, batch.start_utc, batch.end_utc),
            )
        job_id = _mark_and_queue(conn, affected, upload_id, received_at, "batches upload")
        conn.execute("UPDATE uploads SET job_id = ? WHERE id = ?", (job_id, upload_id))

    return {
        "upload_id": upload_id,
        "job_id": job_id,
        "status": "accepted",
        "affected_meters": affected,
        "stats": upload.stats,
    }


def _insert_upload(conn, upload: ParsedUpload, received_at: str) -> int:
    import json

    cursor = conn.execute(
        "INSERT INTO uploads(kind, received_at, filename, sha256, size_bytes, payload, stats) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            upload.kind,
            received_at,
            upload.filename,
            upload.sha256,
            upload.size_bytes,
            upload.payload,
            json.dumps(upload.stats, sort_keys=True),
        ),
    )
    return cursor.lastrowid


def _mark_and_queue(
    conn, meters: list[str], upload_id: int, received_at: str, reason: str
) -> int | None:
    """Bump data versions, mark meters pending and queue one job for them.

    All of it happens in the caller's single transaction, so the meter set of
    an upload is always published together.
    """
    if not meters:
        return None
    for meter_id in meters:
        conn.execute(
            "INSERT INTO meter_state(meter_id, applied_job_id, version, data_version, "
            " pending, pending_job_id, updated_at) "
            "VALUES (?, 0, 0, 1, 1, NULL, ?) "
            "ON CONFLICT(meter_id) DO UPDATE SET "
            "  data_version = data_version + 1,"
            "  pending = 1,"
            "  updated_at = excluded.updated_at",
            (meter_id, received_at),
        )
    cursor = conn.execute(
        "INSERT INTO jobs(upload_id, status, created_at, reason) VALUES (?, 'queued', ?, ?)",
        (upload_id, received_at, reason),
    )
    job_id = cursor.lastrowid
    for meter_id in meters:
        conn.execute("INSERT INTO job_meters(job_id, meter_id) VALUES (?, ?)", (job_id, meter_id))
        # Points at the newest job for the meter; older jobs become superseded
        # once a newer job has published, but may still run to publish a
        # superset of meters together.
        conn.execute(
            "UPDATE meter_state SET pending_job_id = ? WHERE meter_id = ?",
            (job_id, meter_id),
        )
    return job_id


# --------------------------------------------------------------------------
# Queries
# --------------------------------------------------------------------------


def _pending_map(conn, meter_ids: list[str]) -> dict[str, dict]:
    if not meter_ids:
        return {}
    placeholders = ",".join("?" for _ in meter_ids)
    return {
        row["meter_id"]: {
            "pending": bool(row["pending"]),
            "pending_job_id": row["pending_job_id"],
            "applied_job_id": row["applied_job_id"],
            "version": row["version"],
            "updated_at": row["updated_at"],
        }
        for row in conn.execute(
            f"SELECT * FROM meter_state WHERE meter_id IN ({placeholders})", meter_ids
        )
    }


def _reading_refs(conn, snapshot_id: int, batch_no: str) -> dict:
    refs: dict[str, list[dict]] = {"start": [], "end": []}
    for row in conn.execute(
        "SELECT endpoint, role, ts_utc, value_wh FROM batch_reading_refs "
        "WHERE snapshot_id = ? AND batch_no = ? ORDER BY endpoint, ts_utc",
        (snapshot_id, batch_no),
    ):
        refs[row["endpoint"]].append(
            {
                "role": row["role"],
                "ts_utc": row["ts_utc"],
                "time": utc_iso(row["ts_utc"]),
                "value_wh": row["value_wh"],
            }
        )
    return refs


def _result_dict(row, pending: dict | None, refs: dict) -> dict:
    stale = bool(pending and pending["pending"])
    return {
        "batch_no": row["batch_no"],
        "meter_id": row["meter_id"],
        "status": row["status"],
        "energy_wh": row["energy_wh"],
        "energy_wh_exact": row["energy_wh_exact"],
        "start_utc": row["start_ts_utc"],
        "end_utc": row["end_ts_utc"],
        "start_time": utc_iso(row["start_ts_utc"]),
        "end_time": utc_iso(row["end_ts_utc"]),
        "start_value_wh": row["start_value_wh"],
        "end_value_wh": row["end_value_wh"],
        "note": row["note"],
        "used_readings": refs,
        "result_pending_update": stale,
        "pending_job_id": pending["pending_job_id"] if stale else None,
        "snapshot_id": row["snapshot_id"],
        "version": row["snapshot_version"],
    }


def get_batch(db: Database, batch_no: str) -> dict | None:
    with db.read_snapshot() as conn:
        result = conn.execute(
            "SELECT br.*, ms.version AS snapshot_version FROM batch_results br "
            "JOIN meter_snapshots ms ON ms.snapshot_id = br.snapshot_id "
            "WHERE ms.is_current = 1 AND br.batch_no = ?",
            (batch_no,),
        ).fetchone()
        definition = conn.execute(
            "SELECT batch_no, meter_id, start_utc, end_utc FROM batches WHERE batch_no = ?",
            (batch_no,),
        ).fetchone()
        if definition is None:
            return None
        pending = _pending_map(conn, [definition["meter_id"]]).get(definition["meter_id"])
        if result is None:
            return {
                "batch_no": batch_no,
                "definition": dict(definition),
                "result": None,
                "result_pending_update": bool(pending and pending["pending"]),
                "message": "No published result yet; recompute is queued.",
            }
        refs = _reading_refs(conn, result["snapshot_id"], batch_no)
        return {
            "batch_no": batch_no,
            "definition": {
                "batch_no": definition["batch_no"],
                "meter_id": definition["meter_id"],
                "start_utc": definition["start_utc"],
                "end_utc": definition["end_utc"],
                "start_time": utc_iso(definition["start_utc"]),
                "end_time": utc_iso(definition["end_utc"]),
            },
            "result": _result_dict(result, pending, refs),
        }


def list_meter_results(db: Database, meter_id: str) -> dict:
    with db.read_snapshot() as conn:
        pending = _pending_map(conn, [meter_id]).get(meter_id)
        rows = conn.execute(
            "SELECT br.*, ms.version AS snapshot_version FROM batch_results br "
            "JOIN meter_snapshots ms ON ms.snapshot_id = br.snapshot_id "
            "WHERE ms.is_current = 1 AND ms.meter_id = ? ORDER BY br.batch_no",
            (meter_id,),
        ).fetchall()
        results = [
            _result_dict(row, pending, _reading_refs(conn, row["snapshot_id"], row["batch_no"]))
            for row in rows
        ]
        definitions = [
            dict(row)
            for row in conn.execute(
                "SELECT batch_no, meter_id, start_utc, end_utc FROM batches "
                "WHERE meter_id = ? ORDER BY batch_no",
                (meter_id,),
            )
        ]
    stale = bool(pending and pending["pending"])
    return {
        "meter_id": meter_id,
        "result_pending_update": stale,
        "pending_job_id": pending["pending_job_id"] if pending else None,
        "applied_job_id": pending["applied_job_id"] if pending else None,
        "version": pending["version"] if pending else None,
        "has_snapshot": bool(results),
        "results": results,
        "batch_definitions": definitions,
        "message": (
            "Showing the last complete published result set; a recompute is "
            "in progress and these numbers will be replaced atomically."
            if stale
            else "Results are up to date."
        ),
    }


def list_jobs(db: Database, limit: int = 50) -> dict:
    with db.read_snapshot() as conn:
        jobs = [dict(row) for row in conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)
        )]
        counts = {
            row["status"]: row["n"]
            for row in conn.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status")
        }
        for job in jobs:
            job["meters"] = [
                row["meter_id"]
                for row in conn.execute(
                    "SELECT meter_id FROM job_meters WHERE job_id = ? ORDER BY meter_id",
                    (job["id"],),
                )
            ]
        pending = [
            dict(row)
            for row in conn.execute(
                "SELECT meter_id, pending_job_id, applied_job_id, updated_at, last_error "
                "FROM meter_state WHERE pending = 1 ORDER BY meter_id"
            )
        ]
    return {"counts": counts, "pending_meters": pending, "jobs": jobs}


def get_job(db: Database, job_id: int) -> dict | None:
    with db.read_snapshot() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        job = dict(row)
        job["meters"] = [
            dict(r)
            for r in conn.execute(
                "SELECT jm.meter_id, ms.pending, ms.applied_job_id, ms.version "
                "FROM job_meters jm LEFT JOIN meter_state ms ON ms.meter_id = jm.meter_id "
                "WHERE jm.job_id = ? ORDER BY jm.meter_id",
                (job_id,),
            )
        ]
    return job


def get_progress(db: Database) -> dict:
    with db.read_snapshot() as conn:
        counts = {
            row["status"]: row["n"]
            for row in conn.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status")
        }
        pending = [
            dict(row)
            for row in conn.execute(
                "SELECT meter_id, pending_job_id, applied_job_id, updated_at "
                "FROM meter_state WHERE pending = 1 ORDER BY meter_id"
            )
        ]
    outstanding = counts.get("queued", 0) + counts.get("running", 0)
    return {
        "job_counts": counts,
        "recompute_outstanding": outstanding,
        "pending_meters": pending,
        "idle": outstanding == 0,
    }


def get_upload(db: Database, upload_id: int) -> dict | None:
    with db.read_snapshot() as conn:
        row = conn.execute("SELECT * FROM uploads WHERE id = ?", (upload_id,)).fetchone()
    return dict(row) if row else None


def list_uploads(db: Database, limit: int = 50) -> list[dict]:
    with db.read_snapshot() as conn:
        rows = conn.execute(
            "SELECT id, kind, received_at, filename, sha256, size_bytes, stats, job_id "
            "FROM uploads ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]
