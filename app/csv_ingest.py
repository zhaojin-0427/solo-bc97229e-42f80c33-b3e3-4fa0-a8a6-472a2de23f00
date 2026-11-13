"""CSV parsing and upload validation.

Every upload is validated against the *merged* state (existing rows plus the
new rows, with same-key rows overwritten) before a single write happens.  Any
violation rejects the whole upload: nothing is stored, no recompute is queued.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, field
from datetime import datetime, timezone


MAX_REPORTED_ERRORS = 20


class UploadRejected(Exception):
    def __init__(self, code: str, message: str, details: list[dict] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or []


@dataclass
class ReadingRow:
    meter_id: str
    ts_utc: int
    value_wh: int


@dataclass
class BatchRow:
    batch_no: str
    meter_id: str
    start_utc: int
    end_utc: int


@dataclass
class ParsedUpload:
    kind: str
    payload: str
    sha256: str
    size_bytes: int
    filename: str | None = None
    readings: list[ReadingRow] = field(default_factory=list)
    batches: list[BatchRow] = field(default_factory=list)
    stats: dict = field(default_factory=dict)


def utc_iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _decode(raw: bytes) -> str:
    if not raw:
        raise UploadRejected("empty_upload", "Uploaded file is empty.")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise UploadRejected(
            "invalid_encoding", "CSV must be UTF-8 encoded.", [{"message": str(exc)}]
        ) from exc
    return text


def _parse_int(value: str, field_name: str, line: int, errors: list[dict]) -> int | None:
    text = (value or "").strip()
    if text == "":
        errors.append({"line": line, "field": field_name, "message": "missing value"})
        return None
    try:
        number = int(text, 10)
    except ValueError:
        errors.append(
            {
                "line": line,
                "field": field_name,
                "message": f"expected integer seconds, got {text!r}",
            }
        )
        return None
    if number < 0:
        errors.append(
            {"line": line, "field": field_name, "message": "must not be negative"}
        )
        return None
    return number


def _read_csv_rows(text: str, required: list[str]) -> tuple[list[dict], dict]:
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise UploadRejected("empty_upload", "CSV has no header row.")

    header = [h.strip().lstrip("﻿").lower() for h in header]
    missing = [c for c in required if c not in header]
    if missing:
        raise UploadRejected(
            "bad_header",
            "CSV header is missing required column(s): " + ", ".join(missing),
            [{"header": header, "required": required}],
        )
    index = {name: header.index(name) for name in required}

    rows: list[dict] = []
    blank = 0
    malformed = 0
    for line_no, raw_row in enumerate(reader, start=2):
        if all(not (cell or "").strip() for cell in raw_row):
            blank += 1
            continue
        if len(raw_row) < len(header):
            malformed += 1
            continue
        rows.append({"line": line_no, **{name: raw_row[i] for name, i in index.items()}})
    return rows, {"blank_lines": blank, "malformed_rows": malformed}


def parse_readings_csv(raw: bytes, filename: str | None) -> ParsedUpload:
    text = _decode(raw)
    rows, meta = _read_csv_rows(text, ["meter_id", "ts_utc", "value_wh"])
    if not rows:
        raise UploadRejected("no_data_rows", "CSV contains no data rows.")

    errors: list[dict] = []
    by_key: dict[tuple[str, int], ReadingRow] = {}
    file_duplicates = 0
    for row in rows:
        meter_id = (row["meter_id"] or "").strip()
        if not meter_id:
            errors.append({"line": row["line"], "field": "meter_id", "message": "missing value"})
            continue
        ts = _parse_int(row["ts_utc"], "ts_utc", row["line"], errors)
        value = _parse_int(row["value_wh"], "value_wh", row["line"], errors)
        if ts is None or value is None:
            continue
        key = (meter_id, ts)
        if key in by_key:
            file_duplicates += 1
        # Same key in one file: the later row wins, matching overwrite semantics.
        by_key[key] = ReadingRow(meter_id=meter_id, ts_utc=ts, value_wh=value)

    if errors:
        raise UploadRejected(
            "invalid_rows", "CSV contains malformed rows.", errors[:MAX_REPORTED_ERRORS]
        )

    readings = sorted(by_key.values(), key=lambda r: (r.meter_id, r.ts_utc))
    stats = {
        **meta,
        "rows": len(rows),
        "unique_keys": len(readings),
        "duplicate_keys_in_file": file_duplicates,
        "meters": sorted({r.meter_id for r in readings}),
    }
    return ParsedUpload(
        kind="readings",
        payload=text,
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        filename=filename,
        readings=readings,
        stats=stats,
    )


def parse_batches_csv(raw: bytes, filename: str | None) -> ParsedUpload:
    text = _decode(raw)
    rows, meta = _read_csv_rows(text, ["batch_no", "meter_id", "start_utc", "end_utc"])
    if not rows:
        raise UploadRejected("no_data_rows", "CSV contains no data rows.")

    errors: list[dict] = []
    by_batch: dict[str, BatchRow] = {}
    file_duplicates = 0
    for row in rows:
        batch_no = (row["batch_no"] or "").strip()
        meter_id = (row["meter_id"] or "").strip()
        if not batch_no:
            errors.append({"line": row["line"], "field": "batch_no", "message": "missing value"})
            continue
        if not meter_id:
            errors.append({"line": row["line"], "field": "meter_id", "message": "missing value"})
            continue
        start = _parse_int(row["start_utc"], "start_utc", row["line"], errors)
        end = _parse_int(row["end_utc"], "end_utc", row["line"], errors)
        if start is None or end is None:
            continue
        if end < start:
            errors.append(
                {
                    "line": row["line"],
                    "field": "end_utc",
                    "message": f"end_utc {end} is before start_utc {start}",
                }
            )
            continue
        if batch_no in by_batch:
            file_duplicates += 1
        by_batch[batch_no] = BatchRow(batch_no, meter_id, start, end)

    if errors:
        raise UploadRejected(
            "invalid_rows", "CSV contains malformed rows.", errors[:MAX_REPORTED_ERRORS]
        )

    batches = sorted(by_batch.values(), key=lambda b: b.batch_no)
    stats = {
        **meta,
        "rows": len(rows),
        "unique_batches": len(batches),
        "duplicate_batches_in_file": file_duplicates,
        "meters": sorted({b.meter_id for b in batches}),
    }
    return ParsedUpload(
        kind="batches",
        payload=text,
        sha256=hashlib.sha256(raw).hexdigest(),
        size_bytes=len(raw),
        filename=filename,
        batches=batches,
        stats=stats,
    )


def validate_monotonic(meter_id: str, points: list[tuple[int, int]]) -> list[dict]:
    """Return violations of "cumulative reading must never decrease"."""
    points = sorted(points)
    violations = []
    for (prev_ts, prev_value), (ts, value) in zip(points, points[1:]):
        if value < prev_value:
            violations.append(
                {
                    "code": "reading_decreased",
                    "meter_id": meter_id,
                    "ts_utc": ts,
                    "time": utc_iso(ts),
                    "value_wh": value,
                    "previous_ts_utc": prev_ts,
                    "previous_time": utc_iso(prev_ts),
                    "previous_value_wh": prev_value,
                    "message": (
                        f"meter {meter_id}: cumulative reading decreased at {utc_iso(ts)} "
                        f"({value} < {prev_value} at {utc_iso(prev_ts)})"
                    ),
                }
            )
    return violations


def validate_no_overlap(meter_id: str, batches: list[BatchRow]) -> list[dict]:
    """Return overlapping batch pairs for one meter.

    Intervals are half-open: two batches may share a boundary instant (one ends
    exactly when the next starts) but must not intersect over a positive span.
    """
    ordered = sorted(batches, key=lambda b: (b.start_utc, b.end_utc, b.batch_no))
    violations = []
    for previous, current in zip(ordered, ordered[1:]):
        if current.start_utc < previous.end_utc:
            violations.append(
                {
                    "code": "batch_overlap",
                    "meter_id": meter_id,
                    "batches": [previous.batch_no, current.batch_no],
                    "message": (
                        f"meter {meter_id}: batches {previous.batch_no} "
                        f"[{utc_iso(previous.start_utc)}, {utc_iso(previous.end_utc)}] and "
                        f"{current.batch_no} "
                        f"[{utc_iso(current.start_utc)}, {utc_iso(current.end_utc)}] overlap"
                    ),
                }
            )
    return violations
