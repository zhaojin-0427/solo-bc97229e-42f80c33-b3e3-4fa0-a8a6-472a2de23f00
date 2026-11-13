from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path


SCHEMA = """
-- Verbatim accepted uploads.  The raw CSV text and its hash are kept so the
-- original input can always be audited, even after later corrections.
CREATE TABLE IF NOT EXISTS uploads (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('readings', 'batches')),
    received_at TEXT NOT NULL,
    filename    TEXT,
    sha256      TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL,
    payload     TEXT NOT NULL,
    stats       TEXT NOT NULL DEFAULT '{}',
    job_id      INTEGER
);

-- Cumulative meter readings.  (meter_id, ts_utc) is the overwrite key.
CREATE TABLE IF NOT EXISTS readings (
    meter_id TEXT NOT NULL,
    ts_utc   INTEGER NOT NULL CHECK (ts_utc >= 0),
    value_wh INTEGER NOT NULL CHECK (value_wh >= 0),
    PRIMARY KEY (meter_id, ts_utc)
);

-- Batch definitions.  batch_no is the overwrite key.
CREATE TABLE IF NOT EXISTS batches (
    batch_no  TEXT PRIMARY KEY,
    meter_id  TEXT NOT NULL,
    start_utc INTEGER NOT NULL CHECK (start_utc >= 0),
    end_utc   INTEGER NOT NULL CHECK (end_utc >= 0),
    CHECK (end_utc >= start_utc)
);

-- A background recompute job.  id is a global monotonic sequence; a
-- computation may only publish while the data version it read is unchanged.
CREATE TABLE IF NOT EXISTS jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    upload_id     INTEGER REFERENCES uploads(id),
    status        TEXT NOT NULL CHECK (
                      status IN ('queued', 'running', 'done', 'superseded', 'failed')),
    attempts      INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    error         TEXT,
    reason        TEXT NOT NULL,
    origin_job_id INTEGER REFERENCES jobs(id)
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, id);
CREATE INDEX IF NOT EXISTS idx_jobs_upload ON jobs(upload_id);

-- The meters a job must publish together, atomically.
CREATE TABLE IF NOT EXISTS job_meters (
    job_id   INTEGER NOT NULL REFERENCES jobs(id),
    meter_id TEXT NOT NULL,
    PRIMARY KEY (job_id, meter_id)
);
CREATE INDEX IF NOT EXISTS idx_job_meters_meter ON job_meters(meter_id, job_id);

-- Per-meter bookkeeping.
--   data_version   increments on every accepted write affecting the meter
--   applied_job_id last job whose results were published
--   pending        1 = results are stale / recompute outstanding
CREATE TABLE IF NOT EXISTS meter_state (
    meter_id       TEXT PRIMARY KEY,
    applied_job_id INTEGER NOT NULL DEFAULT 0,
    version        INTEGER NOT NULL DEFAULT 0,
    data_version   INTEGER NOT NULL DEFAULT 0,
    pending        INTEGER NOT NULL DEFAULT 0,
    pending_job_id INTEGER,
    updated_at     TEXT,
    last_error     TEXT
);

-- Immutable published result set for one meter.  Exactly one is_current row
-- per meter (partial unique index); publishing replaces it atomically.
CREATE TABLE IF NOT EXISTS meter_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_id    TEXT NOT NULL,
    job_id      INTEGER NOT NULL,
    version     INTEGER NOT NULL,
    created_at  TEXT NOT NULL,
    is_current  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_snapshots_current ON meter_snapshots(meter_id, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS uq_snapshots_one_current
    ON meter_snapshots(meter_id) WHERE is_current = 1;

CREATE TABLE IF NOT EXISTS batch_results (
    snapshot_id    INTEGER NOT NULL REFERENCES meter_snapshots(snapshot_id),
    batch_no       TEXT NOT NULL,
    meter_id       TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('ok', 'pending_readings')),
    energy_wh      INTEGER,
    energy_wh_exact REAL,
    start_value_wh REAL,
    end_value_wh   REAL,
    start_ts_utc   INTEGER NOT NULL,
    end_ts_utc     INTEGER NOT NULL,
    note           TEXT,
    PRIMARY KEY (snapshot_id, batch_no)
);

-- The observed readings that bracket each interpolated endpoint, so the
-- interpolated values can be checked by hand.
CREATE TABLE IF NOT EXISTS batch_reading_refs (
    snapshot_id INTEGER NOT NULL REFERENCES meter_snapshots(snapshot_id),
    batch_no    TEXT NOT NULL,
    endpoint    TEXT NOT NULL CHECK (endpoint IN ('start', 'end')),
    role        TEXT NOT NULL CHECK (role IN ('lower', 'upper', 'exact')),
    meter_id    TEXT NOT NULL,
    ts_utc      INTEGER NOT NULL,
    value_wh    INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, batch_no, endpoint, role)
);

-- Published snapshots are never mutated in place; only whole replacement.
CREATE INDEX IF NOT EXISTS idx_results_batch ON batch_results(batch_no);
"""


class Database:
    """Small SQLite wrapper.

    - WAL mode: query readers never block the background publisher.
    - ``write`` serialises writers in-process and wraps every unit of work in
      ``BEGIN IMMEDIATE``; any error rolls the whole unit back.
    - ``read_snapshot`` opens a read-only transaction so all statements in a
      block observe one committed state.  Computation uses this exclusively:
      it never carries a write transaction into the official database.
    """

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path, timeout=30, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            conn = self._connect()
            try:
                conn.executescript(SCHEMA)
            finally:
                conn.close()

    @contextmanager
    def reader(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def read_snapshot(self):
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    @contextmanager
    def write(self):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            finally:
                conn.close()


_db: Database | None = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database(os.environ.get("CARBON_DB_PATH", "data/carbon.db"))
    return _db


def set_db(db: Database) -> None:
    global _db
    _db = db
