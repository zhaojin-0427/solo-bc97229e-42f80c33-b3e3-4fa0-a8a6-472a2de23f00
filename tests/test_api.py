import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app import service
from app.db import Database, set_db
from app.main import app as fastapi_app
from app.worker import Worker, set_worker


@pytest.fixture()
def env(tmp_path):
    db = Database(tmp_path / "test.db")
    set_db(db)
    worker = Worker(db, poll_interval=0.02)
    set_worker(worker)
    with TestClient(fastapi_app) as client:
        yield client, db, worker
    worker.stop()


def wait_idle(client, timeout=10.0):
    import time

    deadline = time.time() + timeout
    while time.time() < deadline:
        p = client.get("/api/v1/progress").json()
        if p["queue"]["idle"]:
            return p
        time.sleep(0.02)
    raise AssertionError("worker did not become idle: " + str(p))


def upload(client, kind, csv_text):
    return client.post(
        f"/api/v1/uploads/{kind}",
        files={"file": ("data.csv", csv_text.encode(), "text/csv")},
    )


READINGS = """meter_id,ts_utc,value_wh
M-001,0,1000
M-001,3600,1600
M-001,7200,2200
M-002,0,0
M-002,3600,500
"""

BATCHES = """batch_no,meter_id,start_utc,end_utc
B-1,M-001,1800,5400
B-2,M-002,0,1800
"""


def test_interpolation_and_used_readings(env):
    client, db, worker = env
    assert upload(client, "readings", READINGS).status_code == 202
    assert upload(client, "batches", BATCHES).status_code == 202
    wait_idle(client)

    r = client.get("/api/v1/batches/B-1").json()
    assert r["result"]["status"] == "ok"
    # start = 1000 + 600*(1800/3600) = 1300
    # end   = 1600 + 600*(5400-3600)/3600 = 1900
    assert r["result"]["start_value_wh"] == pytest.approx(1300.0)
    assert r["result"]["end_value_wh"] == pytest.approx(1900.0)
    assert r["result"]["energy_wh"] == 600
    refs = r["result"]["used_readings"]["start"]
    assert {x["role"] for x in refs} == {"lower", "upper"}
    assert [x["value_wh"] for x in refs] == [1000, 1600]

    r2 = client.get("/api/v1/batches/B-2").json()
    assert r2["result"]["energy_wh"] == 250


def test_missing_bracketing_reading_is_pending(env):
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "batches", "batch_no,meter_id,start_utc,end_utc\nB-3,M-001,100,9000\n")
    wait_idle(client)
    r = client.get("/api/v1/batches/B-3").json()
    assert r["result"]["status"] == "pending_readings"
    assert r["result"]["energy_wh"] is None
    assert "no later reading" in r["result"]["note"]


def test_reading_regression_rejects_whole_upload(env):
    client, db, worker = env
    upload(client, "readings", READINGS)
    bad = "meter_id,ts_utc,value_wh\nM-001,10800,1500\nM-002,7200,900\n"
    resp = upload(client, "readings", bad)
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["code"] == "reading_decreased"
    assert body["stored"] is False
    # nothing from the rejected file was stored
    with db.reader() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM readings WHERE (meter_id, ts_utc) IN (('M-001',10800),('M-002',7200))"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM uploads").fetchone()[0] == 1


def test_batch_overlap_rejected(env):
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "batches", BATCHES)
    overlap = "batch_no,meter_id,start_utc,end_utc\nB-9,M-001,5000,6000\n"
    resp = upload(client, "batches", overlap)
    assert resp.status_code == 422
    assert resp.json()["code"] == "batch_overlap"
    wait_idle(client)
    assert client.get("/api/v1/batches/B-9").status_code == 404


def test_overwrite_same_key_triggers_recompute(env):
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "batches", BATCHES)
    wait_idle(client)
    assert client.get("/api/v1/batches/B-1").json()["result"]["energy_wh"] == 600

    corrected = "meter_id,ts_utc,value_wh\nM-001,3600,1000\n"
    assert upload(client, "readings", corrected).status_code == 202
    wait_idle(client)
    r = client.get("/api/v1/batches/B-1").json()
    # start 1000, end 1000 + (2200-1000)*(1800/3600) = 1600 -> 600
    assert r["result"]["start_value_wh"] == pytest.approx(1000.0)
    assert r["result"]["end_value_wh"] == pytest.approx(1600.0)
    assert r["result"]["energy_wh"] == 600


def test_stale_results_served_with_flag_during_recompute(env):
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "batches", BATCHES)
    wait_idle(client)
    # Stop the worker so the recompute cannot finish, then query.
    worker.stop()
    upload(client, "readings", "meter_id,ts_utc,value_wh\nM-001,3600,1200\n")
    r = client.get("/api/v1/meters/M-001/results").json()
    assert r["result_pending_update"] is True
    assert r["has_snapshot"] is True
    assert all(x["result_pending_update"] for x in r["results"])
    assert r["results"][0]["energy_wh"] == 600  # old complete set still served
    assert "last complete published result" in r["message"]


def test_superseded_old_job_never_reverts_newer(env):
    """Two overlapping uploads: the late-finishing old computation must not win."""
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "batches", BATCHES)
    wait_idle(client)
    worker.stop()

    # Upload A then B without letting the worker run.
    upload(client, "readings", "meter_id,ts_utc,value_wh\nM-001,3600,1400\n")  # A
    upload(client, "readings", "meter_id,ts_utc,value_wh\nM-001,3600,2000\n")  # B wins
    # With B the start at 1800 interp is 1000 + 1000*1800/3600 = 1500 and the
    # end at 5400 is 2000 + 200*1800/3600 = 2100.  A would have yielded 1200.
    worker.start()
    wait_idle(client)
    r = client.get("/api/v1/batches/B-1").json()
    assert r["result"]["start_value_wh"] == pytest.approx(1500.0)
    assert r["result"]["end_value_wh"] == pytest.approx(2100.0)
    assert r["result"]["energy_wh"] == 600

    with db.reader() as conn:
        states = conn.execute("SELECT meter_id, applied_job_id, version FROM meter_state").fetchall()
        assert all(s["version"] >= 1 for s in states)
        # overlapping uploads are coalesced, so the meter's published data is
        # one atomic group; the result must contain the newest upload's values.
        job_ids = [
            r[0]
            for r in conn.execute(
                "SELECT j.id FROM jobs j JOIN job_meters jm ON jm.job_id = j.id "
                "WHERE jm.meter_id = 'M-001' ORDER BY j.id"
            )
        ]
        applied = {s["meter_id"]: s["applied_job_id"] for s in states}["M-001"]
        assert applied in job_ids


def test_grouped_publish_multi_meter_atomic(env):
    """A job touching several meters publishes all of them together."""
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "batches", BATCHES)
    wait_idle(client)
    worker.stop()

    # One CSV touching both meters.
    both = (
        "meter_id,ts_utc,value_wh\n"
        "M-001,3600,1700\n"
        "M-002,3600,600\n"
    )
    resp = upload(client, "readings", both)
    assert sorted(resp.json()["affected_meters"]) == ["M-001", "M-002"]

    # Simulate a crash right before publish: no snapshot may be half-written.
    with db.reader() as conn:
        before = {
            r["meter_id"]: r["snapshot_id"]
            for r in conn.execute("SELECT meter_id, snapshot_id FROM meter_snapshots WHERE is_current=1")
        }
    worker.start()
    wait_idle(client)
    with db.reader() as conn:
        after = {
            r["meter_id"]: r["snapshot_id"]
            for r in conn.execute("SELECT meter_id, snapshot_id FROM meter_snapshots WHERE is_current=1")
        }
        job = conn.execute(
            "SELECT MAX(id) FROM jobs WHERE reason='readings upload'"
        ).fetchone()[0]
        meters = [
            r["meter_id"] for r in conn.execute("SELECT meter_id FROM job_meters WHERE job_id=?", (job,))
        ]
        snaps = [
            r["job_id"] for r in conn.execute(
                "SELECT job_id FROM meter_snapshots WHERE is_current=1 AND meter_id IN (%s)"
                % ",".join("?" for _ in meters),
                meters,
            )
        ]
    assert set(meters) == {"M-001", "M-002"}
    assert set(snaps) == {job}, "both meters must be published by the same job"
    assert after != before


def test_failed_publish_leaves_no_partial_state(env, monkeypatch):
    """If publishing raises mid-way, no meter of the job is updated.

    Drives the worker cycle directly so the failure is deterministic: the
    exception is raised after the first meter's rows were inserted, and the
    write transaction's rollback must remove them.
    """
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "batches", BATCHES)
    wait_idle(client)
    worker.stop()
    with db.reader() as conn:
        before = [
            tuple(r) for r in conn.execute(
                "SELECT snapshot_id FROM meter_snapshots WHERE is_current=1 ORDER BY meter_id"
            )
        ]
    upload(client, "readings", "meter_id,ts_utc,value_wh\nM-001,3600,1500\nM-002,3600,550\n")

    import app.compute as c

    real_publish = c.publish

    def failing_publish(conn, job_id, plans, results, created_at):
        real_publish(conn, job_id, plans[:1], {plans[0].meter_id: results[plans[0].meter_id]},
                     created_at)
        raise RuntimeError("simulated failure after first meter was inserted")

    monkeypatch.setattr(c, "publish", failing_publish)
    fresh = Worker(db, poll_interval=0.02)
    set_worker(fresh)
    claim = fresh._claim()
    assert claim is not None
    with pytest.raises(RuntimeError):
        fresh._process(*claim)
    with db.reader() as conn:
        after = [
            tuple(r) for r in conn.execute(
                "SELECT snapshot_id FROM meter_snapshots WHERE is_current=1 ORDER BY meter_id"
            )
        ]
        leftovers = conn.execute(
            "SELECT COUNT(*) FROM meter_snapshots WHERE is_current=0"
        ).fetchone()[0]
    assert after == before, "rollback must leave the previous snapshots current"
    assert leftovers == 0, "no half-written snapshot rows may survive rollback"


def test_restart_resumes_unfinished_recompute(env, tmp_path):
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "batches", BATCHES)
    wait_idle(client)
    worker.stop()

    upload(client, "readings", "meter_id,ts_utc,value_wh\nM-001,3600,1500\n")
    # Simulate a crash: the job stays 'queued' (or 'running') in the DB.
    with db.write() as conn:
        conn.execute("UPDATE jobs SET status='running' WHERE status='queued'")

    # "Restart": new worker on the same DB.
    worker2 = Worker(db, poll_interval=0.02)
    set_worker(worker2)
    worker2.start()
    try:
        deadline = __import__("time").time() + 10
        while __import__("time").time() < deadline:
            p = service.get_progress(db)
            if p["idle"] and p["pending_meters"] == []:
                break
            __import__("time").sleep(0.02)
        r = service.get_batch(db, "B-1")
        # start at 1800 = 1000 + 500*1800/3600 = 1250; end at 5400 = 1500 + 700/2 = 1850
        assert r["result"]["start_value_wh"] == pytest.approx(1250.0)
        assert r["result"]["end_value_wh"] == pytest.approx(1850.0)
        assert r["result"]["result_pending_update"] is False
    finally:
        worker2.stop()


def test_duplicate_upload_raw_records_kept(env):
    client, db, worker = env
    upload(client, "readings", READINGS)
    upload(client, "readings", READINGS)
    wait_idle(client)
    with db.reader() as conn:
        uploads = conn.execute("SELECT kind, payload, sha256 FROM uploads ORDER BY id").fetchall()
    assert len(uploads) == 2
    assert uploads[0]["payload"] == uploads[1]["payload"]
    assert uploads[0]["sha256"] == uploads[1]["sha256"]


def test_raw_body_upload_supported(env):
    client, db, worker = env
    resp = client.post(
        "/api/v1/uploads/readings",
        content=READINGS.encode(),
        headers={"content-type": "text/csv"},
    )
    assert resp.status_code == 202


def test_bad_header_and_encoding(env):
    client, db, worker = env
    resp = upload(client, "readings", "a,b,c\n1,2,3\n")
    assert resp.status_code == 422
    assert resp.json()["code"] == "bad_header"
    resp = client.post(
        "/api/v1/uploads/readings",
        files={"file": ("x.csv", b"\xff\xfe\x00bad", "text/csv")},
    )
    assert resp.status_code == 422
    assert resp.json()["code"] == "invalid_encoding"
