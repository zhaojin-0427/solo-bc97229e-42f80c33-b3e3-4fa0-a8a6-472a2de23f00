#!/usr/bin/env python3
"""End-to-end demo against a running service.

Usage:
    python3 scripts/demo.py [http://localhost:8000]

Uploads example CSVs, waits for recompute, prints batch energies and progress.
"""

import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")
EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def call(path, method="GET", body=None, content_type="application/json"):
    req = urllib.request.Request(BASE + path, method=method, data=body)
    if body is not None:
        req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, __import__("json").loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, __import__("json").loads(exc.read())


def upload(kind, name):
    path = EXAMPLES / name
    boundary = "----demo" + uuid.uuid4().hex
    data = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
        f"filename=\"{name}\"\r\nContent-Type: text/csv\r\n\r\n"
    ).encode() + path.read_bytes() + f"\r\n--{boundary}--\r\n".encode()
    status, body = call(
        f"/api/v1/uploads/{kind}",
        method="POST",
        body=data,
        content_type=f"multipart/form-data; boundary={boundary}",
    )
    print(f"  upload {name}: HTTP {status} -> {body.get('message') or body}")
    return body


def wait_idle(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, p = call("/api/v1/progress")
        if p["queue"]["idle"]:
            return p
        time.sleep(0.2)
    raise SystemExit("worker did not become idle")


def main():
    print(f"Service: {BASE}")
    print("\n1) Upload readings and batches")
    upload("readings", "readings_meter01.csv")
    upload("readings", "readings_meter02.csv")
    upload("batches", "batches_2026_01_01.csv")
    wait_idle()

    print("\n2) Batch energies")
    for batch_no in ("B-2026-01-01-A", "B-2026-01-01-B"):
        _, body = call(f"/api/v1/batches/{batch_no}")
        r = body["result"]
        print(
            f"  {batch_no}: {r['energy_wh']} Wh  "
            f"(start {r['start_value_wh']:.1f} @ {r['start_time']}, "
            f"end {r['end_value_wh']:.1f} @ {r['end_time']})"
        )
        for endpoint in ("start", "end"):
            refs = ", ".join(
                f"{x['role']}={x['value_wh']}@{x['time']}"
                for x in r["used_readings"][endpoint]
            )
            print(f"      {endpoint}: {refs}")

    print("\n3) Correction upload triggers background recompute")
    upload("readings", "readings_correction.csv")
    _, progress = call("/api/v1/progress")
    print(f"  immediately after upload: idle={progress['queue']['idle']} "
          f"pending={[m['meter_id'] for m in progress['queue']['pending_meters']]}")
    _, stale = call("/api/v1/batches/B-2026-01-01-A")
    print(f"  query during recompute: pending_update={stale['result']['result_pending_update']} "
          f"energy={stale['result']['energy_wh']} Wh (old complete set)")
    wait_idle()
    _, fresh = call("/api/v1/batches/B-2026-01-01-A")
    print(f"  after recompute:        pending_update={fresh['result']['result_pending_update']} "
          f"energy={fresh['result']['energy_wh']} Wh")

    print("\n4) Progress report")
    _, progress = call("/api/v1/progress")
    print(f"  {progress['queue']['job_counts']}")


if __name__ == "__main__":
    main()
