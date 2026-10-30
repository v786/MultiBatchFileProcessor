"""Acceptance run against a live stack (docker compose or scripts/dev_nodocker.sh).

Uploads a 50-file batch over real HTTP exactly as the browser does (presigned PUTs / multipart parts straight to
S3, then `complete`), with vendor faults on, and asserts:
  * every file reaches a terminal state, the batch is COMPLETED or PARTIAL with the expected failures
  * the vendor never saw more than 20 concurrent calls, never returned 429, and stored no duplicate record
  * a retried POST /batches with the same Idempotency-Key returns the same task ids

    python tests/e2e/acceptance.py --api http://localhost:8000 --vendor http://localhost:4000 [--users 3]
"""

import argparse
import hashlib
import io
import json
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

TERMINAL = {"DONE", "REJECTED", "UPLOAD_EXPIRED", "EXTRACT_FAILED", "DELIVERY_FAILED"}


def sample_files(n: int, seed: str) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for i in range(n - 3):
        rows = "\n".join(f"INV-{seed}-{i}-{r},{r * 3}.25,TX{r}" for r in range(50))
        files[f"invoice-{i:03d}.csv"] = f"invoice,amount,taxId\n{rows}\n".encode()
    files["corrupt-1.pdf"] = f"%PDF-1.7 not really a pdf {seed}".encode()
    files["data.json"] = json.dumps([{"seed": seed, "n": k} for k in range(10)]).encode()
    files["virus.exe"] = b"MZ" + seed.encode()
    return files


def put_file(task: dict, data: bytes) -> list[dict]:
    plan = task["upload"]
    with httpx.Client(timeout=60) as http:
        if not plan["multipart"]:
            r = http.put(plan["url"], content=data, headers=plan["headers"])
            r.raise_for_status()
            return []
        parts = []
        for p in plan["parts"]:
            n = p["partNumber"]
            r = http.put(p["url"], content=data[(n - 1) * plan["partSize"]: n * plan["partSize"]])
            r.raise_for_status()
            parts.append({"partNumber": n, "etag": r.headers["ETag"]})
        return parts


def run_user(api: str, user: str, n_files: int) -> dict:
    h = {"Authorization": f"Bearer {user}"}
    files = sample_files(n_files, user + uuid.uuid4().hex[:6])
    body = {"files": [{"name": k, "size": len(v), "mimeType": "", "sha256": hashlib.sha256(v).hexdigest()}
                      for k, v in files.items()], "notify": {"channels": ["in_app", "email"]}}
    key = str(uuid.uuid4())
    with httpx.Client(base_url=api, headers=h, timeout=60) as c:
        first = c.post("/batches", json=body, headers={"Idempotency-Key": key})
        assert first.status_code == 201, first.text
        again = c.post("/batches", json=body, headers={"Idempotency-Key": key})
        assert again.status_code == 200
        assert [t["taskId"] for t in first.json()["tasks"]] == [t["taskId"] for t in again.json()["tasks"]]
        batch = first.json()

        def upload(task):
            if not task.get("upload"):
                return
            parts = put_file(task, files[task["fileName"]])
            r = c.post(f"/files/{task['fileId']}/complete", json={"parts": parts})
            assert r.status_code == 200, r.text

        with ThreadPoolExecutor(4) as pool:  # 4 files in flight, like the browser
            list(pool.map(upload, batch["tasks"]))
        return {"user": user, "batchId": batch["batchId"], "started": time.time()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--vendor", default="http://localhost:4000")
    ap.add_argument("--users", type=int, default=1)
    ap.add_argument("--files", type=int, default=50)
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--faults", action=argparse.BooleanOptionalAction, default=True)
    args = ap.parse_args()

    faults = {"error_rate": 0.05, "timeout_rate": 0.0, "reject_rate": 0.0} if args.faults else \
        {"error_rate": 0.0, "timeout_rate": 0.0, "reject_rate": 0.0}
    httpx.put(f"{args.vendor}/_control", json={**faults, "outage": False, "reset_stats": True})

    t0 = time.time()
    with ThreadPoolExecutor(args.users) as pool:
        runs = list(pool.map(lambda i: run_user(args.api, f"user{i}", args.files), range(args.users)))
    print(f"uploaded {args.users} x {args.files} files in {time.time() - t0:.1f}s")

    pending = {r["batchId"]: r for r in runs}
    last_print = 0.0
    while pending and time.time() - t0 < args.timeout:
        for bid, r in list(pending.items()):
            v = httpx.get(f"{args.api}/batches/{bid}", headers={"Authorization": f"Bearer {r['user']}"}).json()
            if v["status"] in ("COMPLETED", "PARTIAL", "FAILED"):
                r["view"], r["finished"] = v, time.time()
                print(f"{r['user']}: {v['status']} in {r['finished'] - t0:.1f}s - {v['summary']}")
                del pending[bid]
            elif time.time() - last_print > 5:
                print(f"  {r['user']}: {v['progress']}% {v['summary']}")
        if time.time() - last_print > 5:
            print("  vendor:", httpx.get(f"{args.api}/system/status").json()["vendor"])
            last_print = time.time()
        time.sleep(1)

    stats = httpx.get(f"{args.vendor}/_control/stats").json()
    print("vendor stats:", stats)
    ok = True
    for r in runs:
        v = r.get("view")
        if v is None:
            print(f"FAIL {r['user']}: batch did not finish in {args.timeout}s")
            ok = False
            continue
        if any(t["status"] not in TERMINAL for t in v["tasks"]):
            print(f"FAIL {r['user']}: non-terminal tasks")
            ok = False
        expected = {"EXTRACT_FAILED": 1, "REJECTED": 1}
        failed = {k: n for k, n in v["counts"].items() if k != "DONE"}
        if failed != expected:
            print(f"FAIL {r['user']}: unexpected failures {failed} (expected {expected})")
            ok = False
    checks = {"peak in-flight <= 20": stats["peak_inflight"] <= 20, "no 429s": stats["rejected_429"] == 0,
              "no duplicate records": stats["duplicate_records"] == 0}
    for name, passed in checks.items():
        print(("PASS " if passed else "FAIL ") + name)
        ok &= passed
    print("ACCEPTANCE", "PASSED" if ok else "FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
