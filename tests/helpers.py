"""Drives the pipeline the way the browser and the background processes would."""

import hashlib
import time
import uuid

from pipeline import storage
from pipeline.config import settings
from pipeline.dispatcher import Dispatcher
from pipeline.resilience import fair_queue
from pipeline.tasks.relay import relay_once


def spec(name: str, data: bytes, mime: str = "") -> dict:
    return {"name": name, "size": len(data), "mimeType": mime, "sha256": hashlib.sha256(data).hexdigest()}


def create_batch(api, files: dict[str, bytes], key: str | None = None, notify: dict | None = None):
    body = {"files": [spec(n, d) for n, d in files.items()]}
    if notify:
        body["notify"] = notify
    return api.post("/batches", json=body, headers={"Idempotency-Key": key or str(uuid.uuid4())})


def upload(api, task: dict, data: bytes, complete: bool = True):
    """What the browser does: PUT (or parts) straight to S3, then `complete`."""
    plan = task.get("upload")
    if plan is None:
        return None
    s3 = storage.s3()
    key = storage.object_key("alice", task["sha256"])
    parts = []
    if plan["multipart"]:
        size = plan["partSize"]
        for p in plan["parts"]:
            n = p["partNumber"]
            resp = s3.upload_part(Bucket=settings.s3_bucket, Key=key, UploadId=plan["uploadId"], PartNumber=n,
                                  Body=data[(n - 1) * size: n * size])
            parts.append({"partNumber": n, "etag": resp["ETag"]})
    else:
        s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=data)
    if complete:
        return api.post(f"/files/{task['fileId']}/complete", json={"parts": parts})
    return None


def drain(rounds: int = 60, skip_delays: bool = True) -> None:
    """Run relay + dispatcher until nothing moves. Celery is eager, so tasks run inline."""
    dispatcher = Dispatcher()
    dispatcher.is_leader = True
    for _ in range(rounds):
        moved = relay_once(limit=1000)
        if skip_delays:
            moved += fair_queue().promote_due_delayed(now=time.time() + 10_000)
        while dispatcher.tick() == 0.0:
            moved += 1
        if not moved:
            return
