"""End-to-end through the real code paths: API -> S3 -> outbox relay -> extract -> fair queue -> dispatcher ->
deliver -> vendor, with the scenarios from the doc's chaos list that do not need real processes."""

import hashlib

from sqlalchemy import func, select

from pipeline import storage
from pipeline.config import settings
from pipeline.db.models import Notification, Outbox, TaskEvent
from pipeline.db.session import session_scope
from pipeline.resilience import breaker
from pipeline.resilience.breaker import State
from tests.conftest import make_csv, make_json, make_pdf, make_xlsx
from tests.helpers import create_batch, drain, upload


def _upload_all(api, batch, files):
    by_name = {t["fileName"]: t for t in batch["tasks"]}
    for name, data in files.items():
        if "duplicateOf" not in by_name[name]:
            upload(api, by_name[name], data)


def test_mixed_batch_partial_with_visible_causes(api, vendor):
    files = {
        "invoices.csv": make_csv(rows=20, bad_rows=2),
        "records.json": make_json(),
        "sheet.xlsx": make_xlsx(),
        "scan.pdf": make_pdf(),
        "corrupt.pdf": b"%PDF-1.7 definitely not a pdf",
        "malware.exe": b"MZ\x90\x00",
    }
    r = create_batch(api, files, notify={"channels": ["in_app"]})
    assert r.status_code == 201, r.text
    batch = r.json()
    assert batch["batchId"].startswith("bat_")
    assert all(t["taskId"].startswith("tsk_") for t in batch["tasks"])
    rejected = next(t for t in batch["tasks"] if t["fileName"] == "malware.exe")
    assert rejected["status"] == "REJECTED" and rejected["upload"] is None
    assert rejected["error"]["code"] == "UNSUPPORTED_TYPE"

    _upload_all(api, batch, files)
    drain()

    view = api.get(f"/batches/{batch['batchId']}").json()
    assert view["status"] == "PARTIAL"
    assert view["counts"] == {"DONE": 4, "EXTRACT_FAILED": 1, "REJECTED": 1}
    assert view["summary"] == "4 done, 1 failed extraction (corrupt file), 1 rejected (unsupported type)"
    assert view["progress"] == 100

    tasks = {t["fileName"]: t for t in view["tasks"]}
    assert tasks["invoices.csv"]["warnings"] == 2
    corrupt = api.get(f"/tasks/{tasks['corrupt.pdf']['taskId']}").json()
    assert corrupt["stage"] == "extract"
    assert corrupt["stages"]["extract"]["status"] == "FAILED"
    assert corrupt["stages"]["extract"]["error"]["code"] == "CORRUPT_FILE"
    assert corrupt["actions"] == ["retry_extract", "reupload"]

    done = api.get(f"/tasks/{tasks['records.json']['taskId']}").json()
    assert done["stages"]["deliver"]["status"] == "DONE" and done["stages"]["deliver"]["vendorRef"].startswith("V-")
    history = [e["to"] for e in api.get(done["history"]).json()["events"]]
    assert history == ["PENDING_UPLOAD", "UPLOADED", "EXTRACTING", "EXTRACTED", "DELIVERING", "DONE"]

    stats = vendor.stats
    assert stats.accepted == 4 and stats.duplicate_records == 0 and stats.rejected_429 == 0
    assert stats.peak_inflight <= 20
    with session_scope() as s:
        notes = s.execute(select(Notification)).scalars().all()
        assert [(n.channel, n.status) for n in notes] == [("in_app", "SENT")]


def test_create_batch_is_idempotent(api):
    files = {"a.csv": make_csv(), "b.json": make_json()}
    key = "same-key-123"
    responses = [create_batch(api, files, key=key) for _ in range(3)]
    assert [r.status_code for r in responses] == [201, 200, 200]
    ids = [[t["taskId"] for t in r.json()["tasks"]] for r in responses]
    assert ids[0] == ids[1] == ids[2]
    assert len(api.get("/batches").json()["batches"]) == 1


def test_complete_and_s3_event_enqueue_one_extract(api):
    data = make_csv()
    batch = create_batch(api, {"a.csv": data}).json()
    task = batch["tasks"][0]
    upload(api, task, data, complete=False)
    event = {"Records": [{"eventName": "s3:ObjectCreated:Put",
                          "s3": {"object": {"key": storage.object_key("alice", task["sha256"]), "size": len(data)}}}]}
    assert api.post("/internal/s3-events", json=event,
                    headers={"Authorization": settings.s3_events_token}).status_code == 204
    assert api.post(f"/files/{task['fileId']}/complete", json={"parts": []}).json()["status"] in (
        "UPLOADED", "EXTRACTING", "EXTRACTED")
    assert api.post("/internal/s3-events", json=event, headers={"Authorization": settings.s3_events_token}
                    ).status_code == 204
    drain()
    with session_scope() as s:
        extracts = s.execute(select(func.count()).select_from(Outbox).where(Outbox.topic == "extract")).scalar()
        claims = s.execute(select(func.count()).select_from(TaskEvent)
                           .where(TaskEvent.task_id == task["taskId"], TaskEvent.to_status == "EXTRACTING")).scalar()
    assert extracts == 1 and claims == 1
    assert api.get(f"/tasks/{task['taskId']}").json()["status"] == "DONE"


def test_same_file_twice_in_batch_and_across_batches(api, vendor):
    data = make_json()
    b1 = create_batch(api, {"one.json": data, "copy-of-one.json": data}).json()
    assert len({t["taskId"] for t in b1["tasks"]}) == 1
    assert b1["tasks"][1]["duplicateOf"] == "one.json"
    upload(api, b1["tasks"][0], data)
    drain()
    b2 = create_batch(api, {"again.json": data}).json()
    assert b2["tasks"][0]["status"] == "UPLOADED" and b2["tasks"][0]["upload"] is None  # deduped by hash
    drain()
    assert api.get(f"/batches/{b2['batchId']}").json()["status"] == "COMPLETED"
    objects = storage.s3().list_objects_v2(Bucket=settings.s3_bucket)["Contents"]
    assert sum(o["Key"].startswith("uploads/") for o in objects) == 1
    assert sum(o["Key"].startswith("extracted/") for o in objects) == 1  # extracted once
    # same idempotency key at the vendor: one record, the second call is answered from the original
    assert vendor.stats.accepted == 1 and vendor.stats.duplicate_records == 0


def test_multipart_upload_and_resume(api, monkeypatch):
    monkeypatch.setattr(settings, "single_put_max_bytes", 6 * 1024 * 1024)
    monkeypatch.setattr(settings, "part_size_bytes", 5 * 1024 * 1024)
    data = make_csv(rows=400_000)
    assert len(data) > 6 * 1024 * 1024
    task = create_batch(api, {"big.csv": data}).json()["tasks"][0]
    plan = task["upload"]
    assert plan["multipart"] and plan["partCount"] >= 2
    # upload only the first part, "reload", ask which parts exist, then finish
    s3, key, size = storage.s3(), storage.object_key("alice", task["sha256"]), plan["partSize"]
    s3.upload_part(Bucket=settings.s3_bucket, Key=key, UploadId=plan["uploadId"], PartNumber=1, Body=data[:size])
    have = api.get(f"/files/{task['fileId']}/parts").json()["parts"]
    assert [p["partNumber"] for p in have] == [1]
    parts = [{"partNumber": 1, "etag": have[0]["etag"]}]
    for n in range(2, plan["partCount"] + 1):
        resp = s3.upload_part(Bucket=settings.s3_bucket, Key=key, UploadId=plan["uploadId"], PartNumber=n,
                              Body=data[(n - 1) * size: n * size])
        parts.append({"partNumber": n, "etag": resp["ETag"]})
    assert api.post(f"/files/{task['fileId']}/complete", json={"parts": parts}).json()["status"] != "REJECTED"
    drain()
    detail = api.get(f"/tasks/{task['taskId']}").json()
    assert detail["status"] == "DONE", detail


def test_vendor_rejection_goes_to_dlq_and_replays(api, vendor):
    vendor.cfg.reject_rate = 1.0
    data = make_csv()
    batch = create_batch(api, {"a.csv": data}).json()
    upload(api, batch["tasks"][0], data)
    drain()
    task = api.get(f"/tasks/{batch['tasks'][0]['taskId']}").json()
    assert task["status"] == "DELIVERY_FAILED"
    assert task["stages"]["deliver"]["error"]["message"] == "The vendor rejected the record: field 'taxId' is missing"
    assert task["actions"] == ["retry_delivery"]
    dlq = api.get("/dlq", params={"scope": "delivery"}).json()["items"]
    assert len(dlq) == 1 and dlq[0]["code"] == "VENDOR_REJECTED"
    assert api.get(f"/batches/{batch['batchId']}").json()["status"] == "FAILED"

    vendor.cfg.reject_rate = 0.0
    assert api.post(f"/dlq/{dlq[0]['id']}/replay").status_code == 200
    drain()
    assert api.get(f"/tasks/{task['taskId']}").json()["status"] == "DONE"
    assert api.get(f"/batches/{batch['batchId']}").json()["status"] == "COMPLETED"
    assert api.get("/dlq").json()["items"] == []


def test_transient_vendor_errors_retry_then_succeed(api, vendor):
    vendor.cfg.error_rate = 1.0
    data = make_json()
    batch = create_batch(api, {"a.json": data}).json()
    upload(api, batch["tasks"][0], data)
    drain(rounds=2)
    t = api.get(f"/tasks/{batch['tasks'][0]['taskId']}").json()
    assert t["status"] == "DELIVERING" and t["deliveryStatus"] == "RETRY_WAIT"
    assert "retrying" in t["statusText"]["text"] and t["statusText"]["retryAt"]
    vendor.cfg.error_rate = 0.0
    drain()
    t = api.get(f"/tasks/{t['taskId']}").json()
    assert t["status"] == "DONE" and t["stages"]["deliver"]["attempts"] >= 2


def test_outage_opens_breaker_and_nothing_fails(api, vendor):
    vendor.cfg.outage = True
    files = {f"f{i}.json": f'{{"n": {i}}}'.encode() for i in range(12)}
    batch = create_batch(api, files).json()
    _upload_all(api, batch, files)
    drain(rounds=3)
    assert breaker().state() is State.OPEN
    view = api.get(f"/batches/{batch['batchId']}").json()
    assert view["status"] == "PROCESSING"
    assert not any(t["status"].endswith("FAILED") for t in view["tasks"])
    assert any(t["statusText"]["text"].startswith("Vendor unavailable, paused") for t in view["tasks"])

    vendor.cfg.outage = False
    breaker().r.hset(breaker().key, "open_until", 0)  # skip the 30 s cooldown
    drain()
    assert breaker().state() is State.CLOSED
    assert api.get(f"/batches/{batch['batchId']}").json()["status"] == "COMPLETED"


def test_all_files_rejected_is_422(api):
    r = create_batch(api, {"a.exe": b"x", "b.png": b"y"})
    assert r.status_code == 422
    assert {f["code"] for f in r.json()["files"]} == {"UNSUPPORTED_TYPE"}


def test_sse_replay_since_last_event(api):
    from pipeline.api.events import events_since
    from pipeline.ids import parse_public_id

    data = make_csv()
    batch = create_batch(api, {"a.csv": data}).json()
    upload(api, batch["tasks"][0], data)
    drain()
    bid = parse_public_id(batch["batchId"], "bat")
    all_events = events_since(bid, 0)
    tail = events_since(bid, all_events[2]["id"])
    assert [e["id"] for e in tail] == [e["id"] for e in all_events[3:]]
    progress = [e["progress"] for e in all_events]
    assert progress == sorted(progress)  # progress never goes backwards


def test_presigned_put_pins_checksum_and_type(api):
    data = make_json()
    plan = create_batch(api, {"a.json": data}).json()["tasks"][0]["upload"]
    assert not plan["multipart"]
    signed = plan["url"].split("X-Amz-SignedHeaders=")[1].split("&")[0]
    assert signed == "content-length%3Bcontent-type%3Bhost%3Bx-amz-checksum-sha256"
    assert plan["headers"]["x-amz-checksum-sha256"] == storage.sha256_b64(hashlib.sha256(data).hexdigest())


def test_completion_notifications_once_per_channel(api, vendor, monkeypatch):
    from pipeline.db import repo
    from pipeline.ids import parse_public_id
    from pipeline.tasks import notify as notify_mod
    from pipeline.vendor_client import http

    monkeypatch.setattr(notify_mod.httpx, "post", lambda url, **kw: http().post(url, **kw))  # route to mock app
    data = make_json()
    batch = create_batch(api, {"a.json": data}, notify={"channels": ["in_app", "webhook"],
                                                        "webhookUrl": "http://vendor.test/webhooks"}).json()
    upload(api, batch["tasks"][0], data)
    drain()
    bid = parse_public_id(batch["batchId"], "bat")
    for _ in range(3):  # racing finishers / sweeper backstop: still one notification per channel
        with session_scope() as s:
            assert repo.recompute_batch(s, bid) is None
    drain()
    with session_scope() as s:
        notes = sorted((n.channel, n.status) for n in s.execute(select(Notification)).scalars())
    assert notes == [("in_app", "SENT"), ("webhook", "SENT")]
    assert len(vendor.webhooks) == 1 and vendor.webhooks[0]["valid"]
    assert vendor.webhooks[0]["body"]["text"].startswith(f"Batch {batch['batchId']}: 1 of 1 files processed.")


def test_small_batch_is_not_starved_by_a_big_one(api, vendor):
    """Round robin: a 5-file user arriving behind a 40-file user is served within the first ~10 deliveries."""
    vendor.cfg.latency_scale = 0  # instant replies
    big = {f"big-{i}.json": f'{{"big": {i}}}'.encode() for i in range(40)}
    small = {f"small-{i}.json": f'{{"small": {i}}}'.encode() for i in range(5)}
    b_big = create_batch(api, big).json()
    _upload_all(api, b_big, big)
    api.headers["Authorization"] = "Bearer bob"
    b_small = create_batch(api, small).json()
    for t in b_small["tasks"]:
        upload_as(api, t, small[t["fileName"]], "bob")
    drain()
    with session_scope() as s:
        order = s.execute(select(TaskEvent.task_id).where(TaskEvent.to_status == "DONE").order_by(TaskEvent.id)
                          ).scalars().all()
    small_ids = {t["taskId"] for t in b_small["tasks"]}
    positions = [i for i, tid in enumerate(order) if tid in small_ids]
    assert len(order) == 45 and max(positions) < 12, positions
    assert vendor.stats.duplicate_records == 0


def upload_as(api, task, data, tenant):
    key = storage.object_key(tenant, task["sha256"])
    storage.s3().put_object(Bucket=settings.s3_bucket, Key=key, Body=data)
    return api.post(f"/files/{task['fileId']}/complete", json={"parts": []})
