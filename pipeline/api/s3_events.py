"""S3 `ObjectCreated` webhook (MinIO bucket notification; SQS/EventBridge in production)."""

import hmac
from urllib.parse import unquote_plus

from fastapi import APIRouter, Header, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from pipeline.config import settings
from pipeline.db import repo
from pipeline.db.session import session_scope
from pipeline.log import get_logger

router = APIRouter(tags=["internal"])
log = get_logger(__name__)


@router.post("/internal/s3-events", status_code=204)
async def s3_events(request: Request, authorization: str | None = Header(None)):
    token = (authorization or "").removeprefix("Bearer ").strip()
    if not hmac.compare_digest(token, settings.s3_events_token):
        raise HTTPException(401, "bad token")
    body = await request.json()
    await run_in_threadpool(_handle, body)


def _handle(body: dict) -> None:
    for rec in body.get("Records", []):
        if not rec.get("eventName", "").startswith(("s3:ObjectCreated", "ObjectCreated")):
            continue
        obj = rec.get("s3", {}).get("object", {})
        key = unquote_plus(obj.get("key", ""))
        if not key.startswith("uploads/"):
            continue
        with session_scope() as s:
            rows = repo.mark_uploaded_by_key(s, key, obj.get("size"))
        batches = {r.batch_id for r in rows}
        for bid in batches:
            with session_scope() as s:
                repo.recompute_batch(s, bid)
        log.info("s3_object_created", key=key, files=len(rows))
