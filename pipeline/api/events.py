"""Server-Sent Events. The SSE id is `task_events.id`, so a reconnecting browser sends Last-Event-ID and the
missed events are replayed from Postgres before the live Redis stream continues."""

import asyncio
import json

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from starlette.concurrency import run_in_threadpool

from pipeline.api.deps import current_user, owned_batch, parse_batch_id
from pipeline.config import settings
from pipeline.db import repo
from pipeline.db.models import TaskEvent
from pipeline.db.session import session_scope
from pipeline.ids import batch_public_id

router = APIRouter(tags=["events"])
HEARTBEAT_S = 15


def events_since(batch_uuid, after_id: int, limit: int = 5000) -> list[dict]:
    with session_scope() as s:
        rows = s.execute(select(TaskEvent).where(TaskEvent.batch_id == batch_uuid, TaskEvent.id > after_id)
                         .order_by(TaskEvent.id).limit(limit)).scalars().all()
        return [repo.event_json(e) for e in rows]


def _format(event: str, data: dict, event_id: int | None = None) -> str:
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}event: {event}\ndata: {json.dumps(data)}\n\n"


@router.get("/batches/{batch_id}/events")
async def batch_events(batch_id: str, request: Request, user: str = Depends(current_user),
                       last_event_id: str | None = Header(None, alias="Last-Event-ID"),
                       after: int | None = Query(None, alias="lastEventId")):
    bid = parse_batch_id(batch_id)

    def check():
        with session_scope() as s:
            owned_batch(s, bid, user)

    await run_in_threadpool(check)
    try:
        resume_from = int(last_event_id) if last_event_id else after
    except ValueError:
        resume_from = after

    async def stream():
        r = aioredis.from_url(settings.redis_url, decode_responses=True)
        pubsub = r.pubsub()
        await pubsub.subscribe(f"batch:{batch_public_id(bid)}")  # subscribe first, then replay: nothing falls in the gap
        last = resume_from or 0
        try:
            yield "retry: 3000\n\n"
            if resume_from is not None:
                for ev in await run_in_threadpool(events_since, bid, resume_from):
                    last = ev["id"]
                    yield _format("task", ev, ev["id"])
            while not await request.is_disconnected():
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=HEARTBEAT_S)
                if msg is None:
                    yield ": ping\n\n"
                    continue
                data = json.loads(msg["data"])
                kind = data.pop("type", "task")
                if kind == "task":
                    if data["id"] <= last:
                        continue  # already sent during replay
                    last = data["id"]
                    yield _format("task", data, data["id"])
                else:
                    yield _format(kind, data)
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe()
            await pubsub.aclose()
            await r.aclose()

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
