"""Outbox relay: publishes rows committed alongside state changes. At-least-once; every consumer dedupes."""

import json

from sqlalchemy import select

from pipeline.celery_app import app
from pipeline.db import repo
from pipeline.db.models import Outbox
from pipeline.db.session import session_scope
from pipeline.log import get_logger
from pipeline.redis_client import get_redis, redis_lock
from pipeline.resilience import Job, fair_queue

log = get_logger(__name__)


def publish(row: Outbox) -> None:
    from pipeline.tasks.extract import extract_file
    from pipeline.tasks.notify import send_notification

    p = row.payload
    match row.topic:
        case "extract":
            extract_file.apply_async(args=[p["file_id"]], task_id=f"extract:{p['file_id']}:{row.id}")
        case "deliver":
            fair_queue().push(Job(p["file_id"], p["user_id"], p.get("attempt", 0)))
        case "event":
            get_redis().publish(f"batch:{p['batchId']}", json.dumps({"type": "task", **p}))
        case "batch":
            get_redis().publish(f"batch:{p['batchId']}", json.dumps({"type": "batch", **p}))
        case "notify":
            send_notification.apply_async(args=[p["batch_id"], p["channel"]])
        case _:
            log.error("outbox_unknown_topic", topic=row.topic, outbox_id=row.id)


def relay_once(limit: int = 100) -> int:
    with session_scope() as s:
        rows = s.execute(
            select(Outbox).where(Outbox.sent_at.is_(None)).order_by(Outbox.id).limit(limit)
            .with_for_update(skip_locked=True)
        ).scalars().all()
        for row in rows:
            publish(row)  # a crash before commit republishes; consumers dedupe
            row.sent_at = repo.now()
    return len(rows)


@app.task(name="pipeline.tasks.relay_outbox", ignore_result=True)
def relay_outbox(max_rounds: int = 20) -> int:
    with redis_lock("lock:relay", ttl_s=30) as got:
        if not got:
            return 0
        total = 0
        for _ in range(max_rounds):
            n = relay_once()
            total += n
            if n < 100:
                break
        return total


@app.task(name="pipeline.tasks.noop", ignore_result=True)
def noop() -> str:
    return "ok"
