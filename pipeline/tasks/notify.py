import hashlib
import hmac
import json
import smtplib
from email.message import EmailMessage
from uuid import UUID

import httpx
from sqlalchemy import select, update

from pipeline.celery_app import app
from pipeline.config import settings
from pipeline.db import repo
from pipeline.db.models import Batch, File, FileStatus, Notification
from pipeline.db.session import session_scope
from pipeline.ids import batch_public_id
from pipeline.log import get_logger
from pipeline.redis_client import get_redis
from pipeline.resilience.backoff import full_jitter

log = get_logger(__name__)
N = Notification.__table__


def build_summary(s, b: Batch) -> dict:
    counts = repo.batch_counts(s, b.id)
    total = sum(counts.values())
    done = counts.get(FileStatus.DONE, 0)
    failed = s.execute(
        select(File.task_id, File.name, File.status, File.last_error)
        .where(File.batch_id == b.id, File.status.in_(FileStatus.FAILED)).order_by(File.name).limit(10)
    ).all()
    public = batch_public_id(b.id)
    attention = [{"taskId": f.task_id, "fileName": f.name, "status": f.status,
                  "reason": (f.last_error or {}).get("message", f.status)} for f in failed]
    duration = None
    if b.completed_at and b.created_at:
        duration = round((repo.aware(b.completed_at) - repo.aware(b.created_at)).total_seconds())
    text = f"Batch {public}: {done} of {total} files processed."
    if attention:
        n_failed = total - done
        listed = ", ".join(f"{a['fileName']} ({a['reason']})" for a in attention)
        text += f" {n_failed} need attention: {listed}."
    if duration is not None:
        text += f" Took {duration} s."
    return {"batchId": public, "status": b.status, "total": total, "done": done, "counts": counts,
            "durationS": duration, "needsAttention": attention, "text": text}


def _first_failure_text(summary: dict) -> str:
    first = summary["needsAttention"][0] if summary["needsAttention"] else None
    what = f"{first['fileName']} ({first['reason']})" if first else "a file"
    return f"Batch {summary['batchId']}: {what} failed. The rest of the batch keeps processing."


def send_in_app(b: Batch, summary: dict, kind: str) -> None:
    title = "Batch finished" if kind == "completed" else "A file in your batch failed"
    body = summary["text"] if kind == "completed" else _first_failure_text(summary)
    get_redis().publish(f"batch:{summary['batchId']}", json.dumps(
        {"type": "notification", "kind": kind, "title": title, "body": body, "summary": summary}))


def send_email(b: Batch, summary: dict, kind: str) -> None:
    msg = EmailMessage()
    msg["From"] = settings.mail_from
    msg["To"] = (b.notify or {}).get("email") or f"{b.user_id}@{settings.mail_domain}"
    msg["Subject"] = (f"Batch {summary['batchId']} {summary['status'].lower()}" if kind == "completed"
                      else f"Batch {summary['batchId']}: a file failed")
    msg.set_content(summary["text"] if kind == "completed" else _first_failure_text(summary))
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as smtp:
        smtp.send_message(msg)


def sign(body: bytes) -> str:
    return "sha256=" + hmac.new(settings.webhook_secret.encode(), body, hashlib.sha256).hexdigest()


def send_webhook(b: Batch, summary: dict, kind: str) -> None:
    url = (b.notify or {}).get("webhookUrl")
    if not url:
        raise ValueError("webhook channel requested without webhookUrl")
    body = json.dumps({"event": f"batch.{kind}", **summary}, sort_keys=True).encode()
    resp = httpx.post(url, content=body, timeout=10, headers={"Content-Type": "application/json",
                                                              "X-Signature": sign(body)})
    resp.raise_for_status()


SENDERS = {"in_app": send_in_app, "email": send_email, "webhook": send_webhook}


@app.task(bind=True, name="pipeline.tasks.send_notification", acks_late=True, max_retries=None)
def send_notification(self, batch_id: str, channel: str) -> None:
    bid = UUID(batch_id)
    with session_scope() as s:
        n = s.execute(
            update(N).where(N.c.batch_id == bid, N.c.channel == channel, N.c.status == "PENDING")
            .values(status="SENDING", attempts=N.c.attempts + 1, updated_at=repo.now()).returning(*N.c)
        ).one_or_none()
        if n is None:
            return  # already sent, or another worker is sending it
        b = s.get(Batch, bid)
        summary = build_summary(s, b)
    base, _, suffix = channel.partition(":")
    kind = "first_failure" if suffix == "first_failure" else "completed"
    try:
        SENDERS[base](b, summary, kind)
    except Exception as exc:
        final = n.attempts >= settings.notify_max_attempts
        with session_scope() as s:
            s.execute(update(N).where(N.c.batch_id == bid, N.c.channel == channel)
                      .values(status="FAILED" if final else "PENDING", last_error=str(exc)[:500],
                              updated_at=repo.now()))
        log.warning("notification_failed", batch_id=batch_id, channel=channel, attempt=n.attempts, final=final)
        if not final:
            raise self.retry(exc=exc, countdown=full_jitter(n.attempts, 2, 300))
        return
    with session_scope() as s:
        s.execute(update(N).where(N.c.batch_id == bid, N.c.channel == channel)
                  .values(status="SENT", sent_at=repo.now(), last_error=None, updated_at=repo.now()))
    log.info("notification_sent", batch_id=batch_id, channel=channel)
