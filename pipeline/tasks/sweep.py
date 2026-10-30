"""Sweeper (every 60 s): expired leases, lost jobs, stuck uploads, stuck notifications, batch status backstop."""

from datetime import timedelta

from sqlalchemy import delete, select, update

from pipeline import storage
from pipeline.celery_app import app
from pipeline.config import settings
from pipeline.db import repo
from pipeline.db.models import Batch, BatchStatus, DeliveryStatus, File, FileStatus, Notification, Outbox
from pipeline.db.session import session_scope
from pipeline.errors import ErrorKind, PipelineError
from pipeline.errors.catalog import message_for
from pipeline.log import get_logger
from pipeline.redis_client import redis_lock
from pipeline.resilience import Job, fair_queue

log = get_logger(__name__)
F, D, N = File.__table__, repo.D, Notification.__table__


def expired_extract_leases() -> int:
    """A frozen (not crashed) extract worker: put the file back to UPLOADED and re-enqueue."""
    with session_scope() as s:
        ids = s.execute(select(F.c.id).where(F.c.status == FileStatus.EXTRACTING, F.c.lease_until < repo.now())
                        ).scalars().all()
        for fid in ids:
            row = repo.transition_file(s, fid, FileStatus.EXTRACTING, FileStatus.UPLOADED, stage="extract",
                                       values={"lease_until": None}, message="Worker stopped responding; re-queued")
            if row is not None:
                repo.enqueue_extract(s, row)
    return len(ids)


def stalled_uploaded() -> int:
    """UPLOADED for over 10 min with no extract job (e.g. a retry message lost with its worker)."""
    cutoff = repo.now() - timedelta(minutes=10)
    with session_scope() as s:
        rows = s.execute(select(F).where(F.c.status == FileStatus.UPLOADED, F.c.updated_at < cutoff).limit(500)).all()
        for row in rows:
            s.execute(update(F).where(F.c.id == row.id, F.c.status == FileStatus.UPLOADED)
                      .values(version=F.c.version + 1, updated_at=repo.now()))
            repo.outbox(s, "extract", f"extract:{row.id}:{row.version + 1}", {"file_id": str(row.id)})
    return len(rows)


def expired_delivery_leases() -> int:
    """IN_FLIGHT past its lease: the worker died after claiming. Back to RETRY_WAIT and into the fair queue."""
    fq = fair_queue()
    n = 0
    with session_scope() as s:
        rows = s.execute(select(D).where(D.c.status == DeliveryStatus.IN_FLIGHT, D.c.lease_until < repo.now())).all()
        for d in rows:
            err = PipelineError(kind=ErrorKind.TRANSIENT, code="VENDOR_TIMEOUT", stage="deliver",
                                message=message_for("VENDOR_TIMEOUT"))
            delivery = repo._delivery(s, d)
            if repo.delivery_retry_wait(s, delivery, err, repo.now(), True, "Worker lost during delivery; re-queued"):
                n += 1
                s.flush()
                fq.push(Job(str(d.file_id), d.user_id, d.attempts))
    return n


def orphan_deliveries() -> int:
    """PENDING/RETRY_WAIT in Postgres but in no Redis structure (Redis restart, crash between commit and push)."""
    fq = fair_queue()
    fq.prune_dispatched()
    grace = repo.now() - timedelta(seconds=60)
    n = 0
    with session_scope() as s:
        rows = s.execute(
            select(D.c.file_id, D.c.user_id, D.c.attempts, D.c.next_attempt_at)
            .where(D.c.status.in_((DeliveryStatus.PENDING, DeliveryStatus.RETRY_WAIT))).limit(5000)
        ).all()
    for d in rows:
        fid = str(d.file_id)
        due = repo.aware(d.next_attempt_at)
        if due is not None and due > grace:
            continue
        if fq.contains(fid) or fq.in_transit(fid):
            continue
        if fq.push(Job(fid, d.user_id, d.attempts)):
            n += 1
    return n


def stuck_uploads() -> int:
    """Uploads still open after 24 h: abort the multipart upload and mark UPLOAD_EXPIRED."""
    cutoff = repo.now() - timedelta(seconds=settings.stuck_upload_after_s)
    err = PipelineError(kind=ErrorKind.PERMANENT, code="UPLOAD_EXPIRED", stage="upload",
                        message=message_for("UPLOAD_EXPIRED"))
    with session_scope() as s:
        rows = s.execute(select(F).where(F.c.status.in_(FileStatus.ACTIVE_UPLOAD), F.c.created_at < cutoff)
                         .limit(500)).all()
        batches = set()
        for f in rows:
            if f.upload_id:
                storage.abort_multipart(f.s3_key, f.upload_id)
            if repo.fail_file(s, f.id, FileStatus.ACTIVE_UPLOAD, FileStatus.UPLOAD_EXPIRED, err) is not None:
                batches.add(f.batch_id)
    for bid in batches:
        with session_scope() as s:
            repo.recompute_batch(s, bid)
    return len(rows)


def stuck_notifications() -> int:
    cutoff = repo.now() - timedelta(minutes=5)
    with session_scope() as s:
        rows = s.execute(update(N).where(N.c.status == "SENDING", N.c.updated_at < cutoff)
                         .values(status="PENDING", updated_at=repo.now()).returning(N.c.batch_id, N.c.channel)).all()
        for r in rows:
            repo.outbox(s, "notify", f"notify:{r.batch_id}:{r.channel}:{repo.now().timestamp()}",
                        {"batch_id": str(r.batch_id), "channel": r.channel})
    return len(rows)


def batch_backstop() -> int:
    with session_scope() as s:
        ids = s.execute(select(Batch.id).where(Batch.status.notin_(BatchStatus.FINAL)).limit(500)).scalars().all()
    finished = 0
    for bid in ids:
        with session_scope() as s:
            finished += repo.recompute_batch(s, bid) is not None
    return finished


def prune_outbox() -> None:
    with session_scope() as s:
        s.execute(delete(Outbox).where(Outbox.sent_at < repo.now() - timedelta(days=1)))


@app.task(name="pipeline.tasks.sweep", ignore_result=True)
def sweep() -> dict:
    with redis_lock("lock:sweep", ttl_s=55) as got:
        if not got:
            return {}
        result = {}
        for step in (expired_extract_leases, stalled_uploaded, expired_delivery_leases, orphan_deliveries,
                     stuck_uploads, stuck_notifications, batch_backstop, prune_outbox):
            try:
                result[step.__name__] = step()
            except Exception:
                log.exception("sweep_step_failed", step=step.__name__)
        log.info("sweep", **{k: v for k, v in result.items() if v})
        return result
