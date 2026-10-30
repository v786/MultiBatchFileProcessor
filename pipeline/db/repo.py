"""State transitions. Every change is a conditional `UPDATE ... WHERE status = <expected>`, so concurrent workers
cannot both advance the same row, and every transition writes its task event and outbox row in the same
transaction as the state change."""

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable
from uuid import UUID

from sqlalchemy import Row, case, func, select, update
from sqlalchemy.orm import Session

from pipeline.config import settings
from pipeline.db.models import (
    Batch,
    BatchStatus,
    DeadLetter,
    DeliveryStatus,
    File,
    FileStatus,
    Notification,
    Outbox,
    TaskEvent,
    VendorDelivery,
)
from pipeline.errors import PipelineError
from pipeline.ids import batch_public_id

F = File.__table__
D = VendorDelivery.__table__


def now() -> datetime:
    return datetime.now(UTC)


def aware(dt: datetime | None) -> datetime | None:
    """SQLite hands back naive datetimes; Postgres aware ones. Normalise to aware UTC."""
    if dt is None or dt.tzinfo is not None:
        return dt
    return dt.replace(tzinfo=UTC)


def iso(dt: datetime | None) -> str | None:
    dt = aware(dt)
    return dt.isoformat().replace("+00:00", "Z") if dt else None


def insert_ignore(s: Session, table, values: dict) -> bool:
    """INSERT ... ON CONFLICT DO NOTHING. Returns True if this call inserted the row."""
    if s.get_bind().dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    return s.execute(insert(table).values(**values).on_conflict_do_nothing()).rowcount == 1


def outbox(s: Session, topic: str, dedupe_key: str, payload: dict) -> bool:
    return insert_ignore(s, Outbox.__table__, {"topic": topic, "dedupe_key": dedupe_key, "payload": payload,
                                               "created_at": now()})


# --- task events -----------------------------------------------------------------------------------------


def event_json(ev: TaskEvent) -> dict:
    return {
        "id": ev.id,
        "taskId": ev.task_id,
        "batchId": batch_public_id(ev.batch_id),
        "at": iso(ev.at),
        "stage": ev.stage,
        "vendor": ev.vendor,
        "from": ev.from_status,
        "to": ev.to_status,
        "progress": ev.progress,
        "attempt": ev.attempt,
        "errorKind": ev.error_kind,
        "errorCode": ev.error_code,
        "message": ev.message,
        "nextRetryAt": iso(ev.next_retry_at),
    }


def add_event(s: Session, f: Row, *, stage: str, frm: str | None, to: str, message: str | None = None,
              err: PipelineError | None = None, attempt: int | None = None, next_retry_at: datetime | None = None,
              vendor: str | None = None) -> TaskEvent:
    ev = TaskEvent(
        task_id=f.task_id, batch_id=f.batch_id, at=now(), stage=stage, vendor=vendor, from_status=frm,
        to_status=to, progress=f.progress, attempt=attempt, error_kind=err.kind.value if err else None,
        error_code=err.code if err else None, message=message or (err.message if err else None),
        next_retry_at=next_retry_at,
    )
    s.add(ev)
    s.flush()
    outbox(s, "event", f"event:{ev.id}", event_json(ev))
    return ev


def transition_file(s: Session, file_id: UUID, frm: str | Iterable[str], to: str, *, stage: str,
                    progress: int | None = None, values: dict | None = None, err: PipelineError | None = None,
                    message: str | None = None, attempt: int | None = None,
                    next_retry_at: datetime | None = None, vendor: str | None = None) -> Row | None:
    """Move a file from one of `frm` to `to`. Returns the updated row, or None if another worker got there first."""
    allowed = (frm,) if isinstance(frm, str) else tuple(frm)
    current = s.execute(select(F.c.status).where(F.c.id == file_id)).scalar_one_or_none()
    if current is None or current not in allowed:
        return None
    vals: dict[str, Any] = {"status": to, "version": F.c.version + 1, "updated_at": now()}
    if progress is not None:
        vals["progress"] = case((F.c.progress < progress, progress), else_=F.c.progress)  # only moves forward
    if err is not None:
        vals["last_error"] = err.to_json()
    vals.update(values or {})
    row = s.execute(
        update(F).where(F.c.id == file_id, F.c.status == current).values(**vals).returning(*F.c)
    ).one_or_none()
    if row is not None:
        add_event(s, row, stage=stage, frm=current, to=to, message=message, err=err, attempt=attempt,
                  next_retry_at=next_retry_at, vendor=vendor)
    return row


def get_file(s: Session, file_id: UUID) -> Row | None:
    return s.execute(select(F).where(F.c.id == file_id)).one_or_none()


# --- upload ----------------------------------------------------------------------------------------------


def enqueue_extract(s: Session, f: Row) -> None:
    outbox(s, "extract", f"extract:{f.id}:{f.version}", {"file_id": str(f.id)})


def mark_uploaded(s: Session, file_id: UUID, message: str = "Upload complete") -> Row | None:
    """PENDING_UPLOAD/UPLOADING -> UPLOADED. The `complete` call and the S3 event both land here; one wins."""
    row = transition_file(s, file_id, FileStatus.ACTIVE_UPLOAD, FileStatus.UPLOADED, stage="upload", progress=40,
                          values={"upload_id": None}, message=message)
    if row is not None:
        enqueue_extract(s, row)
    return row


def mark_uploaded_by_key(s: Session, s3_key: str, size: int | None) -> list[Row]:
    ids = s.execute(
        select(F.c.id).where(F.c.s3_key == s3_key, F.c.status.in_(FileStatus.ACTIVE_UPLOAD))
    ).scalars().all()
    rows = []
    for fid in ids:
        f = get_file(s, fid)
        if size is not None and f.size_bytes != size:
            continue  # a different (partial or wrong) object; the complete call reports the mismatch
        row = mark_uploaded(s, fid, message="Upload complete (storage event)")
        if row is not None:
            rows.append(row)
    return rows


def mark_uploading(s: Session, file_id: UUID, parts_done: int | None = None, total_parts: int | None = None) -> None:
    progress = 0
    if parts_done is not None and total_parts:
        progress = int(40 * min(parts_done, total_parts) / total_parts)
    row = transition_file(s, file_id, FileStatus.PENDING_UPLOAD, FileStatus.UPLOADING, stage="upload",
                          progress=progress, message="Uploading")
    if row is None and progress:
        s.execute(update(F).where(F.c.id == file_id, F.c.status == FileStatus.UPLOADING)
                  .values(progress=case((F.c.progress < progress, progress), else_=F.c.progress)))


# --- extract ---------------------------------------------------------------------------------------------


def claim_file(s: Session, file_id: UUID) -> Row | None:
    return transition_file(
        s, file_id, FileStatus.UPLOADED, FileStatus.EXTRACTING, stage="extract", progress=40,
        values={"attempts": F.c.attempts + 1, "lease_until": now() + timedelta(seconds=settings.lease_s)},
        message="Extracting data",
    )


def renew_file_lease(s: Session, file_id: UUID) -> None:
    s.execute(update(F).where(F.c.id == file_id, F.c.status == FileStatus.EXTRACTING)
              .values(lease_until=now() + timedelta(seconds=settings.lease_s)))


def release_file(s: Session, file_id: UUID, err: PipelineError, retry_at: datetime) -> Row | None:
    """A transient extract failure: back to UPLOADED; Celery redelivers after a short countdown."""
    f = get_file(s, file_id)
    return transition_file(
        s, file_id, FileStatus.EXTRACTING, FileStatus.UPLOADED, stage="extract", err=err.model_copy(
            update={"retry_at": retry_at}), values={"lease_until": None}, attempt=f.attempts if f else None,
        message=f"{err.message}, retrying (attempt {f.attempts if f else '?'} of {settings.extract_max_attempts})",
        next_retry_at=retry_at,
    )


def mark_extracted(s: Session, file_id: UUID, out_key: str, warnings: int) -> Row | None:
    msg = "Data extracted" + (f" with {warnings} warning(s)" if warnings else "")
    return transition_file(s, file_id, FileStatus.EXTRACTING, FileStatus.EXTRACTED, stage="extract", progress=60,
                           values={"extracted_key": out_key, "warnings": warnings, "lease_until": None,
                                   "last_error": None}, message=msg)


def delivery_key(sha256: str, vendor: str = settings.vendor_name) -> str:
    return hashlib.sha256(f"{sha256}:{vendor}:v{settings.payload_version}".encode()).hexdigest()


def insert_delivery(s: Session, f: Row, user_id: str) -> None:
    """Fan-out: delivery row + outbox row in the extraction's transaction."""
    insert_ignore(s, D, {"file_id": f.id, "vendor": settings.vendor_name, "user_id": user_id,
                         "idempotency_key": delivery_key(f.sha256), "status": DeliveryStatus.PENDING, "attempts": 0})
    outbox(s, "deliver", f"deliver:{f.id}:0", {"file_id": str(f.id), "user_id": user_id, "attempt": 0})


def fail_file(s: Session, file_id: UUID, frm: str | Iterable[str], to: str, err: PipelineError,
              attempt: int | None = None) -> Row | None:
    row = transition_file(s, file_id, frm, to, stage=err.stage, err=err, progress=100,
                          values={"lease_until": None}, attempt=attempt)
    if row is None:
        return None
    if to in (FileStatus.EXTRACT_FAILED, FileStatus.DELIVERY_FAILED):
        upsert_dlq(s, "extract" if to == FileStatus.EXTRACT_FAILED else "delivery", row, err,
                   attempts=attempt or row.attempts)
    _maybe_first_failure(s, row.batch_id)
    return row


# --- delivery --------------------------------------------------------------------------------------------


@dataclass
class Delivery:
    file_id: UUID
    vendor: str
    user_id: str
    idempotency_key: str
    status: str
    attempts: int
    last_error: dict | None
    task_id: str
    batch_id: UUID
    sha256: str
    name: str
    file_type: str | None
    extracted_key: str | None


def _delivery(s: Session, d: Row) -> Delivery:
    f = get_file(s, d.file_id)
    return Delivery(file_id=d.file_id, vendor=d.vendor, user_id=d.user_id, idempotency_key=d.idempotency_key,
                    status=d.status, attempts=d.attempts, last_error=d.last_error, task_id=f.task_id,
                    batch_id=f.batch_id, sha256=f.sha256, name=f.name, file_type=f.file_type,
                    extracted_key=f.extracted_key)


def claim_delivery(s: Session, file_id: UUID) -> Delivery | None:
    """First step of deliver_file. None: someone else has it, or it is finished."""
    d = s.execute(
        update(D)
        .where(D.c.file_id == file_id, D.c.vendor == settings.vendor_name,
               D.c.status.in_((DeliveryStatus.PENDING, DeliveryStatus.RETRY_WAIT)))
        .values(status=DeliveryStatus.IN_FLIGHT, attempts=D.c.attempts + 1, started_at=now(), next_attempt_at=None,
                lease_until=now() + timedelta(seconds=settings.lease_s))
        .returning(*D.c)
    ).one_or_none()
    if d is None:
        return None
    transition_file(s, file_id, (FileStatus.EXTRACTED, FileStatus.DELIVERING), FileStatus.DELIVERING,
                    stage="deliver", progress=75, attempt=d.attempts, vendor=d.vendor,
                    message=f"Sending to vendor (attempt {d.attempts} of {settings.deliver_max_attempts})")
    return _delivery(s, d)


def _update_delivery(s: Session, d: Delivery, **values) -> bool:
    return s.execute(
        update(D).where(D.c.file_id == d.file_id, D.c.vendor == d.vendor, D.c.status == DeliveryStatus.IN_FLIGHT)
        .values(**values)
    ).rowcount == 1


def mark_delivered(s: Session, d: Delivery, vendor_ref: str) -> bool:
    if not _update_delivery(s, d, status=DeliveryStatus.DELIVERED, vendor_ref=vendor_ref, lease_until=None,
                            last_error=None, finished_at=now()):
        return False
    transition_file(s, d.file_id, FileStatus.DELIVERING, FileStatus.DONE, stage="deliver", progress=100,
                    attempt=d.attempts, vendor=d.vendor, values={"last_error": None},
                    message=f"Delivered to vendor (ref {vendor_ref})")
    return True


def delivery_retry_wait(s: Session, d: Delivery, err: PipelineError, retry_at: datetime, count_attempt: bool,
                        message: str) -> bool:
    values: dict[str, Any] = {"status": DeliveryStatus.RETRY_WAIT, "next_attempt_at": retry_at, "lease_until": None,
                              "last_error": err.to_json()}
    if not count_attempt:
        values["attempts"] = D.c.attempts - 1  # throttling and open breakers never use up retries
    if not _update_delivery(s, d, **values):
        return False
    transition_file(s, d.file_id, FileStatus.DELIVERING, FileStatus.DELIVERING, stage="deliver", err=err,
                    progress=60, attempt=d.attempts if count_attempt else None, vendor=d.vendor,
                    next_retry_at=retry_at, message=message)
    return True


def delivery_failed(s: Session, d: Delivery, err: PipelineError) -> bool:
    if not _update_delivery(s, d, status=DeliveryStatus.FAILED, lease_until=None, last_error=err.to_json(),
                            finished_at=now()):
        return False
    fail_file(s, d.file_id, (FileStatus.DELIVERING, FileStatus.EXTRACTED), FileStatus.DELIVERY_FAILED, err,
              attempt=d.attempts)
    return True


def get_delivery(s: Session, file_id: UUID) -> Row | None:
    return s.execute(select(D).where(D.c.file_id == file_id, D.c.vendor == settings.vendor_name)).one_or_none()


# --- dead letters and replay -----------------------------------------------------------------------------


def upsert_dlq(s: Session, scope: str, f: Row, err: PipelineError, attempts: int) -> None:
    row = s.execute(select(DeadLetter).where(DeadLetter.scope == scope, DeadLetter.ref_id == f.id)).scalar_one_or_none()
    payload_ref = f.extracted_key if scope == "delivery" else f.s3_key
    if row is None:
        s.add(DeadLetter(scope=scope, ref_id=f.id, task_id=f.task_id, batch_id=f.batch_id, kind=err.kind.value,
                         code=err.code, message=err.message, attempts=attempts, payload_ref=payload_ref,
                         first_seen=now(), last_seen=now()))
    else:
        row.kind, row.code, row.message, row.attempts = err.kind.value, err.code, err.message, attempts
        row.last_seen, row.resolved_at, row.payload_ref = now(), None, payload_ref
    s.flush()


def _resolve_dlq(s: Session, scope: str, file_id: UUID) -> int:
    row = s.execute(select(DeadLetter).where(DeadLetter.scope == scope, DeadLetter.ref_id == file_id)).scalar_one_or_none()
    if row is None:
        return 0
    row.resolved_at = now()
    row.replays += 1
    return row.replays


def replay_delivery(s: Session, file_id: UUID) -> bool:
    """Reset a failed delivery to PENDING and re-queue it; the vendor idempotency key makes this safe."""
    d = s.execute(
        update(D).where(D.c.file_id == file_id, D.c.vendor == settings.vendor_name,
                        D.c.status == DeliveryStatus.FAILED)
        .values(status=DeliveryStatus.PENDING, attempts=0, next_attempt_at=None, last_error=None, finished_at=None)
        .returning(*D.c)
    ).one_or_none()
    if d is None:
        return False
    replays = _resolve_dlq(s, "delivery", file_id)
    transition_file(s, file_id, FileStatus.DELIVERY_FAILED, FileStatus.EXTRACTED, stage="deliver",
                    values={"progress": 60, "last_error": None}, message="Delivery retry requested")
    outbox(s, "deliver", f"deliver:{file_id}:r{replays}:{now().timestamp()}",
           {"file_id": str(file_id), "user_id": d.user_id, "attempt": 0})
    return True


def replay_extract(s: Session, file_id: UUID) -> bool:
    row = transition_file(s, file_id, FileStatus.EXTRACT_FAILED, FileStatus.UPLOADED, stage="extract",
                          values={"progress": 40, "attempts": 0, "last_error": None},
                          message="Extraction retry requested")
    if row is None:
        return False
    _resolve_dlq(s, "extract", file_id)
    enqueue_extract(s, row)
    return True


def replay_file(s: Session, file_id: UUID) -> bool:
    f = get_file(s, file_id)
    if f is None:
        return False
    if f.status == FileStatus.DELIVERY_FAILED:
        ok = replay_delivery(s, file_id)
    elif f.status == FileStatus.EXTRACT_FAILED:
        ok = replay_extract(s, file_id)
    else:
        return False
    if ok:
        recompute_batch(s, f.batch_id)
    return ok


# --- batch status and notifications ----------------------------------------------------------------------


def batch_counts(s: Session, batch_id: UUID) -> dict[str, int]:
    return dict(s.execute(select(F.c.status, func.count()).where(F.c.batch_id == batch_id).group_by(F.c.status)).all())


def derive_batch_status(counts: dict[str, int]) -> str:
    total = sum(counts.values())
    terminal = sum(n for st, n in counts.items() if st in FileStatus.TERMINAL)
    done = counts.get(FileStatus.DONE, 0)
    if total and terminal == total:
        return BatchStatus.COMPLETED if done == total else BatchStatus.FAILED if done == 0 else BatchStatus.PARTIAL
    if counts.get(FileStatus.PENDING_UPLOAD, 0) == total:
        return BatchStatus.OPEN
    return BatchStatus.PROCESSING


def recompute_batch(s: Session, batch_id: UUID) -> str | None:
    """Derive batch status after a file transition. Returns the final status if this call finalised the batch.

    The batch row is locked, and every caller runs this after committing its own file transition, so when the
    last two files finish on two workers at once exactly one of them sees all files terminal and finalises.
    """
    b = s.execute(select(Batch).where(Batch.id == batch_id).with_for_update()).scalar_one()
    status = derive_batch_status(batch_counts(s, batch_id))
    if status in BatchStatus.FINAL:
        if b.completed_at is None:
            b.status, b.completed_at = status, now()
            _queue_notifications(s, b, suffix="")
            outbox(s, "batch", f"batch:{b.id}:{status}:{b.completed_at.timestamp()}",
                   {"batchId": batch_public_id(b.id), "status": status, "completedAt": iso(b.completed_at)})
            return status
        b.status = status
        return None
    if b.status != status or b.completed_at is not None:
        b.status, b.completed_at = status, None  # a replay re-opened the batch
    return None


def _queue_notifications(s: Session, b: Batch, suffix: str) -> None:
    for channel in (b.notify or {}).get("channels", []):
        name = channel + suffix
        if insert_ignore(s, Notification.__table__, {"batch_id": b.id, "channel": name, "status": "PENDING",
                                                     "attempts": 0}):
            outbox(s, "notify", f"notify:{b.id}:{name}", {"batch_id": str(b.id), "channel": name})


def _maybe_first_failure(s: Session, batch_id: UUID) -> None:
    b = s.get(Batch, batch_id)
    if b and "first_failure" in (b.notify or {}).get("notifyOn", []):
        _queue_notifications(s, b, suffix=":first_failure")
