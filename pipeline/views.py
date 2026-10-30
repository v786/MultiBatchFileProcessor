"""What the user sees: task and batch views with stage, progress, cause, and what happens next."""

from collections import Counter
from datetime import UTC, datetime

from sqlalchemy import Row, select
from sqlalchemy.orm import Session

from pipeline.config import settings
from pipeline.db import repo
from pipeline.db.models import Batch, DeliveryStatus, FileStatus, TaskEvent
from pipeline.ids import batch_public_id
from pipeline.resilience import breaker, fair_queue

STAGE_OF = {
    FileStatus.PENDING_UPLOAD: "upload", FileStatus.UPLOADING: "upload", FileStatus.REJECTED: "upload",
    FileStatus.UPLOAD_EXPIRED: "upload", FileStatus.UPLOADED: "extract", FileStatus.EXTRACTING: "extract",
    FileStatus.EXTRACT_FAILED: "extract", FileStatus.EXTRACTED: "deliver", FileStatus.DELIVERING: "deliver",
    FileStatus.DONE: "deliver", FileStatus.DELIVERY_FAILED: "deliver",
}
ACTIONS = {
    FileStatus.REJECTED: ["upload_different_file"],
    FileStatus.UPLOAD_EXPIRED: ["reupload"],
    FileStatus.EXTRACT_FAILED: ["retry_extract", "reupload"],
    FileStatus.DELIVERY_FAILED: ["retry_delivery"],
}
SHORT_REASON = {
    "UNSUPPORTED_TYPE": "unsupported type", "FILE_TOO_LARGE": "too large", "EMPTY_FILE": "empty file",
    "SIZE_MISMATCH": "incomplete upload", "UPLOAD_EXPIRED": "upload expired", "CHECKSUM_MISMATCH": "checksum mismatch",
    "CORRUPT_FILE": "corrupt file", "EXTRACT_TIMEOUT": "took too long to parse", "OBJECT_MISSING": "file missing",
    "VENDOR_REJECTED": "vendor rejected record", "DELIVERY_UNKNOWN": "needs manual review",
    "RETRIES_EXHAUSTED": "retries exhausted",
}


def _throughput() -> float:
    from pipeline.tasks.deliver import throughput_per_s

    return throughput_per_s()


def describe(f: Row, d: Row | None, throughput: float | None = None) -> dict:
    """The one-line status the UI shows, plus the facts behind it."""
    st = f.status
    err = f.last_error or {}
    out: dict = {"text": "", "retryAt": None, "queuePosition": None, "etaS": None, "startedAt": None}
    if st == FileStatus.PENDING_UPLOAD:
        out["text"] = "Waiting for upload"
    elif st == FileStatus.UPLOADING:
        out["text"] = "Uploading"
    elif st == FileStatus.UPLOADED:
        if err and err.get("retry_at"):
            out["text"] = f"{err['message']}; retrying extraction (attempt {f.attempts} of {settings.extract_max_attempts})"
            out["retryAt"] = err["retry_at"]
        else:
            out["text"] = "Queued for extraction"
    elif st == FileStatus.EXTRACTING:
        out["text"] = "Extracting data"
    elif st in (FileStatus.EXTRACTED, FileStatus.DELIVERING):
        out.update(_describe_delivery(f, d, throughput))
    elif st == FileStatus.DONE:
        out["text"] = f"Delivered (ref {d.vendor_ref})" if d is not None and d.vendor_ref else "Delivered"
    else:
        out["text"] = err.get("message") or st.replace("_", " ").capitalize()
    return out


def _describe_delivery(f: Row, d: Row | None, throughput: float | None) -> dict:
    if d is None:
        return {"text": "Queued for delivery"}
    if d.status == DeliveryStatus.IN_FLIGHT:
        return {"text": "Sending to vendor", "startedAt": repo.iso(d.started_at)}
    snap = breaker().snapshot()
    if snap["state"] == "open":  # every waiting delivery is paused, whatever its own last error was
        due = max(snap["openUntil"] or 0, repo.aware(d.next_attempt_at).timestamp() if d.next_attempt_at else 0)
        return {"text": "Vendor unavailable, paused", "retryAt": repo.iso(datetime.fromtimestamp(due, UTC))}
    if d.status == DeliveryStatus.RETRY_WAIT and d.next_attempt_at and repo.aware(d.next_attempt_at) > repo.now():
        err = d.last_error or {}
        retry_at = repo.iso(d.next_attempt_at)
        if err.get("kind") == "CircuitOpen":
            return {"text": "Vendor unavailable, paused", "retryAt": retry_at}
        if err.get("kind") == "Throttled":
            return {"text": "Vendor busy, waiting for a free slot", "retryAt": retry_at}
        return {"text": f"{err.get('message', 'Temporary error')}; retrying "
                        f"(attempt {d.attempts} of {settings.deliver_max_attempts})", "retryAt": retry_at}
    position = fair_queue().position(d.user_id, str(f.id))
    if position is None:
        return {"text": "Waiting for vendor"}
    eta = round(position / (throughput or _throughput()))
    return {"text": f"Waiting for vendor: position {position}, about {eta} s", "queuePosition": position,
            "etaS": eta}


def task_summary(f: Row, d: Row | None, throughput: float | None = None) -> dict:
    """Compact view used in batch listings."""
    return {
        "taskId": f.task_id,
        "fileId": str(f.id),
        "fileName": f.name,
        "fileType": f.file_type,
        "sizeBytes": f.size_bytes,
        "status": f.status,
        "stage": STAGE_OF[f.status],
        "progress": f.progress,
        "deliveryStatus": d.status if d is not None else None,
        "attempts": {"extract": f.attempts, "deliver": d.attempts if d is not None else 0},
        "error": f.last_error if f.status in FileStatus.FAILED or f.last_error else None,
        "statusText": describe(f, d, throughput),
        "actions": ACTIONS.get(f.status, []),
        "warnings": f.warnings,
        "updatedAt": repo.iso(f.updated_at),
    }


def _finished(events: list[TaskEvent], statuses: set[str]) -> str | None:
    for ev in events:
        if ev.to_status in statuses:
            return repo.iso(ev.at)
    return None


def task_detail(s: Session, f: Row) -> dict:
    d = repo.get_delivery(s, f.id)
    events = s.execute(select(TaskEvent).where(TaskEvent.task_id == f.task_id).order_by(TaskEvent.id)).scalars().all()
    st = f.status
    order = [FileStatus.UPLOADED, FileStatus.EXTRACTED, FileStatus.DONE]

    def stage_status(stage: str) -> str:
        current = STAGE_OF[st]
        idx = ["upload", "extract", "deliver"]
        if st in FileStatus.FAILED and current == stage:
            return "FAILED"
        if st == FileStatus.DONE or idx.index(current) > idx.index(stage):
            return "DONE"
        if current == stage:
            return "PENDING" if st in (FileStatus.PENDING_UPLOAD, FileStatus.UPLOADED, FileStatus.EXTRACTED) else "IN_PROGRESS"
        return "PENDING"

    stages = {
        "upload": {"status": stage_status("upload"), "finishedAt": _finished(events, {order[0]})},
        "extract": {"status": stage_status("extract"), "attempts": f.attempts, "warnings": f.warnings,
                    "finishedAt": _finished(events, {order[1]})},
        "deliver": {"status": stage_status("deliver"), "vendor": settings.vendor_name,
                    "deliveryStatus": d.status if d is not None else None,
                    "attempts": d.attempts if d is not None else 0,
                    "maxAttempts": settings.deliver_max_attempts,
                    "vendorRef": d.vendor_ref if d is not None else None,
                    "nextAttemptAt": repo.iso(d.next_attempt_at) if d is not None else None,
                    "finishedAt": _finished(events, {FileStatus.DONE, FileStatus.DELIVERY_FAILED})},
    }
    failed_stage = STAGE_OF[st] if st in FileStatus.FAILED else None
    if failed_stage:
        stages[failed_stage]["error"] = f.last_error
    return {
        **task_summary(f, d),
        "batchId": batch_public_id(f.batch_id),
        "sha256": f.sha256,
        "stages": stages,
        "lastError": f.last_error,
        "history": f"/tasks/{f.task_id}/history",
    }


def summary_text(counts: dict[str, int], retrying: int, failures: Counter) -> str:
    parts = []
    if counts.get(FileStatus.DONE):
        parts.append(f"{counts[FileStatus.DONE]} done")
    uploading = counts.get(FileStatus.PENDING_UPLOAD, 0) + counts.get(FileStatus.UPLOADING, 0)
    extracting = counts.get(FileStatus.UPLOADED, 0) + counts.get(FileStatus.EXTRACTING, 0)
    delivering = counts.get(FileStatus.EXTRACTED, 0) + counts.get(FileStatus.DELIVERING, 0) - retrying
    for n, label in ((uploading, "uploading"), (extracting, "extracting"), (delivering, "delivering"),
                     (retrying, "retrying")):
        if n > 0:
            parts.append(f"{n} {label}")
    labels = {FileStatus.REJECTED: "rejected", FileStatus.UPLOAD_EXPIRED: "expired",
              FileStatus.EXTRACT_FAILED: "failed extraction", FileStatus.DELIVERY_FAILED: "failed delivery"}
    for (status, code), n in sorted(failures.items()):
        reason = SHORT_REASON.get(code)
        parts.append(f"{n} {labels[status]}" + (f" ({reason})" if reason else ""))
    return ", ".join(parts) or "empty"


def batch_view(s: Session, b: Batch, offset: int = 0, limit: int = 100) -> dict:
    files = s.execute(select(repo.F).where(repo.F.c.batch_id == b.id).order_by(repo.F.c.created_at, repo.F.c.name)).all()
    deliveries = {d.file_id: d for d in s.execute(select(repo.D).where(repo.D.c.file_id.in_([f.id for f in files]))).all()}
    counts = Counter(f.status for f in files)
    retrying = sum(1 for d in deliveries.values() if d.status == DeliveryStatus.RETRY_WAIT)
    failures = Counter((f.status, (f.last_error or {}).get("code")) for f in files if f.status in FileStatus.FAILED)
    last_event = s.execute(select(TaskEvent.id).where(TaskEvent.batch_id == b.id).order_by(TaskEvent.id.desc())
                           .limit(1)).scalar_one_or_none()
    throughput = _throughput()
    public = batch_public_id(b.id)
    return {
        "batchId": public,
        "status": b.status,
        "progress": round(sum(f.progress for f in files) / len(files)) if files else 0,
        "fileCount": len(files),
        "counts": dict(counts),
        "summary": summary_text(counts, retrying, failures),
        "createdAt": repo.iso(b.created_at),
        "completedAt": repo.iso(b.completed_at),
        "notify": b.notify,
        "lastEventId": last_event or 0,
        "tasks": [task_summary(f, deliveries.get(f.id), throughput) for f in files[offset:offset + limit]],
        "page": {"offset": offset, "limit": limit, "total": len(files)},
        "links": {"self": f"/batches/{public}", "events": f"/batches/{public}/events"},
    }


def parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_file_by_task(s: Session, task_id: str) -> Row | None:
    return s.execute(select(repo.F).where(repo.F.c.task_id == task_id)).one_or_none()


def file_owner(s: Session, f: Row) -> str:
    return s.execute(select(Batch.user_id).where(Batch.id == f.batch_id)).scalar_one()

