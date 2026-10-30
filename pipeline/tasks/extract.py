import json
import threading
from contextlib import contextmanager
from datetime import timedelta
from uuid import UUID

import structlog

from pipeline import storage
from pipeline.celery_app import app
from pipeline.config import settings
from pipeline.db import repo
from pipeline.db.models import Batch, FileStatus
from pipeline.db.session import session_scope
from pipeline.errors import ErrorKind, classify
from pipeline.errors.catalog import message_for
from pipeline.errors.model import ChecksumMismatch, ObjectMissing, SizeMismatch
from pipeline.extract import extract_stream
from pipeline.log import get_logger
from pipeline.resilience.backoff import full_jitter

log = get_logger(__name__)


@contextmanager
def lease_keeper(file_id: UUID):
    """Renew the row lease every 30 s so the sweeper can tell a slow worker from a frozen one."""
    stop = threading.Event()

    def renew():
        while not stop.wait(settings.lease_renew_s):
            try:
                with session_scope() as s:
                    repo.renew_file_lease(s, file_id)
            except Exception:
                log.warning("lease_renew_failed", file_id=str(file_id), exc_info=True)

    t = threading.Thread(target=renew, daemon=True)
    t.start()
    try:
        yield
    finally:
        stop.set()


def verify_object(file) -> None:
    """head_object: size and (for single PUTs) the S3-verified SHA-256 must match what the client declared."""
    meta = storage.head(file.s3_key)
    if meta is None:
        raise ObjectMissing()
    if meta["ContentLength"] != file.size_bytes:
        raise SizeMismatch(f"stored {meta['ContentLength']} bytes, declared {file.size_bytes}")
    checksum = meta.get("ChecksumSHA256")
    if checksum and "-" not in checksum and checksum != storage.sha256_b64(file.sha256):
        raise ChecksumMismatch()


def existing_output(out_key: str) -> int | None:
    """Same bytes already extracted by this parser version (another batch or a retry): reuse it."""
    meta = storage.head(out_key)
    if meta is None:
        return None
    return int(meta.get("Metadata", {}).get("warnings", "0"))


def run_extractor(file):
    body = storage.s3().get_object(Bucket=settings.s3_bucket, Key=file.s3_key)["Body"]
    try:
        return extract_stream(body.iter_chunks(1024 * 1024), file.file_type, file.sha256, file.size_bytes)
    finally:
        body.close()


@app.task(bind=True, name="pipeline.tasks.extract_file", acks_late=True, max_retries=None,
          soft_time_limit=settings.extract_soft_limit_s, time_limit=settings.extract_soft_limit_s + 30)
def extract_file(self, file_id: str) -> None:
    fid = UUID(file_id)
    with session_scope() as s:
        file = repo.claim_file(s, fid)
        user_id = s.get(Batch, file.batch_id).user_id if file is not None else None
    if file is None:
        return  # already handled: duplicate message, S3 event + complete race, or sweeper re-enqueue
    bound = log.bind(file_id=file_id, task_id=file.task_id, batch_id=str(file.batch_id), attempt=file.attempts,
                     celery_task_id=self.request.id)
    structlog.contextvars.bind_contextvars(task_id=file.task_id)
    try:
        with lease_keeper(fid):
            verify_object(file)
            out_key = storage.extracted_key(file.sha256)
            warnings = existing_output(out_key)
            if warnings is None:
                out = run_extractor(file)
                warnings = out.warning_count
                storage.s3().put_object(Bucket=settings.s3_bucket, Key=out_key, Body=out.to_json_bytes(),
                                        ContentType="application/json", Metadata={"warnings": str(warnings)})
        with session_scope() as s:  # one transaction: EXTRACTED + delivery row + outbox row
            row = repo.mark_extracted(s, fid, out_key, warnings)
            if row is not None:
                repo.insert_delivery(s, row, user_id)
        bound.info("extracted", warnings=warnings)
    except Exception as exc:
        err = classify(exc, "extract")
        if err.kind is ErrorKind.TRANSIENT and file.attempts < settings.extract_max_attempts:
            delay = full_jitter(file.attempts - 1, settings.extract_base_s, settings.extract_cap_s)
            with session_scope() as s:
                repo.release_file(s, fid, err, repo.now() + timedelta(seconds=delay))
            bound.warning("extract_retry", error_code=err.code, delay=round(delay, 1), exc_info=True)
            raise self.retry(exc=exc, countdown=delay)
        if err.kind is ErrorKind.TRANSIENT:
            err = err.model_copy(update={"kind": ErrorKind.PERMANENT, "code": "RETRIES_EXHAUSTED", "message":
                                         message_for("RETRIES_EXHAUSTED", attempts=file.attempts, last=err.message)})
        bound.error("extract_failed", error_code=err.code, exc_info=True)
        with session_scope() as s:
            repo.fail_file(s, fid, FileStatus.EXTRACTING, FileStatus.EXTRACT_FAILED, err, attempt=file.attempts)
        with session_scope() as s:
            repo.recompute_batch(s, file.batch_id)
    finally:
        structlog.contextvars.unbind_contextvars("task_id")


def extracted_payload(key: str) -> dict:
    body = storage.s3().get_object(Bucket=settings.s3_bucket, Key=key)["Body"]
    try:
        return json.loads(body.read())
    finally:
        body.close()
