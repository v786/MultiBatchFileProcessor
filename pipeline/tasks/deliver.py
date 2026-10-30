import time
from datetime import timedelta
from uuid import UUID, uuid4

from pipeline.celery_app import app
from pipeline.config import settings
from pipeline.db import repo
from pipeline.db.session import session_scope
from pipeline.errors import ErrorKind, PipelineError, classify
from pipeline.errors.catalog import message_for
from pipeline.errors.model import Throttled, UnknownOutcome, VendorTimeout
from pipeline.log import get_logger
from pipeline.redis_client import get_redis
from pipeline.resilience import Job, breaker, fair_queue, limiter, semaphore
from pipeline.resilience.backoff import full_jitter
from pipeline.resilience.breaker import State
from pipeline.tasks.extract import extracted_payload
from pipeline.vendor_client import build_payload, call_vendor, lookup_record

log = get_logger(__name__)

THROUGHPUT_KEY = "stats:delivered"


def record_throughput() -> None:
    r, t = get_redis(), time.time()
    r.zadd(THROUGHPUT_KEY, {uuid4().hex: t})
    r.zremrangebyscore(THROUGHPUT_KEY, "-inf", t - 300)


def throughput_per_s() -> float:
    """Moving average over the last 5 minutes; falls back to the theoretical 20 slots / 15 s."""
    r = get_redis()
    r.zremrangebyscore(THROUGHPUT_KEY, "-inf", time.time() - 300)
    n = r.zcard(THROUGHPUT_KEY)
    return n / 300 if n >= 20 else settings.vendor_slots / 15


def load_payload(d: repo.Delivery) -> dict:
    return build_payload(d, extracted_payload(d.extracted_key))


def _needs_unknown_check(d: repo.Delivery) -> bool:
    return (not settings.vendor_supports_idempotency
            and (d.last_error or {}).get("code") == VendorTimeout.code)


@app.task(bind=True, name="pipeline.tasks.deliver_file", acks_late=True)
def deliver_file(self, file_id: str, lease: str, gate: str | None, user_id: str | None = None) -> None:
    fid = UUID(file_id)
    sem, fq = semaphore(), fair_queue()
    with session_scope() as s:
        d = repo.claim_delivery(s, fid)
    fq.claimed(file_id)
    if d is None:  # duplicate message or already finished: costs one SQL statement
        sem.release(lease)
        breaker().return_probe(gate)
        if user_id:
            fq.slot_done(user_id)
        return

    bound = log.bind(file_id=file_id, task_id=d.task_id, batch_id=str(d.batch_id), attempt=d.attempts,
                     celery_task_id=self.request.id, gate=gate)
    keep_slot = False  # True after a timeout: the vendor may still be working, so let the lease expire
    called = False
    try:
        sem.refresh(lease)
        if _needs_unknown_check(d):
            ref = lookup_record(d.idempotency_key) if settings.vendor_supports_lookup else None
            if ref is None and not settings.vendor_supports_lookup:
                raise UnknownOutcome()
            if ref is not None:
                with session_scope() as s:
                    repo.mark_delivered(s, d, ref)
                return
        payload = load_payload(d)
        called = True
        started = time.monotonic()
        vendor_ref = call_vendor(d, payload, gate)  # waits 10-20 s
        with session_scope() as s:
            repo.mark_delivered(s, d, vendor_ref)
        record_throughput()
        bound.info("delivered", vendor_ref=vendor_ref, vendor_latency_seconds=round(time.monotonic() - started, 2))
    except VendorTimeout as exc:
        keep_slot = True
        handle_delivery_failure(d, exc, bound)
    except Exception as exc:
        if not called:
            breaker().return_probe(gate)
        handle_delivery_failure(d, exc, bound)
    finally:
        if not keep_slot:
            sem.release(lease)
        fq.slot_done(d.user_id)
        with session_scope() as s:
            repo.recompute_batch(s, d.batch_id)  # may queue completion notifications


def handle_delivery_failure(d: repo.Delivery, exc: Exception, bound=log) -> None:
    """Re-delay through the dispatcher's delayed set (never a loop or countdown in the task), or park in the DLQ."""
    err = classify(exc, "deliver")
    count_attempt = True
    if err.kind is ErrorKind.THROTTLED:
        retry_after = exc.retry_after_s if isinstance(exc, Throttled) else 2.0
        limiter().on_429(retry_after)
        delay, count_attempt = retry_after, False
        message = "Vendor busy, waiting for a free slot"
    elif err.kind is ErrorKind.CIRCUIT_OPEN:
        delay, count_attempt = max(1.0, breaker().seconds_until_probe()), False
        message = "Vendor unavailable, paused"
    elif err.kind is ErrorKind.TRANSIENT and d.attempts < settings.deliver_max_attempts:
        delay = full_jitter(d.attempts - 1, settings.deliver_base_s, settings.deliver_cap_s)
        message = f"{err.message}; retrying (attempt {d.attempts} of {settings.deliver_max_attempts})"
        if breaker().state() is State.OPEN:
            delay = max(delay, breaker().seconds_until_probe())
            message = f"Vendor unavailable, paused (attempt {d.attempts} of {settings.deliver_max_attempts})"
    else:
        if err.kind is ErrorKind.TRANSIENT:
            err = PipelineError(kind=ErrorKind.PERMANENT, code="RETRIES_EXHAUSTED", stage="deliver",
                                message=message_for("RETRIES_EXHAUSTED", attempts=d.attempts, last=err.message))
        bound.error("delivery_failed", error_code=err.code, error_kind=err.kind)
        with session_scope() as s:
            repo.delivery_failed(s, d, err)
        return

    retry_at = repo.now() + timedelta(seconds=delay)
    err = err.model_copy(update={"retry_at": retry_at})
    with session_scope() as s:
        ok = repo.delivery_retry_wait(s, d, err, retry_at, count_attempt, message)
    if ok:
        attempt = d.attempts if count_attempt else d.attempts - 1
        fair_queue().delay(Job(str(d.file_id), d.user_id, attempt), retry_at.timestamp())
    bound.warning("delivery_retry", error_code=err.code, error_kind=err.kind, delay=round(delay, 1))
