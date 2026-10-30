"""Health, live system status for the UI, and Prometheus metrics."""

from fastapi import APIRouter, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, generate_latest
from sqlalchemy import func, select, text

from pipeline import storage
from pipeline.config import settings
from pipeline.db.models import DeadLetter, File
from pipeline.db.session import session_scope
from pipeline.redis_client import get_redis
from pipeline.resilience import breaker, fair_queue, limiter, semaphore

router = APIRouter(tags=["system"])


@router.get("/health")
def health():
    checks = {}
    try:
        with session_scope() as s:
            s.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {type(exc).__name__}"
    try:
        get_redis().ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"error: {type(exc).__name__}"
    try:
        storage.s3().head_bucket(Bucket=settings.s3_bucket)
        checks["s3"] = "ok"
    except Exception as exc:
        checks["s3"] = f"error: {type(exc).__name__}"
    ok = all(v == "ok" for v in checks.values())
    return JSONResponse({"ok": ok, **checks}, status_code=200 if ok else 503)


def system_status() -> dict:
    from pipeline.tasks.deliver import throughput_per_s

    fq = fair_queue()
    return {
        "vendor": {
            "inflight": semaphore().in_use(),
            "slots": limiter().effective_slots(),
            "maxConcurrent": settings.vendor_max_concurrent,
            "pausedForS": round(limiter().paused_for(), 1),
            "breaker": breaker().snapshot(),
            "throughputPerS": round(throughput_per_s(), 2),
        },
        "queue": {
            "waiting": fq.ready_count(),
            "delayed": fq.waiting_count() - fq.ready_count(),
            "waitingByUser": fq.waiting_by_user(),
            "inflightByUser": fq.inflight_by_user(),
        },
    }


@router.get("/system/status")
def status():
    return system_status()


@router.get("/metrics")
def metrics():
    """Gauges read from shared state at scrape time, so every API replica reports the same numbers."""
    reg = CollectorRegistry()
    st = system_status()
    Gauge("vendor_inflight", "Vendor calls holding a slot (must stay <= 20)", registry=reg).set(st["vendor"]["inflight"])
    Gauge("vendor_effective_slots", "Slots currently usable", registry=reg).set(st["vendor"]["slots"])
    Gauge("breaker_state", "0 closed, 1 half-open, 2 open", ["vendor"], registry=reg).labels(
        settings.vendor_name).set(st["vendor"]["breaker"]["gauge"])
    Gauge("deliver_waiting_total", "Deliveries waiting for a slot", registry=reg).set(st["queue"]["waiting"])
    Gauge("deliver_delayed_total", "Deliveries in backoff", registry=reg).set(st["queue"]["delayed"])
    by_user = Gauge("deliver_waiting_by_user", "Waiting deliveries per user", ["user"], registry=reg)
    for user, n in st["queue"]["waitingByUser"].items():
        by_user.labels(user).set(n)
    files = Gauge("pipeline_files", "Files by status", ["status"], registry=reg)
    dlq = Gauge("dlq_size", "Unresolved dead letters", ["scope"], registry=reg)
    with session_scope() as s:
        for status_, n in s.execute(select(File.status, func.count()).group_by(File.status)):
            files.labels(status_).set(n)
        for scope, n in s.execute(select(DeadLetter.scope, func.count()).where(DeadLetter.resolved_at.is_(None))
                                  .group_by(DeadLetter.scope)):
            dlq.labels(scope).set(n)
    return Response(generate_latest(reg), media_type=CONTENT_TYPE_LATEST)
