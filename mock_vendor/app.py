"""Mock vendor: enforces its own 20-call limit, replies after a random 10-20 s, with switchable faults.

Runs as a single Uvicorn process so its in-flight counter is exact. Also hosts a tiny webhook receiver that
records completion-notification webhooks for tests.
"""

import asyncio
import hashlib
import hmac
import json
import os
import random
from dataclasses import asdict, dataclass, field
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel


@dataclass
class MockConfig:
    latency_s: tuple[float, float] = (10.0, 20.0)
    latency_scale: float = 1.0
    max_concurrent: int = 20
    error_rate: float = 0.05
    timeout_rate: float = 0.02
    reject_rate: float = 0.01
    outage: bool = False
    honor_idempotency: bool = True
    seed: int | None = None
    rng: random.Random = field(default_factory=random.Random, repr=False)

    def reseed(self) -> None:
        self.rng = random.Random(self.seed)


@dataclass
class Stats:
    received: int = 0
    accepted: int = 0
    duplicates_seen: int = 0
    duplicate_records: int = 0
    rejected_429: int = 0
    errors_500: int = 0
    rejected_422: int = 0
    outage_503: int = 0
    timeouts: int = 0
    inflight: int = 0
    peak_inflight: int = 0


def _env_config() -> MockConfig:
    cfg = MockConfig()
    if "MOCK_LATENCY_SCALE" in os.environ:
        cfg.latency_scale = float(os.environ["MOCK_LATENCY_SCALE"])
    for name in ("error_rate", "timeout_rate", "reject_rate"):
        if f"MOCK_{name.upper()}" in os.environ:
            setattr(cfg, name, float(os.environ[f"MOCK_{name.upper()}"]))
    if "MOCK_SEED" in os.environ:
        cfg.seed = int(os.environ["MOCK_SEED"])
    cfg.reseed()
    return cfg


app = FastAPI(title="Mock Vendor")
cfg = _env_config()
stats = Stats()
records: dict[str, dict] = {}  # idempotency key -> record
record_counts: dict[str, int] = {}
webhooks: list[dict] = []
inflight = 0


@app.post("/records")
async def create_record(body: dict, idempotency_key: str = Header(...)):
    global inflight
    stats.received += 1
    if cfg.honor_idempotency and idempotency_key in records:
        stats.duplicates_seen += 1
        return records[idempotency_key]  # same answer, no new record
    if cfg.outage:
        stats.outage_503 += 1
        raise HTTPException(503, "vendor down")
    if inflight >= cfg.max_concurrent:
        stats.rejected_429 += 1  # our limiter should keep this at 0
        return JSONResponse({"error": "too many concurrent calls"}, 429, headers={"Retry-After": "2"})

    inflight += 1
    stats.inflight = inflight
    stats.peak_inflight = max(stats.peak_inflight, inflight)
    try:
        await asyncio.sleep(cfg.rng.uniform(*cfg.latency_s) * cfg.latency_scale)  # 10-20 s
        roll = cfg.rng.random()
        if roll < cfg.timeout_rate:
            stats.timeouts += 1
            await asyncio.sleep(40 * cfg.latency_scale)  # past the client's 30 s timeout
        elif roll < cfg.timeout_rate + cfg.error_rate:
            stats.errors_500 += 1
            raise HTTPException(500, "random failure")
        elif roll < cfg.timeout_rate + cfg.error_rate + cfg.reject_rate:
            stats.rejected_422 += 1
            raise HTTPException(422, {"field": "taxId", "error": "missing"})
        record = {"id": f"V-{uuid4().hex[:8]}", "externalId": idempotency_key,
                  "fileName": body.get("fileName"), "taskId": body.get("taskId")}
        if idempotency_key in records:
            stats.duplicate_records += 1  # only possible with honor_idempotency off
        record_counts[idempotency_key] = record_counts.get(idempotency_key, 0) + 1
        records[idempotency_key] = record
        stats.accepted += 1
        return record
    finally:
        inflight -= 1
        stats.inflight = inflight


@app.get("/records")
async def lookup_record(externalId: str = Query(...)):
    if externalId not in records:
        raise HTTPException(404, "not found")
    return records[externalId]


@app.get("/health")
async def health():
    return {"ok": not cfg.outage}


class ControlUpdate(BaseModel):
    latency_s: tuple[float, float] | None = None
    latency_scale: float | None = None
    max_concurrent: int | None = None
    error_rate: float | None = None
    timeout_rate: float | None = None
    reject_rate: float | None = None
    outage: bool | None = None
    honor_idempotency: bool | None = None
    seed: int | None = None
    reset_stats: bool = False
    reset_records: bool = False


def _public_config() -> dict:
    out = asdict(cfg)
    out.pop("rng", None)
    return out


@app.put("/_control")
async def control(update: ControlUpdate):
    global stats
    changes = update.model_dump(exclude_none=True, exclude={"reset_stats", "reset_records"})
    for key, value in changes.items():
        setattr(cfg, key, tuple(value) if key == "latency_s" else value)
    if "seed" in changes:
        cfg.reseed()
    if update.reset_stats:
        stats = Stats(inflight=inflight)
    if update.reset_records:
        records.clear()
        record_counts.clear()
    return _public_config()


@app.get("/_control")
async def get_control():
    return _public_config()


@app.get("/_control/stats")
async def get_stats():
    return {**asdict(stats), "records": len(records)}


# --- webhook receiver for notification tests ---------------------------------------------------------------

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "dev-webhook-secret")


@app.post("/webhooks")
async def receive_webhook(request: Request, x_signature: str | None = Header(None)):
    raw = await request.body()
    expected = "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    webhooks.append({"valid": hmac.compare_digest(expected, x_signature or ""), "body": json.loads(raw)})
    return {"ok": True}


@app.get("/webhooks")
async def list_webhooks():
    return {"webhooks": webhooks}
