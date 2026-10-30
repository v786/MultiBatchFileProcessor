"""Test harness: the real pipeline code, in one process.

Postgres -> SQLite, Redis -> fakeredis (with Lua), S3 -> moto, vendor -> the mock vendor app via TestClient
with `latency_scale` shrunk, Celery -> eager. The docker-compose stack runs the same code for real.
"""

import io
import os
import sys
from pathlib import Path

os.environ.update({
    "DATABASE_URL": "sqlite:///:memory:",
    "REDIS_URL": "redis://fake",
    "S3_ENDPOINT_URL": "",
    "S3_PUBLIC_ENDPOINT_URL": "",
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "S3_ACCESS_KEY": "test",
    "S3_SECRET_KEY": "test",
    "VENDOR_URL": "http://vendor.test",
    "LOG_LEVEL": "WARNING",
})
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fakeredis  # noqa: E402
import pytest  # noqa: E402
from moto import mock_aws  # noqa: E402
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from pipeline import resilience, storage  # noqa: E402
from pipeline.celery_app import app as celery_app  # noqa: E402
from pipeline.config import settings  # noqa: E402
from pipeline.db.models import Base  # noqa: E402
from pipeline.db.session import set_engine  # noqa: E402
from pipeline.redis_client import set_redis  # noqa: E402


@pytest.fixture
def redis():
    r = fakeredis.FakeRedis(decode_responses=True)
    set_redis(r)
    resilience._cache.clear()
    yield r
    resilience._cache.clear()


@pytest.fixture
def db():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    set_engine(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def s3():
    with mock_aws():
        storage.reset_clients()
        storage.s3().create_bucket(Bucket=settings.s3_bucket)
        yield storage.s3()
    storage.reset_clients()


@pytest.fixture
def vendor():
    """The mock vendor app, fast: 1-2 ms per call, faults off unless a test turns them on."""
    from starlette.testclient import TestClient

    import mock_vendor.app as mv
    from pipeline import vendor_client

    mv.cfg.latency_scale = 0.0001
    mv.cfg.error_rate = mv.cfg.timeout_rate = mv.cfg.reject_rate = 0.0
    mv.cfg.outage = False
    mv.cfg.honor_idempotency = True
    mv.cfg.max_concurrent = 20
    mv.cfg.seed = 7
    mv.cfg.reseed()
    mv.records.clear()
    mv.record_counts.clear()
    mv.webhooks.clear()
    mv.stats = mv.Stats()
    client = TestClient(mv.app, base_url=settings.vendor_url)
    vendor_client.set_http(client)
    yield mv
    client.close()


@pytest.fixture
def eager():
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True
    yield
    celery_app.conf.task_always_eager = False


@pytest.fixture
def api(db, redis, s3, vendor, eager):
    from fastapi.testclient import TestClient

    from pipeline.api.app import create_app

    with TestClient(create_app()) as client:
        client.headers["Authorization"] = "Bearer alice"
        yield client


# --- sample files ------------------------------------------------------------------------------------------


def make_csv(rows: int = 5, bad_rows: int = 0) -> bytes:
    lines = ["invoice,amount,taxId"] + [f"INV-{i},{i * 10}.50,TX{i}" for i in range(rows)]
    lines += ["broken,row"] * bad_rows
    return ("\n".join(lines) + "\n").encode()


def make_json() -> bytes:
    return b'[{"invoice": "A-1", "amount": 10}, {"invoice": "A-2", "amount": 20}]'


def make_xlsx() -> bytes:
    from openpyxl import Workbook

    wb = Workbook()
    ws = wb.active
    ws.title = "Invoices"
    ws.append(["invoice", "amount"])
    ws.append(["X-1", 12.5])
    ws.append(["X-2", 99])
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def make_pdf(text: str = "Invoice 42 total 100.00") -> bytes:
    from reportlab.pdfgen import canvas

    buf = io.BytesIO()
    c = canvas.Canvas(buf)
    c.drawString(72, 720, text)
    c.showPage()
    c.save()
    return buf.getvalue()
