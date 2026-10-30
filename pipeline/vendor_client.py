"""Vendor HTTP client: breaker -> timeout -> HTTP. Classifies every outcome into a pipeline exception."""

import os
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from pipeline.config import settings
from pipeline.db.repo import Delivery
from pipeline.errors.model import Throttled, VendorRejected, VendorTimeout, VendorUnavailable
from pipeline.resilience import breaker

_http: httpx.Client | None = None
_pid: int | None = None


def http() -> httpx.Client:
    global _http, _pid
    if _http is None or _pid != os.getpid():
        _http = httpx.Client(limits=httpx.Limits(max_connections=100, max_keepalive_connections=40))
        _pid = os.getpid()
    return _http


def set_http(client: httpx.Client) -> None:
    """Test hook."""
    global _http, _pid
    _http, _pid = client, os.getpid()


def parse_retry_after(resp: httpx.Response, default: float = 2.0) -> float:
    value = resp.headers.get("Retry-After")
    if not value:
        return default
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        return max(0.0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
    except (TypeError, ValueError):
        return default


def build_payload(d: Delivery, extracted: dict) -> dict:
    return {
        "externalId": d.idempotency_key,
        "taskId": d.task_id,
        "fileName": d.name,
        "fileType": d.file_type,
        "sha256": d.sha256,
        "payloadVersion": settings.payload_version,
        "extracted": extracted,
    }


def call_vendor(delivery: Delivery, payload: dict, gate: str | None) -> str:
    b = breaker()
    try:
        resp = http().post(
            f"{settings.vendor_url}/records",
            json=payload,
            headers={"Idempotency-Key": delivery.idempotency_key},
            timeout=httpx.Timeout(settings.vendor_timeout_s, connect=5.0),
        )
    except (httpx.TimeoutException, httpx.TransportError) as err:
        b.on_failure(gate)
        raise VendorTimeout(type(err).__name__) from err

    if resp.status_code >= 500:
        b.on_failure(gate)
        raise VendorUnavailable(resp.status_code)
    if resp.status_code == 429:
        b.return_probe(gate)
        raise Throttled(retry_after_s=parse_retry_after(resp))
    if resp.status_code >= 400:
        b.on_success(gate)  # the vendor is healthy; the payload is at fault
        try:
            body = resp.json()
        except ValueError:
            body = resp.text
        raise VendorRejected(resp.status_code, body)

    b.on_success(gate)
    return resp.json()["id"]


def lookup_record(external_id: str) -> str | None:
    """For vendors without idempotency keys: did an earlier, timed-out call actually create the record?"""
    resp = http().get(f"{settings.vendor_url}/records", params={"externalId": external_id},
                      timeout=httpx.Timeout(10.0, connect=5.0))
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["id"]
