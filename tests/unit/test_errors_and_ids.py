from botocore.exceptions import ClientError
from celery.exceptions import SoftTimeLimitExceeded

from pipeline.errors import ErrorKind, classify
from pipeline.errors.model import (
    CircuitOpen,
    CorruptFile,
    Throttled,
    VendorRejected,
    VendorTimeout,
    VendorUnavailable,
)
from pipeline.ids import batch_public_id, new_ulid, parse_public_id, task_public_id, ulid_to_uuid


def test_classify_kinds():
    assert classify(Throttled(2), "deliver").kind is ErrorKind.THROTTLED
    assert classify(CircuitOpen(30), "deliver").kind is ErrorKind.CIRCUIT_OPEN
    assert classify(VendorTimeout(), "deliver").kind is ErrorKind.TRANSIENT
    e = classify(VendorUnavailable(503), "deliver")
    assert e.kind is ErrorKind.TRANSIENT and e.message == "Vendor unavailable (HTTP 503)"
    e = classify(VendorRejected(422, {"detail": {"field": "taxId", "error": "missing"}}), "deliver")
    assert e.kind is ErrorKind.PERMANENT
    assert e.message == "The vendor rejected the record: field 'taxId' is missing"
    assert classify(CorruptFile("bad xref"), "extract").message == "Extraction failed: file is corrupt (bad xref)"
    assert classify(SoftTimeLimitExceeded(), "extract").code == "EXTRACT_TIMEOUT"
    slow = ClientError({"Error": {"Code": "SlowDown"}}, "GetObject")
    assert classify(slow, "extract").kind is ErrorKind.TRANSIENT
    assert classify(RuntimeError("boom"), "extract").code == "UNEXPECTED"


def test_public_ids_roundtrip_and_sort():
    a, b = new_ulid(), new_ulid()
    u = ulid_to_uuid(a)
    assert parse_public_id(batch_public_id(u), "bat") == u
    assert task_public_id(u).startswith("tsk_")
    assert a[:10] <= b[:10]  # time prefix sorts
