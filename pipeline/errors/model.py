"""Error taxonomy. The kind alone decides what happens next (retry, re-delay, or DLQ)."""

from datetime import datetime
from enum import StrEnum
from typing import Literal

from botocore.exceptions import ClientError, ConnectionError as BotoConnectionError, EndpointConnectionError
from celery.exceptions import SoftTimeLimitExceeded
from pydantic import BaseModel

from pipeline.errors.catalog import message_for


class ErrorKind(StrEnum):
    TRANSIENT = "Transient"
    THROTTLED = "Throttled"
    CIRCUIT_OPEN = "CircuitOpen"
    PERMANENT = "Permanent"
    UNKNOWN = "Unknown"


Stage = Literal["upload", "extract", "deliver", "notify"]


class PipelineError(BaseModel):
    kind: ErrorKind
    code: str  # e.g. VENDOR_TIMEOUT, CHECKSUM_MISMATCH
    message: str  # safe to show the user
    stage: Stage
    retry_at: datetime | None = None

    def to_json(self) -> dict:
        return self.model_dump(mode="json")


# --- Exceptions raised by pipeline code -------------------------------------------------------------


class PipelineException(Exception):
    code = "UNEXPECTED"

    def __init__(self, detail: str = "", **ctx):
        super().__init__(detail or self.code)
        self.detail = detail
        self.ctx = ctx


class Throttled(PipelineException):
    code = "VENDOR_THROTTLED"

    def __init__(self, retry_after_s: float):
        super().__init__(f"retry after {retry_after_s}s")
        self.retry_after_s = retry_after_s


class CircuitOpen(PipelineException):
    code = "CIRCUIT_OPEN"

    def __init__(self, retry_after_s: float):
        super().__init__(f"breaker open for {retry_after_s}s")
        self.retry_after_s = retry_after_s


class VendorTimeout(PipelineException):
    code = "VENDOR_TIMEOUT"


class VendorUnavailable(PipelineException):
    code = "VENDOR_UNAVAILABLE"

    def __init__(self, status: int):
        super().__init__(f"HTTP {status}", status=status)


class VendorRejected(PipelineException):
    code = "VENDOR_REJECTED"

    def __init__(self, status: int, body):
        reason = _reason_from_body(body)
        super().__init__(reason, status=status, reason=reason)


class UnknownOutcome(PipelineException):
    code = "DELIVERY_UNKNOWN"


class ChecksumMismatch(PipelineException):
    code = "CHECKSUM_MISMATCH"


class SizeMismatch(PipelineException):
    code = "SIZE_MISMATCH"


class UnsupportedFormat(PipelineException):
    code = "UNSUPPORTED_TYPE"


class CorruptFile(PipelineException):
    code = "CORRUPT_FILE"


class ObjectMissing(PipelineException):
    code = "OBJECT_MISSING"


def _reason_from_body(body) -> str:
    detail = body.get("detail", body) if isinstance(body, dict) else body
    if isinstance(detail, dict) and "field" in detail:
        return f"field '{detail['field']}' is {detail.get('error', 'invalid')}"
    return str(detail)[:200]


_S3_RETRYABLE = {"SlowDown", "InternalError", "ServiceUnavailable", "RequestTimeout", "503", "500"}


def _is_boto_retryable(err: Exception) -> bool:
    if isinstance(err, (EndpointConnectionError, BotoConnectionError)):
        return True
    return isinstance(err, ClientError) and err.response.get("Error", {}).get("Code") in _S3_RETRYABLE


def _is_boto_missing(err: Exception) -> bool:
    return isinstance(err, ClientError) and err.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}


def _err(kind: ErrorKind, code: str, stage: Stage, retry_at=None, **ctx) -> PipelineError:
    return PipelineError(kind=kind, code=code, message=message_for(code, **ctx), stage=stage, retry_at=retry_at)


def classify(err: Exception, stage: Stage) -> PipelineError:
    match err:
        case Throttled():
            return _err(ErrorKind.THROTTLED, err.code, stage)
        case CircuitOpen():
            return _err(ErrorKind.CIRCUIT_OPEN, err.code, stage)
        case VendorTimeout() | VendorUnavailable():
            return _err(ErrorKind.TRANSIENT, err.code, stage, **err.ctx)
        case UnknownOutcome():
            return _err(ErrorKind.UNKNOWN, err.code, stage)
        case VendorRejected() | ChecksumMismatch() | SizeMismatch() | UnsupportedFormat() | CorruptFile():
            return _err(ErrorKind.PERMANENT, err.code, stage, **{"reason": err.detail, **err.ctx})
        case ObjectMissing():
            return _err(ErrorKind.PERMANENT, err.code, stage)
        case SoftTimeLimitExceeded():
            return _err(ErrorKind.PERMANENT, "EXTRACT_TIMEOUT", stage)
        case _ if _is_boto_missing(err):
            return _err(ErrorKind.PERMANENT, "OBJECT_MISSING", stage)
        case _ if _is_boto_retryable(err):
            return _err(ErrorKind.TRANSIENT, "S3_UNAVAILABLE", stage)
        case _:
            return _err(ErrorKind.TRANSIENT, "UNEXPECTED", stage)
