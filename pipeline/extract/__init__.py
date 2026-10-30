"""Extractors. The object is streamed from S3 in chunks into a disk-backed spool (never fully into memory),
hashed on the way so the content is verified against the declared SHA-256, then handed to the parser."""

import hashlib
import importlib
import json as _json  # aliased: the `json` name belongs to the pipeline.extract.json submodule
import os
import tempfile
from dataclasses import dataclass, field
from typing import IO, Callable

from pipeline.config import settings
from pipeline.errors.model import ChecksumMismatch, CorruptFile, SizeMismatch, UnsupportedFormat

ALLOWED_TYPES: dict[str, set[str]] = {
    "pdf": {"application/pdf"},
    "csv": {"text/csv", "application/csv", "text/plain", "application/vnd.ms-excel"},
    "xlsx": {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
    "json": {"application/json", "text/json"},
}
GENERIC_MIME = {"", "application/octet-stream"}
CONTENT_TYPE = {
    "pdf": "application/pdf",
    "csv": "text/csv",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "json": "application/json",
}
MAX_ROWS = 5000  # rows kept in the extracted JSON per sheet/file
MAX_WARNINGS = 100


def detect_type(name: str, mime_type: str) -> str | None:
    ext = os.path.splitext(name.lower())[1].lstrip(".")
    if ext not in ALLOWED_TYPES:
        return None
    if mime_type and mime_type not in GENERIC_MIME and mime_type not in ALLOWED_TYPES[ext]:
        return None
    return ext


@dataclass
class Extracted:
    file_type: str
    data: dict
    warnings: list[str] = field(default_factory=list)
    warning_count: int = 0

    def warn(self, msg: str) -> None:
        self.warning_count += 1
        if len(self.warnings) < MAX_WARNINGS:
            self.warnings.append(msg)

    def to_json_bytes(self) -> bytes:
        body = {"fileType": self.file_type, "parserVersion": settings.parser_version, "data": self.data,
                "warnings": self.warnings, "warningCount": self.warning_count}
        return _json.dumps(body, default=str, sort_keys=True, separators=(",", ":")).encode()


def _extractors() -> dict[str, Callable[[IO[bytes], Extracted], None]]:
    # import_module: `from pipeline.extract import json` could resolve to a stale package attribute
    return {t: importlib.import_module(f"pipeline.extract.{t}").extract for t in ALLOWED_TYPES}


def spool_and_verify(chunks, expected_sha256: str, expected_size: int, spool: IO[bytes]) -> None:
    h, size = hashlib.sha256(), 0
    for chunk in chunks:
        h.update(chunk)
        size += len(chunk)
        spool.write(chunk)
    if size != expected_size:
        raise SizeMismatch(f"stored {size} bytes, declared {expected_size}")
    if h.hexdigest() != expected_sha256:
        raise ChecksumMismatch()
    spool.seek(0)


def extract_stream(chunks, file_type: str, sha256: str, size: int) -> Extracted:
    parser = _extractors().get(file_type)
    if parser is None:
        raise UnsupportedFormat(file_type)
    with tempfile.SpooledTemporaryFile(max_size=16 * 1024 * 1024) as spool:
        spool_and_verify(chunks, sha256, size, spool)
        out = Extracted(file_type=file_type, data={})
        try:
            parser(spool, out)
        except (CorruptFile, UnsupportedFormat):
            raise
        except Exception as exc:  # parser libraries raise a zoo of exception types for bad input
            raise CorruptFile(type(exc).__name__) from exc
        return out
