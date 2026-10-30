import hashlib
import json

import pytest

from pipeline.errors.model import ChecksumMismatch, CorruptFile
from pipeline.extract import detect_type, extract_stream
from tests.conftest import make_csv, make_json, make_pdf, make_xlsx


def run(data: bytes, file_type: str, sha: str | None = None):
    chunks = [data[i:i + 1000] for i in range(0, len(data), 1000)]
    return extract_stream(iter(chunks), file_type, sha or hashlib.sha256(data).hexdigest(), len(data))


def test_csv_with_partial_rows_succeeds_with_warnings():
    out = run(make_csv(rows=1000, bad_rows=3), "csv")
    assert out.data["rowCount"] == 1000
    assert out.warning_count == 3
    assert json.loads(out.to_json_bytes())["warningCount"] == 3


def test_json_xlsx_pdf():
    assert run(make_json(), "json").data["recordCount"] == 2
    sheet = run(make_xlsx(), "xlsx").data["sheets"][0]
    assert sheet["rowCount"] == 2 and sheet["columns"] == ["invoice", "amount"]
    pdf = run(make_pdf("Hello vendor"), "pdf").data
    assert pdf["pageCount"] == 1 and "Hello vendor" in pdf["pages"][0]["text"]


def test_corrupt_files():
    with pytest.raises(CorruptFile):
        run(b"%PDF-1.4 this is not a pdf", "pdf")
    with pytest.raises(CorruptFile):
        run(b"{not json", "json")
    with pytest.raises(CorruptFile):
        run(b"PK\x03\x04garbage", "xlsx")


def test_checksum_mismatch():
    with pytest.raises(ChecksumMismatch):
        run(make_json(), "json", sha="0" * 64)


def test_detect_type():
    assert detect_type("a.PDF", "application/pdf") == "pdf"
    assert detect_type("a.csv", "") == "csv"
    assert detect_type("a.exe", "") is None
    assert detect_type("a.pdf", "image/png") is None
