import csv
import io
from typing import IO

from pipeline.errors.model import CorruptFile
from pipeline.extract import MAX_ROWS, Extracted


def extract(stream: IO[bytes], out: Extracted) -> None:
    """Rows as objects keyed by header. Bad rows become warnings instead of failing the file."""
    text = io.TextIOWrapper(stream, encoding="utf-8-sig", newline="")
    try:
        sample = text.read(64 * 1024)
    except UnicodeDecodeError as exc:
        raise CorruptFile("not valid UTF-8 text") from exc
    if not sample.strip():
        raise CorruptFile("empty file")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
    text.seek(0)
    reader = csv.reader(text, dialect)
    try:
        header = next(reader)
        rows, count = [], 0
        for line_no, row in enumerate(reader, start=2):
            if not any(cell.strip() for cell in row):
                continue
            if len(row) != len(header):
                out.warn(f"line {line_no}: expected {len(header)} columns, got {len(row)}")
                continue
            count += 1
            if len(rows) < MAX_ROWS:
                rows.append(dict(zip(header, row)))
    except UnicodeDecodeError as exc:
        raise CorruptFile("not valid UTF-8 text") from exc
    if count > MAX_ROWS:
        out.warn(f"only the first {MAX_ROWS} of {count} rows are included")
    out.data = {"columns": header, "rowCount": count, "rows": rows}
    text.detach()
