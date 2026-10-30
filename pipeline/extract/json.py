import json
from typing import IO

from pipeline.errors.model import CorruptFile
from pipeline.extract import MAX_ROWS, Extracted


def extract(stream: IO[bytes], out: Extracted) -> None:
    try:
        doc = json.load(stream)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise CorruptFile("invalid JSON") from exc
    if isinstance(doc, list):
        if len(doc) > MAX_ROWS:
            out.warn(f"only the first {MAX_ROWS} of {len(doc)} records are included")
        out.data = {"kind": "array", "recordCount": len(doc), "records": doc[:MAX_ROWS]}
    elif isinstance(doc, dict):
        out.data = {"kind": "object", "keys": sorted(doc.keys())[:500], "record": doc}
    else:
        out.data = {"kind": type(doc).__name__, "value": doc}
