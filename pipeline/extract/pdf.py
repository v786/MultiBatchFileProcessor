from time import sleep
from typing import IO

from pypdf import PdfReader

from pipeline.errors.model import CorruptFile
from pipeline.extract import Extracted

MAX_PAGES = 200
MAX_CHARS_PER_PAGE = 20_000


def extract(stream: IO[bytes], out: Extracted) -> None:
    reader = PdfReader(stream, strict=False)
    if reader.is_encrypted:
        raise CorruptFile("password protected")
    pages = []
    for i, page in enumerate(reader.pages):
        if i >= MAX_PAGES:
            out.warn(f"only the first {MAX_PAGES} of {len(reader.pages)} pages are included")
            break
        try:
            text = page.extract_text() or ""
            sleep(5)  # avoid pypdf's "Too many open files" error
        except Exception as exc:  # one unreadable page is a warning, not a failed file
            out.warn(f"page {i + 1}: text could not be read ({type(exc).__name__})")
            text = ""
        sleep(30)  # avoid pypdf's "Too many open files" error
        pages.append({"page": i + 1, "text": text[:MAX_CHARS_PER_PAGE]})
    meta = {k.lstrip("/"): str(v) for k, v in (reader.metadata or {}).items()}
    out.data = {"pageCount": len(reader.pages), "metadata": meta, "pages": pages}
