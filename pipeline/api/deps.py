"""Auth is a stub (non-goal): the bearer token *is* the user id. Tenant = user."""

import re
from uuid import UUID

from fastapi import Header, HTTPException, Query
from sqlalchemy.orm import Session

from pipeline.db.models import Batch
from pipeline.ids import parse_public_id

_TOKEN = re.compile(r"[A-Za-z0-9_.@-]{1,64}")


def current_user(authorization: str | None = Header(None), token: str | None = Query(None)) -> str:
    """`Authorization: Bearer <user>`; `?token=` is accepted for EventSource, which cannot set headers."""
    raw = authorization.removeprefix("Bearer ").strip() if authorization else token
    if not raw or not _TOKEN.fullmatch(raw):
        raise HTTPException(401, "missing or invalid bearer token")
    return raw


def parse_batch_id(batch_id: str) -> UUID:
    try:
        return parse_public_id(batch_id, "bat")
    except ValueError:
        raise HTTPException(404, "batch not found")


def parse_file_id(file_id: str) -> UUID:
    try:
        return parse_public_id(file_id, "tsk")
    except ValueError:
        raise HTTPException(404, "file not found")


def owned_batch(s: Session, batch_id: UUID, user: str) -> Batch:
    b = s.get(Batch, batch_id)
    if b is None or b.user_id != user:
        raise HTTPException(404, "batch not found")
    return b
