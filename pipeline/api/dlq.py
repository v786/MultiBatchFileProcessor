"""Dead-letter admin endpoints. Auth is a stub, so these are scoped to the caller's own batches."""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

from pipeline.api.deps import current_user
from pipeline.db import repo
from pipeline.db.models import Batch, DeadLetter
from pipeline.db.session import session_scope
from pipeline.ids import batch_public_id

router = APIRouter(tags=["dlq"])


def _json(d: DeadLetter) -> dict:
    return {"id": d.id, "scope": d.scope, "fileId": str(d.ref_id), "taskId": d.task_id,
            "batchId": batch_public_id(d.batch_id), "kind": d.kind, "code": d.code, "message": d.message,
            "attempts": d.attempts, "payloadRef": d.payload_ref, "replays": d.replays,
            "firstSeen": repo.iso(d.first_seen), "lastSeen": repo.iso(d.last_seen),
            "resolvedAt": repo.iso(d.resolved_at)}


@router.get("/dlq")
def list_dlq(user: str = Depends(current_user), scope: str | None = Query(None, pattern="^(extract|delivery)$"),
             include_resolved: bool = False, limit: int = Query(100, le=500)):
    q = select(DeadLetter).join(Batch, Batch.id == DeadLetter.batch_id).where(Batch.user_id == user)
    if scope:
        q = q.where(DeadLetter.scope == scope)
    if not include_resolved:
        q = q.where(DeadLetter.resolved_at.is_(None))
    with session_scope() as s:
        return {"items": [_json(d) for d in s.execute(q.order_by(DeadLetter.last_seen.desc()).limit(limit)).scalars()]}


@router.post("/dlq/{dlq_id}/replay")
def replay(dlq_id: int, user: str = Depends(current_user)):
    """Reset the item to PENDING and re-queue it. Idempotency keys make a replay of a success harmless."""
    with session_scope() as s:
        d = s.get(DeadLetter, dlq_id)
        if d is None or s.get(Batch, d.batch_id).user_id != user:
            raise HTTPException(404, "dead letter not found")
        if d.resolved_at is not None:
            raise HTTPException(409, "already replayed")
        if not repo.replay_file(s, d.ref_id):
            raise HTTPException(409, "item is no longer in a failed state")
        return _json(d)
