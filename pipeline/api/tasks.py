from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select

from pipeline.api.deps import current_user
from pipeline.db import repo
from pipeline.db.models import Batch, FileStatus, TaskEvent
from pipeline.db.session import session_scope
from pipeline.views import file_owner, get_file_by_task, parse_since, task_detail, task_summary

router = APIRouter(tags=["tasks"])


def _owned_task(s, task_id: str, user: str):
    f = get_file_by_task(s, task_id)
    if f is None or file_owner(s, f) != user:
        raise HTTPException(404, "task not found")
    return f


@router.get("/tasks/{task_id}")
def get_task(task_id: str, user: str = Depends(current_user)):
    with session_scope() as s:
        return task_detail(s, _owned_task(s, task_id, user))


@router.get("/tasks/{task_id}/history")
def task_history(task_id: str, user: str = Depends(current_user)):
    with session_scope() as s:
        _owned_task(s, task_id, user)
        events = s.execute(select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.id)).scalars().all()
        return {"taskId": task_id, "events": [repo.event_json(e) for e in events]}


@router.post("/tasks/{task_id}/retry")
def retry_task(task_id: str, user: str = Depends(current_user)):
    with session_scope() as s:
        f = _owned_task(s, task_id, user)
        if not repo.replay_file(s, f.id):
            raise HTTPException(409, f"task is {f.status}; only failed extraction or delivery can be retried")
    with session_scope() as s:
        return task_detail(s, repo.get_file(s, f.id))


@router.get("/tasks")
def list_tasks(user: str = Depends(current_user), status: str | None = None, since: str | None = None,
               limit: int = Query(100, ge=1, le=500)):
    """The user's tasks across batches, filterable by state. `status=FAILED` matches every failure state."""
    q = select(repo.F).join(Batch, Batch.id == repo.F.c.batch_id).where(Batch.user_id == user)
    if status == "FAILED":
        q = q.where(repo.F.c.status.in_(FileStatus.FAILED))
    elif status:
        q = q.where(repo.F.c.status == status)
    if since:
        try:
            q = q.where(repo.F.c.updated_at >= parse_since(since))
        except ValueError:
            raise HTTPException(422, "since must be an ISO-8601 timestamp")
    with session_scope() as s:
        rows = s.execute(q.order_by(repo.F.c.updated_at.desc()).limit(limit)).all()
        return {"tasks": [task_summary(f, repo.get_delivery(s, f.id)) for f in rows]}
