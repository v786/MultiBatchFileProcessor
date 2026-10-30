from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pipeline import storage
from pipeline.api.deps import current_user, owned_batch, parse_batch_id
from pipeline.config import settings
from pipeline.db import repo
from pipeline.db.models import Batch, BatchStatus, File, FileStatus
from pipeline.db.session import session_scope
from pipeline.errors import ErrorKind, PipelineError
from pipeline.errors.catalog import message_for
from pipeline.extract import CONTENT_TYPE, detect_type
from pipeline.ids import batch_public_id, new_ulid, task_public_id, ulid_to_uuid
from pipeline.redis_client import get_redis
from pipeline.resilience.admission import TokenBucket
from pipeline.views import batch_view

router = APIRouter(tags=["batches"])


class FileSpec(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    size: int = Field(ge=0)
    mimeType: str = ""
    sha256: str = Field(pattern=r"^[A-Fa-f0-9]{64}$")

    @field_validator("sha256")
    @classmethod
    def lower(cls, v: str) -> str:
        return v.lower()


class NotifySpec(BaseModel):
    channels: list[Literal["in_app", "email", "webhook"]] = []
    webhookUrl: str | None = None
    email: str | None = None
    notifyOn: list[Literal["first_failure"]] = []


class CreateBatch(BaseModel):
    files: list[FileSpec] = Field(min_length=1)
    notify: NotifySpec | None = None


def _rejection(spec: FileSpec) -> PipelineError | None:
    code, ctx = None, {}
    if detect_type(spec.name, spec.mimeType) is None:
        code = "UNSUPPORTED_TYPE"
    elif spec.size > settings.max_file_bytes:
        code, ctx = "FILE_TOO_LARGE", {"limit_mb": settings.max_file_bytes // (1024 * 1024)}
    elif spec.size == 0:
        code = "EMPTY_FILE"
    if code is None:
        return None
    return PipelineError(kind=ErrorKind.PERMANENT, code=code, stage="upload", message=message_for(code, **ctx))


def upload_plan(f) -> dict | None:
    if f.status not in FileStatus.ACTIVE_UPLOAD:
        return None
    if f.upload_id:
        urls = storage.presign_parts(f.s3_key, f.upload_id, list(range(1, f.part_count + 1)))
        return {"multipart": True, "uploadId": f.upload_id, "partSize": settings.part_size_bytes,
                "partCount": f.part_count, "parts": [{"partNumber": n, "url": u} for n, u in urls.items()],
                "expiresIn": settings.presign_expiry_s}
    return {"multipart": False, **storage.presign_put(f.s3_key, f.size_bytes, f.mime_type, f.sha256),
            "expiresIn": settings.presign_expiry_s}


def build_response(s: Session, b: Batch) -> dict:
    """Rebuilt from the database, so a retried POST with the same Idempotency-Key gets the same task ids."""
    files = {f.sha256: f for f in s.execute(select(repo.F).where(repo.F.c.batch_id == b.id)).all()}
    seen: dict[str, str] = {}
    tasks = []
    for spec in b.request_files:
        f = files[spec["sha256"]]
        task = {"taskId": f.task_id, "fileId": str(f.id), "fileName": spec["name"], "sha256": f.sha256,
                "sizeBytes": f.size_bytes, "status": f.status, "error": f.last_error}
        if f.task_id in seen:
            task["duplicateOf"] = seen[f.task_id]
        else:
            seen[f.task_id] = spec["name"]
            task["upload"] = upload_plan(f)
        tasks.append(task)
    public = batch_public_id(b.id)
    return {"batchId": public, "status": b.status, "notify": b.notify, "tasks": tasks,
            "links": {"batch": f"/batches/{public}", "events": f"/batches/{public}/events"}}


def _admit(s: Session, user: str) -> None:
    active = s.execute(
        select(func.count()).select_from(File).join(Batch, Batch.id == File.batch_id)
        .where(Batch.user_id == user, File.status.notin_(FileStatus.TERMINAL))
    ).scalar_one()
    if active >= settings.admission_max_active_tasks:
        raise HTTPException(429, f"too many active tasks ({active}); wait for some to finish",
                            headers={"Retry-After": "30"})
    allowed, wait = TokenBucket(get_redis(), "batches", settings.admission_batches_per_min).take(user)
    if not allowed:
        raise HTTPException(429, "too many batches created; slow down", headers={"Retry-After": str(int(wait) + 1)})


@router.post("/batches", status_code=201)
def create_batch(body: CreateBatch, response: Response, user: str = Depends(current_user),
                 idempotency_key: str | None = Header(None, alias="Idempotency-Key")):
    if not idempotency_key:
        raise HTTPException(400, "Idempotency-Key header is required")
    with session_scope() as s:
        existing = s.execute(select(Batch).where(Batch.tenant_id == user, Batch.idempotency_key == idempotency_key)
                             ).scalar_one_or_none()
        if existing is not None:
            response.status_code = 200
            return build_response(s, existing)

    if len(body.files) > settings.max_files_per_batch:
        raise HTTPException(422, f"at most {settings.max_files_per_batch} files per batch")
    rejections = {i: _rejection(spec) for i, spec in enumerate(body.files)}
    if all(rejections.values()):
        return JSONResponse(status_code=422, content={
            "detail": "no file in the batch can be accepted",
            "files": [{"fileName": spec.name, "code": rejections[i].code, "message": rejections[i].message}
                      for i, spec in enumerate(body.files)],
        })

    try:
        with session_scope() as s:
            _admit(s, user)
            batch_ulid = new_ulid()
            b = Batch(id=ulid_to_uuid(batch_ulid), tenant_id=user, user_id=user, idempotency_key=idempotency_key,
                      status=BatchStatus.OPEN, file_count=0, created_at=repo.now(),
                      request_files=[spec.model_dump() for spec in body.files],
                      notify=body.notify.model_dump(exclude_none=True) if body.notify else None)
            s.add(b)
            s.flush()  # a concurrent duplicate request fails here, before any S3 call
            created = 0
            for i, spec in enumerate(body.files):
                if any(spec.sha256 == other.sha256 for other in body.files[:i]):
                    continue  # same content twice in one batch: one task, stored and extracted once
                _create_file(s, b, spec, rejections[i])
                created += 1
            b.file_count = created
            s.flush()
            repo.recompute_batch(s, b.id)
            return build_response(s, b)
    except IntegrityError:
        with session_scope() as s:
            existing = s.execute(select(Batch).where(Batch.tenant_id == user,
                                                     Batch.idempotency_key == idempotency_key)).scalar_one()
            response.status_code = 200
            return build_response(s, existing)


def _create_file(s: Session, b: Batch, spec: FileSpec, rejection: PipelineError | None) -> None:
    ulid = new_ulid()
    fid = ulid_to_uuid(ulid)
    file_type = detect_type(spec.name, spec.mimeType)
    key = storage.object_key(b.tenant_id, spec.sha256)
    mime = spec.mimeType or CONTENT_TYPE.get(file_type, "application/octet-stream")
    row = {"id": fid, "batch_id": b.id, "task_id": task_public_id(fid), "name": spec.name, "mime_type": mime,
           "file_type": file_type, "sha256": spec.sha256, "s3_key": key, "size_bytes": spec.size, "attempts": 0,
           "version": 0, "warnings": 0, "part_count": storage.part_count(spec.size), "created_at": repo.now(),
           "updated_at": repo.now(), "progress": 0}
    if rejection is not None:
        row.update(status=FileStatus.REJECTED, last_error=rejection.to_json(), progress=100)
        message = rejection.message
    else:
        existing = storage.head(key)
        if existing is not None and existing["ContentLength"] == spec.size:
            row.update(status=FileStatus.UPLOADED, progress=40)  # already in S3: dedupe, skip the upload
            message = "Already uploaded (same content); skipping upload"
        else:
            row["status"] = FileStatus.PENDING_UPLOAD
            if storage.is_multipart(spec.size):
                row["upload_id"] = storage.create_multipart(key, mime)
            else:
                row["part_count"] = 1
            message = "Task created; waiting for upload"
    s.execute(repo.F.insert().values(**row))
    f = repo.get_file(s, fid)
    repo.add_event(s, f, stage="upload", frm=None, to=f.status, message=message,
                   err=rejection)
    if f.status == FileStatus.UPLOADED:
        repo.enqueue_extract(s, f)


@router.get("/batches")
def list_batches(user: str = Depends(current_user), limit: int = Query(20, le=100)):
    with session_scope() as s:
        rows = s.execute(select(Batch).where(Batch.user_id == user).order_by(Batch.created_at.desc()).limit(limit)
                         ).scalars().all()
        out = []
        for b in rows:
            counts = repo.batch_counts(s, b.id)
            out.append({"batchId": batch_public_id(b.id), "status": b.status, "fileCount": sum(counts.values()),
                        "counts": counts, "createdAt": repo.iso(b.created_at), "completedAt": repo.iso(b.completed_at)})
        return {"batches": out}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: str, user: str = Depends(current_user), offset: int = Query(0, ge=0),
              limit: int = Query(100, ge=1, le=500)):
    with session_scope() as s:
        return batch_view(s, owned_batch(s, parse_batch_id(batch_id), user), offset, limit)


@router.put("/batches/{batch_id}/notify")
def set_notify(batch_id: str, body: NotifySpec, user: str = Depends(current_user)):
    with session_scope() as s:
        b = owned_batch(s, parse_batch_id(batch_id), user)
        b.notify = body.model_dump(exclude_none=True)
        if b.completed_at is not None:  # already finished: send now on the newly requested channels
            repo._queue_notifications(s, b, suffix="")
        return {"batchId": batch_id, "notify": b.notify}


@router.post("/batches/{batch_id}/retry-failed")
def retry_failed(batch_id: str, user: str = Depends(current_user)):
    with session_scope() as s:
        b = owned_batch(s, parse_batch_id(batch_id), user)
        ids = s.execute(select(File.id).where(
            File.batch_id == b.id, File.status.in_((FileStatus.EXTRACT_FAILED, FileStatus.DELIVERY_FAILED))
        )).scalars().all()
    replayed = []
    for fid in ids:
        with session_scope() as s:
            if repo.replay_file(s, fid):
                replayed.append(str(fid))
    return {"batchId": batch_id, "replayed": len(replayed), "fileIds": replayed}
