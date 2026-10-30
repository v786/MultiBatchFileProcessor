from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from pipeline import storage
from pipeline.api.deps import current_user, parse_file_id
from pipeline.db import repo
from pipeline.db.models import FileStatus
from pipeline.db.session import session_scope
from pipeline.errors import ErrorKind, PipelineError
from pipeline.errors.catalog import message_for
from pipeline.views import file_owner, task_summary

router = APIRouter(tags=["files"])


class Part(BaseModel):
    partNumber: int = Field(ge=1, le=10000)
    etag: str


class CompleteBody(BaseModel):
    parts: list[Part] = []


class RefreshBody(BaseModel):
    parts: list[int] | None = None  # part numbers to re-sign; all when omitted


class ProgressBody(BaseModel):
    partsDone: int = Field(ge=0)
    totalParts: int = Field(ge=1)


def _owned_file(s, file_id: str, user: str):
    f = repo.get_file(s, parse_file_id(file_id))
    if f is None or file_owner(s, f) != user:
        raise HTTPException(404, "file not found")
    return f


@router.post("/files/{file_id}/complete")
def complete_file(file_id: str, body: CompleteBody, user: str = Depends(current_user)):
    """Client says the last byte is in S3. Idempotent: the S3 event may already have moved the file on."""
    with session_scope() as s:
        f = _owned_file(s, file_id, user)
    if f.status not in FileStatus.ACTIVE_UPLOAD:
        with session_scope() as s:
            return task_summary(repo.get_file(s, f.id), repo.get_delivery(s, f.id))

    if f.upload_id:
        if not body.parts:
            raise HTTPException(422, "multipart upload needs the part ETags")
        try:
            storage.complete_multipart(f.s3_key, f.upload_id, [p.model_dump() for p in body.parts])
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code")
            if code in ("InvalidPart", "InvalidPartOrder", "EntityTooSmall"):
                raise HTTPException(409, f"multipart completion failed: {code}; re-upload missing parts")
            if code != "NoSuchUpload":  # NoSuchUpload: a retried complete after success
                raise
    meta = storage.head(f.s3_key)
    if meta is None:
        raise HTTPException(409, "object not found in storage; upload it first")
    with session_scope() as s:
        if meta["ContentLength"] != f.size_bytes:
            err = PipelineError(kind=ErrorKind.PERMANENT, code="SIZE_MISMATCH", stage="upload",
                                message=message_for("SIZE_MISMATCH"))
            repo.fail_file(s, f.id, FileStatus.ACTIVE_UPLOAD, FileStatus.REJECTED, err)
        else:
            repo.mark_uploaded(s, f.id)
    with session_scope() as s:
        repo.recompute_batch(s, f.batch_id)
        return task_summary(repo.get_file(s, f.id), repo.get_delivery(s, f.id))


@router.post("/files/{file_id}/refresh-urls")
def refresh_urls(file_id: str, body: RefreshBody | None = None, user: str = Depends(current_user)):
    """Presigned URLs expire after 15 minutes; a 403 from S3 lands here."""
    with session_scope() as s:
        f = _owned_file(s, file_id, user)
        if f.status not in FileStatus.ACTIVE_UPLOAD:
            raise HTTPException(409, f"file is {f.status}; nothing to upload")
        repo.mark_uploading(s, f.id)
    if f.upload_id:
        wanted = (body.parts if body and body.parts else None) or list(range(1, f.part_count + 1))
        urls = storage.presign_parts(f.s3_key, f.upload_id, wanted)
        return {"multipart": True, "uploadId": f.upload_id, "parts": [{"partNumber": n, "url": u} for n, u in urls.items()]}
    return {"multipart": False, **storage.presign_put(f.s3_key, f.size_bytes, f.mime_type, f.sha256)}


@router.get("/files/{file_id}/parts")
def list_parts(file_id: str, user: str = Depends(current_user)):
    """Resume after a reload: which parts are already in S3 (S3 ListParts)."""
    with session_scope() as s:
        f = _owned_file(s, file_id, user)
    if not f.upload_id or f.status not in FileStatus.ACTIVE_UPLOAD:
        return {"status": f.status, "parts": []}
    try:
        parts = storage.list_parts(f.s3_key, f.upload_id)
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") == "NoSuchUpload":
            return {"status": f.status, "parts": []}
        raise
    return {"status": f.status, "uploadId": f.upload_id, "partCount": f.part_count, "parts": parts}


@router.post("/files/{file_id}/progress", status_code=204)
def report_progress(file_id: str, body: ProgressBody, user: str = Depends(current_user)):
    """Optional: parts finished so far, so other viewers see upload progress (0-40%)."""
    with session_scope() as s:
        f = _owned_file(s, file_id, user)
        repo.mark_uploading(s, f.id, body.partsDone, body.totalParts)
