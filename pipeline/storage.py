"""S3 access. boto3 works against MinIO unchanged; switching to real S3 is configuration only."""

import base64
import math
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from pipeline.config import settings

_clients: dict[tuple[str, int], object] = {}


def _client(endpoint: str | None):
    endpoint = endpoint or None  # "" from the environment means "real AWS"
    key = (endpoint or "aws", os.getpid())
    if key not in _clients:
        _clients[key] = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 3, "mode": "standard"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
    return _clients[key]


def s3():
    """Client for server-side calls (inside the compose network)."""
    return _client(settings.s3_endpoint_url)


def s3_public():
    """Client used only to sign URLs the browser will call."""
    return _client(settings.s3_public_endpoint_url or settings.s3_endpoint_url)


def reset_clients() -> None:
    _clients.clear()


def object_key(tenant_id: str, sha256: str) -> str:
    """Content-addressed: the same bytes always land on the same key, which dedupes uploads."""
    return f"uploads/{tenant_id}/{sha256}"


def extracted_key(sha256: str) -> str:
    return f"extracted/{sha256}/v{settings.parser_version}.json"


def sha256_b64(hex_digest: str) -> str:
    return base64.b64encode(bytes.fromhex(hex_digest)).decode()


def head(key: str) -> dict | None:
    try:
        return s3().head_object(Bucket=settings.s3_bucket, Key=key, ChecksumMode="ENABLED")
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def part_count(size: int) -> int:
    return max(1, math.ceil(size / settings.part_size_bytes))


def is_multipart(size: int) -> bool:
    return size >= settings.single_put_max_bytes


def presign_put(key: str, size: int, content_type: str, sha256: str) -> dict:
    """Single PUT that pins Content-Type, Content-Length and x-amz-checksum-sha256."""
    params = {"Bucket": settings.s3_bucket, "Key": key, "ContentType": content_type, "ContentLength": size}
    headers = {"Content-Type": content_type}
    if settings.s3_pin_checksum:
        params["ChecksumSHA256"] = sha256_b64(sha256)
        headers["x-amz-checksum-sha256"] = params["ChecksumSHA256"]
    url = s3_public().generate_presigned_url("put_object", Params=params, ExpiresIn=settings.presign_expiry_s)
    return {"url": url, "headers": headers}


def create_multipart(key: str, content_type: str) -> str:
    return s3().create_multipart_upload(Bucket=settings.s3_bucket, Key=key, ContentType=content_type)["UploadId"]


def presign_parts(key: str, upload_id: str, parts: list[int]) -> dict[int, str]:
    client = s3_public()
    return {
        n: client.generate_presigned_url(
            "upload_part",
            Params={"Bucket": settings.s3_bucket, "Key": key, "UploadId": upload_id, "PartNumber": n},
            ExpiresIn=settings.presign_expiry_s,
        )
        for n in parts
    }


def list_parts(key: str, upload_id: str) -> list[dict]:
    out, marker = [], 0
    while True:
        resp = s3().list_parts(Bucket=settings.s3_bucket, Key=key, UploadId=upload_id, PartNumberMarker=marker)
        out += [{"partNumber": p["PartNumber"], "etag": p["ETag"], "size": p["Size"]} for p in resp.get("Parts", [])]
        if not resp.get("IsTruncated"):
            return out
        marker = resp["NextPartNumberMarker"]


def complete_multipart(key: str, upload_id: str, parts: list[dict]) -> None:
    s3().complete_multipart_upload(
        Bucket=settings.s3_bucket, Key=key, UploadId=upload_id,
        MultipartUpload={"Parts": [{"PartNumber": p["partNumber"], "ETag": p["etag"]} for p in
                                   sorted(parts, key=lambda p: p["partNumber"])]},
    )


def abort_multipart(key: str, upload_id: str) -> None:
    try:
        s3().abort_multipart_upload(Bucket=settings.s3_bucket, Key=key, UploadId=upload_id)
    except ClientError:
        pass  # already completed or aborted


def ensure_bucket() -> None:
    try:
        s3().head_bucket(Bucket=settings.s3_bucket)
    except ClientError:
        s3().create_bucket(Bucket=settings.s3_bucket)
