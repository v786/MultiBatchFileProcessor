"""Postgres is the source of truth for every state. Types degrade to SQLite for fast in-process tests."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JsonType = JSON().with_variant(JSONB(), "postgresql")
BigId = BigInteger().with_variant(Integer(), "sqlite")


class Base(DeclarativeBase):
    pass


class BatchStatus:
    OPEN = "OPEN"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    FINAL = {COMPLETED, PARTIAL, FAILED}


class FileStatus:
    PENDING_UPLOAD = "PENDING_UPLOAD"
    UPLOADING = "UPLOADING"
    UPLOADED = "UPLOADED"
    EXTRACTING = "EXTRACTING"
    EXTRACTED = "EXTRACTED"
    DELIVERING = "DELIVERING"
    DONE = "DONE"
    REJECTED = "REJECTED"
    UPLOAD_EXPIRED = "UPLOAD_EXPIRED"
    EXTRACT_FAILED = "EXTRACT_FAILED"
    DELIVERY_FAILED = "DELIVERY_FAILED"

    FAILED = {REJECTED, UPLOAD_EXPIRED, EXTRACT_FAILED, DELIVERY_FAILED}
    TERMINAL = FAILED | {DONE}
    ACTIVE_UPLOAD = (PENDING_UPLOAD, UPLOADING)


class DeliveryStatus:
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    DELIVERED = "DELIVERED"
    RETRY_WAIT = "RETRY_WAIT"
    FAILED = "FAILED"


class Batch(Base):
    __tablename__ = "batches"
    __table_args__ = (UniqueConstraint("tenant_id", "idempotency_key"),)

    id: Mapped[UUID] = mapped_column(primary_key=True)
    tenant_id: Mapped[str] = mapped_column(Text)
    user_id: Mapped[str] = mapped_column(Text, index=True)
    idempotency_key: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text)  # OPEN | PROCESSING | COMPLETED | PARTIAL | FAILED
    file_count: Mapped[int] = mapped_column(Integer)
    request_files: Mapped[list] = mapped_column(JsonType)  # as submitted, to rebuild the response on replay
    notify: Mapped[dict | None] = mapped_column(JsonType, nullable=True)  # channels requested
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class File(Base):
    __tablename__ = "files"
    __table_args__ = (UniqueConstraint("batch_id", "sha256"), Index("ix_files_s3_key_status", "s3_key", "status"))

    id: Mapped[UUID] = mapped_column(primary_key=True)
    batch_id: Mapped[UUID] = mapped_column(ForeignKey("batches.id"), index=True)
    task_id: Mapped[str] = mapped_column(Text, unique=True)  # tsk_<ULID>
    name: Mapped[str] = mapped_column(Text)
    mime_type: Mapped[str] = mapped_column(Text)
    file_type: Mapped[str | None] = mapped_column(String(16), nullable=True)  # pdf | csv | xlsx | json
    sha256: Mapped[str] = mapped_column(Text)
    s3_key: Mapped[str] = mapped_column(Text)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(Text, index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[dict | None] = mapped_column(JsonType, nullable=True)
    upload_id: Mapped[str | None] = mapped_column(Text, nullable=True)  # S3 multipart upload id
    part_count: Mapped[int] = mapped_column(Integer, default=1)
    extracted_key: Mapped[str | None] = mapped_column(Text, nullable=True)  # S3 key of extracted JSON
    warnings: Mapped[int] = mapped_column(Integer, default=0)
    progress: Mapped[int] = mapped_column(SmallInteger, default=0)  # 0-100
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class VendorDelivery(Base):
    """One row per file today (single vendor); a second vendor later is a new row per file, not a schema change."""

    __tablename__ = "vendor_deliveries"

    file_id: Mapped[UUID] = mapped_column(ForeignKey("files.id"), primary_key=True)
    vendor: Mapped[str] = mapped_column(Text, primary_key=True, default="vendor")
    user_id: Mapped[str] = mapped_column(Text)  # owner, for the fair queue
    idempotency_key: Mapped[str] = mapped_column(Text)  # sha256(file.sha256 + vendor + payload_version)
    status: Mapped[str] = mapped_column(Text, index=True)  # PENDING | IN_FLIGHT | DELIVERED | RETRY_WAIT | FAILED
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    vendor_ref: Mapped[str | None] = mapped_column(Text, nullable=True)  # id returned by vendor
    last_error: Mapped[dict | None] = mapped_column(JsonType, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Outbox(Base):
    __tablename__ = "outbox"
    __table_args__ = (Index("ix_outbox_unsent", "sent_at", "id"),)

    id: Mapped[int] = mapped_column(BigId, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(Text)
    dedupe_key: Mapped[str] = mapped_column(Text, unique=True)
    payload: Mapped[dict] = mapped_column(JsonType)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TaskEvent(Base):
    """Append-only timeline. `id` doubles as the SSE event id."""

    __tablename__ = "task_events"
    __table_args__ = (Index("ix_task_events_task", "task_id", "id"), Index("ix_task_events_batch", "batch_id", "id"))

    id: Mapped[int] = mapped_column(BigId, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(Text)
    batch_id: Mapped[UUID] = mapped_column()
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    stage: Mapped[str] = mapped_column(Text)  # upload | extract | deliver | notify
    vendor: Mapped[str | None] = mapped_column(Text, nullable=True)
    from_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    to_status: Mapped[str] = mapped_column(Text)
    progress: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    attempt: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_kind: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)  # safe to show the user
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class Notification(Base):
    __tablename__ = "notifications"

    batch_id: Mapped[UUID] = mapped_column(primary_key=True)
    channel: Mapped[str] = mapped_column(Text, primary_key=True)  # email | webhook | in_app (+ ":first_failure")
    status: Mapped[str] = mapped_column(Text)  # PENDING | SENDING | SENT | FAILED
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DeadLetter(Base):
    """The single place to look for parked work. Celery's own failed results are not used."""

    __tablename__ = "dlq"
    __table_args__ = (UniqueConstraint("scope", "ref_id"),)

    id: Mapped[int] = mapped_column(BigId, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(Text)  # extract | delivery
    ref_id: Mapped[UUID] = mapped_column()  # file id
    task_id: Mapped[str] = mapped_column(Text)
    batch_id: Mapped[UUID] = mapped_column()
    kind: Mapped[str] = mapped_column(Text)
    code: Mapped[str] = mapped_column(Text)
    message: Mapped[str] = mapped_column(Text)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    payload_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    replays: Mapped[int] = mapped_column(Integer, default=0)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
