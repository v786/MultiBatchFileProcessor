"""All tunables in one place. Every value can be overridden by an environment variable of the same name."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Infrastructure
    database_url: str = "postgresql+psycopg2://pipeline:pipeline@localhost:5432/pipeline"
    redis_url: str = "redis://localhost:6379/0"

    # S3 / MinIO. The API talks to `s3_endpoint_url`; URLs handed to the browser are signed for the public one.
    s3_endpoint_url: str | None = "http://localhost:9000"
    s3_public_endpoint_url: str | None = "http://localhost:9000"
    s3_region: str = "us-east-1"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket: str = "uploads"
    s3_pin_checksum: bool = True  # presigned PUT pins x-amz-checksum-sha256
    s3_events_token: str = "dev-s3-events-token"

    # Upload rules
    max_file_bytes: int = 100 * 1024 * 1024
    max_files_per_batch: int = 50
    single_put_max_bytes: int = 16 * 1024 * 1024
    part_size_bytes: int = 8 * 1024 * 1024
    presign_expiry_s: int = 900
    stuck_upload_after_s: int = 24 * 3600

    # Vendor
    vendor_url: str = "http://localhost:4000"
    vendor_name: str = "vendor"
    vendor_max_concurrent: int = 20
    vendor_slots: int = 20
    vendor_timeout_s: float = 30.0
    vendor_slot_lease_s: int = 45
    vendor_per_user_slots: int = 10
    vendor_supports_idempotency: bool = True
    vendor_supports_lookup: bool = True
    payload_version: int = 1
    throttle_cut_after: int = 3  # 429s within 60 s before effective slots are cut by 25%
    throttle_regrow_s: int = 30  # one slot regained every N seconds

    # Circuit breaker
    breaker_window_s: int = 60
    breaker_min_calls: int = 20
    breaker_failure_ratio: float = 0.5
    breaker_consecutive_failures: int = 10
    breaker_cooldown_s: int = 30
    breaker_cooldown_max_s: int = 300
    breaker_probes: int = 3

    # Retries
    extract_base_s: float = 2
    extract_cap_s: float = 120
    extract_max_attempts: int = 5
    extract_soft_limit_s: int = 60
    deliver_base_s: float = 5
    deliver_cap_s: float = 600
    deliver_max_attempts: int = 8
    notify_max_attempts: int = 6

    # Leases and housekeeping
    lease_s: int = 120
    lease_renew_s: int = 30
    backlog_pause: int = 5000
    backlog_resume: int = 2500

    # Admission control (per user)
    admission_batches_per_min: int = 10
    admission_max_active_tasks: int = 1000

    # Notifications
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    mail_from: str = "pipeline@localhost"
    mail_domain: str = "example.test"
    webhook_secret: str = "dev-webhook-secret"

    # API
    cors_origins: list[str] = ["http://localhost:5173"]
    parser_version: int = 1
    log_level: str = "INFO"


settings = Settings()
