"""One catalogue of error codes and user-facing wording, shared by the API, UI and notifications."""

CATALOG: dict[str, str] = {
    # upload
    "UNSUPPORTED_TYPE": "Rejected: unsupported file type (allowed: PDF, CSV, XLSX, JSON)",
    "FILE_TOO_LARGE": "Rejected: file is larger than {limit_mb} MB",
    "EMPTY_FILE": "Rejected: file is empty",
    "SIZE_MISMATCH": "Upload incomplete: stored size does not match the declared size",
    "UPLOAD_EXPIRED": "Upload was not finished within 24 hours",
    # extract
    "CHECKSUM_MISMATCH": "Extraction failed: file content does not match its checksum",
    "CORRUPT_FILE": "Extraction failed: file is corrupt ({reason})",
    "EXTRACT_TIMEOUT": "Extraction failed: file took too long to parse",
    "OBJECT_MISSING": "Extraction failed: uploaded file not found in storage",
    "S3_UNAVAILABLE": "Storage temporarily unavailable",
    # deliver
    "VENDOR_TIMEOUT": "The vendor did not reply in time",
    "VENDOR_UNAVAILABLE": "Vendor unavailable (HTTP {status})",
    "VENDOR_REJECTED": "The vendor rejected the record: {reason}",
    "VENDOR_THROTTLED": "Vendor asked us to slow down",
    "CIRCUIT_OPEN": "Vendor unavailable, paused",
    "DELIVERY_UNKNOWN": "Delivery outcome unknown after a timeout; needs manual review",
    "RETRIES_EXHAUSTED": "Gave up after {attempts} attempts: {last}",
    # anything else
    "UNEXPECTED": "Unexpected error",
}


def message_for(code: str, **ctx) -> str:
    template = CATALOG.get(code, CATALOG["UNEXPECTED"])
    try:
        return template.format(**ctx)
    except (KeyError, IndexError):
        return template.split(" (")[0].split(": {")[0]
