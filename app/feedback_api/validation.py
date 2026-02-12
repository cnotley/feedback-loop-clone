"""Validation rules for incoming feedback payloads."""

from typing import List
from .models import FeedbackPayload


ALLOWED_SCHEMA_VERSIONS = {"v1"}
SCORE_MIN = 1
SCORE_MAX = 5
MAX_COMMENT_LENGTH = 2000
MAX_ID_LENGTH = 255
MAX_NAME_LENGTH = 255


def validate_payload(payload: FeedbackPayload) -> List[str]:
    """Return a list of human readable validation errors."""
    errors: List[str] = []

    if payload.schema_version not in ALLOWED_SCHEMA_VERSIONS:
        errors.append(
            f"schema_version must be one of {sorted(ALLOWED_SCHEMA_VERSIONS)}"
        )

    if not payload.tracking_id.strip():
        errors.append("tracking_id is required")

    if not payload.pims.strip():
        errors.append("pims is required")

    if payload.timestamp.tzinfo is None or payload.timestamp.utcoffset() is None:
        errors.append("timestamp must include timezone (ISO-8601 UTC)")
    elif payload.timestamp.utcoffset().total_seconds() != 0:
        errors.append("timestamp must be UTC (ISO-8601 with Z)")

    _check_length(errors, "tracking_id", payload.tracking_id, MAX_ID_LENGTH)
    _check_length(errors, "trace_id", payload.trace_id, MAX_ID_LENGTH)
    _check_length(errors, "site_id", payload.site_id, MAX_ID_LENGTH)
    _check_length(errors, "user_id", payload.user_id, MAX_ID_LENGTH)
    _check_length(errors, "pims_id", payload.pims_id, MAX_ID_LENGTH)
    _check_length(errors, "pims", payload.pims, MAX_NAME_LENGTH)
    _check_length(errors, "source_app", payload.source_app, MAX_NAME_LENGTH)
    _check_length(errors, "service_name", payload.service_name, MAX_NAME_LENGTH)
    _check_length(errors, "consumer_id", payload.consumer_id, MAX_NAME_LENGTH)
    _check_length(errors, "request_id", payload.request_id, MAX_ID_LENGTH)

    if payload.feedback_score_1 is not None:
        if payload.feedback_score_1 < SCORE_MIN or payload.feedback_score_1 > SCORE_MAX:
            errors.append("feedback_score_1 must be between 1 and 5")

    if payload.feedback_score_2 is not None:
        if payload.feedback_score_2 < SCORE_MIN or payload.feedback_score_2 > SCORE_MAX:
            errors.append("feedback_score_2 must be between 1 and 5")

    if payload.feedback_comment_1 and len(payload.feedback_comment_1) > MAX_COMMENT_LENGTH:
        errors.append("feedback_comment_1 exceeds max length")

    if payload.feedback_comment_2 and len(payload.feedback_comment_2) > MAX_COMMENT_LENGTH:
        errors.append("feedback_comment_2 exceeds max length")

    return errors


def _check_length(errors: List[str], field: str, value: str | None, max_len: int) -> None:
    """Append a length error if a string value exceeds the limit."""
    if value is None:
        return
    if len(value) > max_len:
        errors.append(f"{field} exceeds max length {max_len}")
