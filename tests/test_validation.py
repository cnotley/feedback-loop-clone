"""Tests for payload validation rules."""

from datetime import datetime, timezone

from app.feedback_api.models import FeedbackPayload
from app.feedback_api.validation import validate_payload, MAX_COMMENT_LENGTH, MAX_ID_LENGTH


def _payload(**overrides):
    """Build a feedback payload for validation tests."""
    base = {
        "schema_version": "v1",
        "tracking_id": "track-1",
        "trace_id": "trace-1",
        "site_id": "site-1",
        "user_id": "user-1",
        "pims_id": "pims-1",
        "pims": "ezyvet",
        "feedback_boolean": True,
        "feedback_score_1": 4,
        "feedback_score_2": 5,
        "feedback_comment_1": "ok",
        "feedback_comment_2": "ok",
        "timestamp": datetime.now(timezone.utc),
        "source_app": "app",
        "service_name": "service",
        "consumer_id": "consumer",
        "request_id": "req-1",
    }
    base.update(overrides)
    return FeedbackPayload(**base)


def test_validate_payload_happy_path():
    """Valid payload returns no errors."""
    payload = _payload()
    assert not validate_payload(payload)


def test_validate_payload_requires_tracking_id_and_pims():
    """Missing required fields return errors."""
    payload = _payload(tracking_id=" ", pims=" ")
    errors = validate_payload(payload)
    assert "tracking_id is required" in errors
    assert "pims is required" in errors


def test_validate_payload_schema_version():
    """Invalid schema version returns error."""
    payload = _payload(schema_version="v2")
    errors = validate_payload(payload)
    assert any("schema_version" in err for err in errors)


def test_validate_payload_timestamp_tz_required():
    """Timezone-less timestamps are rejected."""
    payload = _payload(timestamp=datetime.now())
    errors = validate_payload(payload)
    assert "timestamp must include timezone (ISO-8601 UTC)" in errors


def test_validate_payload_timestamp_utc_only():
    """UTC timestamp passes validation."""
    payload = _payload(timestamp=datetime.now(timezone.utc).astimezone(timezone.utc))
    assert not validate_payload(payload)


def test_validate_payload_score_range():
    """Score bounds are enforced."""
    payload = _payload(feedback_score_1=0, feedback_score_2=6)
    errors = validate_payload(payload)
    assert "feedback_score_1 must be between 1 and 5" in errors
    assert "feedback_score_2 must be between 1 and 5" in errors


def test_validate_payload_comment_length():
    """Comment length is enforced."""
    payload = _payload(feedback_comment_1="a" * (MAX_COMMENT_LENGTH + 1))
    errors = validate_payload(payload)
    assert "feedback_comment_1 exceeds max length" in errors


def test_validate_payload_id_length():
    """ID length is enforced."""
    payload = _payload(tracking_id="x" * (MAX_ID_LENGTH + 1))
    errors = validate_payload(payload)
    assert any(f"tracking_id exceeds max length {MAX_ID_LENGTH}" == err for err in errors)
