from datetime import datetime, timezone

import pytest

from app.feedback_api.models import FeedbackPayload
from app.feedback_api.storage import (
    DuplicatePayloadError,
    payload_hash,
    payload_hash_exists,
    tracking_id_recent_exists,
    write_feedback,
)


def _payload(**overrides):
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


def test_payload_hash_deterministic():
    payload = _payload()
    assert payload_hash(payload) == payload_hash(payload)


def test_payload_hash_exists(monkeypatch):
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    monkeypatch.setattr("app.feedback_api.storage.execute_statement", lambda *args, **kwargs: [[1]])
    assert payload_hash_exists("hash") is True


def test_tracking_id_recent_exists(monkeypatch):
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    monkeypatch.setattr("app.feedback_api.storage.execute_statement", lambda *args, **kwargs: [[1]])
    assert tracking_id_recent_exists("track-1", 3600, pims="ezyvet", user_id="user-1") is True


def test_write_feedback_duplicate(monkeypatch):
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    monkeypatch.setattr("app.feedback_api.storage.payload_hash_exists", lambda *args, **kwargs: True)
    payload = _payload()
    with pytest.raises(DuplicatePayloadError):
        write_feedback(payload, {"link_mode": "trace_id"})


def test_write_feedback_executes(monkeypatch):
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    monkeypatch.setattr("app.feedback_api.storage.payload_hash_exists", lambda *args, **kwargs: False)
    called = {}

    def fake_execute(statement, warehouse_id):
        called["statement"] = statement
        return []

    monkeypatch.setattr("app.feedback_api.storage.execute_statement", fake_execute)
    payload = _payload()
    feedback_id = write_feedback(payload, {"link_mode": "trace_id"})
    assert feedback_id.startswith("fb_")
    assert "INSERT INTO" in called["statement"]
