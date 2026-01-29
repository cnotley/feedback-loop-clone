from datetime import datetime, timezone

from app.feedback_api.linking import get_trace_timestamp, link_run
from app.feedback_api.models import FeedbackPayload


def _payload(**overrides):
    base = {
        "schema_version": "v1",
        "tracking_id": "track-1",
        "trace_id": None,
        "site_id": None,
        "user_id": "user-1",
        "pims_id": None,
        "pims": "ezyvet",
        "feedback_boolean": None,
        "feedback_score_1": None,
        "feedback_score_2": None,
        "feedback_comment_1": None,
        "feedback_comment_2": None,
        "timestamp": datetime.now(timezone.utc),
    }
    base.update(overrides)
    return FeedbackPayload(**base)


def test_link_trace_id_verified(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    def fake_execute(statement, warehouse_id):
        return [[1]]

    monkeypatch.setattr("app.feedback_api.linking.execute_statement", fake_execute)
    payload = _payload(trace_id="trace-1")
    result = link_run(payload)
    assert result["link_mode"] == "trace_id_match"


def test_link_trace_id_not_found_falls_back_to_no_match(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    monkeypatch.setattr("app.feedback_api.linking.execute_statement", lambda *args, **kwargs: [])
    payload = _payload(trace_id="trace-1")
    result = link_run(payload)
    assert result["link_mode"] == "no_match"


def test_get_trace_timestamp_datetime(monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setattr("app.feedback_api.linking.execute_statement", lambda *args, **kwargs: [[now]])
    ts = get_trace_timestamp("trace_table", "wh-1", "trace-1")
    assert ts == now


def test_trace_id_unverified_when_missing_env(monkeypatch):
    monkeypatch.delenv("TRACE_TABLE", raising=False)
    monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
    payload = _payload(trace_id="trace-1")
    result = link_run(payload)
    assert result["link_mode"] == "no_match"


def test_tracking_id_exact_match(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    def fake_execute(statement, warehouse_id):
        if "SELECT COUNT(1)" in statement:
            return [[1]]
        if "ORDER BY request_time DESC" in statement:
            return [["trace-9", datetime.now(timezone.utc)]]
        return []

    monkeypatch.setattr("app.feedback_api.linking.execute_statement", fake_execute)
    payload = _payload(tracking_id="track-1", trace_id=None)
    result = link_run(payload)
    assert result["link_mode"] == "tracking_id_exact_match"
    assert result["link_target_trace_id"] == "trace-9"


def test_tracking_id_recent_match(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    def fake_execute(statement, warehouse_id):
        if "SELECT COUNT(1)" in statement:
            return [[2]]
        if "ORDER BY request_time DESC" in statement:
            return [["trace-2", datetime.now(timezone.utc)]]
        return []

    monkeypatch.setattr("app.feedback_api.linking.execute_statement", fake_execute)
    payload = _payload(tracking_id="track-1", trace_id=None)
    result = link_run(payload)
    assert result["link_mode"] == "tracking_id_recent_match"
    assert result["link_target_trace_id"] == "trace-2"


def test_link_missing_tracking_id(monkeypatch):
    payload = _payload(tracking_id="", trace_id=None)
    result = link_run(payload)
    assert result["link_mode"] == "no_match"
    assert result["link_reason"] == "missing_tracking_id"


def test_tracking_id_lookup_failed_returns_no_match(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("app.feedback_api.linking.execute_statement", boom)
    payload = _payload(trace_id=None)
    result = link_run(payload)
    assert result["link_mode"] == "no_match"