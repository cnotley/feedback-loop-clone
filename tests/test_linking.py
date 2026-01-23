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
    assert result["link_mode"] == "trace_id"


def test_link_trace_id_not_found(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    monkeypatch.setattr("app.feedback_api.linking.execute_statement", lambda *args, **kwargs: [])
    payload = _payload(trace_id="trace-1")
    result = link_run(payload)
    assert result["link_mode"] == "trace_id_not_found"


def test_link_best_match_ambiguous(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    def fake_execute(statement, warehouse_id):
        if "SELECT trace_id" in statement:
            return [["trace-1"], ["trace-2"]]
        return []

    monkeypatch.setattr("app.feedback_api.linking.execute_statement", fake_execute)
    payload = _payload(tracking_id="")
    result = link_run(payload)
    assert result["link_mode"] == "unlinked"
    assert result["link_reason"] == "ambiguous_candidates"


def test_link_best_match(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    monkeypatch.setattr(
        "app.feedback_api.linking.execute_statement",
        lambda *args, **kwargs: [["trace-9"]],
    )
    payload = _payload(tracking_id="track-1")
    result = link_run(payload)
    assert result["link_mode"] == "best_match"
    assert result["link_target_trace_id"] == "trace-9"


def test_link_missing_user_id(monkeypatch):
    payload = _payload(user_id=None)
    result = link_run(payload)
    assert result["link_mode"] == "unlinked"
    assert result["link_reason"] == "missing_user_id"


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
    assert result["link_mode"] == "trace_id_unverified"


def test_best_match_unresolved_on_exception(monkeypatch):
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("app.feedback_api.linking.execute_statement", boom)
    payload = _payload()
    result = link_run(payload)
    assert result["link_mode"] == "best_match_unresolved"