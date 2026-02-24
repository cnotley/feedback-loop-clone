"""Tests for the feedback API endpoints and policies"""

# pylint: disable=line-too-long,wrong-import-position,import-outside-toplevel,reimported
# pylint: disable=redefined-outer-name,unused-argument,unused-import,import-error,no-name-in-module

from datetime import datetime, timezone

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient

from app.feedback_api.api import create_app
from app.feedback_api.storage import DuplicatePayloadError


def _payload(**overrides):
    """Build a valid feedback payload for tests"""
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_app": "app",
        "service_name": "service",
        "consumer_id": "consumer",
        "request_id": "req-1",
    }
    base.update(overrides)
    return base


@pytest.fixture()
def client(monkeypatch):
    """Provide a test client with required environment variables set."""
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    app = create_app()
    return TestClient(app)


def test_health_endpoint(client):
    """Health endpoint returns ok"""
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_metrics_endpoint(client):
    """Metrics endpoint returns counters"""
    resp = client.get("/metrics")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["service"] == "feedback-api"
    assert "metrics" in payload
    assert "counters" in payload
    assert "submissions" in payload["metrics"]
    assert "requests" in payload["metrics"]


def test_metrics_increment_on_representative_paths(client, monkeypatch):
    """Metrics increment across accepted, rejected, and auth failure paths"""
    before = client.get("/metrics").json()

    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id_match"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", lambda payload, link_info, payload_hash_value=None: "fb_1")
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")

    assert client.post("/feedback/submit", json=_payload()).status_code == 200
    assert client.post("/feedback/submit", json=_payload(tracking_id=" ")).status_code == 400
    assert (
        client.post(
            "/feedback/submit",
            json=_payload(tracking_id="track-auth"),
            headers={"Authorization": "Basic abc"},
        ).status_code
        == 200
    )

    after = client.get("/metrics").json()
    assert after["metrics"]["submissions"]["accepted"] >= before["metrics"]["submissions"]["accepted"] + 2
    assert (
        after["metrics"]["submissions"]["rejected_by_reason"]["validation_schema_violations"]
        >= before["metrics"]["submissions"]["rejected_by_reason"]["validation_schema_violations"] + 1
    )
    assert after["metrics"]["auth"]["failures"] >= before["metrics"]["auth"]["failures"] + 1
    assert after["metrics"]["requests"]["total"] >= before["metrics"]["requests"]["total"] + 3
    assert after["metrics"]["requests"]["latency_ms"]["count"] >= before["metrics"]["requests"]["latency_ms"]["count"] + 3


def test_submit_feedback_happy_path(client, monkeypatch):
    """Submitting feedback returns accepted response"""
    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", lambda payload, link_info, payload_hash_value=None: "fb_1")
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")
    resp = client.post("/feedback/submit", json=_payload())
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "accepted"
    assert body["feedback_id"] == "fb_1"
    assert "X-Correlation-ID" in resp.headers


def test_submit_feedback_validation_error(client):
    """Validation errors yield 400 response"""
    resp = client.post("/feedback/submit", json=_payload(tracking_id=" "))
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "validation_failed"


def test_submit_feedback_dedup_rejected(client, monkeypatch):
    """Duplicate payload yields 409 response"""
    def _raise_dup(*args, **kwargs):
        """Raise a duplicate payload error for testing."""
        raise DuplicatePayloadError("payload_hash already ingested")

    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", _raise_dup)
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")
    resp = client.post("/feedback/submit", json=_payload())
    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "duplicate_payload"


def test_submit_feedback_rate_limit_ip(client, monkeypatch):
    """IP rate limit blocks after threshold"""
    monkeypatch.setenv("RATE_LIMIT_PER_IP", "1")
    app = create_app()
    local_client = TestClient(app)
    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", lambda payload, link_info, payload_hash_value=None: "fb_1")
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")
    assert local_client.post("/feedback/submit", json=_payload()).status_code == 200
    resp = local_client.post("/feedback/submit", json=_payload(tracking_id="track-2"))
    assert resp.status_code == 429


def test_submit_feedback_rate_limit_token(client, monkeypatch):
    """Token rate limit blocks after threshold"""
    monkeypatch.setenv("RATE_LIMIT_PER_TOKEN", "1")
    app = create_app()
    local_client = TestClient(app)
    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", lambda payload, link_info, payload_hash_value=None: "fb_1")
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")
    headers = {"Authorization": "Bearer token-1"}
    assert local_client.post("/feedback/submit", json=_payload(), headers=headers).status_code == 200
    resp = local_client.post("/feedback/submit", json=_payload(tracking_id="track-2"), headers=headers)
    assert resp.status_code == 429


def test_trace_id_policy_warn_allows(client, monkeypatch):
    """Warn policy allows invalid trace_id"""
    monkeypatch.setenv("TRACE_ID_POLICY", "warn")
    monkeypatch.setenv("TRACE_ID_REGEX", "^trace-[0-9]+$")
    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", lambda payload, link_info, payload_hash_value=None: "fb_1")
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")
    resp = client.post("/feedback/submit", json=_payload(trace_id="bad"))
    assert resp.status_code == 200


def test_trace_id_policy_max_age_strict_rejects(client, monkeypatch):
    """Max age strict policy rejects old traces"""
    monkeypatch.setenv("TRACE_ID_POLICY", "strict")
    monkeypatch.setenv("TRACE_ID_MAX_AGE_SECONDS", "1")
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")

    from datetime import datetime, timedelta, timezone

    old = datetime.now(timezone.utc) - timedelta(seconds=10)
    monkeypatch.setattr("app.feedback_api.api.get_trace_timestamp", lambda *args, **kwargs: old)
    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", lambda payload, link_info, payload_hash_value=None: "fb_1")
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")
    resp = client.post("/feedback/submit", json=_payload(trace_id="trace-1"))
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "trace_id_policy_failed"


def test_policy_check_failed_returns_500(client, monkeypatch):
    """Policy lookup failure returns 500"""
    monkeypatch.setenv("TRACE_ID_POLICY", "warn")
    monkeypatch.setenv("TRACE_ID_MAX_AGE_SECONDS", "1")
    monkeypatch.setenv("TRACE_TABLE", "trace_table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    monkeypatch.setattr("app.feedback_api.api.get_trace_timestamp", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    resp = client.post("/feedback/submit", json=_payload(trace_id="trace-1"))
    assert resp.status_code == 500


def test_startup_missing_envs(monkeypatch):
    """Startup fails when required environment variables are missing"""
    monkeypatch.delenv("FEEDBACK_TABLE", raising=False)
    monkeypatch.delenv("DATABRICKS_WAREHOUSE_ID", raising=False)
    with pytest.raises(RuntimeError):
        with TestClient(create_app()) as client:
            client.get("/health")


def test_startup_rejects_invalid_feedback_table(monkeypatch):
    """Startup fails fast when FEEDBACK_TABLE is not a valid UC 3 part name"""
    monkeypatch.setenv("FEEDBACK_TABLE", "schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    with pytest.raises(RuntimeError) as excinfo:
        with TestClient(create_app()) as client:
            client.get("/health")
    assert "FEEDBACK_TABLE" in str(excinfo.value)


def test_app_module_import():
    """App module imports without error"""
    import app.app


def test_trace_id_policy_strict_rejects(client, monkeypatch):
    """Strict trace policy rejects invalid trace_id"""
    monkeypatch.setenv("TRACE_ID_POLICY", "strict")
    monkeypatch.setenv("TRACE_ID_REGEX", "^trace-[0-9]+$")
    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", lambda payload, link_info, payload_hash_value=None: "fb_1")
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")
    resp = client.post("/feedback/submit", json=_payload(trace_id="bad"))
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "trace_id_policy_failed"


def test_tracking_id_policy_strict_rejects(client, monkeypatch):
    """Strict tracking policy rejects duplicates"""
    monkeypatch.setenv("TRACKING_ID_POLICY", "strict")
    monkeypatch.setenv("TRACKING_ID_UNIQUENESS_SECONDS", "3600")
    monkeypatch.setattr("app.feedback_api.api.tracking_id_recent_exists", lambda *args, **kwargs: True)
    monkeypatch.setattr("app.feedback_api.api.link_run", lambda payload: {"link_mode": "trace_id"})
    monkeypatch.setattr("app.feedback_api.api.write_feedback", lambda payload, link_info, payload_hash_value=None: "fb_1")
    monkeypatch.setattr("app.feedback_api.api.payload_hash", lambda payload: "hash1")
    resp = client.post("/feedback/submit", json=_payload())
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "tracking_id_policy_failed"