"""Tests for observability utilities"""

from app.feedback_api.observability import Metrics, attach_correlation_id


class DummyResponse:  # pylint: disable=too-few-public-methods
    """Minimal response stub for header assertions"""
    def __init__(self):
        """Simple response stub with headers dict"""
        self.headers = {}


def test_metrics_increment():
    """Metrics increments counters as expected"""
    metrics = Metrics()
    metrics.inc("a")
    metrics.inc("a")
    assert metrics.counters["a"] == 2


def test_metrics_snapshot_contains_groups():
    """Snapshot exposes JSON friendly grouped metrics"""
    metrics = Metrics()
    metrics.inc("accepted")
    metrics.inc("validation_failed")
    metrics.inc("link_mode_trace_id_match")
    metrics.observe_ms("request_latency_ms", 12)
    metrics.observe_ms("request_latency_ms", 20)
    snapshot = metrics.snapshot()
    assert snapshot["service"] == "feedback-api"
    assert "metrics" in snapshot
    assert snapshot["metrics"]["submissions"]["accepted"] == 1
    assert snapshot["metrics"]["submissions"]["rejected_by_reason"]["validation_schema_violations"] == 1
    assert snapshot["metrics"]["link_modes"]["trace_id_match"] == 1
    latency_metrics = snapshot["metrics"]["requests"]["latency_ms"]
    assert latency_metrics["count"] == 2
    assert latency_metrics["min_ms"] == 12
    assert latency_metrics["max_ms"] == 20
    assert latency_metrics["sum_ms"] == 32
    assert latency_metrics["avg_ms"] == 16


def test_attach_correlation_id():
    """Correlation header is attached to response"""
    response = DummyResponse()
    attach_correlation_id(response, "cid-1")
    assert response.headers["X-Correlation-ID"] == "cid-1"