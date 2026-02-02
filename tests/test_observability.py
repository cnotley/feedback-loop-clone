"""Tests for observability utilities."""

from app.feedback_api.observability import Metrics, attach_correlation_id


class DummyResponse:  # pylint: disable=too-few-public-methods
    """Minimal response stub for header assertions."""
    def __init__(self):
        """Simple response stub with headers dict."""
        self.headers = {}


def test_metrics_increment():
    """Metrics increments counters as expected."""
    metrics = Metrics()
    metrics.inc("a")
    metrics.inc("a")
    assert metrics.counters["a"] == 2


def test_attach_correlation_id():
    """Correlation header is attached to response."""
    response = DummyResponse()
    attach_correlation_id(response, "cid-1")
    assert response.headers["X-Correlation-ID"] == "cid-1"
