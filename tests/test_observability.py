from app.feedback_api.observability import Metrics, attach_correlation_id


class DummyResponse:
    def __init__(self):
        self.headers = {}


def test_metrics_increment():
    metrics = Metrics()
    metrics.inc("a")
    metrics.inc("a")
    assert metrics.counters["a"] == 2


def test_attach_correlation_id():
    response = DummyResponse()
    attach_correlation_id(response, "cid-1")
    assert response.headers["X-Correlation-ID"] == "cid-1"
