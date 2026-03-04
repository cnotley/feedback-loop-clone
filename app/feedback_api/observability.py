"""Observability helpers for logging, metrics, and correlation IDs."""

import json
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict

from fastapi import Request, Response

CORRELATION_HEADER = "X-Correlation-ID"


def _get_log_level() -> int:
    """Resolve log level from environment with INFO default"""
    raw = (os.environ.get("LOG_LEVEL") or "INFO").upper()
    return getattr(logging, raw, logging.INFO)


logger = logging.getLogger("feedback_api")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(_get_log_level())
_dd_socket = None
_dd_target = None
_dd_lock = Lock()


def get_or_create_correlation_id(request: Request) -> str:
    """Use existing correlation ID header or generate a new UUID"""
    existing = request.headers.get(CORRELATION_HEADER)
    return existing or str(uuid.uuid4())


def log_event(event: Dict) -> None:
    """Log a JSON encoded event to the service logger"""
    logger.info(json.dumps(event, default=str))


@dataclass
class Metrics:
    """Thread safe in-memory counters and latency aggregates"""
    counters: Dict[str, int] = field(default_factory=dict)
    histograms: Dict[str, Dict[str, float]] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def inc(self, name: str, value: int = 1) -> None:
        """Increment a named counter in a thread safe way"""
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + value
        _emit_datadog_counter(name, value)

    def observe_ms(self, name: str, value_ms: int) -> None:
        """Record a millisecond observation with summary stats"""
        with self._lock:
            current = self.histograms.get(name)
            if current is None:
                self.histograms[name] = {
                    "count": 1,
                    "sum_ms": float(value_ms),
                    "min_ms": float(value_ms),
                    "max_ms": float(value_ms),
                }
            else:
                current["count"] += 1
                current["sum_ms"] += float(value_ms)
                current["min_ms"] = min(current["min_ms"], float(value_ms))
                current["max_ms"] = max(current["max_ms"], float(value_ms))
        _emit_datadog_histogram(name, value_ms)

    def snapshot(self) -> Dict:
        """Return a JSON friendly metrics payload for diagnostics"""
        with self._lock:
            counters = dict(self.counters)
            histograms = {
                key: {
                    "count": int(value["count"]),
                    "sum_ms": int(value["sum_ms"]),
                    "min_ms": int(value["min_ms"]),
                    "max_ms": int(value["max_ms"]),
                    "avg_ms": round(value["sum_ms"] / value["count"], 2)
                    if value["count"] > 0
                    else 0,
                }
                for key, value in self.histograms.items()
            }

        rate_limited_total = (
            counters.get("rate_limited_ip", 0)
            + counters.get("rate_limited_token", 0)
            + counters.get("rate_limited_user", 0)
        )
        rejected_by_reason = {
            "validation_schema_violations": counters.get("validation_failed", 0),
            "rate_limited": rate_limited_total,
            "policy_failures": counters.get("policy_check_failed", 0)
            + counters.get("trace_policy_rejected", 0)
            + counters.get("tracking_policy_rejected", 0),
            "dedup": counters.get("dedup_rejected", 0),
        }
        link_modes = {
            key.replace("link_mode_", ""): value
            for key, value in counters.items()
            if key.startswith("link_mode_")
        }
        for required_mode in [
            "trace_id_match",
            "tracking_id_exact_match",
            "tracking_id_recent_match",
            "no_match",
            "unknown",
        ]:
            link_modes.setdefault(required_mode, 0)
        return {
            "service": "feedback-api",
            "datadog": {
                "enabled": bool(os.environ.get("DD_AGENT_HOST")),
                "namespace": os.environ.get("DD_METRIC_NAMESPACE", "feedback_api"),
            },
            "metrics": {
                "submissions": {
                    "accepted": counters.get("accepted", 0),
                    "rejected": sum(rejected_by_reason.values()),
                    "rejected_by_reason": rejected_by_reason,
                },
                "auth": {
                    "invalid_attempts": counters.get("invalid_auth_attempts", 0),
                },
                "link_modes": link_modes,
                "dedup_events": {
                    "rejected": counters.get("dedup_rejected", 0),
                },
                "requests": {
                    "total": counters.get("requests_total", 0),
                    "errors_4xx": counters.get("requests_errors_4xx", 0),
                    "errors_5xx": counters.get("requests_errors_5xx", 0),
                    "exceptions": counters.get("requests_exception", 0),
                    "latency_ms": histograms.get(
                        "request_latency_ms",
                        {"count": 0, "sum_ms": 0, "min_ms": 0, "max_ms": 0, "avg_ms": 0},
                    ),
                },
            },
            "counters": counters,
        }


metrics = Metrics()


def record_latency(start: float) -> int:
    """Return elapsed time in milliseconds since start"""
    return int((time.time() - start) * 1000)


def _emit_datadog_counter(name: str, value: int) -> None:
    """Emit a counter metric to DogStatsD when configured"""
    _emit_datadog_metric(name, value, "c")


def _emit_datadog_histogram(name: str, value: int) -> None:
    """Emit a histogram metric to DogStatsD when configured"""
    _emit_datadog_metric(name, value, "h")


def _emit_datadog_metric(name: str, value: int, metric_type: str) -> None:
    host = os.environ.get("DD_AGENT_HOST")
    if not host:
        return
    port = int(os.environ.get("DD_DOGSTATSD_PORT", "8125"))
    namespace = os.environ.get("DD_METRIC_NAMESPACE", "feedback_api")
    metric_name = f"{namespace}.{name}"
    tags_raw = os.environ.get("DD_TAGS", "")
    tags = [tag.strip() for tag in tags_raw.split(",") if tag.strip()]
    payload = f"{metric_name}:{value}|{metric_type}"
    if tags:
        payload = f"{payload}|#{','.join(tags)}"
    try:
        global _dd_socket, _dd_target
        target = (host, port)
        if _dd_socket is None or _dd_target != target:
            with _dd_lock:
                if _dd_socket is not None and _dd_target != target:
                    _dd_socket.close()
                    _dd_socket = None
                if _dd_socket is None:
                    _dd_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    _dd_target = target
        _dd_socket.sendto(payload.encode("utf-8"), target)
    except Exception:  # pylint: disable=broad-exception-caught
        return


def attach_correlation_id(response: Response, correlation_id: str) -> None:
    """Attach correlation ID header to the response"""
    response.headers[CORRELATION_HEADER] = correlation_id