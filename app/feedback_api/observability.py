"""Observability helpers for logging, metrics, and correlation IDs."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict

from fastapi import Request, Response

CORRELATION_HEADER = "X-Correlation-ID"


def _get_log_level() -> int:
    """Resolve log level from environment with INFO default."""
    raw = (os.environ.get("LOG_LEVEL") or "INFO").upper()
    return getattr(logging, raw, logging.INFO)


logger = logging.getLogger("feedback_api")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(_get_log_level())


def get_or_create_correlation_id(request: Request) -> str:
    """Use existing correlation ID header or generate a new UUID."""
    existing = request.headers.get(CORRELATION_HEADER)
    return existing or str(uuid.uuid4())


def log_event(event: Dict) -> None:
    """Log a JSON encoded event to the service logger."""
    logger.info(json.dumps(event, default=str))


@dataclass
class Metrics:
    """Thread safe in memory counters."""
    counters: Dict[str, int] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def inc(self, name: str, value: int = 1) -> None:
        """Increment a named counter in a thread-safe way."""
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + value


metrics = Metrics()


def record_latency(start: float) -> int:
    """Return elapsed time in milliseconds since start."""
    return int((time.time() - start) * 1000)


def attach_correlation_id(response: Response, correlation_id: str) -> None:
    """Attach correlation ID header to the response."""
    response.headers[CORRELATION_HEADER] = correlation_id
