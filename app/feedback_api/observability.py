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
    existing = request.headers.get(CORRELATION_HEADER)
    return existing or str(uuid.uuid4())


def log_event(event: Dict) -> None:
    logger.info(json.dumps(event, default=str))


@dataclass
class Metrics:
    counters: Dict[str, int] = field(default_factory=dict)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def inc(self, name: str, value: int = 1) -> None:
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + value


metrics = Metrics()


def record_latency(start: float) -> int:
    return int((time.time() - start) * 1000)


def attach_correlation_id(response: Response, correlation_id: str) -> None:
    response.headers[CORRELATION_HEADER] = correlation_id
