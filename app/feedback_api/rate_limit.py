"""In memory rate limiting helpers."""

from __future__ import annotations

import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock
from typing import Deque, Dict, Optional, Tuple

DEFAULT_WINDOW_SECONDS = 60
DEFAULT_LIMIT_PER_USER = 60
DEFAULT_LIMIT_PER_TOKEN = 120
DEFAULT_LIMIT_PER_IP = 300


def _get_int_env(name: str, default: int) -> int:
    """Read an int environment variable with fallback."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid int for env var {name}: {raw}") from exc


@dataclass
class RateLimitConfig:  # pylint: disable=too-few-public-methods
    """Configuration values for rate limiting."""
    window_seconds: int
    limit_per_user: int
    limit_per_token: int
    limit_per_ip: int

    @classmethod
    def from_env(cls) -> "RateLimitConfig":
        """Load rate limit settings from environment variables."""
        return cls(
            window_seconds=_get_int_env("RATE_LIMIT_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS),
            limit_per_user=_get_int_env("RATE_LIMIT_PER_USER", DEFAULT_LIMIT_PER_USER),
            limit_per_token=_get_int_env("RATE_LIMIT_PER_TOKEN", DEFAULT_LIMIT_PER_TOKEN),
            limit_per_ip=_get_int_env("RATE_LIMIT_PER_IP", DEFAULT_LIMIT_PER_IP),
        )


class RateLimiter:  # pylint: disable=too-few-public-methods
    """Thread safe sliding window rate limiter."""

    def __init__(self, config: Optional[RateLimitConfig] = None) -> None:
        """Initialize a thread safe sliding window limiter."""
        self.config = config or RateLimitConfig.from_env()
        self._buckets: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check(self, key: str, limit: int) -> Tuple[bool, int]:
        """Check a key against the window and return allowance and retry_after."""
        now = time.time()
        window_start = now - self.config.window_seconds
        with self._lock:
            bucket = self._buckets[key]
            while bucket and bucket[0] < window_start:
                bucket.popleft()
            if len(bucket) >= limit:
                retry_after = int(bucket[0] + self.config.window_seconds - now) + 1
                return False, max(retry_after, 1)
            bucket.append(now)
            return True, 0
