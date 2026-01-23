from __future__ import annotations

import os
import re
from typing import Optional, Pattern

VALID_POLICIES = {"off", "warn", "strict"}


def get_policy(name: str, default: str = "warn") -> str:
    raw = (os.environ.get(name) or default).strip().lower()
    if raw not in VALID_POLICIES:
        raise RuntimeError(f"Invalid policy for {name}: {raw}")
    return raw


def get_int_env_optional(name: str) -> Optional[int]:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid int for env var {name}: {raw}") from exc


def compile_regex(name: str) -> Optional[Pattern[str]]:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    try:
        return re.compile(raw)
    except re.error as exc:
        raise RuntimeError(f"Invalid regex for {name}: {raw}") from exc
