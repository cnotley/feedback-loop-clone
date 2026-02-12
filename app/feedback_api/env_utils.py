"""Shared environment variable helpers."""

from __future__ import annotations

import os


def get_int_env(name: str, default: int) -> int:
    """Read an int environment variable with fallback."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid int for env var {name}: {raw}") from exc


def get_env(name: str) -> str:
    """Fetch a required environment variable or fail fast."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value
