"""Databricks SQL execution helpers."""

# pylint: disable=invalid-name,global-statement,too-many-locals,broad-exception-caught

from __future__ import annotations

import os
import time
from typing import List

from databricks.sdk import WorkspaceClient

DEFAULT_SQL_WAIT_TIMEOUT = "10s"
DEFAULT_SQL_EXEC_RETRIES = 2
DEFAULT_SQL_BACKOFF_SECONDS = 0.5

_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    """Return a cached Databricks workspace client."""
    global _client
    if _client is None:
        _client = WorkspaceClient()
    return _client


def _get_int_env(name: str, default: int) -> int:
    """Read an int environment variable with fallback."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid int for env var {name}: {raw}") from exc


def _get_float_env(name: str, default: float) -> float:
    """Read a float environment variable with fallback."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid float for env var {name}: {raw}") from exc


def _get_str_env(name: str, default: str) -> str:
    """Read a string environment variable with fallback."""
    raw = os.environ.get(name)
    return raw if raw not in (None, "") else default


def sql_literal(value) -> str:
    """Convert a Python value to a safe SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def execute_statement(statement: str, warehouse_id: str) -> List[List]:
    """Execute a SQL statement with retries and return rows."""
    wait_timeout = _get_str_env("SQL_WAIT_TIMEOUT", DEFAULT_SQL_WAIT_TIMEOUT)
    retries = _get_int_env("SQL_EXEC_RETRIES", DEFAULT_SQL_EXEC_RETRIES)
    backoff_seconds = _get_float_env(
        "SQL_EXEC_BACKOFF_SECONDS", DEFAULT_SQL_BACKOFF_SECONDS
    )
    client = _get_client()
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            resp = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
                wait_timeout=wait_timeout,
            )
            status = getattr(resp, "status", None)
            if not status:
                raise RuntimeError("SQL execution failed: missing status")
            state = getattr(status, "state", None)
            state_value = getattr(state, "value", state)
            if state_value != "SUCCEEDED":
                err = getattr(status, "error", None)
                err_message = getattr(status, "error_message", None)
                err_code = getattr(status, "error_code", None)
                raise RuntimeError(
                    "SQL execution failed: "
                    f"state={state_value} error_code={err_code} "
                    f"error={err} error_message={err_message}"
                )
            result = getattr(resp, "result", None)
            rows = getattr(result, "data_array", None) if result else None
            return rows or []
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                break
            time.sleep(backoff_seconds * (2**attempt))
    raise RuntimeError(f"SQL execution failed after retries: {last_exc}")
