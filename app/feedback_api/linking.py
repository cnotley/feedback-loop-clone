from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

from .models import FeedbackPayload
from .sql_utils import execute_statement, sql_literal

DEFAULT_LINK_WINDOW_SECONDS = 3600
DEFAULT_LINK_MAX_CANDIDATES = 5


def _get_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid int for env var {name}: {raw}") from exc


def _fetch_tracking_id_match(
    tracking_id: str,
    trace_table: str,
    warehouse_id: str,
) -> Tuple[Optional[str], int]:
    count_statement = f"""
    SELECT COUNT(1)
    FROM {trace_table}
    WHERE tags['tracking_id'] = {sql_literal(tracking_id)}
    """
    count_rows = execute_statement(count_statement, warehouse_id)
    match_count = int(count_rows[0][0]) if count_rows and count_rows[0] else 0
    if match_count == 0:
        return None, 0

    latest_statement = f"""
    SELECT trace_id, request_time
    FROM {trace_table}
    WHERE tags['tracking_id'] = {sql_literal(tracking_id)}
    ORDER BY request_time DESC
    LIMIT 1
    """
    rows = execute_statement(latest_statement, warehouse_id)
    if not rows:
        return None, match_count
    return rows[0][0], match_count


def _trace_id_exists(trace_table: str, warehouse_id: str, trace_id: str) -> bool:
    statement = f"""
    SELECT 1
    FROM {trace_table}
    WHERE trace_id = {sql_literal(trace_id)}
    LIMIT 1
    """
    rows = execute_statement(statement, warehouse_id)
    return bool(rows)


def get_trace_timestamp(
    trace_table: str, warehouse_id: str, trace_id: str
) -> Optional[datetime]:
    statement = f"""
    SELECT request_time
    FROM {trace_table}
    WHERE trace_id = {sql_literal(trace_id)}
    LIMIT 1
    """
    rows = execute_statement(statement, warehouse_id)
    if not rows or not rows[0]:
        return None
    value = rows[0][0]
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def link_run(payload: FeedbackPayload) -> Dict[str, Optional[str]]:
    trace_table = os.environ.get("TRACE_TABLE")
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")

    if payload.trace_id:
        if not trace_table or not warehouse_id:
            return {
                "link_mode": "no_match",
                "link_target_trace_id": None,
                "link_window_seconds": None,
                "link_match_count": 0,
                "link_reason": "trace_table_unconfigured",
            }
        try:
            if _trace_id_exists(trace_table, warehouse_id, payload.trace_id):
                return {
                    "link_mode": "trace_id_match",
                    "link_target_trace_id": payload.trace_id,
                    "link_window_seconds": None,
                    "link_match_count": 1,
                    "link_reason": "trace_id_verified",
                }
        except Exception as exc:
            return {
                "link_mode": "no_match",
                "link_target_trace_id": None,
                "link_window_seconds": None,
                "link_match_count": 0,
                "link_reason": f"trace_lookup_failed:{exc}",
            }

    if not payload.tracking_id:
        return {
            "link_mode": "no_match",
            "link_target_trace_id": None,
            "link_window_seconds": None,
            "link_match_count": 0,
            "link_reason": "missing_tracking_id",
        }

    if not trace_table or not warehouse_id:
        return {
            "link_mode": "no_match",
            "link_target_trace_id": None,
            "link_window_seconds": None,
            "link_match_count": 0,
            "link_reason": "trace_table_unconfigured",
        }

    try:
        best_trace_id, match_count = _fetch_tracking_id_match(
            payload.tracking_id, trace_table, warehouse_id
        )
    except Exception as exc:
        return {
            "link_mode": "no_match",
            "link_target_trace_id": None,
            "link_window_seconds": None,
            "link_match_count": 0,
            "link_reason": f"tracking_lookup_failed:{exc}",
        }

    if not best_trace_id:
        return {
            "link_mode": "no_match",
            "link_target_trace_id": None,
            "link_window_seconds": None,
            "link_match_count": 0,
            "link_reason": "no_match",
        }

    return {
        "link_mode": "tracking_id_exact_match" if match_count == 1 else "tracking_id_recent_match",
        "link_target_trace_id": best_trace_id,
        "link_window_seconds": None,
        "link_match_count": match_count,
        "link_reason": "tracking_id_match",
    }