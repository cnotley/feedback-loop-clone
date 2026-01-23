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


def _fetch_best_match(
    payload: FeedbackPayload,
    trace_table: str,
    warehouse_id: str,
    window_seconds: int,
    max_candidates: int,
) -> Tuple[Optional[str], int]:
    window = timedelta(seconds=window_seconds)
    payload_ts = payload.timestamp
    if payload_ts.tzinfo is None:
        payload_ts = payload_ts.replace(tzinfo=timezone.utc)
    start = (payload_ts - window).isoformat()
    end = (payload_ts + window).isoformat()
    payload_ts_literal = sql_literal(payload_ts.isoformat())

    order_by_parts = []
    if payload.tracking_id:
        order_by_parts.append(
            "CASE WHEN tags['tracking_id'] = "
            f"{sql_literal(payload.tracking_id)} THEN 0 ELSE 1 END"
        )
    order_by_parts.append(
        f"ABS(TIMESTAMPDIFF(SECOND, request_time, {payload_ts_literal}))"
    )
    order_by = ", ".join(order_by_parts)

    statement = f"""
    SELECT trace_id, tags['tracking_id'], tags['mlflow.user'], request_time
    FROM {trace_table}
    WHERE tags['mlflow.user'] = {sql_literal(payload.user_id)}
      AND request_time BETWEEN {sql_literal(start)} AND {sql_literal(end)}
    ORDER BY {order_by}
    LIMIT {max_candidates}
    """
    rows = execute_statement(statement, warehouse_id)
    if not rows:
        return None, 0
    best_trace_id = rows[0][0]
    return best_trace_id, len(rows)


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
    """Link feedback to a model run using trace-first policy.

    Policy:
    - If trace_id is present, use it as the linkage key.
    - Otherwise, attempt best-match within a time window and same user_id.
    """
    if payload.trace_id:
        trace_table = os.environ.get("TRACE_TABLE")
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
        if not trace_table or not warehouse_id:
            return {
                "link_mode": "trace_id_unverified",
                "link_target_trace_id": payload.trace_id,
                "link_window_seconds": None,
                "link_match_count": None,
                "link_reason": "trace_table_unconfigured",
            }
        try:
            if _trace_id_exists(trace_table, warehouse_id, payload.trace_id):
                return {
                    "link_mode": "trace_id",
                    "link_target_trace_id": payload.trace_id,
                    "link_window_seconds": None,
                    "link_match_count": None,
                    "link_reason": "trace_id_verified",
                }
            return {
                "link_mode": "trace_id_not_found",
                "link_target_trace_id": payload.trace_id,
                "link_window_seconds": None,
                "link_match_count": 0,
                "link_reason": "trace_id_not_found",
            }
        except Exception as exc:
            return {
                "link_mode": "trace_id_unverified",
                "link_target_trace_id": payload.trace_id,
                "link_window_seconds": None,
                "link_match_count": None,
                "link_reason": f"trace_lookup_failed:{exc}",
            }

    window_seconds = _get_int_env("LINK_WINDOW_SECONDS", DEFAULT_LINK_WINDOW_SECONDS)
    max_candidates = _get_int_env("LINK_MAX_CANDIDATES", DEFAULT_LINK_MAX_CANDIDATES)

    if not payload.user_id:
        return {
            "link_mode": "unlinked",
            "link_target_trace_id": None,
            "link_window_seconds": window_seconds,
            "link_match_count": 0,
            "link_reason": "missing_user_id",
        }

    trace_table = os.environ.get("TRACE_TABLE")
    if not trace_table:
        return {
            "link_mode": "best_match_unresolved",
            "link_target_trace_id": None,
            "link_window_seconds": window_seconds,
            "link_match_count": 0,
            "link_reason": "trace_table_unconfigured",
        }

    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        return {
            "link_mode": "best_match_unresolved",
            "link_target_trace_id": None,
            "link_window_seconds": window_seconds,
            "link_match_count": 0,
            "link_reason": "warehouse_unconfigured",
        }

    try:
        best_trace_id, match_count = _fetch_best_match(
            payload, trace_table, warehouse_id, window_seconds, max_candidates
        )
    except Exception as exc:
        return {
            "link_mode": "best_match_unresolved",
            "link_target_trace_id": None,
            "link_window_seconds": window_seconds,
            "link_match_count": 0,
            "link_reason": f"lookup_failed:{exc}",
        }

    if not best_trace_id:
        return {
            "link_mode": "unlinked",
            "link_target_trace_id": None,
            "link_window_seconds": window_seconds,
            "link_match_count": 0,
            "link_reason": "no_candidates",
        }

    if match_count > 1 and not payload.tracking_id:
        return {
            "link_mode": "unlinked",
            "link_target_trace_id": None,
            "link_window_seconds": window_seconds,
            "link_match_count": match_count,
            "link_reason": "ambiguous_candidates",
        }

    return {
        "link_mode": "best_match",
        "link_target_trace_id": best_trace_id,
        "link_window_seconds": window_seconds,
        "link_match_count": match_count,
        "link_reason": "best_match_user_id_window",
    }