"""Storage helpers for persisting feedback to Databricks SQL."""

import hashlib
import json
from datetime import datetime, timedelta, timezone

from .env_utils import get_env
from .models import FeedbackPayload
from .sql_utils import execute_statement, sql_literal


def _payload_hash(payload: FeedbackPayload) -> str:
    """Compute a stable hash for a feedback payload."""
    raw = json.dumps(payload.model_dump(), sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def payload_hash(payload: FeedbackPayload) -> str:
    """Public helper for payload hash computation."""
    return _payload_hash(payload)


def _feedback_id_for_payload_hash(payload_hash_value: str) -> str:
    """Derive a deterministic feedback_id from the payload hash."""
    return f"fb_{payload_hash_value[:16]}"


def write_feedback(
    payload: FeedbackPayload,
    link_info: dict,
    payload_hash_value: str | None = None,
) -> tuple[str, bool]:
    """Insert feedback idempotently and return (feedback_id, was_duplicate).

    Contract:
    - For a given payload, the API returns a deterministic feedback_id.
    - If the payload was already ingested, we return the same feedback_id and
      do not create a duplicate row.
    """
    table = get_env("FEEDBACK_TABLE")
    warehouse_id = get_env("DATABRICKS_WAREHOUSE_ID")

    digest = payload_hash_value or _payload_hash(payload)
    feedback_id = _feedback_id_for_payload_hash(digest)
    ingested_at = datetime.now(timezone.utc).isoformat()

    if payload_hash_exists(digest):
        return feedback_id, True

    statement = f"""
    INSERT INTO {table} (
        feedback_id,
        tracking_id,
        trace_id,
        site_id,
        user_id,
        pims_id,
        pims,
        feedback_boolean,
        feedback_score_1,
        feedback_score_2,
        feedback_comment_1,
        feedback_comment_2,
        timestamp,
        schema_version,
        ingested_at,
        source_app,
        service_name,
        consumer_id,
        request_id,
        payload_hash,
        link_mode,
        link_target_trace_id,
        link_window_seconds,
        link_match_count,
        link_reason,
        auth_subject,
        auth_scopes,
        is_valid,
        validation_errors
    )
    SELECT
        {sql_literal(feedback_id)},
        {sql_literal(payload.tracking_id)},
        {sql_literal(payload.trace_id)},
        {sql_literal(payload.site_id)},
        {sql_literal(payload.user_id)},
        {sql_literal(payload.pims_id)},
        {sql_literal(payload.pims)},
        {sql_literal(payload.feedback_boolean)},
        {sql_literal(payload.feedback_score_1)},
        {sql_literal(payload.feedback_score_2)},
        {sql_literal(payload.feedback_comment_1)},
        {sql_literal(payload.feedback_comment_2)},
        {sql_literal(payload.timestamp.isoformat())},
        {sql_literal(payload.schema_version)},
        {sql_literal(ingested_at)},
        {sql_literal(payload.source_app)},
        {sql_literal(payload.service_name)},
        {sql_literal(payload.consumer_id)},
        {sql_literal(payload.request_id)},
        {sql_literal(digest)},
        {sql_literal(link_info.get("link_mode"))},
        {sql_literal(link_info.get("link_target_trace_id"))},
        {sql_literal(link_info.get("link_window_seconds"))},
        {sql_literal(link_info.get("link_match_count"))},
        {sql_literal(link_info.get("link_reason"))},
        NULL,
        NULL,
        TRUE,
        NULL
    WHERE NOT EXISTS (
        SELECT 1 FROM {table} WHERE payload_hash = {sql_literal(digest)}
    )
    """
    execute_statement(statement, warehouse_id)

    return feedback_id, False


def payload_hash_exists(payload_hash_value: str) -> bool:
    """Check for an existing payload hash in storage."""
    table = get_env("FEEDBACK_TABLE")
    warehouse_id = get_env("DATABRICKS_WAREHOUSE_ID")
    statement = f"""
    SELECT 1
    FROM {table}
    WHERE payload_hash = {sql_literal(payload_hash_value)}
    LIMIT 1
    """
    rows = execute_statement(statement, warehouse_id)
    return bool(rows)

def tracking_id_recent_exists(
    tracking_id: str,
    window_seconds: int,
    *,
    pims: str | None = None,
    user_id: str | None = None,
) -> bool:
    """Check if a tracking_id was recently ingested within a window."""
    table = get_env("FEEDBACK_TABLE")
    warehouse_id = get_env("DATABRICKS_WAREHOUSE_ID")
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
    conditions = [f"tracking_id = {sql_literal(tracking_id)}"]
    if pims:
        conditions.append(f"pims = {sql_literal(pims)}")
    if user_id:
        conditions.append(f"user_id = {sql_literal(user_id)}")
    conditions.append(f"ingested_at >= {sql_literal(cutoff.isoformat())}")
    where_clause = " AND ".join(conditions)
    statement = f"""
    SELECT 1
    FROM {table}
    WHERE {where_clause}
    LIMIT 1
    """
    rows = execute_statement(statement, warehouse_id)
    return bool(rows)