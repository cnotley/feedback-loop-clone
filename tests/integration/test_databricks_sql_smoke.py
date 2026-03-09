"""Databricks SQL integration smoke tests."""

import os
from uuid import uuid4

import pytest

from app.feedback_api.linking import get_trace_timestamp
from app.feedback_api.storage import payload_hash_exists
from app.feedback_api.sql_utils import execute_statement
from tests.conftest import require_databricks_env


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} not set")
    return value


@pytest.mark.integration
def test_databricks_select_one() -> None:
    require_databricks_env()
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    rows = execute_statement("SELECT 1", warehouse_id)
    assert rows and rows[0] and int(rows[0][0]) == 1


@pytest.mark.integration
def test_payload_hash_exists_returns_false_for_missing_hash() -> None:
    require_databricks_env()
    _require_env("FEEDBACK_TABLE")
    assert payload_hash_exists(f"integration-missing-{uuid4().hex}") is False


@pytest.mark.integration
def test_get_trace_timestamp_returns_none_for_missing_trace_id() -> None:
    require_databricks_env()
    trace_table = _require_env("TRACE_TABLE")
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    assert (
        get_trace_timestamp(trace_table, warehouse_id, f"integration-missing-{uuid4().hex}")
        is None
    )