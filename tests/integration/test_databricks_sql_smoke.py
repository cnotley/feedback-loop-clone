"""Databricks SQL integration smoke tests."""

import os

import pytest

from app.feedback_api.sql_utils import execute_statement
from tests.conftest import require_databricks_env


@pytest.mark.integration
def test_databricks_select_one() -> None:
    require_databricks_env()
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    rows = execute_statement("SELECT 1", warehouse_id)
    assert rows and rows[0] and int(rows[0][0]) == 1