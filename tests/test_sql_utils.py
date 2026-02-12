"""Tests for SQL utility helpers."""

# pylint: disable=unused-argument,unnecessary-lambda,too-few-public-methods,import-error,no-name-in-module

import types

import pytest

from app.feedback_api import sql_utils
from app.feedback_api.sql_utils import execute_statement, sql_literal


def test_sql_literal_values():
    """sql_literal formats core types correctly."""
    assert sql_literal(None) == "NULL"
    assert sql_literal(True) == "TRUE"
    assert sql_literal(False) == "FALSE"
    assert sql_literal(5) == "5"
    assert sql_literal("a'b") == "'a''b'"


def _make_response(state_value="SUCCEEDED", rows=None):
    """Create a fake response object for SQL tests."""
    status = types.SimpleNamespace(
        state=types.SimpleNamespace(value=state_value),
        error=None,
        error_message=None,
        error_code=None,
    )
    result = types.SimpleNamespace(data_array=rows)
    return types.SimpleNamespace(status=status, result=result)


def test_execute_statement_success(monkeypatch):
    """Successful execution returns rows."""
    class FakeClient:
        """Fake client that returns successful responses."""
        def __init__(self):
            """Initialize fake statement execution client."""
            self.statement_execution = self

        def execute_statement(self, **kwargs):
            """Return a successful response with rows."""
            return _make_response(rows=[[1]])

    monkeypatch.setattr(sql_utils, "_get_client", lambda: FakeClient())
    rows = execute_statement("select 1", "wh-1")
    assert rows == [[1]]


def test_execute_statement_failure_retries(monkeypatch):
    """Transient failures retry and eventually succeed."""
    class FakeClient:
        """Fake client that fails before succeeding."""
        def __init__(self):
            """Initialize fake client with call counter."""
            self.statement_execution = self
            self.calls = 0

        def execute_statement(self, **kwargs):
            """Fail twice before returning an empty result."""
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("boom")
            return _make_response(rows=[])

    monkeypatch.setenv("SQL_EXEC_RETRIES", "2")
    monkeypatch.setenv("SQL_EXEC_BACKOFF_SECONDS", "0")
    fake = FakeClient()
    monkeypatch.setattr(sql_utils, "_get_client", lambda: fake)
    rows = execute_statement("select 1", "wh-1")
    assert not rows
    assert fake.calls == 3


def test_execute_statement_exhausts_retries(monkeypatch):
    """Exhausted retries raise RuntimeError."""
    class FakeClient:
        """Fake client that always fails."""
        def __init__(self):
            """Initialize fake client for failure scenario."""
            self.statement_execution = self

        def execute_statement(self, **kwargs):
            """Always raise to exhaust retries."""
            raise RuntimeError("boom")

    monkeypatch.setenv("SQL_EXEC_RETRIES", "1")
    monkeypatch.setenv("SQL_EXEC_BACKOFF_SECONDS", "0")
    monkeypatch.setattr(sql_utils, "_get_client", lambda: FakeClient())
    with pytest.raises(RuntimeError):
        execute_statement("select 1", "wh-1")


def test_execute_statement_invalid_env(monkeypatch):
    """Invalid env values raise RuntimeError."""
    monkeypatch.setenv("SQL_EXEC_RETRIES", "bad")
    with pytest.raises(RuntimeError):
        execute_statement("select 1", "wh-1")
