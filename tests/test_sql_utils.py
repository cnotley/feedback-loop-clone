import types

import pytest

from app.feedback_api import sql_utils
from app.feedback_api.sql_utils import execute_statement, sql_literal


def test_sql_literal_values():
    assert sql_literal(None) == "NULL"
    assert sql_literal(True) == "TRUE"
    assert sql_literal(False) == "FALSE"
    assert sql_literal(5) == "5"
    assert sql_literal("a'b") == "'a''b'"


def _make_response(state_value="SUCCEEDED", rows=None):
    status = types.SimpleNamespace(
        state=types.SimpleNamespace(value=state_value),
        error=None,
        error_message=None,
        error_code=None,
    )
    result = types.SimpleNamespace(data_array=rows)
    return types.SimpleNamespace(status=status, result=result)


def test_execute_statement_success(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.statement_execution = self

        def execute_statement(self, **kwargs):
            return _make_response(rows=[[1]])

    monkeypatch.setattr(sql_utils, "_get_client", lambda: FakeClient())
    rows = execute_statement("select 1", "wh-1")
    assert rows == [[1]]


def test_execute_statement_failure_retries(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.statement_execution = self
            self.calls = 0

        def execute_statement(self, **kwargs):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("boom")
            return _make_response(rows=[])

    monkeypatch.setenv("SQL_EXEC_RETRIES", "2")
    monkeypatch.setenv("SQL_EXEC_BACKOFF_SECONDS", "0")
    fake = FakeClient()
    monkeypatch.setattr(sql_utils, "_get_client", lambda: fake)
    rows = execute_statement("select 1", "wh-1")
    assert rows == []
    assert fake.calls == 3


def test_execute_statement_exhausts_retries(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.statement_execution = self

        def execute_statement(self, **kwargs):
            raise RuntimeError("boom")

    monkeypatch.setenv("SQL_EXEC_RETRIES", "1")
    monkeypatch.setenv("SQL_EXEC_BACKOFF_SECONDS", "0")
    monkeypatch.setattr(sql_utils, "_get_client", lambda: FakeClient())
    with pytest.raises(RuntimeError):
        execute_statement("select 1", "wh-1")


def test_execute_statement_invalid_env(monkeypatch):
    monkeypatch.setenv("SQL_EXEC_RETRIES", "bad")
    with pytest.raises(RuntimeError):
        execute_statement("select 1", "wh-1")