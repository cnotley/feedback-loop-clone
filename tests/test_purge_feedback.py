"""Tests for purge feedback operational safeguards"""

import pytest

from scripts import purge_feedback


def test_purge_requires_force_for_non_dry_run(monkeypatch):
    """Non dry run purge requires --force"""
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    monkeypatch.setattr(
        "sys.argv",
        ["prog", "--tracking-id", "track-1"],
    )

    with pytest.raises(SystemExit, match="--force"):
        purge_feedback.main()


def test_purge_dry_run_prints_plan_and_dry_run_event(monkeypatch, capsys):
    """Dry run logs intended scope and does not execute delete"""
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    statements = []

    def fake_execute(statement, _warehouse_id):
        statements.append(statement)
        if statement.startswith("SELECT COUNT(*)"):
            return [[3]]
        return []

    monkeypatch.setattr("scripts.purge_feedback.execute_statement", fake_execute)
    monkeypatch.setattr(
        "sys.argv",
        ["prog", "--tracking-id", "track-1", "--dry-run"],
    )

    purge_feedback.main()
    out = capsys.readouterr().out
    assert '"event": "purge_plan"' in out
    assert '"event": "purge_dry_run"' in out
    assert len(statements) == 1
    assert statements[0].startswith("SELECT COUNT(*)")


def test_purge_refuses_large_delete(monkeypatch):
    """Purge fails when estimate exceeds max-delete-rows"""
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    monkeypatch.setattr(
        "scripts.purge_feedback.execute_statement",
        lambda *_args, **_kwargs: [[50001]],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "--tracking-id",
            "track-1",
            "--force",
            "--max-delete-rows",
            "50000",
        ],
    )

    with pytest.raises(SystemExit, match="Refusing to delete estimated"):
        purge_feedback.main()


def test_purge_executes_delete_when_forced_and_safe(monkeypatch):
    """Purge executes delete when force is provided and estimate is safe"""
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    statements = []

    def fake_execute(statement, _warehouse_id):
        statements.append(statement)
        if statement.startswith("SELECT COUNT(*)"):
            return [[2]]
        return []

    monkeypatch.setattr("scripts.purge_feedback.execute_statement", fake_execute)
    monkeypatch.setattr(
        "sys.argv",
        [
            "prog",
            "--tracking-id",
            "track-1",
            "--force",
            "--max-delete-rows",
            "50000",
        ],
    )

    purge_feedback.main()
    assert len(statements) == 2
    assert statements[0].startswith("SELECT COUNT(*)")
    assert statements[1].startswith("DELETE FROM")