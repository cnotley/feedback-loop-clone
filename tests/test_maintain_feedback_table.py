"""Tests for feedback table maintenance script"""

from scripts import maintain_feedback_table


def test_maintain_dry_run_logs_statements(monkeypatch, capsys):
    """Dry run should not execute SQL statements"""
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    called = {"count": 0}

    def fake_execute(*_args, **_kwargs):
        called["count"] += 1
        return []

    monkeypatch.setattr("scripts.maintain_feedback_table.execute_statement", fake_execute)
    monkeypatch.setattr("sys.argv", ["prog", "--dry-run"])

    maintain_feedback_table.main()
    out = capsys.readouterr().out
    assert '"event": "maintenance_dry_run_statement"' in out
    assert called["count"] == 0


def test_maintain_executes_optimize_and_vacuum(monkeypatch):
    """Maintenance should execute OPTIMIZE and VACUUM statements"""
    monkeypatch.setenv("FEEDBACK_TABLE", "db.schema.table")
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-1")
    statements = []

    def fake_execute(statement, _warehouse_id):
        statements.append(statement)
        return []

    monkeypatch.setattr("scripts.maintain_feedback_table.execute_statement", fake_execute)
    monkeypatch.setattr("sys.argv", ["prog", "--vacuum-retain-hours", "72"])

    maintain_feedback_table.main()
    assert len(statements) == 2
    assert statements[0] == "OPTIMIZE db.schema.table"
    assert statements[1] == "VACUUM db.schema.table RETAIN 72 HOURS"