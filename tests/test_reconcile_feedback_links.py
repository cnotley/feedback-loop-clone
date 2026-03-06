"""Tests for standalone reconcile sweep script."""

from scripts import reconcile_feedback_links


def test_parse_args_defaults(monkeypatch):
    """Default standalone sweep args are applied."""
    argv = [
        "prog",
        "--feedback-table",
        "f",
        "--index-table",
        "i",
    ]
    monkeypatch.setattr("sys.argv", argv)
    args = reconcile_feedback_links.parse_args()
    assert args.lookback_hours == 720
    assert args.max_rows == 1000000


def test_main_invokes_both_reconcile_paths(monkeypatch):
    """Standalone sweep should run both trace-id and tracking-id reconciliation."""
    argv = [
        "prog",
        "--feedback-table",
        "f_tbl",
        "--index-table",
        "i_tbl",
        "--lookback-hours",
        "24",
        "--max-rows",
        "500",
    ]
    monkeypatch.setattr("sys.argv", argv)
    calls = {"trace": 0, "tracking": 0}

    class _Builder:  # pylint: disable=too-few-public-methods
        def getOrCreate(self):
            return object()

    monkeypatch.setattr(reconcile_feedback_links.SparkSession, "builder", _Builder())
    monkeypatch.setattr(reconcile_feedback_links, "_log_event", lambda _event: None)

    def trace_call(_spark, _feedback_table, _index_table, _lookback_hours, _max_rows):
        calls["trace"] += 1

    def tracking_call(_spark, _feedback_table, _index_table, _lookback_hours, _max_rows):
        calls["tracking"] += 1

    monkeypatch.setattr(reconcile_feedback_links, "_reconcile_feedback_by_trace_id", trace_call)
    monkeypatch.setattr(
        reconcile_feedback_links, "_reconcile_feedback_by_tracking_id", tracking_call
    )

    reconcile_feedback_links.main()
    assert calls["trace"] == 1
    assert calls["tracking"] == 1