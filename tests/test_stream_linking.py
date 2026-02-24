"""Tests for the stream linking job SQL and batching."""
# pylint: disable=invalid-name,protected-access,duplicate-code

from scripts import stream_linking


class FakeSpark:  # pylint: disable=too-few-public-methods
    """Minimal Spark stub that records SQL queries."""
    def __init__(self):
        """Capture executed SQL for assertions."""
        self.queries = []

    def sql(self, query: str):
        """Record SQL queries called by the job."""
        self.queries.append(query)


class FakeRDD:  # pylint: disable=too-few-public-methods
    """Minimal RDD stub for empty checks."""
    def __init__(self, empty: bool):
        """Stub RDD with configurable emptiness."""
        self._empty = empty

    def isEmpty(self):
        """Return whether the batch is empty."""
        return self._empty


class FakeBatchDF:  # pylint: disable=too-few-public-methods
    """Minimal batch DataFrame stub for tests."""
    def __init__(self, empty: bool = False):
        """Stub batch DataFrame used for batch processing tests."""
        self.rdd = FakeRDD(empty)
        self.view_name = None
        self._count = 0 if empty else 1

    def createOrReplaceGlobalTempView(self, name: str):
        """Capture created view name for assertions."""
        self.view_name = name

    def count(self) -> int:
        """Return a configured row count."""
        return self._count


def test_create_index_table_sql():
    """Index table creation uses expected SQL."""
    spark = FakeSpark()
    stream_linking._create_index_table(spark, "catalog.schema.index_table")
    assert "CREATE TABLE IF NOT EXISTS catalog.schema.index_table" in spark.queries[0]
    assert "trace_id STRING" in spark.queries[0]


def test_merge_index_table_uses_view():
    """Index merge references the provided view name."""
    spark = FakeSpark()
    stream_linking._merge_index_table(spark, "tbl", "global_temp.batch_traces")
    query = spark.queries[0]
    assert "MERGE INTO tbl" in query
    assert "USING global_temp.batch_traces" in query


def test_merge_feedback_by_trace_id_uses_view():
    """Trace ID merge uses the provided view name."""
    spark = FakeSpark()
    stream_linking._merge_feedback_by_trace_id(
        spark, "feedback_tbl", "global_temp.batch_traces"
    )
    query = spark.queries[0]
    assert "MERGE INTO feedback_tbl" in query
    assert "USING global_temp.batch_traces" in query
    assert "link_mode = 'no_match'" in query


def test_merge_feedback_by_tracking_id_uses_view():
    """Tracking ID merge references expected tables."""
    spark = FakeSpark()
    stream_linking._merge_feedback_by_tracking_id(
        spark, "feedback_tbl", "index_tbl", "global_temp.batch_traces"
    )
    query = spark.queries[0]
    assert "FROM global_temp.batch_traces" in query
    assert "MERGE INTO feedback_tbl" in query
    assert "FROM index_tbl" in query


def test_process_batch_skips_empty():
    """Empty batches are processed (isEmpty check removed for resilience)."""
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=True)
    stream_linking.process_batch(spark, batch_df, "feedback_tbl", "index_tbl")

    assert batch_df.view_name == "batch_traces"

    assert len(spark.queries) == 3


def test_process_batch_executes_merges():
    """Non-empty batches execute merges."""
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=False)
    timings = stream_linking.process_batch(spark, batch_df, "feedback_tbl", "index_tbl")
    assert batch_df.view_name == "batch_traces"
    assert len(spark.queries) == 3
    assert "merge_index_ms" in timings
    assert "merge_feedback_trace_id_ms" in timings
    assert "merge_feedback_tracking_id_ms" in timings


def test_parse_args_defaults(monkeypatch):
    """Default CLI arguments are applied."""
    argv = [
        "prog",
        "--feedback-table",
        "f",
        "--trace-table",
        "t",
        "--index-table",
        "i",
        "--checkpoint",
        "c",
    ]
    monkeypatch.setattr("sys.argv", argv)
    args = stream_linking.parse_args()
    assert args.trigger_seconds == 60
    assert args.watermark_minutes == 15


def test_process_microbatch_logs_and_reraises_on_error(monkeypatch):
    """Microbatch wrapper logs failure details and re-raises exceptions."""
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=False)
    events = []

    monkeypatch.setattr(stream_linking, "log_event", lambda event: events.append(event))
    monkeypatch.setattr(stream_linking, "process_batch", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))

    try:
        stream_linking.process_microbatch(
            spark=spark,
            batch_df=batch_df,
            batch_id=7,
            feedback_table="feedback_tbl",
            index_table="index_tbl",
        )
        assert False, "expected exception"
    except RuntimeError as exc:
        assert "boom" in str(exc)

    assert events[0]["event"] == "microbatch_start"
    assert events[0]["batch_id"] == 7
    assert events[-1]["event"] == "microbatch_failed"
    assert events[-1]["batch_id"] == 7
    assert "traceback" in events[-1]


def test_process_microbatch_logs_success(monkeypatch):
    """Microbatch wrapper logs success and includes timings."""
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=False)
    events = []

    monkeypatch.setattr(stream_linking, "log_event", lambda event: events.append(event))
    monkeypatch.setattr(
        stream_linking,
        "process_batch",
        lambda *args, **kwargs: {
            "merge_index_ms": 1,
            "merge_feedback_trace_id_ms": 2,
            "merge_feedback_tracking_id_ms": 3,
        },
    )

    stream_linking.process_microbatch(
        spark=spark,
        batch_df=batch_df,
        batch_id=8,
        feedback_table="feedback_tbl",
        index_table="index_tbl",
    )

    assert events[0]["event"] == "microbatch_start"
    assert events[-1]["event"] == "microbatch_success"
    assert events[-1]["batch_id"] == 8
    assert "elapsed_ms" in events[-1]
    assert events[-1]["merge_index_ms"] == 1