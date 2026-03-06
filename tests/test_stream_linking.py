"""Tests for the stream linking job SQL and batching."""
# pylint: disable=invalid-name,protected-access,duplicate-code

import pytest

from scripts import stream_linking


class FakeSpark:
    def __init__(self):
        self.queries = []

    def sql(self, query: str):
        self.queries.append(query)


class FakeRDD:
    def __init__(self, empty: bool):
        self._empty = empty

    def isEmpty(self):
        return self._empty


class FakeBatchDF:
    def __init__(self, empty: bool = False):
        self.rdd = FakeRDD(empty)
        self.view_name = None
        self._count = 0 if empty else 1
        self.persisted = False
        self.unpersisted = False

    def createOrReplaceGlobalTempView(self, name: str):
        self.view_name = name

    def count(self) -> int:
        return self._count

    def persist(self):
        self.persisted = True
        return self

    def unpersist(self):
        self.unpersisted = True


class RecordingReadStream:
    def __init__(self, columns=None):
        self.operations = []
        self.columns = columns or ["trace_id", "tracking_id", "tags", "trace_metadata", "request_time"]

    def option(self, key, value):
        self.operations.append(("option", key, value))
        return self

    def table(self, name):
        self.operations.append(("table", name))
        return self

    def select(self, *cols):
        self.operations.append(("select", len(cols)))
        return self

    def where(self, *_args, **_kwargs):
        self.operations.append(("where",))
        return self

    def withWatermark(self, *_args, **_kwargs):
        self.operations.append(("withWatermark",))
        return self

    def dropDuplicates(self, *_args, **_kwargs):
        self.operations.append(("dropDuplicates",))
        return self

    @property
    def writeStream(self):
        return RecordingWriteStream(self.operations)


class RecordingWriteStream:
    def __init__(self, operations):
        self.operations = operations

    def foreachBatch(self, _fn):
        self.operations.append(("foreachBatch",))
        return self

    def option(self, key, value):
        self.operations.append(("write_option", key, value))
        return self

    def trigger(self, **kwargs):
        self.operations.append(("trigger", kwargs))
        return self

    def start(self):
        self.operations.append(("start",))
        return self

    def awaitTermination(self):
        self.operations.append(("awaitTermination",))
        return None


class RecordingSpark:
    def __init__(self, columns=None):
        self.readStream = RecordingReadStream(columns=columns)
        self.conf = self
        self.queries = []

    def set(self, key, value):
        self.queries.append(("conf_set", key, value))

    def sql(self, query):
        self.queries.append(("sql", query))


def test_create_index_table_sql():
    spark = FakeSpark()
    stream_linking._create_index_table(spark, "catalog.schema.index_table")
    assert "CREATE TABLE IF NOT EXISTS catalog.schema.index_table" in spark.queries[0]
    assert "trace_id STRING" in spark.queries[0]


def test_merge_index_table_uses_view():
    spark = FakeSpark()
    stream_linking._merge_index_table(spark, "tbl", "global_temp.batch_traces")
    query = spark.queries[0]
    assert "MERGE INTO tbl" in query
    assert "USING global_temp.batch_traces" in query


def test_merge_feedback_by_trace_id_uses_view():
    spark = FakeSpark()
    stream_linking._merge_feedback_by_trace_id(
        spark, "feedback_tbl", "global_temp.batch_traces"
    )
    query = spark.queries[0]
    assert "MERGE INTO feedback_tbl" in query
    assert "USING global_temp.batch_traces" in query
    assert "link_mode = 'no_match'" in query


def test_merge_feedback_by_tracking_id_uses_view():
    spark = FakeSpark()
    stream_linking._merge_feedback_by_tracking_id(
        spark, "feedback_tbl", "index_tbl", "global_temp.batch_traces"
    )
    query = spark.queries[0]
    assert "FROM global_temp.batch_traces" in query
    assert "MERGE INTO feedback_tbl" in query
    assert "FROM index_tbl" in query


def test_reconcile_feedback_by_trace_id_uses_index_wide_merge():
    spark = FakeSpark()
    stream_linking._reconcile_feedback_by_trace_id(
        spark,
        "feedback_tbl",
        "index_tbl",
        lookback_hours=24,
        max_rows=500,
    )
    query = spark.queries[0]
    assert "FROM feedback_tbl" in query
    assert "JOIN index_tbl" in query
    assert "link_reason = 'trace_id_verified_reconciled'" in query
    assert "LIMIT 500" in query


def test_reconcile_feedback_by_tracking_id_uses_index_wide_merge():
    spark = FakeSpark()
    stream_linking._reconcile_feedback_by_tracking_id(
        spark,
        "feedback_tbl",
        "index_tbl",
        lookback_hours=24,
        max_rows=500,
    )
    query = spark.queries[0]
    assert "FROM feedback_tbl" in query
    assert "FROM index_tbl" in query
    assert "tracking_id_match_reconciled" in query
    assert "LIMIT 500" in query


def test_process_batch_processes_empty_batch():
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=True)
    timings = stream_linking.process_batch(
        spark,
        batch_df,
        "feedback_tbl",
        "index_tbl",
        lookback_hours=24,
        max_rows=1000,
    )
    assert batch_df.view_name == "batch_traces"
    assert len(spark.queries) == 5
    assert "merge_index_ms" in timings
    assert "reconcile_feedback_tracking_id_ms" in timings


def test_process_batch_executes_merges():
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=False)
    timings = stream_linking.process_batch(
        spark,
        batch_df,
        "feedback_tbl",
        "index_tbl",
        lookback_hours=24,
        max_rows=1000,
    )
    assert batch_df.view_name == "batch_traces"
    assert len(spark.queries) == 5
    assert "merge_index_ms" in timings
    assert "merge_feedback_trace_id_ms" in timings
    assert "merge_feedback_tracking_id_ms" in timings
    assert "reconcile_feedback_trace_id_ms" in timings
    assert "reconcile_feedback_tracking_id_ms" in timings


def test_parse_args_defaults(monkeypatch):
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
    assert args.reconcile_lookback_hours == 168
    assert args.reconcile_max_rows == 100000


def test_process_microbatch_logs_and_reraises_on_error(monkeypatch):
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=False)
    events = []

    monkeypatch.setattr(stream_linking, "log_event", lambda event: events.append(event))
    monkeypatch.setattr(
        stream_linking,
        "process_batch",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError) as excinfo:
        stream_linking.process_microbatch(
            spark=spark,
            batch_df=batch_df,
            batch_id=7,
            feedback_table="feedback_tbl",
            index_table="index_tbl",
            lookback_hours=24,
            max_rows=1000,
        )
    assert "boom" in str(excinfo.value)

    assert events[0]["event"] == "microbatch_start"
    assert events[0]["batch_id"] == 7
    assert events[-1]["event"] == "microbatch_failed"
    assert events[-1]["batch_id"] == 7
    assert "traceback" in events[-1]
    assert batch_df.persisted is True
    assert batch_df.unpersisted is True


def test_process_microbatch_logs_success(monkeypatch):
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
            "reconcile_feedback_trace_id_ms": 4,
            "reconcile_feedback_tracking_id_ms": 5,
        },
    )

    stream_linking.process_microbatch(
        spark=spark,
        batch_df=batch_df,
        batch_id=8,
        feedback_table="feedback_tbl",
        index_table="index_tbl",
        lookback_hours=24,
        max_rows=1000,
    )

    assert events[0]["event"] == "microbatch_start"
    assert events[-1]["event"] == "microbatch_success"
    assert events[-1]["batch_id"] == 8
    assert "elapsed_ms" in events[-1]
    assert events[-1]["merge_index_ms"] == 1
    assert events[-1]["reconcile_feedback_tracking_id_ms"] == 5
    assert batch_df.persisted is True
    assert batch_df.unpersisted is True


def test_main_does_not_apply_watermark_or_dropduplicates(monkeypatch):
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
    fake_spark = RecordingSpark()

    class _Builder:
        def getOrCreate(self):
            return fake_spark

    monkeypatch.setattr(stream_linking.SparkSession, "builder", _Builder())
    stream_linking.main()

    ops = fake_spark.readStream.operations
    op_names = [entry[0] for entry in ops]
    assert "withWatermark" not in op_names
    assert "dropDuplicates" not in op_names


def test_main_uses_versioned_checkpoint_location(monkeypatch):
    argv = [
        "prog",
        "--feedback-table",
        "f",
        "--trace-table",
        "t",
        "--index-table",
        "i",
        "--checkpoint",
        "dbfs:/tmp/feedback-linking-stream-v3",
    ]
    monkeypatch.setattr("sys.argv", argv)
    fake_spark = RecordingSpark()

    class _Builder:
        def getOrCreate(self):
            return fake_spark

    monkeypatch.setattr(stream_linking.SparkSession, "builder", _Builder())
    stream_linking.main()

    ops = fake_spark.readStream.operations
    checkpoint_ops = [op for op in ops if op[0] == "write_option" and op[1] == "checkpointLocation"]
    assert checkpoint_ops, "checkpointLocation option must be set"
    assert checkpoint_ops[0][2] == (
        "dbfs:/tmp/feedback-linking-stream-v3/stream_query_no_stateful_ops_v1"
    )


def test_tracking_id_source_names_all_supported_paths():
    result = stream_linking._tracking_id_source_names(
        ["trace_id", "tracking_id", "tags", "trace_metadata"]
    )
    assert result == ["tracking_id", "tags", "trace_metadata"]


def test_tracking_id_source_names_without_top_level():
    result = stream_linking._tracking_id_source_names(["trace_id", "tags", "request_time"])
    assert result == ["tags"]


def test_tracking_id_source_names_trace_metadata_only():
    result = stream_linking._tracking_id_source_names(
        ["trace_id", "trace_metadata", "request_time"]
    )
    assert result == ["trace_metadata"]


def test_tracking_id_source_names_no_supported_columns():
    result = stream_linking._tracking_id_source_names(["trace_id", "request_time"])
    assert result == []


class _FakeExpr:
    def __init__(self, value):
        self.value = value

    def __getitem__(self, key):
        return _FakeExpr(f"{self.value}[{key}]")

    def cast(self, dtype):
        return _FakeExpr(f"cast({self.value} as {dtype})")

    def alias(self, name):
        return _FakeExpr(f"{self.value} as {name}")

    def isNotNull(self):
        return _FakeExpr(f"{self.value} is not null")


class _FakeFunctions:
    def __init__(self):
        self.col_calls = []
        self.coalesce_calls = []
        self.lit_calls = []

    def col(self, name):
        self.col_calls.append(name)
        return _FakeExpr(f"col({name})")

    def coalesce(self, *args):
        values = [arg.value for arg in args]
        self.coalesce_calls.append(values)
        return _FakeExpr(f"coalesce({', '.join(values)})")

    def lit(self, value):
        self.lit_calls.append(value)
        return _FakeExpr(f"lit({value})")


def test_build_tracking_id_expr_prefers_single_top_level(monkeypatch):
    fake_f = _FakeFunctions()
    monkeypatch.setattr(stream_linking, "F", fake_f)
    expr = stream_linking._build_tracking_id_expr(["trace_id", "tracking_id", "request_time"])
    assert expr.value == "col(tracking_id)"
    assert fake_f.coalesce_calls == []
    assert fake_f.col_calls == ["tracking_id"]


def test_build_tracking_id_expr_uses_tags_when_top_level_missing(monkeypatch):
    fake_f = _FakeFunctions()
    monkeypatch.setattr(stream_linking, "F", fake_f)
    expr = stream_linking._build_tracking_id_expr(["trace_id", "tags", "request_time"])
    assert expr.value == "col(tags)[tracking_id]"
    assert fake_f.col_calls == ["tags"]
    assert "tracking_id" not in fake_f.col_calls


def test_build_tracking_id_expr_uses_trace_metadata_when_tags_missing(monkeypatch):
    fake_f = _FakeFunctions()
    monkeypatch.setattr(stream_linking, "F", fake_f)
    expr = stream_linking._build_tracking_id_expr(
        ["trace_id", "trace_metadata", "request_time"]
    )
    assert expr.value == "col(trace_metadata)[tracking_id]"
    assert fake_f.col_calls == ["trace_metadata"]


def test_build_tracking_id_expr_coalesces_multiple_sources_in_order(monkeypatch):
    fake_f = _FakeFunctions()
    monkeypatch.setattr(stream_linking, "F", fake_f)
    expr = stream_linking._build_tracking_id_expr(
        ["trace_id", "trace_metadata", "tracking_id", "tags"]
    )
    assert expr.value == (
        "coalesce(col(tracking_id), col(tags)[tracking_id], col(trace_metadata)[tracking_id])"
    )
    assert fake_f.coalesce_calls == [
        ["col(tracking_id)", "col(tags)[tracking_id]", "col(trace_metadata)[tracking_id]"]
    ]


def test_build_tracking_id_expr_returns_typed_null_when_missing(monkeypatch):
    fake_f = _FakeFunctions()
    monkeypatch.setattr(stream_linking, "F", fake_f)
    expr = stream_linking._build_tracking_id_expr(["trace_id", "request_time"])
    assert expr.value == "cast(lit(None) as string)"
    assert fake_f.col_calls == []
    assert fake_f.lit_calls == [None]


def test_main_uses_schema_adaptive_extraction_when_tracking_id_missing(monkeypatch):
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
    fake_spark = RecordingSpark(columns=["trace_id", "tags", "request_time"])
    fake_f = _FakeFunctions()

    class _Builder:
        def getOrCreate(self):
            return fake_spark

    monkeypatch.setattr(stream_linking.SparkSession, "builder", _Builder())
    monkeypatch.setattr(stream_linking, "F", fake_f)
    stream_linking.main()

    assert "tracking_id" not in fake_f.col_calls
    assert "tags" in fake_f.col_calls