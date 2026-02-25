"""Tests for the stream linking job SQL and batching"""
# pylint: disable=invalid-name,protected-access,duplicate-code

from scripts import stream_linking


class FakeSpark:  # pylint: disable=too-few-public-methods
    def __init__(self):
        self.queries = []

    def sql(self, query: str):
        self.queries.append(query)


class FakeRDD:  # pylint: disable=too-few-public-methods
    def __init__(self, empty: bool):
        self._empty = empty

    def isEmpty(self):
        return self._empty


class FakeBatchDF:  # pylint: disable=too-few-public-methods
    def __init__(self, empty: bool = False):
        self.rdd = FakeRDD(empty)
        self.view_name = None

    def createOrReplaceGlobalTempView(self, name: str):
        self.view_name = name


class RecordingReadStream:  # pylint: disable=too-few-public-methods
    def __init__(self):
        self.operations = []

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


class RecordingWriteStream:  # pylint: disable=too-few-public-methods
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


class RecordingSpark:  # pylint: disable=too-few-public-methods
    def __init__(self):
        self.readStream = RecordingReadStream()
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


def test_process_batch_skips_empty():
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=True)
    stream_linking.process_batch(
        spark,
        batch_df,
        "feedback_tbl",
        "index_tbl",
        lookback_hours=24,
        max_rows=1000,
    )

    assert batch_df.view_name == "batch_traces"
    assert len(spark.queries) == 5


def test_process_batch_executes_merges():
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=False)
    stream_linking.process_batch(
        spark,
        batch_df,
        "feedback_tbl",
        "index_tbl",
        lookback_hours=24,
        max_rows=1000,
    )
    assert batch_df.view_name == "batch_traces"
    assert len(spark.queries) == 5


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

    class _Builder:  # pylint: disable=too-few-public-methods
        def getOrCreate(self):
            return fake_spark

    monkeypatch.setattr(stream_linking.SparkSession, "builder", _Builder())
    stream_linking.main()

    ops = fake_spark.readStream.operations
    op_names = [entry[0] for entry in ops]
    assert "withWatermark" not in op_names
    assert "dropDuplicates" not in op_names