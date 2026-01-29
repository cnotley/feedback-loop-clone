from types import SimpleNamespace

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

    def createOrReplaceGlobalTempView(self, name: str):
        self.view_name = name


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


def test_process_batch_skips_empty():
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=True)
    stream_linking.process_batch(spark, batch_df, "feedback_tbl", "index_tbl")
    assert batch_df.view_name is None
    assert spark.queries == []


def test_process_batch_executes_merges():
    spark = FakeSpark()
    batch_df = FakeBatchDF(empty=False)
    stream_linking.process_batch(spark, batch_df, "feedback_tbl", "index_tbl")
    assert batch_df.view_name == "batch_traces"
    assert len(spark.queries) == 3


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
    assert args.watermark_minutes == 15
