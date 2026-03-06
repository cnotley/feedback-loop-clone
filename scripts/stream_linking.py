"""Structured streaming job to link feedback to traces."""

from __future__ import annotations

import argparse
import json
import logging
import traceback
from time import perf_counter

try:
    from pyspark.sql import SparkSession, functions as F
except ModuleNotFoundError as exc:
    _PYSPARK_IMPORT_ERROR = exc

    class _Expression:
        """Small expression shim used by tests when pyspark is unavailable."""

        def __init__(self, value):
            self.value = value

        def __getitem__(self, key):
            return _Expression(f"{self.value}[{key}]")

        def cast(self, dtype):
            return _Expression(f"cast({self.value} as {dtype})")

        def alias(self, name):
            return _Expression(f"{self.value} as {name}")

        def isNotNull(self):
            return _Expression(f"{self.value} is not null")

    class _FallbackFunctions:
        """Subset of pyspark.sql.functions used by unit tests."""

        @staticmethod
        def col(name):
            return _Expression(f"col({name})")

        @staticmethod
        def coalesce(*args):
            values = [arg.value for arg in args]
            return _Expression(f"coalesce({', '.join(values)})")

        @staticmethod
        def lit(value):
            return _Expression(f"lit({value})")

    class _SparkSessionProxy:
        """Raise the original pyspark import error when Spark is actually needed."""

        class builder:  # pylint: disable=too-few-public-methods,invalid-name
            """Builder proxy used in tests via monkeypatch."""

            @staticmethod
            def getOrCreate():
                raise ModuleNotFoundError("pyspark is unavailable in this environment") from _PYSPARK_IMPORT_ERROR

    SparkSession = _SparkSessionProxy
    F = _FallbackFunctions()

STREAMING_QUERY_VERSION = "no_stateful_ops_v1"


logger = logging.getLogger("feedback_linking_stream")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)


def log_event(event: dict) -> None:
    """Emit a structured JSON log line."""
    logger.info(json.dumps(event, default=str))


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the streaming job."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--feedback-table", required=True)
    parser.add_argument("--trace-table", required=True)
    parser.add_argument("--index-table", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--query-version", default=STREAMING_QUERY_VERSION)
    parser.add_argument("--trigger-seconds", type=int, default=60)
    parser.add_argument("--reconcile-lookback-hours", type=int, default=168)
    parser.add_argument("--reconcile-max-rows", type=int, default=100000)
    return parser.parse_args()


def _checkpoint_location(base_checkpoint: str, query_version: str) -> str:
    base = base_checkpoint.rstrip("/")
    return f"{base}/stream_query_{query_version}"


def _tracking_id_source_names(columns: list[str]) -> list[str]:
    available = set(columns)
    source_names: list[str] = []
    if "tracking_id" in available:
        source_names.append("tracking_id")
    if "tags" in available:
        source_names.append("tags")
    if "trace_metadata" in available:
        source_names.append("trace_metadata")
    return source_names


def _build_tracking_id_expr(columns: list[str]):
    source_names = _tracking_id_source_names(columns)
    candidates = []
    for source_name in source_names:
        if source_name == "tracking_id":
            candidates.append(F.col("tracking_id"))
        else:
            candidates.append(F.col(source_name)["tracking_id"])
    if not candidates:
        return F.lit(None).cast("string")
    if len(candidates) == 1:
        return candidates[0]
    return F.coalesce(*candidates)


def _create_index_table(spark: SparkSession, index_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {index_table} (
          trace_id STRING,
          tracking_id STRING,
          request_time TIMESTAMP
        )
        USING DELTA
        """
    )


def _merge_index_table(spark: SparkSession, index_table: str, view_name: str) -> None:
    spark.sql(
        f"""
        MERGE INTO {index_table} AS t
    USING {view_name} AS s
        ON t.trace_id = s.trace_id
        WHEN MATCHED THEN UPDATE SET
          t.tracking_id = s.tracking_id,
          t.request_time = s.request_time
        WHEN NOT MATCHED THEN INSERT (trace_id, tracking_id, request_time)
        VALUES (s.trace_id, s.tracking_id, s.request_time)
        """
    )


def _merge_feedback_by_trace_id(
    spark: SparkSession, feedback_table: str, view_name: str
) -> None:
    spark.sql(
        f"""
        MERGE INTO {feedback_table} AS f
        USING {view_name} AS s
        ON f.trace_id = s.trace_id AND f.link_mode = 'no_match'
        WHEN MATCHED THEN UPDATE SET
          f.link_mode = 'trace_id_match',
          f.link_target_trace_id = s.trace_id,
          f.link_match_count = 1,
          f.link_reason = 'trace_id_verified'
        """
    )


def _merge_feedback_by_tracking_id(
    spark: SparkSession, feedback_table: str, index_table: str, view_name: str
) -> None:
    spark.sql(
        f"""
        WITH batch_tracking_ids AS (
          SELECT DISTINCT tracking_id
          FROM {view_name}
          WHERE tracking_id IS NOT NULL
        ),
        candidates AS (
          SELECT
            i.tracking_id AS tracking_id,
            COUNT(1) AS match_count,
            max_by(i.trace_id, i.request_time) AS latest_trace_id
          FROM {index_table} AS i
          INNER JOIN batch_tracking_ids AS b
            ON i.tracking_id = b.tracking_id
          GROUP BY i.tracking_id
        )
        MERGE INTO {feedback_table} AS f
        USING (
          SELECT
            f.feedback_id AS feedback_id,
            c.latest_trace_id AS link_target_trace_id,
            c.match_count AS link_match_count,
            CASE WHEN c.match_count = 1
              THEN 'tracking_id_exact_match'
              ELSE 'tracking_id_recent_match'
            END AS link_mode
          FROM {feedback_table} AS f
          INNER JOIN candidates AS c
            ON f.tracking_id = c.tracking_id
          WHERE f.link_mode = 'no_match'
        ) AS s
        ON f.feedback_id = s.feedback_id
        WHEN MATCHED THEN UPDATE SET
          f.link_mode = s.link_mode,
          f.link_target_trace_id = s.link_target_trace_id,
          f.link_match_count = s.link_match_count,
          f.link_reason = 'tracking_id_match'
        """
    )


def _reconcile_feedback_by_trace_id(
    spark: SparkSession,
    feedback_table: str,
    index_table: str,
    lookback_hours: int,
    max_rows: int,
) -> None:
    spark.sql(
        f"""
        WITH unmatched AS (
          SELECT feedback_id, trace_id
          FROM {feedback_table}
          WHERE link_mode = 'no_match'
            AND trace_id IS NOT NULL
            AND ingested_at >= current_timestamp() - INTERVAL {lookback_hours} HOURS
          ORDER BY ingested_at ASC
          LIMIT {max_rows}
        )
        MERGE INTO {feedback_table} AS f
        USING (
          SELECT u.feedback_id AS feedback_id, i.trace_id AS link_target_trace_id
          FROM unmatched AS u
          INNER JOIN {index_table} AS i
            ON u.trace_id = i.trace_id
        ) AS s
        ON f.feedback_id = s.feedback_id
        WHEN MATCHED THEN UPDATE SET
          f.link_mode = 'trace_id_match',
          f.link_target_trace_id = s.link_target_trace_id,
          f.link_match_count = 1,
          f.link_reason = 'trace_id_verified_reconciled'
        """
    )


def _reconcile_feedback_by_tracking_id(
    spark: SparkSession,
    feedback_table: str,
    index_table: str,
    lookback_hours: int,
    max_rows: int,
) -> None:
    spark.sql(
        f"""
        WITH unmatched AS (
          SELECT feedback_id, tracking_id
          FROM {feedback_table}
          WHERE link_mode = 'no_match'
            AND tracking_id IS NOT NULL
            AND ingested_at >= current_timestamp() - INTERVAL {lookback_hours} HOURS
          ORDER BY ingested_at ASC
          LIMIT {max_rows}
        ),
        candidates AS (
          SELECT
            i.tracking_id AS tracking_id,
            COUNT(1) AS link_match_count,
            max_by(i.trace_id, i.request_time) AS link_target_trace_id
          FROM {index_table} AS i
          INNER JOIN (
            SELECT DISTINCT tracking_id FROM unmatched
          ) AS u
            ON i.tracking_id = u.tracking_id
          GROUP BY i.tracking_id
        )
        MERGE INTO {feedback_table} AS f
        USING (
          SELECT
            u.feedback_id AS feedback_id,
            c.link_target_trace_id AS link_target_trace_id,
            c.link_match_count AS link_match_count,
            CASE
              WHEN c.link_match_count = 1 THEN 'tracking_id_exact_match'
              ELSE 'tracking_id_recent_match'
            END AS link_mode
          FROM unmatched AS u
          INNER JOIN candidates AS c
            ON u.tracking_id = c.tracking_id
        ) AS s
        ON f.feedback_id = s.feedback_id
        WHEN MATCHED THEN UPDATE SET
          f.link_mode = s.link_mode,
          f.link_target_trace_id = s.link_target_trace_id,
          f.link_match_count = s.link_match_count,
          f.link_reason = 'tracking_id_match_reconciled'
        """
    )


def process_batch(
    spark: SparkSession,
    batch_df,
    feedback_table: str,
    index_table: str,
    lookback_hours: int,
    max_rows: int,
) -> dict[str, int]:
    """Execute linking and reconciliation merge operations for one micro-batch."""
    batch_df.createOrReplaceGlobalTempView("batch_traces")
    view_name = "global_temp.batch_traces"

    timings_ms: dict[str, int] = {}

    start = perf_counter()
    _merge_index_table(spark, index_table, view_name)
    timings_ms["merge_index_ms"] = int((perf_counter() - start) * 1000)

    start = perf_counter()
    _merge_feedback_by_trace_id(spark, feedback_table, view_name)
    timings_ms["merge_feedback_trace_id_ms"] = int((perf_counter() - start) * 1000)

    start = perf_counter()
    _merge_feedback_by_tracking_id(
        spark, feedback_table, index_table, view_name
    )
    timings_ms["merge_feedback_tracking_id_ms"] = int((perf_counter() - start) * 1000)

    start = perf_counter()
    _reconcile_feedback_by_trace_id(
        spark, feedback_table, index_table, lookback_hours, max_rows
    )
    timings_ms["reconcile_feedback_trace_id_ms"] = int((perf_counter() - start) * 1000)

    start = perf_counter()
    _reconcile_feedback_by_tracking_id(
        spark, feedback_table, index_table, lookback_hours, max_rows
    )
    timings_ms["reconcile_feedback_tracking_id_ms"] = int((perf_counter() - start) * 1000)

    return timings_ms


def _get_input_row_count(batch_df) -> int | None:
    try:
        return int(batch_df.count())
    except Exception:  # pylint: disable=broad-exception-caught
        return None


def process_microbatch(
    *,
    spark: SparkSession,
    batch_df,
    batch_id: int,
    feedback_table: str,
    index_table: str,
    lookback_hours: int,
    max_rows: int,
) -> None:
    """Wrap foreachBatch work with structured logs and fail-fast behavior."""
    batch_start = perf_counter()
    row_count = None
    persisted = False
    cached_batch_df = batch_df
    try:
        cached_batch_df = batch_df.persist()
        persisted = True
        row_count = _get_input_row_count(cached_batch_df)
        log_event(
            {
                "event": "microbatch_start",
                "batch_id": batch_id,
                "input_row_count": row_count,
            }
        )
        timings_ms = process_batch(
            spark,
            cached_batch_df,
            feedback_table,
            index_table,
            lookback_hours,
            max_rows,
        )
        log_event(
            {
                "event": "microbatch_success",
                "batch_id": batch_id,
                "input_row_count": row_count,
                "elapsed_ms": int((perf_counter() - batch_start) * 1000),
                **timings_ms,
            }
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        log_event(
            {
                "event": "microbatch_failed",
                "batch_id": batch_id,
                "input_row_count": row_count,
                "elapsed_ms": int((perf_counter() - batch_start) * 1000),
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
        raise
    finally:
        if persisted:
            cached_batch_df.unpersist()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

    _create_index_table(spark, args.index_table)

    trace_source = (
        spark.readStream
        .option("ignoreChanges", "true")
        .option("ignoreDeletes", "true")
        .table(args.trace_table)
    )

    trace_stream = (
        trace_source
        .select(
            F.col("trace_id").alias("trace_id"),
            _build_tracking_id_expr(trace_source.columns).alias("tracking_id"),
            F.col("request_time").alias("request_time"),
        )
        .where(F.col("trace_id").isNotNull())
    )

    checkpoint_location = _checkpoint_location(args.checkpoint, args.query_version)
    print(f"Using checkpointLocation={checkpoint_location}")

    (
        trace_stream.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: process_microbatch(
                spark=spark,
                batch_df=batch_df,
                batch_id=batch_id,
                feedback_table=args.feedback_table,
                index_table=args.index_table,
                lookback_hours=args.reconcile_lookback_hours,
                max_rows=args.reconcile_max_rows,
            )
        )
        .option("checkpointLocation", checkpoint_location)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()