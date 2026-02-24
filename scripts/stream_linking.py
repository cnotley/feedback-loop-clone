"""Structured streaming job to link feedback to traces"""

import argparse
import json
import logging
import time
import traceback

from pyspark.sql import SparkSession, functions as F


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
    """Parse CLI arguments for the stream linking job"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--feedback-table", required=True)
    parser.add_argument("--trace-table", required=True)
    parser.add_argument("--index-table", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--trigger-seconds", type=int, default=60)
    parser.add_argument("--watermark-minutes", type=int, default=15)
    return parser.parse_args()


def _create_index_table(spark: SparkSession, index_table: str) -> None:
    """Create the index table if it does not exist"""
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
    """Upsert batch trace records into the index table"""
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
    """Link feedback rows using trace_id matches"""
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
    """Link feedback rows using tracking_id matches"""
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


def process_batch(
    spark: SparkSession,
    batch_df,
    feedback_table: str,
    index_table: str,
) -> dict:
    """Process a micro batch for linking updates.

    Returns:
        Dict containing timing information for merge operations.
    """
    batch_df.createOrReplaceGlobalTempView("batch_traces")
    view_name = "global_temp.batch_traces"

    timings_ms: dict[str, int] = {}

    start = time.time()
    _merge_index_table(spark, index_table, view_name)
    timings_ms["merge_index_ms"] = int((time.time() - start) * 1000)

    start = time.time()
    _merge_feedback_by_trace_id(spark, feedback_table, view_name)
    timings_ms["merge_feedback_trace_id_ms"] = int((time.time() - start) * 1000)

    start = time.time()
    _merge_feedback_by_tracking_id(spark, feedback_table, index_table, view_name)
    timings_ms["merge_feedback_tracking_id_ms"] = int((time.time() - start) * 1000)

    return timings_ms


def process_microbatch(
    *,
    spark: SparkSession,
    batch_df,
    batch_id: int,
    feedback_table: str,
    index_table: str,
) -> None:
    """Wrapper for foreachBatch with logging and fail-fast behavior."""
    batch_start = time.time()
    row_count = None
    try:
        row_count = int(batch_df.count())
    except Exception:  # pylint: disable=broad-exception-caught
        row_count = None

    log_event(
        {
            "event": "microbatch_start",
            "batch_id": batch_id,
            "input_row_count": row_count,
        }
    )

    try:
        timings_ms = process_batch(spark, batch_df, feedback_table, index_table)
        log_event(
            {
                "event": "microbatch_success",
                "batch_id": batch_id,
                "input_row_count": row_count,
                "elapsed_ms": int((time.time() - batch_start) * 1000),
                **timings_ms,
            }
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        log_event(
            {
                "event": "microbatch_failed",
                "batch_id": batch_id,
                "input_row_count": row_count,
                "elapsed_ms": int((time.time() - batch_start) * 1000),
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )

        raise


def main() -> None:
    """Start the structured streaming job"""
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")

    _create_index_table(spark, args.index_table)

    trace_stream = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .option("ignoreDeletes", "true")
        .table(args.trace_table)
        .select(
            F.col("trace_id").alias("trace_id"),
            F.col("tags")["tracking_id"].alias("tracking_id"),
            F.col("request_time").alias("request_time"),
        )
        .where(F.col("trace_id").isNotNull())
        .withWatermark("request_time", f"{args.watermark_minutes} minutes")
        .dropDuplicates(["trace_id"])
    )

    def process_batch_wrapper(batch_df, batch_id) -> None:
        """Delegate batch processing for each micro-batch."""
        process_microbatch(
            spark=spark,
            batch_df=batch_df,
            batch_id=batch_id,
            feedback_table=args.feedback_table,
            index_table=args.index_table,
        )

    (
        trace_stream.writeStream
        .foreachBatch(process_batch_wrapper)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()