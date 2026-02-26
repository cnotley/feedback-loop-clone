from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--feedback-table", required=True)
    parser.add_argument("--trace-table", required=True)
    parser.add_argument("--index-table", required=True)
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--trigger-seconds", type=int, default=60)
    parser.add_argument("--reconcile-lookback-hours", type=int, default=168)
    parser.add_argument("--reconcile-max-rows", type=int, default=100000)
    return parser.parse_args()


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
) -> None:
    batch_df.createOrReplaceGlobalTempView("batch_traces")
    view_name = "global_temp.batch_traces"
    _merge_index_table(spark, index_table, view_name)
    _merge_feedback_by_trace_id(spark, feedback_table, view_name)
    _merge_feedback_by_tracking_id(
        spark, feedback_table, index_table, view_name
    )
    _reconcile_feedback_by_trace_id(
        spark, feedback_table, index_table, lookback_hours, max_rows
    )
    _reconcile_feedback_by_tracking_id(
        spark, feedback_table, index_table, lookback_hours, max_rows
    )


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

    def process_batch_wrapper(batch_df, _batch_id) -> None:
        process_batch(
            spark,
            batch_df,
            args.feedback_table,
            args.index_table,
            args.reconcile_lookback_hours,
            args.reconcile_max_rows,
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