import argparse
import json

from pyspark.sql import SparkSession

try:
    from scripts.stream_linking import (
        _reconcile_feedback_by_trace_id,
        _reconcile_feedback_by_tracking_id,
    )
except ModuleNotFoundError:
    from stream_linking import (
        _reconcile_feedback_by_trace_id,
        _reconcile_feedback_by_tracking_id,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Standalone feedback link reconcile sweep.")
    parser.add_argument("--feedback-table", required=True)
    parser.add_argument("--index-table", required=True)
    parser.add_argument("--lookback-hours", type=int, default=720)
    parser.add_argument("--max-rows", type=int, default=1000000)
    return parser.parse_args()


def _log_event(event: dict) -> None:
    print(json.dumps(event, default=str))


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    _log_event(
        {
            "event": "reconcile_sweep_start",
            "feedback_table": args.feedback_table,
            "index_table": args.index_table,
            "lookback_hours": args.lookback_hours,
            "max_rows": args.max_rows,
        }
    )

    _reconcile_feedback_by_trace_id(
        spark,
        args.feedback_table,
        args.index_table,
        args.lookback_hours,
        args.max_rows,
    )
    _reconcile_feedback_by_tracking_id(
        spark,
        args.feedback_table,
        args.index_table,
        args.lookback_hours,
        args.max_rows,
    )

    _log_event(
        {
            "event": "reconcile_sweep_complete",
            "feedback_table": args.feedback_table,
            "index_table": args.index_table,
        }
    )


if __name__ == "__main__":
    main()