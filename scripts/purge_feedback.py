"""Purge feedback records from the configured table"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from typing import List

from app.feedback_api.env_utils import get_env
from app.feedback_api.sql_utils import execute_statement, sql_literal


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{value} is not a valid integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _build_conditions(args: argparse.Namespace) -> List[str]:
    """Build WHERE clause conditions from CLI arguments"""
    conditions: List[str] = []
    if args.retention_days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=args.retention_days)
        conditions.append(f"ingested_at < {sql_literal(cutoff.isoformat())}")
    if args.tracking_id:
        conditions.append(f"tracking_id = {sql_literal(args.tracking_id)}")
    if args.user_id:
        conditions.append(f"user_id = {sql_literal(args.user_id)}")
    if args.pims:
        conditions.append(f"pims = {sql_literal(args.pims)}")
    return conditions


def _build_scope(args: argparse.Namespace) -> dict:
    """Build a normalized scope object for logging"""
    return {
        "retention_days": args.retention_days,
        "tracking_id": args.tracking_id,
        "user_id": args.user_id,
        "pims": args.pims,
    }


def _extract_row_count(rows) -> int:
    """Extract a count integer from execute_statement response rows"""
    if not rows or not rows[0]:
        return 0
    return int(rows[0][0])


def main() -> None:
    """Parse args and purge feedback rows by condition"""
    parser = argparse.ArgumentParser(description="Purge feedback records")
    parser.add_argument("--retention-days", type=_non_negative_int, default=None)
    parser.add_argument("--tracking-id", type=str, default=None)
    parser.add_argument("--user-id", type=str, default=None)
    parser.add_argument("--pims", type=str, default=None)
    parser.add_argument(
        "--max-delete-rows",
        type=_non_negative_int,
        default=50000,
        help="Safety threshold for maximum rows to delete in one run",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Required for non-dry-run deletes",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conditions = _build_conditions(args)
    if not conditions:
        raise SystemExit("Provide at least one condition to purge")
    if not args.dry_run and not args.force:
        raise SystemExit("Refusing to execute delete without --force")

    table = get_env("FEEDBACK_TABLE")
    warehouse_id = get_env("DATABRICKS_WAREHOUSE_ID")
    where_clause = " AND ".join(conditions)
    scope = _build_scope(args)
    statement = f"DELETE FROM {table} WHERE {where_clause}"
    count_statement = f"SELECT COUNT(*) FROM {table} WHERE {where_clause}"

    estimate_rows = _extract_row_count(execute_statement(count_statement, warehouse_id))
    print(
        json.dumps(
            {
                "event": "purge_plan",
                "table": table,
                "scope": scope,
                "estimated_rows": estimate_rows,
                "max_delete_rows": args.max_delete_rows,
                "dry_run": args.dry_run,
                "statement": statement,
            },
            default=str,
        )
    )

    if args.dry_run:
        print(json.dumps({"event": "purge_dry_run", "statement": statement}))
        return

    if estimate_rows > args.max_delete_rows:
        raise SystemExit(
            "Refusing to delete estimated "
            f"{estimate_rows} rows (> {args.max_delete_rows}). "
            "Narrow scope or raise --max-delete-rows."
        )

    execute_statement(statement, warehouse_id)
    print(
        json.dumps(
            {
                "event": "purge_executed",
                "table": table,
                "estimated_rows": estimate_rows,
            },
            default=str,
        )
    )


if __name__ == "__main__":
    main()