"""Purge feedback records from the configured table."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import List

from app.feedback_api.env_utils import get_env
from app.feedback_api.sql_utils import execute_statement, sql_literal

def _build_conditions(args: argparse.Namespace) -> List[str]:
    """Build WHERE clause conditions from CLI arguments."""
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


def main() -> None:
    """Parse args and purge feedback rows by condition."""
    parser = argparse.ArgumentParser(description="Purge feedback records.")
    parser.add_argument("--retention-days", type=int, default=None)
    parser.add_argument("--tracking-id", type=str, default=None)
    parser.add_argument("--user-id", type=str, default=None)
    parser.add_argument("--pims", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conditions = _build_conditions(args)
    if not conditions:
        raise SystemExit("Provide at least one condition to purge.")

    table = get_env("FEEDBACK_TABLE")
    warehouse_id = get_env("DATABRICKS_WAREHOUSE_ID")
    where_clause = " AND ".join(conditions)
    statement = f"DELETE FROM {table} WHERE {where_clause}"

    if args.dry_run:
        print(statement)
        return

    execute_statement(statement, warehouse_id)


if __name__ == "__main__":
    main()
