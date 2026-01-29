from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from typing import List

from databricks.sdk import WorkspaceClient

DEFAULT_WAIT_TIMEOUT = "10s"
DEFAULT_RETRIES = 2
DEFAULT_BACKOFF_SECONDS = 0.5


def _get_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _execute(statement: str, warehouse_id: str) -> None:
    client = WorkspaceClient()
    last_exc: Exception | None = None
    for attempt in range(DEFAULT_RETRIES + 1):
        try:
            resp = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
                wait_timeout=DEFAULT_WAIT_TIMEOUT,
            )
            status = getattr(resp, "status", None)
            if not status:
                raise RuntimeError("SQL execution failed: missing status")
            state = getattr(status, "state", None)
            state_value = getattr(state, "value", state)
            if state_value != "SUCCEEDED":
                err = getattr(status, "error", None)
                err_message = getattr(status, "error_message", None)
                err_code = getattr(status, "error_code", None)
                raise RuntimeError(
                    "SQL execution failed: "
                    f"state={state_value} error_code={err_code} "
                    f"error={err} error_message={err_message}"
                )
            return
        except Exception as exc:
            last_exc = exc
            if attempt >= DEFAULT_RETRIES:
                break
            time.sleep(DEFAULT_BACKOFF_SECONDS * (2**attempt))
    raise RuntimeError(f"SQL execution failed after retries: {last_exc}")


def _build_conditions(args: argparse.Namespace) -> List[str]:
    conditions: List[str] = []
    if args.retention_days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=args.retention_days)
        conditions.append(f"ingested_at < {_sql_literal(cutoff.isoformat())}")
    if args.tracking_id:
        conditions.append(f"tracking_id = {_sql_literal(args.tracking_id)}")
    if args.user_id:
        conditions.append(f"user_id = {_sql_literal(args.user_id)}")
    if args.pims:
        conditions.append(f"pims = {_sql_literal(args.pims)}")
    return conditions


def main() -> None:
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

    table = _get_env("FEEDBACK_TABLE")
    warehouse_id = _get_env("DATABRICKS_WAREHOUSE_ID")
    where_clause = " AND ".join(conditions)
    statement = f"DELETE FROM {table} WHERE {where_clause}"

    if args.dry_run:
        print(statement)
        return

    _execute(statement, warehouse_id)


if __name__ == "__main__":
    main()
