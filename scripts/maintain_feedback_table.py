"""Run maintenance operations for the feedback Delta table"""

import argparse
import json
import time

from app.feedback_api.env_utils import get_env
from app.feedback_api.sql_utils import execute_statement


def _log_event(event: dict) -> None:
    """Emit a JSON log line for maintenance telemetry"""
    print(json.dumps(event, default=str))


def _run_statement(statement: str, warehouse_id: str, dry_run: bool) -> int:
    """Execute a statement and return elapsed milliseconds"""
    start = time.time()
    if dry_run:
        _log_event({"event": "maintenance_dry_run_statement", "statement": statement})
        return int((time.time() - start) * 1000)
    execute_statement(statement, warehouse_id)
    return int((time.time() - start) * 1000)


def parse_args() -> argparse.Namespace:
    """Parse CLI args for maintenance operations"""
    parser = argparse.ArgumentParser(description="Maintain feedback Delta table")
    parser.add_argument("--table", type=str, default=None)
    parser.add_argument("--warehouse-id", type=str, default=None)
    parser.add_argument("--vacuum-retain-hours", type=int, default=168)
    parser.add_argument("--skip-optimize", action="store_true")
    parser.add_argument("--skip-vacuum", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    """Run OPTIMIZE/VACUUM for feedback table maintenance"""
    args = parse_args()
    table = args.table or get_env("FEEDBACK_TABLE")
    warehouse_id = args.warehouse_id or get_env("DATABRICKS_WAREHOUSE_ID")

    _log_event(
        {
            "event": "maintenance_start",
            "table": table,
            "warehouse_id_set": bool(warehouse_id),
            "vacuum_retain_hours": args.vacuum_retain_hours,
            "dry_run": args.dry_run,
        }
    )

    if not args.skip_optimize:
        optimize_statement = f"OPTIMIZE {table}"
        elapsed_ms = _run_statement(optimize_statement, warehouse_id, args.dry_run)
        _log_event(
            {
                "event": "maintenance_optimize_complete",
                "table": table,
                "elapsed_ms": elapsed_ms,
            }
        )

    if not args.skip_vacuum:
        vacuum_statement = f"VACUUM {table} RETAIN {args.vacuum_retain_hours} HOURS"
        elapsed_ms = _run_statement(vacuum_statement, warehouse_id, args.dry_run)
        _log_event(
            {
                "event": "maintenance_vacuum_complete",
                "table": table,
                "retain_hours": args.vacuum_retain_hours,
                "elapsed_ms": elapsed_ms,
            }
        )

    _log_event({"event": "maintenance_complete", "table": table})


if __name__ == "__main__":
    main()