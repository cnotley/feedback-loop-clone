"""Test configuration and shared pytest helpers."""

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REQUIRED_DATABRICKS_ENV = [
    "DATABRICKS_HOST",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_WAREHOUSE_ID",
]


def _missing_databricks_env() -> list[str]:
    return [name for name in REQUIRED_DATABRICKS_ENV if not os.environ.get(name)]


def require_databricks_env() -> None:
    missing = _missing_databricks_env()
    if missing:
        pytest.skip(f"Databricks credentials missing: {', '.join(missing)}")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: requires real Databricks credentials and warehouse access")