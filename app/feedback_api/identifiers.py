"""Validation helpers for Databricks SQL identifiers."""

from __future__ import annotations

import re

_UC_PART = r"[A-Za-z_][A-Za-z0-9_]*"
_UC_TABLE_RE = re.compile(rf"^{_UC_PART}\.{_UC_PART}\.{_UC_PART}$")


def validate_uc_table_name(env_var_name: str, value: str) -> str:
    # validate a Unity Catalog 3 part table name
    
    raw = (value or "").strip()
    if not raw:
        raise RuntimeError(
            f"Invalid {env_var_name}: value is empty; expected catalog.schema.table"
        )
    if not _UC_TABLE_RE.match(raw):
        raise RuntimeError(
            f"Invalid {env_var_name}: {raw!r}; expected Unity Catalog table name "
            "catalog.schema.table using only letters, numbers, and underscores"
        )
    return raw
