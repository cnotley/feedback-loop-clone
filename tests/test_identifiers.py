"""Tests for identifier validation helpers."""

import pytest

from app.feedback_api.identifiers import validate_uc_table_name


@pytest.mark.parametrize(
    "value",
    [
        "llm_sandbox.voice_to_soap.feedback_ingest",
        "a.b.c",
        "A1._b2.C_3",
    ],
)
def test_validate_uc_table_name_accepts_valid(value: str) -> None:
    """Valid UC table names should pass."""
    assert validate_uc_table_name("FEEDBACK_TABLE", value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "schema.table",
        "catalog.schema.table.extra",
        "catalog..table",
        "catalog.schema.ta-ble",
        "catalog.schema.`table`",
        "catalog.schema.table;",
        "catalog.schema.table -- comment",
        "catalog.schema.table/*x*/",
        "catalog.schema.table with space",
    ],
)
def test_validate_uc_table_name_rejects_invalid(value: str) -> None:
    """Invalid UC table names should fail with a clear error."""
    with pytest.raises(RuntimeError) as excinfo:
        validate_uc_table_name("FEEDBACK_TABLE", value)
    assert "FEEDBACK_TABLE" in str(excinfo.value)
    assert "catalog.schema.table" in str(excinfo.value)