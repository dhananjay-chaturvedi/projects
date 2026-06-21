"""Tests for user-defined schema type mapping overrides."""

from __future__ import annotations

import pytest

from schema_converter.type_overrides import (
    apply_type_override,
    parse_base_type,
    parse_type_overrides,
    resolve_type_overrides,
)
from schema_converter.schema_full import _convert_column


def test_parse_type_overrides_quoted_and_unquoted():
    assert parse_type_overrides('"varchar2:text, int:decimal"') == {
        "VARCHAR2": "TEXT",
        "INT": "DECIMAL",
    }
    assert parse_type_overrides("varchar2:text") == {"VARCHAR2": "TEXT"}


def test_apply_type_override_drops_size_for_text():
    assert apply_type_override("varchar2(50)", "TEXT") == "TEXT"
    assert apply_type_override("varchar2(50)", "VARCHAR") == "VARCHAR(50)"


def test_apply_type_override_int_to_decimal_default_precision():
    assert apply_type_override("int", "DECIMAL") == "DECIMAL(10,0)"
    assert apply_type_override("number(12,2)", "DECIMAL") == "DECIMAL(12,2)"


def test_convert_column_applies_override():
    col = {"name": "note", "type": "varchar2(100)", "nullable": True}
    out = _convert_column(
        col,
        "Oracle",
        "PostgreSQL",
        type_overrides={"VARCHAR2": "TEXT"},
    )
    assert out["type"] == "TEXT"
    assert out["source_type"] == "varchar2(100)"


def test_resolve_type_overrides_merges_config(monkeypatch):
    monkeypatch.setattr(
        "schema_converter.module_config.get",
        lambda section, key, default="": "int:decimal" if key == "type_overrides" else default,
    )
    merged = resolve_type_overrides("varchar2:text")
    assert merged["INT"] == "DECIMAL"
    assert merged["VARCHAR2"] == "TEXT"
