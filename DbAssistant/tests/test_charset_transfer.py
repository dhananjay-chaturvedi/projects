"""Tests for charset-aware data transfer helpers."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from schema_converter.charset import convert_cell_value, get_conversion_charset


def test_convert_cell_value_decodes_text_bytes():
    raw = "こんにちは".encode("utf-8")
    assert convert_cell_value(raw, charset="utf-8", is_binary=False) == "こんにちは"


def test_convert_cell_value_keeps_binary_bytes():
    raw = b"\x00\x01\xff"
    assert convert_cell_value(raw, charset="utf-8", is_binary=True) == raw


def test_convert_cell_value_oracle_bool():
    assert convert_cell_value(True, charset="utf-8", target_db_type="Oracle") == 1
    assert convert_cell_value(False, charset="utf-8", target_db_type="MySQL") is False


def test_convert_cell_value_decimal_and_datetime():
    assert convert_cell_value(Decimal("1.5"), charset="utf-8") == Decimal("1.5")
    dt = datetime(2026, 1, 2, 3, 4, 5)
    assert convert_cell_value(dt, charset="utf-8") == dt


def test_get_conversion_charset_default(monkeypatch):
    monkeypatch.setattr(
        "schema_converter.module_config.get",
        lambda section, key, default="": default,
    )
    assert get_conversion_charset() == "utf-8"
