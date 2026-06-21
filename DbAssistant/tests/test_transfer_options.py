"""Unit tests for data-transfer options and per-value policies (G1-G7)."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from schema_converter.transfer_options import (
    RowSkip,
    TransferOptions,
    ValueOverflow,
    build_select_sql,
    merge_options,
    parse_column_map,
    parse_columns,
    transform_value,
)


# --------------------------------------------------------------------------- #
# Parsing helpers (G2)
# --------------------------------------------------------------------------- #
def test_parse_columns():
    assert parse_columns("id, name ,email") == ("id", "name", "email")
    assert parse_columns("") == ()


def test_parse_column_map():
    assert parse_column_map("name:full_name, a:b") == {"name": "full_name", "a": "b"}
    assert parse_column_map('"name:full_name"') == {"name": "full_name"}
    assert parse_column_map("") == {}
    assert parse_column_map("bad_part,ok:fine") == {"ok": "fine"}


# --------------------------------------------------------------------------- #
# SQL builder (G1)
# --------------------------------------------------------------------------- #
def test_build_select_sql_basic():
    assert build_select_sql("t", None, "", None, "PostgreSQL") == "SELECT * FROM t"


def test_build_select_sql_columns_where_limit():
    sql = build_select_sql(
        "t", ("a", "b"), "x = 1", 50, "MySQL"
    )
    assert sql == "SELECT a, b FROM t WHERE x = 1 LIMIT 50"


def test_build_select_sql_oracle_limit_uses_fetch_first():
    sql = build_select_sql("t", None, "", 10, "Oracle")
    assert "FETCH FIRST 10 ROWS ONLY" in sql
    assert "LIMIT" not in sql


def test_build_select_sql_order_by():
    sql = build_select_sql("t", None, "", None, "PostgreSQL", order_by=["id"])
    assert sql.endswith("ORDER BY id")


# --------------------------------------------------------------------------- #
# Truncation / overflow (G4)
# --------------------------------------------------------------------------- #
def _opts(**kw):
    return TransferOptions(**kw)


def test_truncate_policy_shortens_string():
    out = transform_value(
        "abcdefgh",
        col_limit={"char_max": 4, "is_text": True},
        options=_opts(overflow_policy="truncate"),
        target_db_type="MySQL",
        charset="utf-8",
        is_binary=False,
        column_name="c",
    )
    assert out == "abcd"


def test_skip_policy_raises_rowskip():
    with pytest.raises(RowSkip):
        transform_value(
            "abcdefgh",
            col_limit={"char_max": 4, "is_text": True},
            options=_opts(overflow_policy="skip"),
            target_db_type="MySQL",
            charset="utf-8",
            is_binary=False,
            column_name="c",
        )


def test_fail_policy_raises_overflow():
    with pytest.raises(ValueOverflow):
        transform_value(
            "abcdefgh",
            col_limit={"char_max": 4, "is_text": True},
            options=_opts(overflow_policy="fail"),
            target_db_type="MySQL",
            charset="utf-8",
            is_binary=False,
            column_name="c",
        )


def test_numeric_overflow_fail():
    with pytest.raises(ValueOverflow):
        transform_value(
            12345,
            col_limit={"num_precision": 3, "num_scale": 0},
            options=_opts(overflow_policy="fail"),
            target_db_type="MySQL",
            charset="utf-8",
            is_binary=False,
            column_name="n",
        )


# --------------------------------------------------------------------------- #
# NULL / empty / bool (G6)
# --------------------------------------------------------------------------- #
def test_empty_to_null():
    out = transform_value(
        "",
        col_limit=None,
        options=_opts(null_policy="empty_to_null"),
        target_db_type="PostgreSQL",
        charset="utf-8",
        is_binary=False,
    )
    assert out is None


def test_null_to_empty_text_column():
    out = transform_value(
        None,
        col_limit={"is_text": True, "char_max": 10},
        options=_opts(null_policy="null_to_empty"),
        target_db_type="PostgreSQL",
        charset="utf-8",
        is_binary=False,
    )
    assert out == ""


def test_bool_policy_int():
    assert transform_value(
        True, col_limit=None, options=_opts(bool_policy="int"),
        target_db_type="MySQL", charset="utf-8", is_binary=False,
    ) == 1


def test_bool_policy_true_false():
    assert transform_value(
        False, col_limit=None, options=_opts(bool_policy="true_false"),
        target_db_type="MySQL", charset="utf-8", is_binary=False,
    ) == "false"


def test_bool_auto_oracle():
    assert transform_value(
        True, col_limit=None, options=_opts(bool_policy="auto"),
        target_db_type="Oracle", charset="utf-8", is_binary=False,
    ) == 1


# --------------------------------------------------------------------------- #
# Timezone (G7)
# --------------------------------------------------------------------------- #
def test_timezone_naive_strips_tzinfo():
    aware = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    out = transform_value(
        aware, col_limit=None, options=_opts(timezone_policy="naive"),
        target_db_type="MySQL", charset="utf-8", is_binary=False,
    )
    assert out.tzinfo is None


def test_timezone_utc_converts():
    tz = timezone(timedelta(hours=5, minutes=30))
    aware = datetime(2024, 1, 1, 12, 0, tzinfo=tz)
    out = transform_value(
        aware, col_limit=None, options=_opts(timezone_policy="utc"),
        target_db_type="PostgreSQL", charset="utf-8", is_binary=False,
    )
    assert out.utcoffset() == timedelta(0)
    assert out.hour == 6  # 12:00 +5:30 -> 06:30 UTC ... hour 6


def test_decimal_passthrough_when_in_range():
    out = transform_value(
        Decimal("12.50"),
        col_limit={"num_precision": 10, "num_scale": 2},
        options=_opts(overflow_policy="fail"),
        target_db_type="MySQL", charset="utf-8", is_binary=False,
    )
    assert out == Decimal("12.50")


# --------------------------------------------------------------------------- #
# merge / config
# --------------------------------------------------------------------------- #
def test_merge_options_override_wins():
    base = TransferOptions(overflow_policy="fail", null_policy="keep")
    override = TransferOptions(overflow_policy="truncate")
    merged = merge_options(base, override)
    assert merged.overflow_policy == "truncate"
    assert merged.null_policy == "keep"


def test_invalid_policy_falls_back_to_default():
    opt = TransferOptions(overflow_policy="nonsense", bool_policy="weird")
    assert opt.overflow_policy == "fail"
    assert opt.bool_policy == "auto"
