"""Schema converter UI target table naming tests."""

from __future__ import annotations


class _Entry:
    def __init__(self, value=""):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value

    def __setitem__(self, key, value):
        setattr(self, key, value)


def _ui(prefix="", suffix="", target_db=""):
    from schema_converter.schema_converter_ui import SchemaConverterUI

    ui = SchemaConverterUI.__new__(SchemaConverterUI)
    ui.target_prefix_entry = _Entry(prefix)
    ui.target_suffix_entry = _Entry(suffix)
    ui.target_database_combo = _Entry(target_db)
    return ui


def test_target_table_name_strips_postgres_schema():
    assert _ui().get_target_table_name("public.orders") == "orders"


def test_target_table_name_strips_quoted_schema():
    assert _ui().get_target_table_name('"public"."orders"') == "orders"


def test_target_table_name_strips_mysql_database():
    assert _ui().get_target_table_name("source_db.orders") == "orders"


def test_target_table_name_applies_prefix_suffix_to_base_name():
    assert _ui(prefix="mig_", suffix="_copy").get_target_table_name(
        "public.orders"
    ) == "mig_orders_copy"


def test_target_table_name_qualifies_with_target_database():
    assert _ui(target_db="test").get_target_table_name(
        "public.orders"
    ) == "test.orders"


def test_target_table_name_qualifies_prefix_suffix_with_target_database():
    assert _ui(prefix="mig_", suffix="_copy", target_db="test").get_target_table_name(
        "public.orders"
    ) == "test.mig_orders_copy"


def test_filter_tables_keeps_only_selected_source_schema():
    ui = _ui()
    ui.source_database_combo = _Entry("app")
    tables = ["public.orders", "app.users", "app.items"]
    assert ui._filter_tables_by_source_namespace(tables) == ["app.users", "app.items"]


def test_filter_tables_no_selection_returns_all():
    ui = _ui()
    ui.source_database_combo = _Entry("")
    tables = ["public.orders", "app.users"]
    assert ui._filter_tables_by_source_namespace(tables) == tables


def test_filter_tables_unqualified_list_unchanged():
    ui = _ui()
    ui.source_database_combo = _Entry("mydb")
    tables = ["orders", "users"]
    assert ui._filter_tables_by_source_namespace(tables) == tables


def test_filter_tables_no_match_falls_back_to_all():
    ui = _ui()
    ui.source_database_combo = _Entry("missing")
    tables = ["public.orders", "app.users"]
    assert ui._filter_tables_by_source_namespace(tables) == tables
