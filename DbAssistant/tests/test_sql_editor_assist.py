"""Unit tests for SQL editor assist helpers (no Tk mainloop)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from common.ui.tk.sql_editor_assist import (
    SqlCompleter,
    SqlFormatter,
    SqlHighlighter,
    current_statement,
    keyword_tag,
    keywords_for_dialect,
    parse_table_aliases,
    quote_table_name,
    statement_at_offset,
    token_before_cursor,
)


class TestKeywords:
    def test_base_keywords_contain_select(self):
        kws = keywords_for_dialect(None)
        assert "SELECT" in kws

    def test_mysql_dialect_extras(self):
        kws = keywords_for_dialect("MySQL")
        assert "AUTO_INCREMENT" in kws
        assert "SELECT" in kws

    def test_unknown_dialect_returns_base_only(self):
        kws = keywords_for_dialect("UnknownDB")
        assert "SELECT" in kws
        assert "AUTO_INCREMENT" not in kws


class TestAliasParsing:
    def test_from_with_alias(self):
        sql = "SELECT * FROM orders o WHERE o.id = 1"
        aliases = parse_table_aliases(sql)
        assert aliases["o"] == "orders"
        assert aliases["orders"] == "orders"

    def test_join_with_as(self):
        sql = "SELECT * FROM orders o JOIN users AS u ON o.user_id = u.id"
        aliases = parse_table_aliases(sql)
        assert aliases["o"] == "orders"
        assert aliases["u"] == "users"

    def test_schema_qualified_table(self):
        sql = "SELECT * FROM public.accounts a"
        aliases = parse_table_aliases(sql)
        assert aliases["a"] == "public.accounts"
        assert aliases["accounts"] == "public.accounts"


class TestStatementAtOffset:
    def test_first_statement(self):
        assert statement_at_offset("SELECT 1; SELECT 2", 3) == "SELECT 1"

    def test_second_statement(self):
        assert statement_at_offset("SELECT 1; SELECT 2", 12) == "SELECT 2"

    def test_no_semicolon(self):
        assert statement_at_offset("SELECT a FROM t", 5) == "SELECT a FROM t"


class TestKeywordsForDialectCache:
    def test_same_object_returned(self):
        # lru_cache should return the identical frozenset instance.
        a = keywords_for_dialect("MySQL")
        b = keywords_for_dialect("MySQL")
        assert a is b


class TestMaxTablesCap:
    def test_default_floor(self):
        c = SqlCompleter(
            MagicMock(),
            get_db_manager=lambda: None,
            get_connection_name=lambda: "c",
            get_db_type=lambda: "MySQL",
            max_tables=0,
        )
        assert c.max_tables == 5000

    @patch("common.ui.tk.sql_editor_assist.DatabaseRegistry.execute_operation")
    def test_list_tables_capped(self, mock_exec):
        mock_exec.return_value = [f"t{i}" for i in range(100)]
        c = SqlCompleter(
            MagicMock(),
            get_db_manager=lambda: None,
            get_connection_name=lambda: "c",
            get_db_type=lambda: "PostgreSQL",
            max_tables=10,
        )
        mgr = MagicMock()
        mgr.db_type = "PostgreSQL"
        mgr.conn = object()
        result = c._list_tables_locked(mgr)
        assert len(result) == 10


class TestCurrentStatement:
    def test_single_statement(self):
        text = "SELECT 1;\nSELECT 2;"
        assert "SELECT 1" in current_statement(text, "1.5")

    def test_second_statement(self):
        text = "SELECT 1;\nSELECT 2;"
        stmt = current_statement(text, "2.5")
        assert "SELECT 2" in stmt
        assert "SELECT 1" not in stmt


class TestQuoteTableName:
    def test_mysql_backticks(self):
        assert quote_table_name("mydb.users", "MySQL") == "`mydb`.`users`"

    def test_postgres_quotes(self):
        assert quote_table_name("public.users", "PostgreSQL") == '"public"."users"'

    def test_plain_fallback(self):
        assert quote_table_name("users", "SQLite") == "users"


class TestSqlFormatter:
    def test_format_uppercases_keywords(self):
        if not SqlFormatter.available():
            pytest.skip("sqlparse not installed")
        raw = "select id from users where active = 1"
        formatted = SqlFormatter.format_sql(raw)
        assert "SELECT" in formatted
        assert "FROM" in formatted

    def test_missing_sqlparse_returns_original(self):
        original = SqlFormatter._sqlparse
        SqlFormatter._sqlparse = None
        SqlFormatter._import_attempted = True
        try:
            sql = "select 1"
            assert SqlFormatter.format_sql(sql) == sql
        finally:
            SqlFormatter._sqlparse = original
            SqlFormatter._import_attempted = False

    def test_empty_sql_unchanged(self):
        assert SqlFormatter.format_sql("") == ""
        assert SqlFormatter.format_sql("   ") == "   "


class TestKeywordTag:
    def test_dml_group(self):
        assert keyword_tag("SELECT") == "sql_kw_dml"
        assert keyword_tag("insert") == "sql_kw_dml"

    def test_clause_group(self):
        assert keyword_tag("FROM") == "sql_kw_clause"
        assert keyword_tag("WHERE") == "sql_kw_clause"

    def test_join_group(self):
        assert keyword_tag("JOIN") == "sql_kw_join"
        assert keyword_tag("ON") == "sql_kw_join"

    def test_logic_group(self):
        assert keyword_tag("AND") == "sql_kw_logic"
        assert keyword_tag("UNION") == "sql_kw_logic"

    def test_ddl_group(self):
        assert keyword_tag("CREATE") == "sql_kw_ddl"
        assert keyword_tag("KEY") == "sql_kw_ddl"

    def test_txn_group(self):
        assert keyword_tag("COMMIT") == "sql_kw_txn"

    def test_same_semantics_same_color(self):
        # SELECT and DELETE are both DML verbs -> identical tag/color.
        assert keyword_tag("SELECT") == keyword_tag("DELETE")
        # FROM and WHERE are both clauses.
        assert keyword_tag("FROM") == keyword_tag("WHERE")

    def test_unknown_defaults_to_generic(self):
        assert keyword_tag("FOOBAR") == "sql_keyword"


class TestCrossSchemaTables:
    def _mysql_mgr(self, current_db, rows):
        cursor = MagicMock()
        executed = {"sql": []}

        def execute(sql):
            executed["sql"].append(sql)

        def fetchone():
            return (current_db,) if current_db else (None,)

        def fetchall():
            return rows

        cursor.execute.side_effect = execute
        cursor.fetchone.side_effect = fetchone
        cursor.fetchall.side_effect = fetchall

        module = MagicMock()
        module.get_cursor.return_value = cursor

        mgr = MagicMock()
        mgr.db_type = "MariaDB"
        mgr.conn = object()
        mgr.config = {"module": module}
        return mgr

    def _completer(self):
        return SqlCompleter(
            MagicMock(),
            get_db_manager=lambda: None,
            get_connection_name=lambda: "c1",
            get_db_type=lambda: "MariaDB",
        )

    def test_no_db_returns_qualified_names(self):
        completer = self._completer()
        mgr = self._mysql_mgr(None, [("shop", "orders"), ("hr", "employees")])
        result = completer._mysql_cross_schema_tables(mgr)
        assert result == ["shop.orders", "hr.employees"]

    def test_db_selected_returns_none(self):
        completer = self._completer()
        mgr = self._mysql_mgr("shop", [])
        assert completer._mysql_cross_schema_tables(mgr) is None


class TestSqlHighlighter:
    def test_token_to_tag_keyword(self):
        assert SqlHighlighter.token_to_tag("Token.Keyword") == "sql_keyword"

    def test_token_to_tag_string(self):
        assert SqlHighlighter.token_to_tag("Token.Literal.String.Single") == "sql_string"

    def test_token_to_tag_unknown(self):
        assert SqlHighlighter.token_to_tag("Token.Text") is None

    def test_available_when_pygments_present(self):
        assert SqlHighlighter.available() is True


class TestTokenBeforeCursor:
    def _mock_text(self, line_text: str, insert_col: int):
        text = MagicMock()
        text.index.return_value = f"1.{insert_col}"
        text.get.return_value = line_text
        return text

    def test_keyword_prefix(self):
        text = self._mock_text("SELECT sel", 10)
        prefix, context, _, _ = token_before_cursor(text)
        assert prefix == "sel"
        assert context == "keyword"

    def test_column_context_after_dot(self):
        text = self._mock_text("SELECT o.", 9)
        prefix, context, _, _ = token_before_cursor(text)
        assert prefix == "o"
        assert context == "column"


class TestSqlCompleterCache:
    def _completer(self, **kwargs):
        text = MagicMock()
        defaults = dict(
            get_db_manager=lambda: None,
            get_connection_name=lambda: "conn1",
            get_db_type=lambda: "PostgreSQL",
        )
        defaults.update(kwargs)
        return SqlCompleter(text, **defaults)

    def test_build_suggestions_keywords(self):
        completer = self._completer()
        completer._table_cache["conn1"] = ["users", "orders"]
        suggestions = completer.build_suggestions("sel", "keyword", "SELECT sel")
        assert "SELECT" in suggestions

    def test_build_suggestions_tables(self):
        completer = self._completer(get_db_type=lambda: "MySQL")
        completer._table_cache["conn1"] = ["users", "user_roles"]
        suggestions = completer.build_suggestions("user", "keyword", "FROM user")
        assert "users" in suggestions

    def test_build_suggestions_columns_with_alias(self):
        completer = self._completer(get_db_type=lambda: "MySQL")
        completer._column_cache[("conn1", "orders")] = ["id", "amount", "status"]
        sql = "SELECT o. FROM orders o"
        suggestions = completer.build_suggestions("o", "column", sql)
        assert "id" in suggestions
        assert "amount" in suggestions

    @patch("common.ui.tk.sql_editor_assist.DatabaseRegistry.execute_operation")
    def test_prefetch_tables_caches(self, mock_exec):
        import time

        mock_exec.return_value = ["t1", "t2"]
        fake_mgr = MagicMock()
        fake_mgr.db_type = "MySQL"
        fake_mgr.conn = object()

        completer = self._completer(
            get_db_manager=lambda: fake_mgr,
            get_connection_name=lambda: "myconn",
            get_db_type=lambda: "MySQL",
        )
        completer.prefetch_tables("myconn")
        for _ in range(50):
            if "myconn" in completer._table_cache:
                break
            time.sleep(0.02)
        assert completer._table_cache.get("myconn") == ["t1", "t2"]
        mock_exec.assert_called_once()

    def test_dedupe_limit(self):
        completer = self._completer(get_connection_name=lambda: None, get_db_type=lambda: None)
        out = completer._dedupe_limit(["A", "a", "B", "b"])
        assert out == ["A", "B"]

    def test_prefer_tables_orders_tables_first(self):
        completer = self._completer(get_db_type=lambda: "MySQL")
        completer._table_cache["conn1"] = ["orders"]
        # With prefer_tables, a matching table should come before keywords.
        suggestions = completer.build_suggestions(
            "or", "keyword", "SELECT * FROM or", prefer_tables=True
        )
        assert suggestions[0] == "orders"

    def test_keyword_first_without_prefer(self):
        completer = self._completer(get_db_type=lambda: "MySQL")
        completer._table_cache["conn1"] = ["orders"]
        suggestions = completer.build_suggestions(
            "or", "keyword", "SELECT or", prefer_tables=False
        )
        # ORDER (keyword) should be present and precede the table.
        assert "ORDER" in suggestions
        assert suggestions.index("ORDER") < suggestions.index("orders")

    def test_empty_prefix_keyword_context_returns_keywords(self):
        completer = self._completer(get_db_type=lambda: "MySQL")
        completer._table_cache["conn1"] = ["orders", "users"]
        suggestions = completer.build_suggestions("", "keyword", "SELECT ")
        # Empty prefix -> capped, alphabetically-sorted keyword list.
        assert suggestions
        assert "AND" in suggestions
        assert suggestions == sorted(suggestions)

    def test_empty_prefix_prefer_tables_lists_tables_first(self):
        completer = self._completer(get_db_type=lambda: "MySQL")
        completer._table_cache["conn1"] = ["orders", "users"]
        suggestions = completer.build_suggestions(
            "", "keyword", "SELECT * FROM ", prefer_tables=True
        )
        assert suggestions[0] in ("orders", "users")


class TestLazyPrefetch:
    @patch("common.ui.tk.sql_editor_assist.DatabaseRegistry.execute_operation")
    def test_build_suggestions_kicks_prefetch_when_cache_empty(self, mock_exec):
        import time

        mock_exec.return_value = ["users", "orders"]
        fake_mgr = MagicMock()
        fake_mgr.db_type = "MySQL"
        fake_mgr.conn = object()
        fake_mgr.lock = None

        text = MagicMock()
        completer = SqlCompleter(
            text,
            get_db_manager=lambda: fake_mgr,
            get_connection_name=lambda: "c1",
            get_db_type=lambda: "MySQL",
        )
        # First call: cache empty -> returns keywords only but starts a fetch.
        first = completer.build_suggestions("us", "keyword", "SELECT us")
        for _ in range(50):
            if "c1" in completer._table_cache:
                break
            time.sleep(0.02)
        # After fetch completes, the table is now suggestible.
        second = completer.build_suggestions("us", "keyword", "SELECT us")
        assert "users" not in first
        assert "users" in second

    def test_refresh_now_reshows_when_awaiting(self):
        text = MagicMock()
        completer = SqlCompleter(
            text,
            get_db_manager=lambda: None,
            get_connection_name=lambda: "c1",
            get_db_type=lambda: "MySQL",
        )
        completer._awaiting = True
        completer._visible = False
        called = {"n": 0}

        def fake_show(explicit=False):
            called["n"] += 1

        completer._show = fake_show
        completer._refresh_now()
        assert called["n"] == 1
        assert completer._awaiting is False

    def test_refresh_now_noop_when_idle(self):
        text = MagicMock()
        completer = SqlCompleter(
            text,
            get_db_manager=lambda: None,
            get_connection_name=lambda: "c1",
            get_db_type=lambda: "MySQL",
        )
        completer._awaiting = False
        completer._visible = False
        called = {"n": 0}
        completer._show = lambda explicit=False: called.__setitem__("n", called["n"] + 1)
        completer._refresh_now()
        assert called["n"] == 0


class _FakeText:
    """Minimal single-line tk.Text stand-in for accept() tests."""

    def __init__(self, content: str, insert_col: int):
        self.content = content
        self.insert_col = insert_col

    def index(self, mark):
        if mark in ("insert", "1.end"):
            return f"1.{self.insert_col if mark == 'insert' else len(self.content)}"
        return mark

    def _col(self, idx: str) -> int:
        # Supports "1.N" and "1.N+Mc".
        base, _, rest = idx.partition("+")
        col = int(base.split(".")[1])
        if rest:
            col += int(rest.rstrip("c"))
        return col

    def get(self, a, b=None):
        if b is None:
            return self.content
        ca, cb = self._col(a), self._col(b)
        return self.content[ca:cb]

    def delete(self, a, b=None):
        ca = self._col(a)
        cb = self._col(b) if b else ca + 1
        self.content = self.content[:ca] + self.content[cb:]

    def insert(self, idx, text):
        c = self._col(idx)
        self.content = self.content[:c] + text + self.content[c:]
        self.insert_col = c + len(text)

    def mark_set(self, *_a):
        pass


class TestAcceptTrailingSpace:
    def _completer(self, text):
        c = SqlCompleter(
            text,
            get_db_manager=lambda: None,
            get_connection_name=lambda: "c1",
            get_db_type=lambda: "MySQL",
        )
        c._visible = True
        c._listbox = MagicMock()
        c._listbox.curselection.return_value = (0,)
        return c

    def test_keyword_gets_trailing_space(self):
        text = _FakeText("SELE", 4)
        c = self._completer(text)
        c._listbox.get.return_value = "SELECT"
        c._replace_start = "1.0"
        c._replace_end = "1.4"
        c.accept()
        assert text.content == "SELECT "

    def test_keyword_no_double_space(self):
        text = _FakeText("SELE next", 4)
        c = self._completer(text)
        c._listbox.get.return_value = "SELECT"
        c._replace_start = "1.0"
        c._replace_end = "1.4"
        c.accept()
        assert text.content == "SELECT next"

    def test_non_keyword_no_space(self):
        text = _FakeText("ord", 3)
        c = self._completer(text)
        c._listbox.get.return_value = "orders"
        c._replace_start = "1.0"
        c._replace_end = "1.3"
        c.accept()
        assert text.content == "orders"

    def test_followed_by_space_detection(self):
        text = _FakeText("AB CD", 0)
        c = self._completer(text)
        c._replace_end = "1.2"  # char at col 2 is a space
        assert c._followed_by_space() is True
        c._replace_end = "1.1"  # char at col 1 is 'B'
        assert c._followed_by_space() is False
        c._replace_end = "1.5"  # end of buffer
        assert c._followed_by_space() is False


class TestPrefersTables:
    def test_detects_from_keyword(self):
        text = MagicMock()
        text.get.return_value = "SELECT * FROM "
        completer = SqlCompleter(
            text,
            get_db_manager=lambda: None,
            get_connection_name=lambda: "c",
            get_db_type=lambda: "MySQL",
        )
        assert completer._prefers_tables("1.14") is True

    def test_no_table_intro(self):
        text = MagicMock()
        text.get.return_value = "SELECT "
        completer = SqlCompleter(
            text,
            get_db_manager=lambda: None,
            get_connection_name=lambda: "c",
            get_db_type=lambda: "MySQL",
        )
        assert completer._prefers_tables("1.7") is False
