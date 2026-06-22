"""Lightweight SQL editor helpers: syntax highlighting, formatting, autocomplete."""
from __future__ import annotations

import functools
import re
import threading
import tkinter as tk
from dataclasses import dataclass
from typing import Callable

from common.config_loader import properties
from common.database_registry import DatabaseRegistry
from common.ui.tk.theme import ColorTheme

# ---------------------------------------------------------------------------
# Shared SQL keyword lists (pure in-memory, no DB cost)
# ---------------------------------------------------------------------------

_BASE_KEYWORDS = frozenset(
    kw.upper()
    for kw in (
        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
        "DELETE", "CREATE", "ALTER", "DROP", "TABLE", "INDEX", "VIEW", "JOIN",
        "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "ON", "AS", "AND",
        "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "NULL", "DISTINCT",
        "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET",
        "UNION", "ALL", "CASE", "WHEN", "THEN", "ELSE", "END", "WITH", "RECURSIVE",
        "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CONSTRAINT", "DEFAULT",
        "UNIQUE", "CHECK", "CASCADE", "TRUNCATE", "GRANT", "REVOKE", "COMMIT",
        "ROLLBACK", "BEGIN", "TRANSACTION", "EXPLAIN", "ANALYZE", "COUNT", "SUM",
        "AVG", "MIN", "MAX", "CAST", "COALESCE", "NULLIF", "OVER", "PARTITION",
        "ROW_NUMBER", "RANK", "DENSE_RANK", "FETCH", "FIRST", "NEXT", "ONLY",
        "RETURNING", "INTERSECT", "EXCEPT", "MINUS",
    )
)

_DIALECT_KEYWORDS: dict[str, frozenset[str]] = {
    "MySQL": frozenset({"AUTO_INCREMENT", "ENGINE", "CHARSET", "COLLATE", "REPLACE", "STRAIGHT_JOIN", "USE", "SHOW", "DESCRIBE", "IF", "IGNORE"}),
    "MariaDB": frozenset({"AUTO_INCREMENT", "ENGINE", "CHARSET", "COLLATE", "REPLACE", "STRAIGHT_JOIN", "USE", "SHOW", "DESCRIBE", "IF", "IGNORE"}),
    "PostgreSQL": frozenset({"SERIAL", "BIGSERIAL", "RETURNING", "ILIKE", "SIMILAR", "TO", "LATERAL", "MATERIALIZED", "CONCURRENTLY", "VACUUM", "COPY"}),
    "Oracle": frozenset({"DUAL", "ROWNUM", "ROWID", "NVL", "DECODE", "CONNECT", "START", "PRIOR", "LEVEL", "SYSDATE", "SYSTIMESTAMP", "VARCHAR2", "NUMBER", "CLOB", "BLOB", "MERGE", "USING", "MATCHED"}),
    "SQL Server": frozenset({"TOP", "IDENTITY", "NVARCHAR", "DATETIME2", "GO", "EXEC", "EXECUTE", "NOLOCK", "OUTPUT", "INSERTED", "DELETED"}),
    "SQLite": frozenset({"AUTOINCREMENT", "GLOB", "PRAGMA", "VACUUM", "ATTACH", "DETACH"}),
}

# Pygments token type -> our tag name
_TOKEN_TAG_MAP = {
    "Token.Keyword": "sql_keyword",
    "Token.Keyword.DML": "sql_keyword",
    "Token.Keyword.DDL": "sql_keyword",
    "Token.Name.Builtin": "sql_type",
    "Token.Literal.String": "sql_string",
    "Token.Literal.String.Single": "sql_string",
    "Token.Literal.String.Double": "sql_string",
    "Token.Literal.Number": "sql_number",
    "Token.Literal.Number.Integer": "sql_number",
    "Token.Literal.Number.Float": "sql_number",
    "Token.Comment": "sql_comment",
    "Token.Comment.Single": "sql_comment",
    "Token.Comment.Multiline": "sql_comment",
    "Token.Name.Function": "sql_function",
    "Token.Operator": "sql_operator",
    "Token.Punctuation": "sql_operator",
}

# Semantic keyword categories -> tag. Keywords sharing a meaning share a color.
_KW_DML = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE", "UPSERT", "INTO", "VALUES", "RETURNING"})
_KW_CLAUSE = frozenset({"FROM", "WHERE", "GROUP", "BY", "HAVING", "ORDER", "LIMIT", "OFFSET", "SET", "WITH", "AS", "FETCH", "FIRST", "NEXT", "ONLY", "OVER", "PARTITION", "ASC", "DESC", "DISTINCT", "TOP"})
_KW_JOIN = frozenset({"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "ON", "USING", "NATURAL"})
_KW_LOGIC = frozenset({"AND", "OR", "NOT", "IN", "EXISTS", "BETWEEN", "LIKE", "ILIKE", "IS", "NULL", "ALL", "ANY", "SOME", "UNION", "INTERSECT", "EXCEPT", "MINUS", "CASE", "WHEN", "THEN", "ELSE", "END"})
_KW_DDL = frozenset({"CREATE", "ALTER", "DROP", "TRUNCATE", "TABLE", "INDEX", "VIEW", "SEQUENCE", "DATABASE", "SCHEMA", "CONSTRAINT", "PRIMARY", "FOREIGN", "KEY", "REFERENCES", "DEFAULT", "UNIQUE", "CHECK", "CASCADE", "ADD", "COLUMN", "RENAME", "GRANT", "REVOKE", "RECURSIVE", "AUTO_INCREMENT", "AUTOINCREMENT", "SERIAL", "BIGSERIAL", "IDENTITY", "ENGINE", "CHARSET", "COLLATE"})
_KW_TXN = frozenset({"COMMIT", "ROLLBACK", "BEGIN", "TRANSACTION", "SAVEPOINT", "EXPLAIN", "ANALYZE", "VACUUM", "USE", "SHOW", "DESCRIBE", "PRAGMA"})

_KEYWORD_TAGS: dict[str, str] = {}
for _kw_set, _kw_tag in (
    (_KW_DML, "sql_kw_dml"),
    (_KW_CLAUSE, "sql_kw_clause"),
    (_KW_JOIN, "sql_kw_join"),
    (_KW_LOGIC, "sql_kw_logic"),
    (_KW_DDL, "sql_kw_ddl"),
    (_KW_TXN, "sql_kw_txn"),
):
    for _kw in _kw_set:
        _KEYWORD_TAGS.setdefault(_kw, _kw_tag)

# Every tag the highlighter may add (used to clear/reapply per line).
_ALL_HIGHLIGHT_TAGS = set(_TOKEN_TAG_MAP.values()) | set(_KEYWORD_TAGS.values())


def keyword_tag(word: str) -> str:
    """Return the semantic tag for a SQL keyword (defaults to ``sql_keyword``)."""
    return _KEYWORD_TAGS.get(word.upper(), "sql_keyword")

_FROM_JOIN_RE = re.compile(
    r"(?:FROM|JOIN)\s+"
    r"(?:`([^`]+)`|\"([^\"]+)\"|'([^']+)'|\[([^\]]+)\]|([\w.]+))"
    r"(?:\s+(?:AS\s+)?(`([^`]+)`|\"([^\"]+)\"|'([^']+)'|\[([^\]]+)\]|([\w]+)))?",
    re.IGNORECASE,
)


@functools.lru_cache(maxsize=16)
def keywords_for_dialect(db_type: str | None) -> frozenset[str]:
    """Return keyword set for *db_type* (base + dialect extras), memoized."""
    extra = _DIALECT_KEYWORDS.get(db_type or "", frozenset())
    return _BASE_KEYWORDS | extra


def parse_table_aliases(sql: str) -> dict[str, str]:
    """Parse ``alias -> table`` mappings from *sql* (FROM/JOIN clauses)."""
    aliases: dict[str, str] = {}
    for match in _FROM_JOIN_RE.finditer(sql):
        table = next(g for g in match.group(1, 2, 3, 4, 5) if g)
        alias = next((g for g in match.group(6, 7, 8, 9, 10, 11) if g), None)
        if alias:
            aliases[alias.lower()] = table
        # table name itself is also a valid qualifier
        short = table.split(".")[-1]
        aliases[table.lower()] = table
        aliases[short.lower()] = table
    return aliases


def statement_at_offset(text: str, offset: int) -> str:
    """Return the ``;``-delimited statement containing character *offset*."""
    last_semi = text.rfind(";", 0, offset)
    next_semi = text.find(";", offset)
    start = last_semi + 1 if last_semi != -1 else 0
    end = next_semi if next_semi != -1 else len(text)
    return text[start:end].strip().rstrip(";").strip()


def current_statement(text: str, cursor_index: str) -> str:
    """Return the SQL statement containing *cursor_index* (``line.col``)."""
    try:
        line_no = int(cursor_index.split(".")[0])
        col = int(cursor_index.split(".")[1])
    except (ValueError, IndexError):
        return text

    lines = text.split("\n")
    cursor_offset = 0
    for idx, line in enumerate(lines):
        if idx + 1 == line_no:
            cursor_offset += col
            break
        cursor_offset += len(line) + 1
    else:
        cursor_offset = len(text)

    return statement_at_offset(text, cursor_offset)


def token_before_cursor(text_widget: tk.Text) -> tuple[str, str, int, int]:
    """Return ``(prefix, context, start_index, end_index)`` at the insert mark.

    *context* is one of ``keyword``, ``table``, ``column``, ``none``.
    """
    try:
        insert = text_widget.index(tk.INSERT)
        line, col = insert.split(".")
        col = int(col)
        line_text = text_widget.get(f"{line}.0", f"{line}.end")
    except (tk.TclError, ValueError):
        return "", "none", "1.0", "1.0"

    before = line_text[:col]
    # Column context: identifier immediately before a dot
    dot_match = re.search(r"([\w.`\"]+)\.$", before)
    if dot_match:
        qualifier = dot_match.group(1).strip("`\"")
        return qualifier, "column", f"{line}.{col}", f"{line}.{col}"

    # Identifier being typed
    id_match = re.search(r"([\w.`\"]+)$", before)
    if id_match:
        word = id_match.group(1)
        start_col = col - len(word)
        start_idx = f"{line}.{start_col}"
        end_idx = insert
        # If word contains dot, treat as column qualifier start
        if "." in word:
            return word, "column", start_idx, end_idx
        return word, "keyword", start_idx, end_idx

    return "", "none", insert, insert


def quote_table_name(table: str, db_type: str | None) -> str:
    """Best-effort quoting for ``SELECT * FROM … WHERE 1=0`` metadata probe."""
    if not table:
        return table
    if db_type in ("MySQL", "MariaDB"):
        parts = table.split(".")
        return ".".join(f"`{p}`" for p in parts)
    if db_type == "PostgreSQL":
        parts = table.split(".")
        return ".".join(f'"{p}"' for p in parts)
    if db_type == "SQL Server":
        parts = table.split(".")
        return ".".join(f"[{p}]" for p in parts)
    if db_type == "Oracle":
        parts = table.split(".")
        return ".".join(f'"{p.upper()}"' for p in parts)
    return table


# ---------------------------------------------------------------------------
# SqlFormatter
# ---------------------------------------------------------------------------


class SqlFormatter:
    """Thin wrapper around sqlparse with graceful fallback."""

    _sqlparse = None
    _import_attempted = False

    @classmethod
    def _ensure_sqlparse(cls):
        if cls._import_attempted:
            return cls._sqlparse
        cls._import_attempted = True
        try:
            import sqlparse  # noqa: WPS433

            cls._sqlparse = sqlparse
        except ImportError:
            cls._sqlparse = None
        return cls._sqlparse

    @classmethod
    def available(cls) -> bool:
        return cls._ensure_sqlparse() is not None

    @classmethod
    def format_sql(cls, sql: str) -> str:
        mod = cls._ensure_sqlparse()
        if mod is None or not sql.strip():
            return sql
        try:
            return mod.format(
                sql,
                reindent=True,
                keyword_case="upper",
            )
        except Exception:
            return sql


# ---------------------------------------------------------------------------
# SqlHighlighter
# ---------------------------------------------------------------------------


class SqlHighlighter:
    """Debounced Pygments-based syntax highlighter for a ``tk.Text`` widget."""

    _lexer = None
    _import_attempted = False

    def __init__(
        self,
        text_widget: tk.Text,
        *,
        enabled: bool = True,
        max_chars: int = 60000,
        debounce_ms: int = 120,
    ):
        self.text = text_widget
        self.enabled = enabled
        self.max_chars = max_chars
        self.debounce_ms = debounce_ms
        self._after_id: str | None = None
        self._last_line: str | None = None
        self._tags_configured = False

    @classmethod
    def _ensure_lexer(cls):
        if cls._import_attempted:
            return cls._lexer
        cls._import_attempted = True
        try:
            from pygments.lexers.sql import SqlLexer  # noqa: WPS433

            cls._lexer = SqlLexer()
        except ImportError:
            cls._lexer = None
        return cls._lexer

    @classmethod
    def available(cls) -> bool:
        return cls._ensure_lexer() is not None

    @classmethod
    def token_to_tag(cls, token_type_str: str) -> str | None:
        """Map a Pygments token type string to our tag name (for tests)."""
        for prefix, tag in _TOKEN_TAG_MAP.items():
            if token_type_str.startswith(prefix):
                return tag
        return None

    def set_enabled(self, enabled: bool) -> None:
        self.enabled = enabled
        if not enabled:
            self._cancel()
            self._clear_tags()

    def _configure_tags(self) -> None:
        if self._tags_configured:
            return
        tag_colors = {
            # Semantic keyword groups (same color == same semantics).
            "sql_kw_dml": "#0B66C3",      # SELECT / INSERT / UPDATE / DELETE
            "sql_kw_clause": "#8E24AA",   # FROM / WHERE / GROUP BY / ORDER BY
            "sql_kw_join": "#00897B",     # JOIN / ON / USING
            "sql_kw_logic": "#E65100",    # AND / OR / IN / UNION / CASE
            "sql_kw_ddl": "#C2185B",      # CREATE / ALTER / DROP / KEY
            "sql_kw_txn": "#D32F2F",      # COMMIT / ROLLBACK / BEGIN
            "sql_keyword": ColorTheme.PRIMARY_DARK,  # any other keyword
            "sql_type": "#00838F",        # data types (INT, VARCHAR, ...)
            "sql_string": "#2E7D32",
            "sql_number": "#B8860B",
            "sql_comment": ColorTheme.TEXT_SECONDARY,
            "sql_function": "#5D4037",
            "sql_operator": "#455A64",
        }
        for tag, color in tag_colors.items():
            self.text.tag_configure(tag, foreground=color)
        self._tags_configured = True

    def schedule(self, _event=None) -> None:
        if not self.enabled or not self.available():
            return
        self._cancel()
        try:
            self._after_id = self.text.after(self.debounce_ms, self._apply)
        except tk.TclError:
            pass

    def _cancel(self) -> None:
        if self._after_id is not None:
            try:
                self.text.after_cancel(self._after_id)
            except tk.TclError:
                pass
            self._after_id = None

    def _char_count(self) -> int:
        """Return the buffer's character count without copying its contents."""
        try:
            n = self.text.count("1.0", "end-1c", "chars")
        except (tk.TclError, AttributeError, TypeError):
            try:
                return len(self.text.get("1.0", "end-1c"))
            except tk.TclError:
                return 0
        if isinstance(n, tuple):
            n = n[0] if n else 0
        return int(n or 0)

    def _clear_tags(self) -> None:
        for tag in _ALL_HIGHLIGHT_TAGS:
            try:
                self.text.tag_remove(tag, "1.0", tk.END)
            except tk.TclError:
                pass

    def _apply(self) -> None:
        self._after_id = None
        if not self.enabled:
            return
        lexer = self._ensure_lexer()
        if lexer is None:
            return
        if self._char_count() > self.max_chars:
            # Buffer too large to highlight cheaply; skip to keep typing snappy.
            return

        self._configure_tags()

        # Determine line range: current insert line ± context window
        try:
            insert_line = int(self.text.index(tk.INSERT).split(".")[0])
        except (tk.TclError, ValueError):
            insert_line = 1

        if self._last_line is None:
            start_line = max(1, insert_line - 5)
            end_line = insert_line + 5
        else:
            lo = min(insert_line, int(self._last_line))
            hi = max(insert_line, int(self._last_line))
            start_line = max(1, lo - 2)
            end_line = hi + 2

        self._last_line = str(insert_line)
        total_lines = int(self.text.index("end-1c").split(".")[0])
        end_line = min(end_line, total_lines)

        for line_no in range(start_line, end_line + 1):
            self._highlight_line(line_no, lexer)

    def _highlight_line(self, line_no: int, lexer) -> None:
        try:
            line_start = f"{line_no}.0"
            line_end = f"{line_no}.end"
            line_text = self.text.get(line_start, line_end)
        except tk.TclError:
            return

        for tag in _ALL_HIGHLIGHT_TAGS:
            try:
                self.text.tag_remove(tag, line_start, line_end)
            except tk.TclError:
                pass

        if not line_text:
            return

        from pygments import lex  # noqa: WPS433

        col = 0
        for token, value in lex(line_text, lexer):
            tag = self.token_to_tag(str(token))
            if tag == "sql_keyword" and value:
                # Refine the generic keyword color by semantic category.
                tag = keyword_tag(value)
            if tag and value:
                start = f"{line_no}.{col}"
                end = f"{line_no}.{col + len(value)}"
                try:
                    self.text.tag_add(tag, start, end)
                except tk.TclError:
                    pass
            col += len(value)


# ---------------------------------------------------------------------------
# SqlCompleter
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SqlCompleterContext:
    """Callbacks and tuning flags for SQL autocomplete."""

    get_db_manager: Callable[[], object | None]
    get_connection_name: Callable[[], str | None]
    get_db_type: Callable[[], str | None]
    enabled: bool = True
    as_you_type: bool = False
    debounce_ms: int = 150
    max_tables: int = 5000


class SqlCompleter:
    """Keyword / table / column autocomplete with lazy metadata caching."""

    MAX_SUGGESTIONS = 50

    def __init__(
        self,
        text_widget: tk.Text,
        context: SqlCompleterContext | None = None,
        **legacy,
    ):
        context = context or SqlCompleterContext(
            get_db_manager=legacy["get_db_manager"],
            get_connection_name=legacy["get_connection_name"],
            get_db_type=legacy["get_db_type"],
            enabled=legacy.get("enabled", True),
            as_you_type=legacy.get("as_you_type", False),
            debounce_ms=legacy.get("debounce_ms", 150),
            max_tables=legacy.get("max_tables", 5000),
        )
        self.text = text_widget
        self.get_db_manager = context.get_db_manager
        self.get_connection_name = context.get_connection_name
        self.get_db_type = context.get_db_type
        self.enabled = context.enabled
        self.as_you_type = context.as_you_type
        self.debounce_ms = context.debounce_ms
        # Hard cap on cached table names to bound memory/CPU on huge catalogs.
        self.max_tables = context.max_tables if context.max_tables and context.max_tables > 0 else 5000

        self._table_cache: dict[str, list[str]] = {}
        self._column_cache: dict[tuple[str, str], list[str]] = {}
        self._fetch_lock = threading.Lock()
        self._pending_tables: set[str] = set()
        self._pending_columns: set[tuple[str, str]] = set()

        self._popup: tk.Toplevel | None = None
        self._listbox: tk.Listbox | None = None
        self._suggestions: list[str] = []
        self._replace_start: str = "1.0"
        self._replace_end: str = "1.0"
        self._after_id: str | None = None
        self._visible = False
        # True while we showed nothing but a relevant metadata fetch is in
        # flight; lets the popup auto-appear once tables/columns arrive.
        self._awaiting = False
        self._last_explicit = False

    def set_enabled(self, enabled: bool) -> None:
        self.enabled = enabled
        if not enabled:
            self.hide()

    def set_as_you_type(self, enabled: bool) -> None:
        self.as_you_type = enabled

    # -- metadata prefetch (background) -------------------------------------

    def prefetch_tables(self, conn_name: str | None = None) -> None:
        """Start background table-list fetch for *conn_name* (or current)."""
        conn = conn_name or self.get_connection_name()
        if not conn:
            return
        with self._fetch_lock:
            if conn in self._table_cache or conn in self._pending_tables:
                return
            self._pending_tables.add(conn)
        threading.Thread(
            target=self._fetch_tables_bg,
            args=(conn,),
            daemon=True,
            name=f"sql-ac-tables-{conn}",
        ).start()

    def _fetch_tables_bg(self, conn_name: str) -> None:
        tables: list[str] = []
        try:
            mgr = self.get_db_manager()
            if mgr and getattr(mgr, "conn", None):
                lock = getattr(mgr, "lock", None)
                if lock is not None:
                    with lock:
                        tables = self._list_tables_locked(mgr)
                else:
                    tables = self._list_tables_locked(mgr)
        except Exception:
            tables = []
        with self._fetch_lock:
            self._table_cache[conn_name] = tables
            self._pending_tables.discard(conn_name)
        self._notify_ready()

    def _list_tables_locked(self, mgr) -> list[str]:
        """List tables for *mgr* (caller holds the session lock).

        For MySQL/MariaDB connections without a selected database, return
        schema-qualified ``db.table`` names across all non-system schemas so
        both name suggestions and column probing work without a current DB.
        """
        db_type = getattr(mgr, "db_type", None)
        if db_type in ("MySQL", "MariaDB"):
            qualified = self._mysql_cross_schema_tables(mgr)
            if qualified is not None:
                return qualified[: self.max_tables]
        raw = DatabaseRegistry.execute_operation(db_type, "getTables", mgr.conn)
        return [str(t) for t in (raw or [])][: self.max_tables]

    def _mysql_cross_schema_tables(self, mgr) -> list[str] | None:
        """Return ``db.table`` names when no database is selected, else None.

        Capped at ``max_tables`` server-side so a catalog with thousands of
        tables cannot stall the background fetch or bloat the cache.
        """
        config = getattr(mgr, "config", None) or {}
        module = config.get("module") if isinstance(config, dict) else None
        cursor_fn = getattr(module, "get_cursor", None) if module else None
        if not cursor_fn:
            return None
        cursor = cursor_fn(mgr.conn)
        try:
            cursor.execute("SELECT DATABASE()")
            row = cursor.fetchone()
            if row and row[0]:
                return None  # a database is selected; use the normal listing
            cursor.execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA NOT IN "
                "('information_schema', 'mysql', 'performance_schema', 'sys') "
                "AND TABLE_TYPE = 'BASE TABLE' "
                "ORDER BY TABLE_SCHEMA, TABLE_NAME "
                f"LIMIT {int(self.max_tables)}"
            )
            return [f"{r[0]}.{r[1]}" for r in cursor.fetchall()]
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def prefetch_columns_for_tables(self, tables: set[str], conn_name: str | None = None) -> None:
        """Lazy-fetch columns for *tables* referenced in the current statement."""
        conn = conn_name or self.get_connection_name()
        if not conn:
            return
        for table in tables:
            key = (conn, table)
            with self._fetch_lock:
                if key in self._column_cache or key in self._pending_columns:
                    continue
                self._pending_columns.add(key)
            threading.Thread(
                target=self._fetch_columns_bg,
                args=(conn, table),
                daemon=True,
                name=f"sql-ac-cols-{conn}-{table}",
            ).start()

    def _fetch_columns_bg(self, conn_name: str, table: str) -> None:
        cols: list[str] = []
        try:
            mgr = self.get_db_manager()
            if mgr and getattr(mgr, "conn", None):
                cols = self._probe_columns(mgr, table)
        except Exception:
            cols = []
        with self._fetch_lock:
            self._column_cache[(conn_name, table)] = cols
            self._pending_columns.discard((conn_name, table))
        self._notify_ready()

    def _notify_ready(self) -> None:
        """Marshal a popup refresh onto the UI thread after metadata loads."""
        try:
            self.text.after(0, self._refresh_now)
        except (tk.TclError, RuntimeError):
            pass

    def _refresh_now(self) -> None:
        """Rebuild the popup once freshly cached metadata arrives."""
        if not self.enabled:
            return
        if not self._visible and not self._awaiting:
            return
        self._awaiting = False
        try:
            self._show(explicit=self._last_explicit)
        except tk.TclError:
            pass

    def _probe_columns(self, mgr, table: str) -> list[str]:
        db_type = getattr(mgr, "db_type", None)
        quoted = quote_table_name(table, db_type)
        sql = f"SELECT * FROM {quoted} WHERE 1=0"
        conn = mgr.conn
        config = getattr(mgr, "config", None) or {}
        module = config.get("module") if isinstance(config, dict) else None
        if module is None:
            return []
        # Use driver-specific cursor execution
        cursor_fn = getattr(module, "get_cursor", None)
        if not cursor_fn:
            return []
        lock = getattr(mgr, "lock", None)
        if lock is not None:
            lock.acquire()
        try:
            cursor = cursor_fn(conn)
            try:
                cursor.execute(sql)
                if cursor.description:
                    return [col[0] for col in cursor.description if col and col[0]]
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        finally:
            if lock is not None:
                lock.release()
        return []

    def get_tables(self, conn_name: str | None = None) -> list[str]:
        conn = conn_name or self.get_connection_name()
        if not conn:
            return []
        return list(self._table_cache.get(conn, []))

    def get_columns(self, table: str, conn_name: str | None = None) -> list[str]:
        conn = conn_name or self.get_connection_name()
        if not conn:
            return []
        return list(self._column_cache.get((conn, table), []))

    # -- suggestion logic ---------------------------------------------------

    def build_suggestions(
        self,
        prefix: str,
        context: str,
        statement_sql: str,
        prefer_tables: bool = False,
    ) -> list[str]:
        """Return filtered suggestion list for *prefix* in *context*."""
        db_type = self.get_db_type()
        keywords = keywords_for_dialect(db_type)
        conn = self.get_connection_name()
        tables = self.get_tables(conn)
        # Lazily warm the table cache the first time suggestions are requested
        # for this connection (covers the case where the connection was already
        # selected before any change event fired). The popup is refreshed via
        # _notify_ready() once the background fetch completes.
        if conn and not tables and conn not in self._table_cache:
            self.prefetch_tables(conn)
        aliases = parse_table_aliases(statement_sql)
        prefix_lower = prefix.lower().lstrip("`\"")
        results: list[str] = []

        if context == "column":
            qualifier = prefix_lower.rstrip(".")
            col_prefix = ""
            if "." in qualifier:
                parts = qualifier.rsplit(".", 1)
                qualifier = parts[0]
                col_prefix = parts[1]
            table = aliases.get(qualifier) or qualifier
            if table:
                self.prefetch_columns_for_tables({table}, conn)
                for col in self.get_columns(table, conn):
                    if not col_prefix or col.lower().startswith(col_prefix):
                        results.append(col)
            return self._dedupe_limit(results)

        # keyword / table context
        table_matches: list[str] = []
        for tbl in tables:
            if tbl.lower().startswith(prefix_lower):
                table_matches.append(tbl)
            short = tbl.split(".")[-1]
            if short.lower().startswith(prefix_lower) and short not in table_matches:
                table_matches.append(short)

        keyword_matches = sorted(
            kw for kw in keywords if kw.lower().startswith(prefix_lower)
        )

        # Columns of tables referenced in the statement.
        column_matches: list[str] = []
        referenced = set(aliases.values())
        if referenced:
            self.prefetch_columns_for_tables(referenced, conn)
            for tbl in referenced:
                for col in self.get_columns(tbl, conn):
                    if col.lower().startswith(prefix_lower):
                        column_matches.append(col)

        if prefer_tables:
            results = table_matches + column_matches + keyword_matches
        else:
            results = keyword_matches + table_matches + column_matches

        return self._dedupe_limit(results)

    def _dedupe_limit(self, items: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for item in items:
            key = item.lower()
            if key not in seen:
                seen.add(key)
                out.append(item)
            if len(out) >= self.MAX_SUGGESTIONS:
                break
        return out

    # -- popup UI -----------------------------------------------------------

    def trigger(self, _event=None) -> str:
        """Explicit trigger (Ctrl+Space). Returns ``break`` when handled."""
        if not self.enabled:
            return ""
        self._show(explicit=True)
        return "break"

    def on_keyrelease(self, event=None) -> None:
        """Optional as-you-type trigger (debounced)."""
        if not self.enabled or not self.as_you_type:
            return
        if event and event.keysym in (
            "Up", "Down", "Return", "Escape", "Tab",
            "Control_L", "Control_R", "Shift_L", "Shift_R",
        ):
            return
        self._schedule_auto()

    def _schedule_auto(self) -> None:
        self._cancel_auto()
        try:
            self._after_id = self.text.after(self.debounce_ms, self._auto_show)
        except tk.TclError:
            pass

    def _cancel_auto(self) -> None:
        if self._after_id is not None:
            try:
                self.text.after_cancel(self._after_id)
            except tk.TclError:
                pass
            self._after_id = None

    def _auto_show(self) -> None:
        self._after_id = None
        prefix, context, _, _ = token_before_cursor(self.text)
        if context == "none" or (context == "keyword" and len(prefix) < 2):
            self.hide()
            return
        self._show()

    def _show(self, explicit: bool = False) -> None:
        self._last_explicit = explicit
        prefix, context, start_idx, end_idx = token_before_cursor(self.text)
        if context == "none":
            if not explicit:
                return
            # Explicit trigger with no prefix (e.g. cursor right after
            # "SELECT " or "FROM ") -> offer all keywords + table names.
            context = "keyword"
            prefix = ""
            try:
                insert = self.text.index(tk.INSERT)
            except tk.TclError:
                insert = "1.0"
            start_idx = end_idx = insert

        stmt = self._statement_around_cursor()
        prefer_tables = self._prefers_tables(start_idx)
        self._suggestions = self.build_suggestions(
            prefix, context, stmt, prefer_tables=prefer_tables
        )
        if not self._suggestions:
            # Nothing to show yet. If a metadata fetch is in flight for this
            # connection, remember to re-show once it lands.
            conn = self.get_connection_name()
            with self._fetch_lock:
                self._awaiting = bool(self._pending_tables) or any(
                    key[0] == conn for key in self._pending_columns
                )
            self._hide_popup()
            return

        self._awaiting = False
        self._replace_start = start_idx
        self._replace_end = end_idx
        self._ensure_popup()
        self._populate_listbox()
        self._position_popup()
        self._visible = True

    # Chars read on each side of the cursor to locate the current statement.
    # Keeps autocomplete O(window) instead of O(buffer) on large editors.
    STMT_WINDOW_CHARS = 4000

    def _statement_around_cursor(self) -> str:
        """Return the SQL statement around the cursor, reading a bounded window.

        Avoids copying the whole editor buffer on every popup; a few KB on each
        side is far more than any single statement needs for alias detection.
        """
        try:
            insert = self.text.index(tk.INSERT)
            start = self.text.index(f"{insert} - {self.STMT_WINDOW_CHARS} chars")
            end = self.text.index(f"{insert} + {self.STMT_WINDOW_CHARS} chars")
            window = self.text.get(start, end)
            before = self.text.get(start, insert)
        except tk.TclError:
            return ""
        return statement_at_offset(window, len(before))

    _TABLE_INTRO_KEYWORDS = frozenset({"FROM", "JOIN", "INTO", "UPDATE", "TABLE"})

    def _prefers_tables(self, start_idx: str) -> bool:
        """True when the word just before *start_idx* introduces a table name."""
        try:
            line = start_idx.split(".")[0]
            before = self.text.get(f"{line}.0", start_idx)
        except (tk.TclError, ValueError, AttributeError):
            return False
        words = re.findall(r"[\w]+", before)
        if not words:
            return False
        return words[-1].upper() in self._TABLE_INTRO_KEYWORDS

    def _ensure_popup(self) -> None:
        if self._popup is not None:
            return
        self._popup = tk.Toplevel(self.text)
        self._popup.withdraw()
        self._popup.overrideredirect(True)
        self._popup.configure(bg=ColorTheme.BORDER)
        frame = tk.Frame(self._popup, bg=ColorTheme.BG_SECONDARY, bd=1, relief=tk.SOLID)
        frame.pack(fill=tk.BOTH, expand=True)
        self._listbox = tk.Listbox(
            frame,
            height=8,
            width=40,
            exportselection=False,
            activestyle=tk.NONE,
            selectmode=tk.SINGLE,
            bg=ColorTheme.BG_SECONDARY,
            fg=ColorTheme.TEXT_PRIMARY,
            selectbackground=ColorTheme.PRIMARY_LIGHT,
            selectforeground=ColorTheme.TEXT_PRIMARY,
        )
        self._listbox.pack(fill=tk.BOTH, expand=True)
        self._listbox.bind("<Double-Button-1>", lambda e: self.accept())
        self._listbox.bind("<ButtonRelease-1>", lambda e: self.accept())

    def _populate_listbox(self) -> None:
        if self._listbox is None:
            return
        self._listbox.delete(0, tk.END)
        for item in self._suggestions:
            self._listbox.insert(tk.END, item)
        if self._suggestions:
            self._listbox.selection_set(0)
            self._listbox.activate(0)

    def _position_popup(self) -> None:
        if self._popup is None:
            return
        try:
            bbox = self.text.bbox(self._replace_end)
            if not bbox:
                bbox = self.text.bbox(tk.INSERT)
            if bbox:
                x = self.text.winfo_rootx() + bbox[0]
                y = self.text.winfo_rooty() + bbox[1] + bbox[3]
            else:
                x = self.text.winfo_rootx() + 10
                y = self.text.winfo_rooty() + 30
            self._popup.geometry(f"+{x}+{y}")
            self._popup.deiconify()
            self._popup.lift()
        except tk.TclError:
            pass

    def _hide_popup(self) -> None:
        """Withdraw the popup without cancelling a pending metadata refresh."""
        self._visible = False
        if self._popup is not None:
            try:
                self._popup.withdraw()
            except tk.TclError:
                pass

    def hide(self, _event=None) -> str:
        """User-initiated dismiss (Escape): also cancel any pending refresh."""
        self._awaiting = False
        self._hide_popup()
        return "break"

    def accept(self, _event=None) -> str:
        if not self._visible or self._listbox is None:
            return ""
        sel = self._listbox.curselection()
        if not sel:
            return "break"
        value = self._listbox.get(sel[0])
        prefix, context, _, _ = token_before_cursor(self.text)
        # Evaluate the following char BEFORE deleting (indices shift afterwards).
        followed_by_space = self._followed_by_space()
        try:
            self.text.delete(self._replace_start, self._replace_end)
            insert_text = value
            if context == "column":
                if "." in prefix:
                    qualifier = prefix.split(".")[0].strip("`\"")
                    insert_text = f"{qualifier}.{value}"
                # else: qualifier. already typed — insert column name only
            # Append a trailing space after a keyword so the next token can be
            # typed immediately (unless one already follows the cursor).
            is_keyword = value.upper() in keywords_for_dialect(self.get_db_type())
            if is_keyword and not followed_by_space:
                insert_text = f"{insert_text} "
            self.text.insert(self._replace_start, insert_text)
            new_pos = f"{self._replace_start}+{len(insert_text)}c"
            self.text.mark_set(tk.INSERT, new_pos)
        except tk.TclError:
            pass
        self.hide()
        return "break"

    def _followed_by_space(self) -> bool:
        """True if the character right after the replaced token is whitespace.

        End-of-buffer counts as *not* followed by space, so a trailing space is
        still added there.
        """
        try:
            nxt = self.text.get(self._replace_end, f"{self._replace_end}+1c")
        except tk.TclError:
            return False
        return bool(nxt) and nxt.isspace()

    def popup_up(self, _event=None) -> str:
        if not self._visible or self._listbox is None:
            return ""
        cur = self._listbox.curselection()
        idx = (cur[0] - 1) if cur else 0
        idx = max(0, idx)
        self._listbox.selection_clear(0, tk.END)
        self._listbox.selection_set(idx)
        self._listbox.activate(idx)
        self._listbox.see(idx)
        return "break"

    def popup_down(self, _event=None) -> str:
        if not self._visible or self._listbox is None:
            return ""
        cur = self._listbox.curselection()
        idx = (cur[0] + 1) if cur else 0
        idx = min(idx, self._listbox.size() - 1)
        self._listbox.selection_clear(0, tk.END)
        self._listbox.selection_set(idx)
        self._listbox.activate(idx)
        self._listbox.see(idx)
        return "break"

    def dispose(self) -> None:
        self._cancel_auto()
        self.hide()
        if self._popup is not None:
            try:
                self._popup.destroy()
            except tk.TclError:
                pass
            self._popup = None
            self._listbox = None


def editor_settings() -> dict:
    """Read SQL editor assist settings from properties.ini."""
    return {
        "syntax_highlight": properties.get_bool("ui.sql_editor", "syntax_highlight", default=True),
        "autocomplete": properties.get_bool("ui.sql_editor", "autocomplete", default=True),
        # As-you-type popups. Defaults ON so suggestions work out of the box
        # (the explicit Cmd+Space shortcut is unavailable on macOS). Set to
        # false to require the manual Ctrl+Space trigger instead.
        "autocomplete_as_you_type": properties.get_bool(
            "ui.sql_editor", "autocomplete_as_you_type", default=True
        ),
        "highlight_max_chars": properties.get_int(
            "ui.sql_editor", "highlight_max_chars", default=60000
        ),
        "autocomplete_debounce_ms": properties.get_int(
            "ui.sql_editor", "autocomplete_debounce_ms", default=150
        ),
        "highlight_debounce_ms": properties.get_int(
            "ui.sql_editor", "highlight_debounce_ms", default=120
        ),
        "autocomplete_max_tables": properties.get_int(
            "ui.sql_editor", "autocomplete_max_tables", default=5000
        ),
    }
