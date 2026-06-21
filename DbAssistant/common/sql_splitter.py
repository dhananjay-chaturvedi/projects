"""Shared SQL statement splitting helpers."""

from __future__ import annotations

import re


def split_sql_statements(sql: str) -> list[str]:
    """Split SQL on semicolons while respecting strings/comments/procedures."""
    if looks_like_procedural_block(sql):
        stmt = sql.strip()
        return [stmt.rstrip(";").strip()] if stmt else []

    statements: list[str] = []
    current: list[str] = []
    in_string = False
    string_char: str | None = None
    in_multiline_comment = False
    in_single_line_comment = False
    dollar_quote_tag: str | None = None

    i = 0
    while i < len(sql):
        char = sql[i]

        if dollar_quote_tag:
            if sql.startswith(dollar_quote_tag, i):
                current.append(dollar_quote_tag)
                i += len(dollar_quote_tag)
                dollar_quote_tag = None
                continue
            current.append(char)
            i += 1
            continue

        if (
            not in_string
            and not in_single_line_comment
            and not in_multiline_comment
            and char == "$"
        ):
            match = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[i:])
            if match:
                dollar_quote_tag = match.group(0)
                current.append(dollar_quote_tag)
                i += len(dollar_quote_tag)
                continue

        if not in_string and not in_single_line_comment and i < len(sql) - 1:
            if char == "/" and sql[i + 1] == "*":
                in_multiline_comment = True
                current.append(char)
                current.append(sql[i + 1])
                i += 2
                continue

        if in_multiline_comment and i < len(sql) - 1:
            if char == "*" and sql[i + 1] == "/":
                in_multiline_comment = False
                current.append(char)
                current.append(sql[i + 1])
                i += 2
                continue

        if not in_string and not in_multiline_comment and i < len(sql) - 1:
            if char == "-" and sql[i + 1] == "-":
                in_single_line_comment = True
        if not in_string and not in_multiline_comment and char == "#":
            in_single_line_comment = True

        if in_single_line_comment and char == "\n":
            in_single_line_comment = False

        if not in_multiline_comment and not in_single_line_comment:
            if char in ("'", '"') and (i == 0 or sql[i - 1] != "\\"):
                if in_string and char == string_char and i + 1 < len(sql) and sql[i + 1] == char:
                    current.append(char)
                    current.append(sql[i + 1])
                    i += 2
                    continue
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                    string_char = None

        if (
            char == ";"
            and not in_string
            and not in_multiline_comment
            and not in_single_line_comment
        ):
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)
    return statements


def looks_like_procedural_block(sql: str) -> bool:
    stripped = strip_sql_comments(sql).strip().lower()
    return bool(
        re.match(
            r"^(create\s+(or\s+replace\s+)?(procedure|function|package|trigger|type)\b|"
            r"declare\b|begin\b)",
            stripped,
        )
    )


def strip_sql_comments(sql: str) -> str:
    """Remove SQL comments while preserving string literals."""
    out: list[str] = []
    in_string = False
    string_char: str | None = None
    in_multiline_comment = False
    in_single_line_comment = False
    i = 0
    while i < len(sql):
        char = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_multiline_comment:
            if char == "*" and nxt == "/":
                in_multiline_comment = False
                i += 2
                continue
            i += 1
            continue
        if in_single_line_comment:
            if char == "\n":
                in_single_line_comment = False
                out.append(char)
            i += 1
            continue

        if not in_string and char == "/" and nxt == "*":
            in_multiline_comment = True
            i += 2
            continue
        if not in_string and char == "-" and nxt == "-":
            in_single_line_comment = True
            i += 2
            continue
        if not in_string and char == "#":
            in_single_line_comment = True
            i += 1
            continue

        if char in ("'", '"') and (i == 0 or sql[i - 1] != "\\"):
            if in_string and char == string_char and nxt == char:
                out.append(char)
                out.append(nxt)
                i += 2
                continue
            if not in_string:
                in_string = True
                string_char = char
            elif char == string_char:
                in_string = False
                string_char = None
        out.append(char)
        i += 1
    return "".join(out)
