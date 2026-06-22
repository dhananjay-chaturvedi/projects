"""Central validation gate for NL->SQL training pairs and generated SQL.

Every pair persisted for training (miner, capture, RAG, sample seeds) must pass
``validate_pair`` so prose descriptions and malformed SQL never pollute the
tokenizer vocabulary.
"""

from __future__ import annotations

import re
from typing import Any

_READ_ONLY_RE = re.compile(r"^\s*(SELECT|WITH)\b", re.IGNORECASE)
_FORBIDDEN_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|REPLACE|GRANT|REVOKE|MERGE|CALL|EXEC)\b",
    re.IGNORECASE,
)
_PROSE_MARKERS = re.compile(
    r"\b(you execute|you can think|useful to train|example sql queries|"
    r"very common and useful|train llm|natural language)\b",
    re.IGNORECASE,
)
_MAX_QUESTION_LEN = 240
_MAX_DESCRIPTION_LEN = 120
_MIN_SQL_LEN = 8

# Map our db_type strings to sqlglot dialect names.
_DIALECT_MAP = {
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "mariadb": "mysql",
    "sqlite": "sqlite",
    "sqlserver": "tsql",
    "mssql": "tsql",
    "oracle": "oracle",
}


def normalize_sql(sql: str) -> str:
    """Collapse whitespace and strip trailing semicolon."""
    s = (sql or "").strip().rstrip(";")
    return re.sub(r"\s+", " ", s)


def _sqlglot_dialect(db_type: str | None) -> str | None:
    if not db_type:
        return None
    key = db_type.strip().lower().replace(" ", "")
    return _DIALECT_MAP.get(key)


def parse_sql(sql: str, *, db_type: str | None = None) -> tuple[bool, str, str]:
    """Parse *sql* with sqlglot when available.

    Returns ``(ok, normalized, reason)``.
    """
    s = normalize_sql(sql)
    if len(s) < _MIN_SQL_LEN:
        return False, s, "SQL too short"
    if not _READ_ONLY_RE.match(s):
        return False, s, "SQL must start with SELECT or WITH"
    if _FORBIDDEN_RE.search(re.sub(r"'[^']*'", "", s)):
        return False, s, "Non read-only SQL rejected"
    if _PROSE_MARKERS.search(s):
        return False, s, "SQL contains prose / training-instruction text"

    dialect = _sqlglot_dialect(db_type)
    try:
        import sqlglot  # type: ignore

        parsed = sqlglot.parse_one(s, read=dialect) if dialect else sqlglot.parse_one(s)
        if parsed is None:
            return False, s, "SQL did not parse"
        # Reject multi-statement input.
        if ";" in s:
            parts = [p.strip() for p in s.split(";") if p.strip()]
            if len(parts) > 1:
                return False, s, "Multiple SQL statements not allowed"
        normalized = parsed.sql(dialect=dialect) if dialect else parsed.sql()
        normalized = normalize_sql(normalized)
        return True, normalized, ""
    except ImportError:
        pass
    except Exception as exc:  # noqa: BLE001
        return False, s, f"SQL parse failed: {exc}"

    # Lightweight fallback when sqlglot is absent.
    if s.count("(") != s.count(")"):
        return False, s, "Unbalanced parentheses"
    if not re.search(r"\b(FROM|WHERE|GROUP BY|ORDER BY|LIMIT|JOIN|UNION)\b", s, re.I):
        # Allow simple catalog queries like SELECT COUNT(*) ...
        if not re.search(r"\b(COUNT|SUM|AVG|MIN|MAX)\s*\(", s, re.I):
            return False, s, "SQL missing expected clauses"
    return True, s, ""


def normalize_description(description: str, *, category: str = "") -> str:
    """Return a short, generic description safe for metadata (not training vocab)."""
    desc = (description or category or "").strip()
    desc = re.sub(r"\s+", " ", desc)
    # Strip prose-like training instructions.
    if _PROSE_MARKERS.search(desc):
        desc = category or "query"
    if len(desc) > _MAX_DESCRIPTION_LEN:
        desc = desc[: _MAX_DESCRIPTION_LEN - 3].rstrip() + "..."
    return desc


def clean_question(question: str, description: str = "") -> str:
    """Keep question as NL only — never fold long prose descriptions into it."""
    q = (question or "").strip()
    q = re.sub(r"\s+", " ", q)
    # If the question already embeds the full description block, strip it.
    d = (description or "").strip()
    if d and d.lower() in q.lower() and len(d) > 20:
        q = q.replace(d, "").strip()
    if len(q) > _MAX_QUESTION_LEN:
        q = q[: _MAX_QUESTION_LEN - 3].rstrip() + "..."
    return q


def normalize_question_for_match(question: str) -> str:
    """Normalize a question for exact trained-pair lookup.

    Case-, space- and trailing-punctuation-insensitive so trivially different
    phrasings of the same trained question (e.g. a missing "?") still recall the
    saved SQL.  Preserves a leading ``[dialect]`` tag so recall stays
    dialect-scoped.
    """
    from ai_assistant.llm.dataset import extract_db_type_tag

    tag, bare = extract_db_type_tag(question)
    q = clean_question(bare or question).strip().lower()
    q = re.sub(r"[\s?.!;:]+$", "", q)
    if tag:
        return f"[{tag}] {q}"
    return q


def _dedup_key(question: str, sql: str) -> tuple[str, str]:
    """Return a normalised (question, sql) key for near-duplicate detection.

    Strips leading/trailing whitespace and punctuation from the question,
    collapses internal whitespace, and normalises the SQL to uppercase with
    collapsed whitespace. This catches:
    - Trailing "?" vs none: "list users?" == "list users"
    - Extra spaces: "list  users" == "list users"
    - SQL case differences: "select * from users" == "SELECT * FROM users"
    - SQL whitespace: "SELECT * FROM  users" == "SELECT * FROM users"
    """
    q = re.sub(r"\s+", " ", question.strip().lower())
    q = re.sub(r"[\s?.!;:]+$", "", q)
    s = re.sub(r"\s+", " ", sql.strip().upper())
    s = re.sub(r"[\s;]+$", "", s)
    return q, s


def validate_pair(
    pair: dict[str, Any],
    *,
    db_type: str | None = None,
) -> tuple[bool, dict[str, str], str]:
    """Validate and clean an NL->SQL training pair.

    Returns ``(ok, cleaned_pair, reason)``.  On success *cleaned_pair* has
    keys ``question``, ``sql``, ``description``.
    """
    raw_q = (pair.get("question") or "").strip()
    raw_sql = pair.get("sql") or ""
    raw_desc = (pair.get("description") or pair.get("category") or "").strip()

    if not raw_q:
        return False, {}, "Missing question"
    if _PROSE_MARKERS.search(raw_q) and len(raw_q) > 80:
        return False, {}, "Question contains prose / training-instruction text"

    ok, sql, reason = parse_sql(raw_sql, db_type=db_type)
    if not ok:
        return False, {}, reason or "Invalid SQL"

    question = clean_question(raw_q, raw_desc)
    if not question:
        return False, {}, "Question empty after cleaning"

    description = normalize_description(raw_desc, category=pair.get("category", ""))
    explanation = (pair.get("explanation") or "").strip()
    cleaned = {"question": question, "sql": sql, "description": description}
    if explanation:
        cleaned["explanation"] = explanation
    if pair.get("db_type"):
        cleaned["db_type"] = str(pair["db_type"]).strip()
    return True, cleaned, ""


def validate_pairs(
    pairs: list[dict],
    *,
    db_type: str | None = None,
) -> tuple[list[dict], dict[str, int]]:
    """Validate a list of pairs; return kept pairs and rejection stats."""
    kept: list[dict] = []
    stats = {"input": len(pairs), "kept": 0, "rejected": 0, "reasons": {}}
    seen: set[tuple[str, str]] = set()
    for p in pairs:
        ok, cleaned, reason = validate_pair(p, db_type=db_type)
        if not ok:
            stats["rejected"] += 1
            stats["reasons"][reason or "unknown"] = stats["reasons"].get(reason or "unknown", 0) + 1
            continue
        key = _dedup_key(cleaned["question"], cleaned["sql"])
        if key in seen:
            stats["rejected"] += 1
            stats["reasons"]["duplicate"] = stats["reasons"].get("duplicate", 0) + 1
            continue
        seen.add(key)
        kept.append(cleaned)
    stats["kept"] = len(kept)
    return kept, stats
