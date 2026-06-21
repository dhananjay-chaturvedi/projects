"""
Dataset utilities for the tiny LLM.

A training example is a ``{"question": ..., "sql": ...}`` pair. Each pair becomes
a token sequence::

    <bos> <question tokens> <sep> <sql tokens> <eos>

which is then turned into next-token-prediction (context -> target) examples for
the model. A small built-in NL->SQL set (matching the RAG sample schema) ships so
the pipeline can be tested with zero setup; real data can be supplied as JSONL.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from ai_assistant.llm.tokenizer import WordTokenizer

# Stable dialect tag prefix for multi-dialect conditioning, e.g. ``[postgresql]``.
_DIALECT_TAG_RE = re.compile(r"^\[([a-z0-9_]+)\]\s*(.*)$", re.IGNORECASE | re.DOTALL)

_DIALECT_ALIASES = {
    "postgres": "postgresql",
    "mssql": "sqlserver",
    "documentdb": "mongodb",
}


def normalize_db_type(db_type: str) -> str:
    """Normalize a connection db_type string to a stable tag token."""
    key = (db_type or "").strip().lower().replace(" ", "").replace("-", "")
    return _DIALECT_ALIASES.get(key, key)


def extract_db_type_tag(question: str) -> tuple[str, str]:
    """Split ``[dialect] question`` into ``(dialect, bare_question)``."""
    m = _DIALECT_TAG_RE.match((question or "").strip())
    if not m:
        return "", (question or "").strip()
    return normalize_db_type(m.group(1)), m.group(2).strip()


def tag_question(question: str, db_type: str) -> str:
    """Prefix *question* with ``[db_type]`` for dialect-conditioned training."""
    db = normalize_db_type(db_type)
    _, bare = extract_db_type_tag(question)
    bare = (bare or question or "").strip()
    if not bare:
        return bare
    if not db:
        return bare
    return f"[{db}] {bare}"

# Built-in tiny NL->SQL dataset (e-commerce schema, mirrors ai_query/rag sample).
SAMPLE_PAIRS: list[dict] = [
    {"question": "list all customers",
     "sql": "SELECT * FROM customers;"},
    {"question": "show all products",
     "sql": "SELECT * FROM products;"},
    {"question": "count the number of orders",
     "sql": "SELECT COUNT(*) FROM orders;"},
    {"question": "which products are out of stock",
     "sql": "SELECT name FROM products WHERE in_stock = 0;"},
    {"question": "show pending orders",
     "sql": "SELECT * FROM orders WHERE status = 'pending';"},
    {"question": "total amount of all orders",
     "sql": "SELECT SUM(total_amount) FROM orders;"},
    {"question": "list customers from india",
     "sql": "SELECT full_name FROM customers WHERE country = 'India';"},
    {"question": "top customers by total spend",
     "sql": "SELECT customer_id, SUM(total_amount) AS spend FROM orders "
            "GROUP BY customer_id ORDER BY spend DESC;"},
    {"question": "how many products in each category",
     "sql": "SELECT category_id, COUNT(*) FROM products GROUP BY category_id;"},
    {"question": "show all payments by credit card",
     "sql": "SELECT * FROM payments WHERE method = 'credit_card';"},
    {"question": "list gold tier customers",
     "sql": "SELECT full_name FROM customers WHERE loyalty_tier = 'gold';"},
    {"question": "average product price",
     "sql": "SELECT AVG(unit_price) FROM products;"},
    {"question": "orders with total amount over 100",
     "sql": "SELECT order_id FROM orders WHERE total_amount > 100;"},
    {"question": "show shipped orders",
     "sql": "SELECT * FROM orders WHERE status = 'shipped';"},
    {"question": "count customers by country",
     "sql": "SELECT country, COUNT(*) FROM customers GROUP BY country;"},
    {"question": "list product names and prices",
     "sql": "SELECT name, unit_price FROM products;"},
]


def load_jsonl(path: str | Path) -> list[dict]:
    """Load NL->SQL pairs from a JSONL file (one JSON object per line)."""
    pairs: list[dict] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if obj.get("question") and obj.get("sql"):
            row = {"question": obj["question"], "sql": obj["sql"]}
            if obj.get("description"):
                row["description"] = obj["description"]
            if obj.get("explanation"):
                row["explanation"] = obj["explanation"]
            if obj.get("db_type"):
                row["db_type"] = obj["db_type"]
            pairs.append(row)
    return pairs


def save_jsonl(path: str | Path, pairs: list[dict]) -> int:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for pair in pairs:
            row = {"question": pair["question"], "sql": pair["sql"]}
            if pair.get("description"):
                row["description"] = pair["description"]
            if pair.get("explanation"):
                row["explanation"] = pair["explanation"]
            if pair.get("db_type"):
                row["db_type"] = pair["db_type"]
            fh.write(json.dumps(row) + "\n")
    return len(pairs)


def build_tokenizer(pairs: list[dict], *, min_freq: int = 1) -> WordTokenizer:
    corpus = [p["question"] for p in pairs] + [p["sql"] for p in pairs]
    return WordTokenizer().build(corpus, min_freq=min_freq)


def pair_to_sequence(pair: dict, tok: WordTokenizer) -> list[int]:
    return (
        [tok.bos_id]
        + tok.encode(pair["question"])
        + [tok.sep_id]
        + tok.encode(pair["sql"])
        + [tok.eos_id]
    )


def build_sequences(pairs: list[dict], tok: WordTokenizer) -> list[list[int]]:
    return [pair_to_sequence(p, tok) for p in pairs]


def auto_context(sequences: list[list[int]], *, cap: int = 40) -> int:
    """Pick a context window large enough to span the longest prefix.

    A full-span window lets the model condition every generated SQL token on the
    entire question (the whole prefix is always in view), which is what makes a
    tiny model able to memorize the question->SQL mapping. Capped so large
    datasets stay tractable for this pure-Python trainer.
    """
    longest = max((len(s) for s in sequences), default=4)
    return max(2, min(cap, longest))


def make_examples(
    pairs: list[dict],
    tok: WordTokenizer,
    context: int,
    *,
    sql_targets_only: bool = True,
) -> list[tuple[list[int], int]]:
    """Turn pairs into (context_ids, target_id) next-token examples.

    When ``sql_targets_only`` is True we only predict tokens in the SQL region
    (everything after ``<sep>``). That focuses the tiny model's capacity on the
    task we care about — generating SQL — and cuts training cost.
    """
    examples: list[tuple[list[int], int]] = []
    pad = tok.pad_id
    sep = tok.sep_id
    for pair in pairs:
        seq = pair_to_sequence(pair, tok)
        try:
            sep_idx = seq.index(sep)
        except ValueError:
            sep_idx = 0
        start_t = sep_idx + 1 if sql_targets_only else 1
        for t in range(start_t, len(seq)):
            start = t - context
            if start < 0:
                ctx = [pad] * (-start) + seq[0:t]
            else:
                ctx = seq[start:t]
            examples.append((ctx, seq[t]))
    return examples


def question_prefix(question: str, tok: WordTokenizer) -> list[int]:
    """Seed sequence for generation: <bos> question <sep>."""
    return [tok.bos_id] + tok.encode(question) + [tok.sep_id]
