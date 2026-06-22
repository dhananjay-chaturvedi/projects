"""
A built-in library of generic, schema-agnostic analytical query patterns.

These NL->SQL patterns apply to *any* relational schema/data/database. They use
ANSI-SQL placeholders (``<table>``, ``<column>``, ``<date_column>`` …) that a
user (or the AI) substitutes with real identifiers. They serve two purposes:

1.  Seeded into a RAG scope (kind ``analytical``) they give the retriever a set
    of high-quality query *shapes* to ground generation on, even before any
    schema-specific examples exist.
2.  Because they are NL->SQL pairs, they also feed the local LLM trainer (see
    :func:`ai_assistant.llm.service.LlmService._rag_examples`), so a freshly
    seeded scope can immediately teach the model common analytical idioms.

Each entry is ``{category, question, sql, note}``.
"""

from __future__ import annotations

# Placeholder convention used throughout (documented for the UI help panel):
PLACEHOLDERS = {
    "<table>": "a table name",
    "<column>": "a column name",
    "<group_column>": "a categorical column to group by",
    "<date_column>": "a DATE/TIMESTAMP column",
    "<amount_column>": "a numeric/measure column",
    "<id_column>": "a primary/identifier column",
    "<fk_column>": "a foreign-key column",
    "<other_table>": "a related table",
}

ANALYTICAL_QUERIES: list[dict] = [
    # ── Counting & existence ─────────────────────────────────────────────
    {"category": "count", "question": "how many rows are in the table",
     "sql": "SELECT COUNT(*) AS row_count FROM <table>;",
     "note": "Total row count."},
    {"category": "count", "question": "count distinct values of a column",
     "sql": "SELECT COUNT(DISTINCT <column>) AS distinct_count FROM <table>;",
     "note": "Cardinality of a column."},
    {"category": "count", "question": "count rows per category",
     "sql": "SELECT <group_column>, COUNT(*) AS cnt\n"
            "FROM <table>\nGROUP BY <group_column>\nORDER BY cnt DESC;",
     "note": "Frequency by category."},
    {"category": "count", "question": "check whether any rows match a condition",
     "sql": "SELECT EXISTS (SELECT 1 FROM <table> WHERE <column> = :value) AS has_match;",
     "note": "Existence test."},

    # ── Aggregation ──────────────────────────────────────────────────────
    {"category": "aggregate", "question": "total sum of a numeric column",
     "sql": "SELECT SUM(<amount_column>) AS total FROM <table>;",
     "note": "Grand total."},
    {"category": "aggregate", "question": "average value of a column",
     "sql": "SELECT AVG(<amount_column>) AS avg_value FROM <table>;",
     "note": "Mean."},
    {"category": "aggregate", "question": "minimum and maximum of a column",
     "sql": "SELECT MIN(<amount_column>) AS min_value, MAX(<amount_column>) AS max_value FROM <table>;",
     "note": "Range extremes."},
    {"category": "aggregate", "question": "sum and average per category",
     "sql": "SELECT <group_column>,\n       SUM(<amount_column>) AS total,\n"
            "       AVG(<amount_column>) AS avg_value,\n       COUNT(*) AS cnt\n"
            "FROM <table>\nGROUP BY <group_column>\nORDER BY total DESC;",
     "note": "Multi-metric rollup by category."},
    {"category": "aggregate", "question": "categories whose total exceeds a threshold",
     "sql": "SELECT <group_column>, SUM(<amount_column>) AS total\n"
            "FROM <table>\nGROUP BY <group_column>\n"
            "HAVING SUM(<amount_column>) > :threshold\nORDER BY total DESC;",
     "note": "HAVING filters aggregates."},

    # ── Top-N / ranking ──────────────────────────────────────────────────
    {"category": "ranking", "question": "top 10 rows by a value",
     "sql": "SELECT * FROM <table> ORDER BY <amount_column> DESC LIMIT 10;",
     "note": "Top-N (use TOP/FETCH FIRST on some engines)."},
    {"category": "ranking", "question": "top category by total amount",
     "sql": "SELECT <group_column>, SUM(<amount_column>) AS total\n"
            "FROM <table>\nGROUP BY <group_column>\nORDER BY total DESC\nLIMIT 1;",
     "note": "Highest-ranked group."},
    {"category": "ranking", "question": "rank rows by a value within each category",
     "sql": "SELECT <group_column>, <id_column>, <amount_column>,\n"
            "       RANK() OVER (PARTITION BY <group_column> ORDER BY <amount_column> DESC) AS rnk\n"
            "FROM <table>;",
     "note": "Window RANK per partition."},
    {"category": "ranking", "question": "top 3 per category",
     "sql": "SELECT * FROM (\n  SELECT t.*,\n"
            "         ROW_NUMBER() OVER (PARTITION BY <group_column> ORDER BY <amount_column> DESC) AS rn\n"
            "  FROM <table> t\n) ranked\nWHERE rn <= 3;",
     "note": "Top-N-per-group via ROW_NUMBER."},

    # ── Time series ──────────────────────────────────────────────────────
    {"category": "time", "question": "count of rows per day",
     "sql": "SELECT CAST(<date_column> AS DATE) AS day, COUNT(*) AS cnt\n"
            "FROM <table>\nGROUP BY CAST(<date_column> AS DATE)\nORDER BY day;",
     "note": "Daily volume."},
    {"category": "time", "question": "monthly totals",
     "sql": "SELECT EXTRACT(YEAR FROM <date_column>) AS yr,\n"
            "       EXTRACT(MONTH FROM <date_column>) AS mon,\n"
            "       SUM(<amount_column>) AS total\n"
            "FROM <table>\nGROUP BY 1, 2\nORDER BY 1, 2;",
     "note": "Monthly trend."},
    {"category": "time", "question": "rows in the last 30 days",
     "sql": "SELECT * FROM <table>\nWHERE <date_column> >= CURRENT_DATE - INTERVAL '30' DAY;",
     "note": "Recent window (syntax varies by engine)."},
    {"category": "time", "question": "running cumulative total over time",
     "sql": "SELECT <date_column>, <amount_column>,\n"
            "       SUM(<amount_column>) OVER (ORDER BY <date_column>\n"
            "           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total\n"
            "FROM <table>;",
     "note": "Cumulative sum window."},
    {"category": "time", "question": "7-day moving average",
     "sql": "SELECT <date_column>, <amount_column>,\n"
            "       AVG(<amount_column>) OVER (ORDER BY <date_column>\n"
            "           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7\n"
            "FROM <table>;",
     "note": "Rolling average."},
    {"category": "time", "question": "month over month change",
     "sql": "WITH monthly AS (\n"
            "  SELECT DATE_TRUNC('month', <date_column>) AS mon, SUM(<amount_column>) AS total\n"
            "  FROM <table> GROUP BY 1\n)\n"
            "SELECT mon, total,\n"
            "       total - LAG(total) OVER (ORDER BY mon) AS mom_change\n"
            "FROM monthly\nORDER BY mon;",
     "note": "LAG for period-over-period delta."},

    # ── Distribution & statistics ────────────────────────────────────────
    {"category": "distribution", "question": "distribution of a column into buckets",
     "sql": "SELECT WIDTH_BUCKET(<amount_column>, 0, 1000, 10) AS bucket, COUNT(*) AS cnt\n"
            "FROM <table>\nGROUP BY bucket\nORDER BY bucket;",
     "note": "Histogram (engine-specific bucketing)."},
    {"category": "distribution", "question": "median of a column",
     "sql": "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY <amount_column>) AS median\n"
            "FROM <table>;",
     "note": "Median via percentile."},
    {"category": "distribution", "question": "percentiles of a column",
     "sql": "SELECT\n"
            "  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY <amount_column>) AS p25,\n"
            "  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY <amount_column>) AS p50,\n"
            "  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY <amount_column>) AS p90\n"
            "FROM <table>;",
     "note": "Quantiles."},

    # ── Data quality ─────────────────────────────────────────────────────
    {"category": "quality", "question": "count null values in a column",
     "sql": "SELECT COUNT(*) AS null_count FROM <table> WHERE <column> IS NULL;",
     "note": "Null audit."},
    {"category": "quality", "question": "percentage of nulls per column",
     "sql": "SELECT 100.0 * SUM(CASE WHEN <column> IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS pct_null\n"
            "FROM <table>;",
     "note": "Null ratio."},
    {"category": "quality", "question": "find duplicate rows by a key",
     "sql": "SELECT <column>, COUNT(*) AS cnt\n"
            "FROM <table>\nGROUP BY <column>\nHAVING COUNT(*) > 1\nORDER BY cnt DESC;",
     "note": "Duplicate detection."},
    {"category": "quality", "question": "rows with values outside an expected range",
     "sql": "SELECT * FROM <table>\nWHERE <amount_column> < :min_value OR <amount_column> > :max_value;",
     "note": "Outlier / range check."},

    # ── Conditional aggregation (pivot-style) ────────────────────────────
    {"category": "pivot", "question": "pivot counts across a status column",
     "sql": "SELECT\n"
            "  SUM(CASE WHEN <group_column> = 'A' THEN 1 ELSE 0 END) AS a_count,\n"
            "  SUM(CASE WHEN <group_column> = 'B' THEN 1 ELSE 0 END) AS b_count,\n"
            "  COUNT(*) AS total\n"
            "FROM <table>;",
     "note": "Conditional aggregation to pivot categories into columns."},

    # ── Joins & relationships ────────────────────────────────────────────
    {"category": "join", "question": "join two related tables",
     "sql": "SELECT a.*, b.*\nFROM <table> a\n"
            "JOIN <other_table> b ON a.<id_column> = b.<fk_column>;",
     "note": "Inner join on a key."},
    {"category": "join", "question": "rows in one table with no match in another",
     "sql": "SELECT a.*\nFROM <table> a\n"
            "LEFT JOIN <other_table> b ON a.<id_column> = b.<fk_column>\n"
            "WHERE b.<fk_column> IS NULL;",
     "note": "Anti-join (orphans)."},
    {"category": "join", "question": "count of related child rows per parent",
     "sql": "SELECT a.<id_column>, COUNT(b.<fk_column>) AS child_count\n"
            "FROM <table> a\n"
            "LEFT JOIN <other_table> b ON a.<id_column> = b.<fk_column>\n"
            "GROUP BY a.<id_column>\nORDER BY child_count DESC;",
     "note": "Parent-to-child cardinality."},

    # ── Cohort / retention / funnel ──────────────────────────────────────
    {"category": "cohort", "question": "new entities per signup month",
     "sql": "SELECT DATE_TRUNC('month', MIN(<date_column>)) AS cohort_month, COUNT(*) AS new_entities\n"
            "FROM <table>\nGROUP BY DATE_TRUNC('month', <date_column>)\nORDER BY cohort_month;",
     "note": "Cohort sizing."},
    {"category": "funnel", "question": "share of rows reaching each stage",
     "sql": "SELECT <group_column> AS stage, COUNT(*) AS cnt,\n"
            "       100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS pct_of_total\n"
            "FROM <table>\nGROUP BY <group_column>\nORDER BY cnt DESC;",
     "note": "Stage distribution / funnel share."},

    # ── Deduplication / latest record ────────────────────────────────────
    {"category": "latest", "question": "latest record per entity",
     "sql": "SELECT * FROM (\n  SELECT t.*,\n"
            "         ROW_NUMBER() OVER (PARTITION BY <id_column> ORDER BY <date_column> DESC) AS rn\n"
            "  FROM <table> t\n) x\nWHERE rn = 1;",
     "note": "Most-recent row per key."},
]


def categories() -> list[str]:
    """Distinct categories present in the library, in stable order."""
    seen: list[str] = []
    for q in ANALYTICAL_QUERIES:
        if q["category"] not in seen:
            seen.append(q["category"])
    return seen


def queries_for(categories_filter: list[str] | None = None) -> list[dict]:
    """Return library entries, optionally filtered to *categories_filter*."""
    if not categories_filter:
        return list(ANALYTICAL_QUERIES)
    wanted = {c.strip().lower() for c in categories_filter if c.strip()}
    return [q for q in ANALYTICAL_QUERIES if q["category"].lower() in wanted]
