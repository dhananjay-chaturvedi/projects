---
title: ai
description: Natural-language SQL via Claude / Cursor / Codex CLIs, with multi-tab sessions.
sidebar:
  order: 6
---

The `ai` command group requires the **AI Query Assistant** module and
at least one configured CLI backend (`claude`, `cursor`, or `codex`).

## List backends

```bash
python dbtool.py ai --list-backends
# claude  *  (default)
# cursor
# codex
```

The `*` marks the configured default. Set in `ai_query/config.ini`
(module-owned; also editable via **AI Settings** or `dbtool ai config`):

```ini
[ai]
default_backend = auto       # auto | claude | cursor | codex
```

### CLI path resolution

Backends locate their CLI even when the app runs with a minimal `PATH`
(e.g. launched from a GUI/launchd). Resolution order for the `claude` and
`codex` backends:

1. explicit `cli_path` override in `ai_query/config.ini`
2. the current `PATH`
3. common install dirs (`~/.local/bin`, `~/bin`, `~/.claude/local`,
   `/opt/homebrew/bin`, `/usr/local/bin`, …)

```ini
[ai.claude]
cli_path =       # optional: full path to the claude binary

[ai.codex]
cli_path =       # optional: full path to the codex binary
```

## One-shot query

```bash
python dbtool.py ai --conn prod "show me the top 5 customers by revenue"
python dbtool.py ai --conn prod --backend cursor "list inactive users"
python dbtool.py ai --conn prod --sql-mode open "count rows in orders"
```

Output:

```text
Generated SQL:
  SELECT customer_id, SUM(amount) AS revenue
  FROM orders
  GROUP BY customer_id
  ORDER BY revenue DESC
  LIMIT 5;

Results:
┌─────────────┬─────────┐
│ customer_id │ revenue │
├─────────────┼─────────┤
│ 47          │ 18234   │
│ 12          │ 14991   │
│ 102         │ 13720   │
│ 88          │ 12108   │
│ 56          │ 11644   │
└─────────────┴─────────┘

Explanation:
  The query groups orders by customer and sums the amounts, ordering
  descending by total revenue. We limit to 5 customers to answer
  "top 5".
```

## SQL modes

```bash
python dbtool.py ai --conn prod --sql-mode strict_summary \
    "how many tables in this database?"

python dbtool.py ai --conn prod --sql-mode summary \
    "describe the orders table"

python dbtool.py ai --conn prod --sql-mode open \
    "find customers who placed orders in last 30 days but not last 7"
```

| Mode | What runs automatically |
|------|--------------------------|
| `strict_summary` | Only catalog / metadata SQL |
| `summary` *(default)* | Catalog-first; user-table SQL allowed when needed |
| `open` | No catalog/user-table scope restrictions; still read-only |

## Sessions

Sessions persist conversation context. Each session has its own
connection, backend, and history.

### Create

```bash
python dbtool.py ai session new --conn prod --backend claude
# created session: tab1
```

```bash
python dbtool.py ai session new --conn prod --backend cursor --sql-mode open
```

### Ask a question

```bash
python dbtool.py ai session ask --session tab1 "count rows in users"
```

### Follow-up

```bash
python dbtool.py ai session follow-up --session tab1 \
    "add a filter for active users only"
```

### List sessions

```bash
python dbtool.py ai session list
```

```text
┌──────┬────────────┬──────────┬──────────┬────────────────────┐
│ id   │ connection │ backend  │ messages │ last activity      │
├──────┼────────────┼──────────┼──────────┼────────────────────┤
│ tab1 │ prod       │ claude   │ 4        │ 2026-06-01 12:31   │
│ tab2 │ stage      │ cursor   │ 7        │ 2026-05-30 18:02   │
└──────┴────────────┴──────────┴──────────┴────────────────────┘
```

### Save / load sessions

```bash
python dbtool.py ai session save     # writes ~/.dbassistant/session/ai/sessions.json
python dbtool.py ai session load     # restores from that file
```

### Close

```bash
python dbtool.py ai session close --session tab2
# prompt: Save before closing? [Y/n]
```

### Switch SQL mode mid-session

```bash
python dbtool.py ai session set-mode --session tab1 --sql-mode open
```

### Run SQL directly inside a session

```bash
python dbtool.py ai session execute-sql --session tab1 \
    --sql "SELECT id, name FROM users LIMIT 5"
```

## Cross-tab references

Inline:

```bash
python dbtool.py ai session ask --session tab1 \
    "@tab2 list active sessions then compare with what tab1 sees"
```

Or via the dedicated command:

```bash
python dbtool.py ai session cross --session tab1 \
    "talk to tab 3: count rows in orders"
```

Syntax recognized:

| Pattern | Behavior |
|---------|----------|
| `@tab2`, `tab 2`, `use tab 2` | Pull context from tab 2 |
| `talk to tab 3: ...` | Route to tab 3 — SQL runs on tab 3's connection |
| `use tab 2 and tab 4: ...` | Team coordination across the referenced tabs |

## Where sessions are stored

```text
~/.dbassistant/session/ai/sessions.json
```

`[ai.limits]` in `ai_query/config.ini`:

```ini
[ai.limits]
max_stored_sessions = 0      # 0 = keep all
```

When the limit is exceeded, oldest sessions are dropped on save.

## See also

- [AI Query Assistant module](/modules/ai-query/)
- [AI REST API](/api/ai/)
