---
title: AI Query Assistant
description: Natural-language to SQL using Claude, Cursor, or Codex CLIs. Multi-tab sessions, cross-tab references, SQL execution rules.
sidebar:
  order: 2
---

Ask questions in plain English (or any language the chosen backend
supports). The assistant generates SQL, runs read-only queries against your
connection, and returns rows plus a natural-language explanation.

:::caution[Read-only live connections]
AI Query Assistant can never run live database mutations. `DROP`, `DELETE`,
`UPDATE`, `INSERT`, `TRUNCATE`, `ALTER`, `CREATE`, `MERGE`, `GRANT`, and similar
statements are rejected in the shared execution layer before they reach the
database. This applies to generated SQL, manual AI-tab execution, auto-execute,
cross-tab/session execution, Web/Tk/TUI, CLI, API, and headless usage.
:::

## Supported backends

| Backend | Config section (`ai_query/config.ini`) | Install |
|---------|------------------------------------------|---------|
| Claude | `[ai.claude]` | <https://claude.ai/download> |
| Cursor | `[ai.cursor]` | <https://cursor.com> |
| Codex | `[ai.codex]` | <https://github.com/openai/codex> |

All backends are **CLI-based** — no API keys needed. The assistant
reuses your existing CLI login session.

The `claude` and `codex` backends resolve their CLI from an optional
`cli_path` override, then `PATH`, then common install dirs
(`~/.local/bin`, `~/bin`, `~/.claude/local`, Homebrew, `/usr/local/bin`),
so they keep working when the app launches with a minimal GUI `PATH`:

```ini
[ai.claude]
cli_path =       # optional full path to the claude binary

[ai.codex]
cli_path =       # optional full path to the codex binary
```

## Pick a backend

```bash
python dbtool.py ai --list-backends
# claude  *  (default)
# cursor
# codex
```

Set in `ai_query/config.ini` (or use **AI Settings** on the tab):

```ini
[ai]
default_backend = auto      # auto | claude | cursor | codex
```

```bash
python dbtool.py ai config show
python dbtool.py ai config set ai default_backend claude
```

`auto` picks the first available CLI.

## One-shot queries

```bash
python dbtool.py ai --conn prod "count rows in users"

python dbtool.py ai --conn prod --backend cursor \
    "top 5 customers by revenue last 30 days"

python dbtool.py ai --conn prod --sql-mode strict_summary \
    "how many tables in this database?"
```

## SQL modes

| Mode | Behavior |
|------|----------|
| `strict_summary` | Only catalog / metadata SQL is allowed to auto-run. Fastest for "how many" / "what tables" questions. |
| `summary` *(default)* | Catalog-first; user-table SQL is allowed when needed for the answer. |
| `open` | No catalog/user-table scope restriction, but still read-only. Use for analytical SELECT-style queries. |

`SQL execution rules` (LIMIT enforcement, EXPLAIN before multi-joins)
apply in Summary and Open modes — see [AI Query CLI](/cli/ai/) for
details.

## Multi-tab sessions

Sessions persist conversation state and SQL context. Each tab has its
own connection, backend, and history.

```bash
python dbtool.py ai session new --conn prod --backend claude
python dbtool.py ai session ask --session tab1 "count rows in users"
python dbtool.py ai session follow-up --session tab1 "add a date filter"
python dbtool.py ai session list
python dbtool.py ai session save
python dbtool.py ai session load
python dbtool.py ai session close --session tab1
```

Sessions are stored at `~/.dbassistant/session/ai/sessions.json`
(configurable). `max_stored_sessions` in `[ai.limits]`
(`ai_query/config.ini`) caps how many are kept on save
(`0` = keep all).

## Cross-tab references

```bash
python dbtool.py ai session ask --session tab1 \
    "@tab2 show top 5 customers"

python dbtool.py ai session cross --session tab1 \
    "talk to tab 3: count rows in orders"
```

Syntax recognized in chat and CLI:

| Syntax | Meaning |
|--------|---------|
| `@tab2`, `tab 2`, `use tab 2` | Pull context from tab 2 into current tab |
| `talk to tab 3: ...` | Route to tab 3 — read-only SQL runs on tab 3's connection |
| `use tab 2 and tab 4: ...` | Coordinate across the referenced tabs |

## REST API

```bash
# One-shot
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"connection":"prod","question":"count employees","sql_mode":"open"}' \
     "http://localhost:8000/api/ai/query"

# Session lifecycle
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"connection":"prod","backend":"claude","sql_mode":"summary"}' \
     "http://localhost:8000/api/ai/sessions"

curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"message":"count rows in users"}' \
     "http://localhost:8000/api/ai/sessions/tab1/messages"

curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/ai/sessions/save"
```

## Programmatic

```python
from app.headless.db_service import DBService

svc = DBService()
print(svc.list_ai_backends())                       # ["claude*", "cursor", "codex"]
print(svc.ai_query("prod", "count rows in users"))  # {"sql": ..., "rows": [...], "explanation": ...}
```

## Options menu

| Option | Effect |
|--------|--------|
| Uninterrupted follow-ups | After each AI reply, keep refining until `SATISFIED: yes` or iteration limit |
| Auto-execute SQL queries | Runs SUMMARY_SQL automatically after each response |
| Strict summary mode | Catalog/metadata SQL only |
| Summary mode | Catalog-first; user-table SQL allowed when needed |
| Open mode | No catalog/user-table scope restrictions, still read-only |
| Stop | Cancel running SQL or stop iteration loop |

Defaults are set in `ai_query/config.ini`:

```ini
[ui.ai_query]
auto_execute_ai_loop = false
auto_execute_summary_sql = false
auto_loop_max_iterations = 5
default_sql_mode = summary
```

## Chat & follow-ups

Every surface exposes the same **Results & AI insights** notebook with these
panes: **Query results**, **Explanation**, **Optimization**, **RAG context**,
**Chat**, and **Review**. The **Chat** pane has two sections, identical across
the Desktop UI, Terminal UI, and Web UI:

- **Conversation History** — a scrollable transcript of the turn-by-turn
  exchange.
- **Send Follow-up Message** — a free-form input with an *Uninterrupted
  follow-ups* toggle (keep refining automatically until satisfied) plus
  **Send Follow-up**, **Clear Chat**, **Flag incorrect query**, and **Flag
  incorrect interpretation** actions. Flagging routes the query through the
  fallback backend to repair it.

```bash
# CLI equivalents
python dbtool.py ai session follow-up --session tab1 "add a WHERE clause for active users"
python dbtool.py ai correct --conn prod --sql "<bad sql>" --question "<question>"
```

## Local trainable LLM

Beyond the CLI backends, the assistant ships a **local, trainable NL→SQL model**
usable as an offline backend (`local-llm::<model>`). Engines: `python`
(zero-dependency, always available), `numpy`, `pytorch` (default when installed),
and `ollama`.

```bash
python dbtool.py ai llm engines                 # list engines + availability
python dbtool.py ai llm train --name default --conn prod
python dbtool.py ai llm status --name default
python dbtool.py ai llm generate --name default "top 5 products by sales"
python dbtool.py ai llm eval --name default     # execution accuracy / soft-F1 / ESM
python dbtool.py ai llm export --name default --out dataset.jsonl
```

Rich training (from a database, codebase, or scratch), corpus harvesting, and a
**nightly harvest scheduler** are documented in the
[RAG & LLM CLI](/cli/ai-rag-llm/) and [Local LLM guide](/guides/local-llm/).

## Retrieval-augmented generation (RAG)

Flip **Use RAG** and the assistant retrieves only the relevant schema objects,
relationships, glossary terms, examples, and documents for each question — a
local, offline vector store, no embeddings API required.

```bash
python dbtool.py ai rag index --conn prod
python dbtool.py ai rag ask --conn prod "monthly active users by plan"
python dbtool.py ai rag overview --conn prod
```

RAG includes semantic schema cards (relationships, comments, enums, AI table
purpose), hybrid retrieval (RRF) with optional cross-encoder reranking, query
expansion + entity linking, multi-scope search, an evaluation harness, FAISS ANN
for scale, and a **daily incremental re-index scheduler**. See the full
[RAG Manager guide](/guides/rag/) and the [RAG & LLM API](/api/rag-llm/).

## App Builder

The AI tab also launches the **App Builder**, which generates runnable apps from
scratch, an existing codebase, or a live database. See the
[App Builder module](/modules/app-builder/).

## See also

- [RAG Manager guide](/guides/rag/) · [Local LLM guide](/guides/local-llm/)
- [AI CLI](/cli/ai/) · [RAG & LLM CLI](/cli/ai-rag-llm/)
- [AI REST API](/api/ai/) · [RAG & LLM REST API](/api/rag-llm/)
