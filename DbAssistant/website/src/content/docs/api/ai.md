---
title: AI
description: Natural-language SQL queries and multi-tab sessions over HTTP.
sidebar:
  order: 7
---

Requires the **AI Query Assistant** module and a configured CLI backend
(Claude, Cursor, or Codex).

:::caution[Read-only by design]
The AI Query Assistant (and the App Builder) are **strictly read-only** against
your live connections. Any data- or schema-mutating statement
(`DROP`, `DELETE`, `UPDATE`, `INSERT`, `TRUNCATE`, `ALTER`, `CREATE`, `GRANT`, …)
is rejected before it can reach the database — including statements hidden behind
comments, multiple statements, or data-modifying CTEs. This guard is enforced in
the shared execution layer and applies to every surface (Tk, TUI, Web, CLI, API,
headless). Use the **SQL Editor** or **Data Migration** modules for intentional
writes.
:::

## POST /api/ai/query

One-shot natural-language query.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "connection": "prod",
       "question": "show top 5 customers by revenue",
       "backend": "claude",
       "sql_mode": "summary"
     }' \
     http://localhost:8000/api/ai/query
```

```json
{
  "sql": "SELECT customer_id, SUM(amount) AS revenue FROM orders GROUP BY customer_id ORDER BY revenue DESC LIMIT 5",
  "rows": [
    [47, 18234],
    [12, 14991],
    [102, 13720],
    [88, 12108],
    [56, 11644]
  ],
  "columns": ["customer_id", "revenue"],
  "explanation": "Groups orders by customer, sums amounts, returns top 5.",
  "backend": "claude",
  "elapsed_ms": 1820
}
```

### Body

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `connection` | string | yes | Saved connection name |
| `question` | string | yes | Natural-language prompt |
| `backend` | string | no | `claude`, `cursor`, `codex` (defaults to `[ai] default_backend`) |
| `sql_mode` | string | no | `strict_summary`, `summary` (default), `open` |
| `auto_execute` | bool | no | Run the generated SQL automatically (default true) |

## POST /api/ai/execute-sql

Execute a generated SELECT-style statement against a connection without an AI
session. The read-only guard applies — mutating statements return `400` with a
`blocked` reason and never touch the database.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"connection":"prod","sql":"SELECT id, name FROM users LIMIT 5"}' \
     http://localhost:8000/api/ai/execute-sql
```

A blocked write responds with:

```json
{
  "error": "Blocked: the AI assistant is read-only and cannot run data/schema-changing statements (DELETE). Only SELECT-style queries are allowed here.",
  "blocked": true
}
```

## GET /api/ai/backends

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/ai/backends
```

```json
[
  {"name": "claude", "available": true, "default": true},
  {"name": "cursor", "available": true, "default": false},
  {"name": "codex", "available": false, "default": false}
]
```

## Sessions

### POST /api/ai/sessions

Create a new session.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"connection":"prod","backend":"claude","sql_mode":"summary"}' \
     http://localhost:8000/api/ai/sessions
```

```json
{"id": "tab1", "connection": "prod", "backend": "claude", "sql_mode": "summary"}
```

### GET /api/ai/sessions

List sessions.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/ai/sessions
```

```json
[
  {"id": "tab1", "connection": "prod", "backend": "claude", "messages": 4, "last_activity": "2026-06-01T12:31:00Z"},
  {"id": "tab2", "connection": "stage", "backend": "cursor", "messages": 7, "last_activity": "2026-05-30T18:02:14Z"}
]
```

### PATCH /api/ai/sessions/{id}

Update SQL mode or execution rules mid-session.

```bash
curl -X PATCH -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"sql_mode":"open","sql_execution_rules":"Always use LIMIT on user tables"}' \
     http://localhost:8000/api/ai/sessions/tab1
```

### DELETE /api/ai/sessions/{id}

Close a session.

```bash
curl -X DELETE -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/ai/sessions/tab1?save=true"
```

`?save=true` writes the session to disk before closing.

### POST /api/ai/sessions/{id}/messages

Send a message to the session.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"message":"count rows in users"}' \
     http://localhost:8000/api/ai/sessions/tab1/messages
```

```json
{
  "role": "assistant",
  "sql": "SELECT COUNT(*) FROM users",
  "rows": [[42301]],
  "columns": ["count"],
  "explanation": "Counts all rows in the users table."
}
```

### POST /api/ai/sessions/{id}/cross-tab

Route a message to another tab.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"message":"talk to tab 2: count rows in orders"}' \
     http://localhost:8000/api/ai/sessions/tab1/cross-tab
```

### POST /api/ai/sessions/{id}/execute-sql

Run a SQL statement against the session's connection (with execution
rules applied). Read-only guard applies — writes are rejected.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"sql":"SELECT id, name FROM users LIMIT 5"}' \
     http://localhost:8000/api/ai/sessions/tab1/execute-sql
```

### POST /api/ai/sessions/save

Persist all open sessions to disk.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/ai/sessions/save
```

### POST /api/ai/sessions/load

Restore sessions from disk.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/ai/sessions/load
```

```json
{"loaded": 3, "ids": ["tab1", "tab2", "tab3"]}
```

## Module config (`ai_query/config.ini`)

### GET /api/ai/config

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/ai/config
```

### POST /api/ai/config

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"section":"ai","key":"default_backend","value":"claude"}' \
     http://localhost:8000/api/ai/config
```

### POST /api/ai/config/restore

Restores `ai_query/config.ini` from `config.ini.example`.
