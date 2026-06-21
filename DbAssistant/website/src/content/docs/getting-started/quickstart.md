---
title: Quickstart
description: Connect a database and run your first query via UI, CLI, and REST API in under 5 minutes.
sidebar:
  order: 3
---

This walkthrough takes you from a fresh install to running queries
through all three surfaces.

## 1. Verify install

```bash
python dbtool.py modules
```

Expected: a table showing `core`, `migrator`, `ai`, and `monitor` as
**installed** and **ready**.

## 2. Add a database connection (CLI)

```bash
python dbtool.py connections add \
    --name prod \
    --type PostgreSQL \
    --host db.example.com \
    --port 5432 \
    --user app \
    --db appdb
# Prompts: Password? ········
```

The password is encrypted with Fernet and stored under
`~/.dbassistant/connections/`.

Test it:

```bash
python dbtool.py connections test prod
```

Expected: `connection ok — PostgreSQL 16.x`.

## 3. Run a query

```bash
python dbtool.py query --conn prod --sql "SELECT current_database(), current_user"
```

```text
┌──────────────────┬──────────────┐
│ current_database │ current_user │
├──────────────────┼──────────────┤
│ appdb            │ app          │
└──────────────────┴──────────────┘
```

JSON output:

```bash
python dbtool.py query --conn prod --sql "SELECT 1 AS n" --format json
```

```json
{"columns":["n"],"rows":[[1]]}
```

## 4. Browse objects

```bash
python dbtool.py objects --conn prod --type tables
python dbtool.py objects --conn prod --type processlist --format json
```

## 5. Try the AI assistant

The AI module routes through a local CLI backend (Claude, Cursor, or
Codex) — no API keys to manage.

```bash
python dbtool.py ai --list-backends
python dbtool.py ai --conn prod "show me the top 5 customers by revenue"
```

Or start a multi-turn session:

```bash
python dbtool.py ai session new --conn prod --backend claude
python dbtool.py ai session ask --session tab1 "count rows in users"
python dbtool.py ai session follow-up --session tab1 "add a date filter for last 30 days"
```

## 6. Start monitoring

Foreground:

```bash
python dbtool.py monitor --conn prod --interval 30
```

Single poll:

```bash
python dbtool.py monitor --conn prod --once
```

Background daemon:

```bash
python dbtool.py daemon start --connections prod --interval 60
python dbtool.py daemon status
python dbtool.py daemon stop
```

## 7. Start the REST API

```bash
python dbtool.py api --host 127.0.0.1 --port 8000
```

In another shell:

```bash
# Health
curl http://localhost:8000/api/health

# Same query as in step 3
curl -X POST http://localhost:8000/api/query \
    -H "Content-Type: application/json" \
    -H "X-API-Key: $DBTOOL_API_KEY" \
    -d '{"connection":"prod","sql":"SELECT 1 AS n"}'

# OpenAPI / Swagger UI in a browser
open http://localhost:8000/docs
```

See [Authentication](/api/authentication/) for setting `DBTOOL_API_KEY`.

## 8. Launch the desktop UI

```bash
python conDbUi.py
# or:
python dbtool.py ui
```

The Dashboard tab opens first. Switch to **Connections** to add or test
profiles graphically.

## Next steps

- [Configuration reference](/getting-started/configuration/) — adjust `config.ini`
- [CLI reference](/cli/overview/) — every command with examples
- [REST API reference](/api/overview/) — every endpoint with curl
- [Cloud setup](/cloud/aws/) — connect AWS / Azure / GCP
