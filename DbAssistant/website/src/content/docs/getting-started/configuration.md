---
title: Configuration
description: All configuration files DbAssistant reads and how to customise them.
sidebar:
  order: 4
---

## Files at a glance

| File | Owner | Purpose | Where it lives |
|------|-------|---------|----------------|
| `config.ini` | Core | Paths, DB ports/timeouts, security | project root |
| `properties.ini` | Core | UI sizes, colors, panel limits | project root |
| `monitoring/monitor_config.ini` | Monitoring | Refresh/keepalive, SSH, graphs, cloud lookback, notifications (non-secret) | `monitoring/` |
| `monitoring/monitor_thresholds.ini` | Monitoring | Alert rules (DB / OS / AWS / Azure / GCP) | `monitoring/` |
| `ai_query/config.ini` | AI Query | Backends, limits, UI AI defaults | `ai_query/` |
| `schema_converter/config.ini` | Data Migration | Compare sample size, zero-date strategy | `schema_converter/` |
| `.env` | Optional | Legacy Teams webhook fallback | project root |
| `~/.dbassistant/keys/*` | Runtime | Fernet encryption keys | per user |
| `~/.dbassistant/connections/*` | Runtime | Encrypted profiles (DB, cloud, monitor) | per user |
| `~/.dbassistant/runtime/` | Runtime | Daemon PID, log, `metrics.json` | per user |
| `~/.dbassistant/session/` | Runtime | AI sessions, dashboard layout | per user |

Each module ships a `*.ini.example` default. The tool reads that file when no live
copy exists; the first save (module settings button, CLI, or API) creates the live
`*.ini`, which is a per-install artifact and should not be committed.

## Core `config.ini` (selected sections)

```ini
[paths]
oracle_client_path =                # blank = oracledb thin mode
runtime_dir = ~/.dbassistant/runtime
session_dir = ~/.dbassistant/session

[database.ports]
oracle = 1521
mysql = 3306
mariadb = 3306
postgresql = 5432
sqlserver = 1433
mongodb = 27017

[database.connection]
connection_timeout = 30.0
query_timeout = 0
default_autocommit = true
max_connection_attempts = 8

[security]
key_file_permissions = 0o600
config_file_permissions = 0o600
```

Monitoring intervals, SSH timeouts, AI backends, and migration compare settings
**do not** live in core `config.ini` anymore — see the module files above.

## Module settings (UI, CLI, API)

| Module | UI button | CLI | API |
|--------|-----------|-----|-----|
| Monitoring | **Monitor Settings**, **Alert Thresholds** | `monitor-config`, `notify config`, `thresholds` | `/api/monitor/config`, `/api/monitor/notifications` |
| AI Query | **AI Settings** | `ai config` | `/api/ai/config` |
| Data Migration | **Migration Settings** | `migrator config` | `/api/migrator/config` |

```bash
python dbtool.py monitor-config show
python dbtool.py monitor notify config
python dbtool.py ai config show
python dbtool.py migrator config show
```

Notification **secrets** (Teams webhook URL, SMTP password) stay encrypted under
`~/.dbassistant` and are set via Monitor Settings, `monitor notify config set`, or
`/api/monitor/notifications/secret` — never in plain INI text.

## `.env`

```dotenv
# REST API
DBTOOL_API_KEY=replace-with-a-long-random-string

# Legacy Teams webhook (fallback until encrypted store is configured)
ALERT_TEAMS_WEBHOOK_URL=https://outlook.office.com/webhook/...

# Optional layout override (tests, containers, CI)
DBASSISTANT_HOME=/path/to/isolated/home
```

## Per-environment overrides

```bash
export DBASSISTANT_HOME=/path/to/isolated/home
```

Everything the tool writes — keys, connections, runtime, sessions — will live under
that path instead of `~/.dbassistant/`.

## Inspecting config

```bash
# Core only
python dbtool.py config show
python dbtool.py config show --section database.connection

# Module-owned
python dbtool.py monitor-config show
python dbtool.py ai config show
```

```bash
curl "http://localhost:8000/api/config/settings"
curl "http://localhost:8000/api/monitor/config"
curl "http://localhost:8000/api/ai/config"
```

Core settings API is **read-only**. Module config endpoints support read and write
(see each module's API page).

## Reloading

Core `config.ini` / `properties.ini` are read on process start — restart the UI /
CLI / API after edits. Module INI files (`monitor_config.ini`, `ai_query/config.ini`,
etc.) reload when their file mtime changes (typically on the next poll or dialog save).

## Live smoke matrix

Headless verification against saved DB and cloud profiles:

```bash
PYTHONPATH=. .venv/bin/python scripts/live_smoke_matrix.py
```

Override connection names (use your saved profile names):

```bash
export DBTOOL_SMOKE_DB_CONNS="prod,staging"
export DBTOOL_SMOKE_CLOUD_CONNS="cloud-aws-prod,cloud-gcp-staging"
```

## Next steps

- [`config.ini` reference](/reference/config-ini/) — core keys
- [Monitoring module](/modules/monitoring/) — `monitor_config.ini` + thresholds
- [Environment variables](/reference/env-vars/)
- [Threshold rules schema](/reference/threshold-rules/)
