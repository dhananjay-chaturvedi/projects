---
title: config.ini reference
description: Core keys DbAssistant reads from config.ini (module-owned keys live elsewhere).
sidebar:
  order: 3
---

`config.ini` at the project root holds **core** engine and UI-runtime defaults only.
Monitoring, AI Query, and Data Migration settings live in each module's own
`config.ini` (see [Configuration](/getting-started/configuration/)).

## `[paths]`

| Key | Default | Effect |
|-----|---------|--------|
| `oracle_client_path` | (blank) | If set, Oracle thick-mode client directory. Blank = thin mode |
| `runtime_dir` | `~/.dbassistant/runtime` | Daemon PID, log, `metrics.json` |
| `session_dir` | `~/.dbassistant/session` | AI sessions and dashboard layout |

## `[database.ports]`

| Key | Default |
|-----|---------|
| `oracle` | `1521` |
| `mysql` | `3306` |
| `mariadb` | `3306` |
| `postgresql` | `5432` |
| `sqlserver` | `1433` |
| `mongodb` | `27017` |

## `[database.connection]`

| Key | Default | Effect |
|-----|---------|--------|
| `connection_timeout` | `30.0` | seconds before connect fails |
| `query_timeout` | `0` | per-statement timeout where supported (`0` = no timeout) |
| `default_autocommit` | `true` | autocommit mode applied to **new** connections (all engines) |
| `max_connection_attempts` | `8` | connection retry attempts |

`default_autocommit` is honoured at connect time by every SQL driver
(MySQL, MariaDB, PostgreSQL, Oracle, SQLite, SQL Server). In the desktop
**SQL Editor**, the per-tab **Auto-commit** checkbox reflects the live
connection's current state and can be toggled per session; saving a new
`default_autocommit` in the **Settings** tab applies it to open SQL Editor
tabs and to future connections.

## `[database.performance]`

| Key | Default | Effect |
|-----|---------|--------|
| `transfer_batch_size` | `1000` | rows per batch in data transfer |
| `use_buffered_cursor` | `true` | MySQL/MariaDB buffered cursors |

## `[api]`

| Key | Default | Effect |
|-----|---------|--------|
| `host` | `127.0.0.1` | default bind host for `dbtool api` |
| `port` | `8000` | default bind port for `dbtool api` |
| `cors_origins` | *(blank = localhost only)* | comma-separated CORS origins; set `*` only for development |
| `max_body_bytes` | `10485760` | REST request body cap (10 MB) |

## `[security]`

| Key | Default | Effect |
|-----|---------|--------|
| `key_file_permissions` | `0o600` | encryption key file permissions |
| `config_file_permissions` | `0o600` | config file permissions |

## Module-owned files (not in core `config.ini`)

| Module | File | Example sections |
|--------|------|------------------|
| Monitoring | `monitoring/monitor_config.ini` | `[monitoring]`, `[ssh.connection]`, `[cloud.lookback]`, `[notifications]` |
| AI Query | `ai_query/config.ini` | `[ai]`, `[ai.claude]`, `[ai.cursor]`, `[ai.codex]`, `[ui.ai_query]`, `[ai.limits]` |
| Data Migration | `schema_converter/config.ini` | `[schema.conversion]` |

See `*.ini.example` beside each live file for the full schema.

## `[ui.limits]` *(in `properties.ini`)*

| Key | Default | Effect |
|-----|---------|--------|
| `cli_max_display_rows` | `1000` | CLI table display cap (0 = unlimited) |
| `table_export_max_rows` | `0` | export row cap |
| `result_grid_max_rows` | `5000` | UI grid render cap |
| `query_result_max_rows` | `10000` | in-memory SELECT cap |

## Reading config programmatically

```python
from common.config_loader import config, properties

print(config.get("database.connection", "connection_timeout"))
print(properties.get_int("ui.limits", "query_result_max_rows"))

# Module-owned (Monitoring example)
from monitoring import monitor_config
print(monitor_config.get_int("monitoring", "metrics_refresh_interval"))
```

## See also

- [Configuration overview](/getting-started/configuration/)
- [Monitoring module](/modules/monitoring/)
- [Environment variables](/reference/env-vars/)
