---
title: Settings & notifications
description: Core Settings tab, per-module settings buttons, Teams + email alerts, and threshold editing — with CLI / API parity.
sidebar:
  order: 1
---

Configuration is split between **core** files (`config.ini`, `properties.ini`) and
**module-owned** INI files. The desktop **Settings** tab edits core files only.
Each optional module provides its own settings button and CLI/API surface so
modules can ship independently without coupling to the core schema.

## Core Settings tab (UI)

The **Settings** tab (between *Monitor* and *Clear Cache*) edits `config.ini` and
`properties.ini` — database paths, UI limits, security, and other **core** keys.
It does **not** include monitoring refresh intervals, AI backends, or migration
compare options (those moved to module files).

Features:

- grouped categories with descriptions and inline hints,
- **Save changes** (surgical, comment-preserving writes),
- **Restore defaults** (re-copies `common/config/*.ini.example`).

:::note[Auto-commit]
`[database.connection] default_autocommit` (default `true`) is applied at connect
time by every SQL driver. Saving a new value in the Settings tab also pushes it to
**open SQL Editor tabs** and future connections. Each SQL Editor tab keeps its own
**Auto-commit** checkbox synced to its live connection, so you can override the
default per session.
:::

## Module settings buttons

| Tab | Button | File |
|-----|--------|------|
| Monitor | **Monitor Settings** | `monitoring/monitor_config.ini` |
| Monitor | **Alert Thresholds** | `monitoring/monitor_thresholds.ini` |
| AI Query | **AI Settings** | `ai_query/config.ini` |
| Data Migration | **Migration Settings** | `schema_converter/config.ini` |

## Notifications (Monitoring module)

Alert delivery is configured in `monitoring/monitor_config.ini` under
`[notifications]` (non-secret routing). Secrets are **encrypted** under
`~/.dbassistant`:

| Setting | Meaning |
|---------|---------|
| `enabled` | Master switch for threshold alert delivery |
| `min_severity` | `INFO` / `WARNING` / `CRITICAL` floor |
| `teams_enabled` | Microsoft Teams Incoming Webhook |
| `email_enabled` | SMTP email |
| `smtp_*`, `email_from`, `email_to` | Mail server and addressing |
| *Teams webhook URL* (secret) | Encrypted |
| *SMTP password* (secret) | Encrypted |

:::note[Legacy webhook]
`ALERT_TEAMS_WEBHOOK_URL` in `.env` still works as a fallback until the encrypted
webhook is configured.
:::

## CLI parity

**Core** (Settings tab scope):

```bash
python dbtool.py config list --group General
python dbtool.py config set config.database.connection.query_timeout 600
python dbtool.py config restore --target config --yes
```

**Monitoring module:**

```bash
python dbtool.py monitor-config show
python dbtool.py monitor-config set monitoring metrics_refresh_interval 5000
python dbtool.py monitor notify config
python dbtool.py monitor notify config set notifications enabled true
python dbtool.py thresholds set --source db --metric active_connections --critical 300
```

**AI Query module:**

```bash
python dbtool.py ai config show
python dbtool.py ai config set ai default_backend claude
```

**Data Migration module:**

```bash
python dbtool.py migrator config show
python dbtool.py migrator config set schema.conversion compare_sample_size 20
python dbtool.py migrator config set schema.conversion type_overrides "varchar2:text"
python dbtool.py migrator config set schema.conversion conversion_charset utf-8
```

`[schema.conversion]` keys: `compare_sample_size`, `zero_date_strategy`,
`parallel_workers`, `type_overrides`, `conversion_charset`, `overflow_policy`
(`fail`/`truncate`/`skip`), `null_policy` (`keep`/`empty_to_null`/`null_to_empty`),
`bool_policy` (`auto`/`int`/`true_false`), `timezone_policy`
(`preserve`/`naive`/`utc`/`target`), `target_timezone`.

## API

| Surface | Read | Write |
|---------|------|-------|
| Core settings | `GET /api/config/settings` | — (read-only by design) |
| Monitor config | `GET /api/monitor/config` | `POST /api/monitor/config` |
| Notifications | `GET /api/monitor/notifications` | `POST /api/monitor/notifications`, `POST .../secret` |
| AI config | `GET /api/ai/config` | `POST /api/ai/config` |
| Migrator config | `GET /api/migrator/config` | `POST /api/migrator/config` |

Protect all endpoints with `DBTOOL_API_KEY` when the API is reachable beyond
`127.0.0.1`.

## Alert thresholds

Edited from **Alert Thresholds** on the Monitor tab or via `thresholds` CLI/API.
Rules live in `monitor_thresholds.ini` (comment-preserving edits).

## See also

- [`config` CLI](/cli/config/) — core only
- [`notify`](/cli/notify/) — test delivery + monitor notification config
- [Configuration overview](/getting-started/configuration/)
