---
title: config
description: Inspect the effective configuration (config.ini, properties.ini).
sidebar:
  order: 14
---

Read **and edit** configuration values. The `list`/`get`/`describe`/`set`/
`restore` subcommands operate on a *curated, self-describing* set of settings
(the same schema the Settings UI and read-only config API use). The legacy
`show` subcommand dumps raw INI sections.

## list

Show every curated setting with its current value and default:

```bash
python dbtool.py config list
python dbtool.py config list --group Notifications
python dbtool.py config list --format json
```

```text
| group         | id                                    | value   | default | restart? |
|---------------|---------------------------------------|---------|---------|----------|
| Database      | config.database.connection.query_...  | 0       | 0       |          |
| Notifications | properties.notifications.enabled      | false   | false   |          |
```

## describe / get

`describe` prints the human explanation, allowed values, range and current
value; `get` returns the JSON record:

```bash
python dbtool.py config describe config.database.connection.connection_timeout
python dbtool.py config get properties.notifications.smtp_host
```

## set

Validate and persist a single setting. Writes are *surgical* — only the target
`key = value` line in `config.ini` / `properties.ini` changes, so your comments
are preserved:

```bash
python dbtool.py config set config.database.connection.query_timeout 120
python dbtool.py config set properties.notifications.enabled true
python dbtool.py config set properties.notifications.email_to "oncall@x.com, dba@x.com"
```

Notification **secrets** are stored encrypted under `~/.dbassistant`, never in
the INI files:

```bash
python dbtool.py config set secret.notifications.teams_webhook_url "https://outlook.office.com/webhook/..."
python dbtool.py config set secret.notifications.smtp_password "app-password"
```

Secrets are write-only — they are never printed back by `get`/`list`/the API
(those show `***set***`).

## restore

Restore settings to the shipped defaults (`config.ini.example` /
`properties.ini.example`). Prompts for confirmation unless `--yes`:

```bash
python dbtool.py config restore                 # both files, interactive
python dbtool.py config restore --target properties --yes
```

## show

All sections:

```bash
python dbtool.py config show
```

One section:

```bash
python dbtool.py config show --section ai
```

```text
[ai]
  default_backend = auto
  context_max_tables = 50

[ai.claude]
  default_timeout = 120
  max_output_tokens = 4000
```

JSON form:

```bash
python dbtool.py config show --section ai --format json
```

```json
{
  "ai": {
    "default_backend": "auto",
    "context_max_tables": 50
  },
  "ai.claude": {
    "default_timeout": 120,
    "max_output_tokens": 4000
  }
}
```

## Masked values

If `[security] mask_sensitive_in_logs = true` in `config.ini`, secrets
(API keys, webhook URLs, passwords) are redacted:

```text
[paths]
  oracle_client_path =
  runtime_dir = ~/.dbassistant/runtime
  session_dir = ~/.dbassistant/session

ALERT_TEAMS_WEBHOOK_URL = ***REDACTED***
DBTOOL_API_KEY = ***REDACTED***
```

## What `config show` reads

| Source | Priority |
|--------|----------|
| `config.ini` | base |
| `properties.ini` | merged in (UI-specific keys) |
| `.env` | environment overrides |
| Process env vars | highest |

## REST API equivalent

The API exposes config **read-only** (secrets redacted, no write routes):

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" "http://localhost:8000/api/config?section=ai"
curl -H "X-API-Key: $DBTOOL_API_KEY" "http://localhost:8000/api/config/settings?group=Notifications"
curl -H "X-API-Key: $DBTOOL_API_KEY" "http://localhost:8000/api/config/settings/config.database.connection.query_timeout"
```

Editing configuration is intentionally **not** exposed over HTTP — change
settings via the Settings UI tab or the `config set` CLI on the host.

## See also

- [Settings & notifications](/guides/settings/)
- [Configuration overview](/getting-started/configuration/)
- [`config.ini` reference](/reference/config-ini/)
- [Environment variables](/reference/env-vars/)
