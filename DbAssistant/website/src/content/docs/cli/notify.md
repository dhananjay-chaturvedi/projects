---
title: notify
description: Send a test notification and configure alert channels (Monitoring module).
sidebar:
  order: 12
---

Sends a notification through configured channels (Microsoft Teams and/or email) and
shows the current channel configuration.

## Setup

Notifications are owned by the **Monitoring** module. Non-secret routing lives in
`monitoring/monitor_config.ini` under `[notifications]`; secrets are encrypted under
`~/.dbassistant`.

Configure from the Monitor tab (**Monitor Settings**) or CLI:

```bash
python dbtool.py monitor notify config
python dbtool.py monitor notify config set notifications enabled true
python dbtool.py monitor notify config set notifications teams_enabled true
python dbtool.py monitor notify config set secret teams_webhook_url "https://outlook.office.com/webhook/..."

# email
python dbtool.py monitor notify config set notifications email_enabled true
python dbtool.py monitor notify config set notifications smtp_host smtp.example.com
python dbtool.py monitor notify config set notifications smtp_port 587
python dbtool.py monitor notify config set notifications email_from alerts@example.com
python dbtool.py monitor notify config set notifications email_to "oncall@example.com"
python dbtool.py monitor notify config set secret smtp_password "app-password"
```

Refresh/keepalive and cloud lookback use `monitor-config` (same module file):

```bash
python dbtool.py monitor-config show
python dbtool.py monitor-config set monitoring metrics_refresh_interval 5000
```

A legacy `.env` `ALERT_TEAMS_WEBHOOK_URL=...` still works as a fallback.

## config

Show resolved notification configuration (secrets never printed):

```bash
python dbtool.py monitor notify config
python dbtool.py monitor notify config --format json
```

## send

```bash
python dbtool.py monitor notify send \
    --severity WARNING \
    --message "Disk almost full on prod"
```

Severities: `INFO`, `WARNING`, `CRITICAL`.

## REST API

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/monitor/notifications
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"section":"notifications","key":"enabled","value":"true"}' \
     http://localhost:8000/api/monitor/notifications
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"severity":"WARNING","message":"test alert"}' \
     http://localhost:8000/api/notify
```

## See also

- [Settings & notifications](/guides/settings/)
- [`monitor-config`](/cli/monitor/) — module INI reference
