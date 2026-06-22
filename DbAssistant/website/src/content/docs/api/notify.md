---
title: Notifications
description: Send notifications through the configured channel.
sidebar:
  order: 12
---

## POST /api/notify

Send a notification through the configured channel (MS Teams via
webhook).

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"severity":"WARNING","message":"test alert"}' \
     http://localhost:8000/api/notify
```

```json
{"status": "sent", "channel": "teams", "http_status": 200}
```

### Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `severity` | string | yes | `INFO`, `WARNING`, `CRITICAL` |
| `message` | string | yes | Body text |
| `title` | string | no | Card title (defaults to severity) |
| `instance` | string | no | Logical instance to attribute the alert to |

### Errors

| Code | Cause |
|------|-------|
| `400` | Invalid severity |
| `503` | `ALERT_TEAMS_WEBHOOK_URL` not set |
| `503` | Webhook returned non-2xx (`detail` contains HTTP code) |

## Programmatic

```python
import os, requests
requests.post(
    "http://localhost:8000/api/notify",
    headers={"X-API-Key": os.environ["DBTOOL_API_KEY"]},
    json={"severity": "WARNING", "message": "Disk almost full"},
).raise_for_status()
```

## See also

- [`notify` CLI](/cli/notify/) — same channel, terminal-side
- [Thresholds](/api/thresholds/) — what triggers automatic alerts
