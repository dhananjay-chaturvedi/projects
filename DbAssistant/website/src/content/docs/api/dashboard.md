---
title: Dashboard
description: Aggregated overview endpoint used by the desktop Dashboard tab.
sidebar:
  order: 14
---

The Dashboard endpoint returns a structured overview suitable for
rendering a "home" panel — installed modules, connection counts, last
poll times, and pinned cards.

## GET /api/dashboard

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/dashboard
```

```json
{
  "version": "1.0.0",
  "generated_at": "2026-06-01T12:31:05Z",
  "modules": [
    {"name": "migrator",  "installed": true, "ready": true},
    {"name": "ai",      "installed": true, "ready": true},
    {"name": "monitor", "installed": true, "ready": true}
  ],
  "connections": {
    "db": 3,
    "cloud": 2,
    "monitor": 1
  },
  "monitor": {
    "daemon_running": true,
    "last_poll_iso": "2026-06-01T12:31:00Z",
    "alerts_open": 0
  },
  "ai": {
    "default_backend": "claude",
    "open_sessions": 2,
    "saved_sessions": 14
  },
  "layout_version": 2
}
```

## When to use it

Build a "Home" view or external dashboard without making several round
trips. The endpoint never touches a DB — it summarises in-memory state
and the daemon snapshot.

For deep panels (per-connection metrics, per-rule threshold checks),
call the specific endpoints:

- [`/api/metrics`](/api/metrics/)
- [`/api/thresholds`](/api/thresholds/)
- [`/api/connections`](/api/connections/)
- [`/api/modules`](/api/health-modules/)

## Note

This endpoint is part of the **shared core** (always available, no
module gating). Fields under `monitor`, `ai`, and `connections.cloud`
appear only when the corresponding module is installed.
