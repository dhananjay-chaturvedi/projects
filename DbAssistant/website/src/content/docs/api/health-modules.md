---
title: Health & modules
description: Liveness, installed-modules discovery, and config inspection endpoints.
sidebar:
  order: 3
---

## GET /api/health

Liveness check. Returns immediately without touching any database.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/health
```

```json
{"status": "ok", "version": "1.0.0", "modules": ["migrator", "ai", "monitor"]}
```

Always returns `200`. Use for load-balancer / uptime monitoring.

## GET /api/modules

Lists installed modules with their readiness status.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/modules
```

```json
[
  {
    "name": "migrator",
    "display_name": "Data Migration",
    "installed": true,
    "ready": true,
    "missing_requirements": []
  },
  {
    "name": "ai",
    "display_name": "AI Query Assistant",
    "installed": true,
    "ready": true,
    "missing_requirements": []
  },
  {
    "name": "monitor",
    "display_name": "Server Monitor",
    "installed": true,
    "ready": true,
    "missing_requirements": []
  }
]
```

A module is **installed** if its folder exists; **ready** if all its
Python imports succeed.

## GET /api/config

Inspect merged configuration values. Secrets are redacted when
`mask_sensitive_in_logs = true`.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/config?section=ai"
```

```json
{
  "ai": {"default_backend": "auto"},
  "ai.claude": {"default_timeout": 120, "max_output_tokens": 4000}
}
```

Without `section`, all sections are returned.

Query parameters:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `section` | no | `paths`, `ai`, `monitoring`, etc. |
| `key` | no | Single key within `section` |

Example:

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/config?section=monitoring&key=metrics_refresh_interval"
```

```json
{"monitoring": {"metrics_refresh_interval": 5000}}
```
