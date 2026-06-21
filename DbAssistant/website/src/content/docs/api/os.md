---
title: OS metrics
description: Host OS metric endpoint (CPU, memory, disk, load average).
sidebar:
  order: 10
---

Requires the **Server Monitor** module.

## GET /api/os/metrics

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/os/metrics
```

```json
{
  "cpu": {"user": 18.0, "system": 4.1, "idle": 76.0, "percent": 22.1},
  "load_avg": {"1m": 1.45, "5m": 1.12, "15m": 0.94},
  "memory": {
    "total_bytes": 17179869184,
    "used_bytes": 11597039616,
    "free_bytes": 5582829568,
    "percent": 67.5
  },
  "disk": {
    "mount": "/",
    "total_bytes": 250000000000,
    "used_bytes": 190400000000,
    "percent": 76.2
  },
  "processes": {"total": 312, "running": 2},
  "polled_at": "2026-06-01T12:30:45Z"
}
```

## Query parameters

| Parameter | Description |
|-----------|-------------|
| `disk` | Mount point to inspect (default `/`); pass `/var`, `/data`, etc. |

## With threshold evaluation

OS metrics are not auto-evaluated by this endpoint; use:

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"source":"os","metric":"cpu_percent","value":99}' \
     http://localhost:8000/api/thresholds/check
```

The monitor loop and daemon do this automatically every cycle.

## See also

- [`os` CLI](/cli/os/)
- [Thresholds API](/api/thresholds/)
