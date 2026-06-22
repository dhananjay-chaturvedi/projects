---
title: Daemon status
description: Read-only daemon status endpoint.
sidebar:
  order: 13
---

The daemon is controlled from the CLI for safety. The API exposes a
read-only status view only.

## GET /api/daemon/status

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/daemon/status
```

Running:

```json
{
  "running": true,
  "pid": 12345,
  "uptime_seconds": 11640,
  "connections": ["prod", "stage"],
  "interval_seconds": 60,
  "last_poll_iso": "2026-06-01T12:31:05Z",
  "metrics_file": "/Users/me/.dbassistant/runtime/metrics.json",
  "metrics_file_updated_seconds_ago": 3,
  "log_file": "/Users/me/.dbassistant/runtime/daemon.log",
  "log_size_bytes": 116745
}
```

Stopped:

```json
{
  "running": false,
  "pid": null,
  "last_pid_file_seen": null,
  "metrics_file_exists": true,
  "metrics_file_age_seconds": 8421
}
```

## Controlling the daemon

Lifecycle is CLI-only:

```bash
python dbtool.py daemon start
python dbtool.py daemon stop
python dbtool.py daemon status
```

There is no `POST /api/daemon/start` or `/stop`. If you need
remote-controlled lifecycle, wrap the CLI in a sysadmin-controlled
endpoint outside of DbAssistant (Ansible / cloud-init / systemd
remote-control / SSH).

## See also

- [`daemon` CLI](/cli/daemon/)
- [Daemon & systemd](/operations/daemon/) — service-unit examples
