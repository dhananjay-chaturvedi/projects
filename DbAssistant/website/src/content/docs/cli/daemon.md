---
title: daemon
description: Manage the background monitoring process — PID, log, metrics.json.
sidebar:
  order: 8
---

The daemon runs the monitoring loop in the background. It writes a
metrics snapshot to disk, which `GET /api/metrics` serves to dashboards.

## start

Unix double-fork detach:

```bash
python dbtool.py daemon start \
    --connections prod,stage \
    --interval 60
```

Foreground (for Docker / systemd):

```bash
python dbtool.py daemon start --foreground --interval 60
```

Options:

| Flag | Default | Effect |
|------|---------|--------|
| `--connections NAME[,NAME...]` | all | Connections to monitor |
| `--interval SECONDS` | 30 | Poll interval |
| `--foreground` | off | Don't detach (PID 1 process) |
| `--no-alerts` | off | Suppress Teams notifications |

## status

```bash
python dbtool.py daemon status
```

```text
daemon running — pid 12345
  uptime: 3h 14m
  connections: prod, stage
  interval: 60s
  last poll: 2026-06-01 12:31:05 (3s ago)
  metrics file: ~/.dbassistant/runtime/metrics.json (last update 3s ago)
  log: ~/.dbassistant/runtime/daemon.log (114 KB)
```

If not running:

```text
daemon not running
  PID file: not found
exit code 1
```

## stop

```bash
python dbtool.py daemon stop
```

```text
sent SIGTERM to pid 12345
daemon stopped (waited 2.1s)
```

If the PID can't be reaped within 10 seconds, `SIGKILL` is sent.

## Files written

| Path | Configurable | Contents |
|------|-------------|----------|
| `~/.dbassistant/runtime/daemon.pid` | yes (`[paths] runtime_dir`) | PID |
| `~/.dbassistant/runtime/daemon.log` | yes | Structured log lines |
| `~/.dbassistant/runtime/metrics.json` | yes | Last-poll snapshot (read by `GET /api/metrics`) |

## Inspecting the log

```bash
tail -f ~/.dbassistant/runtime/daemon.log
```

```text
2026-06-01 12:30:00 INFO  poll start (cycle=193)
2026-06-01 12:30:01 INFO  prod cpu=42% conn=14/100
2026-06-01 12:30:01 INFO  stage cpu=68% conn=12/100
2026-06-01 12:30:02 WARN  stage cpu=68% above warning=60 (window 1/3)
2026-06-01 12:30:02 INFO  poll complete (1.94s)
```

## Auto-start with systemd

See [Daemon & systemd](/operations/daemon/) for a complete unit-file
example.

## REST API

Read-only status:

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/daemon/status"
```

Start and stop are intentionally CLI-only — the API never exposes
lifecycle to remote callers.

## See also

- [`monitor`](/cli/monitor/) — same loop in foreground
- [Daemon & systemd](/operations/daemon/) — production deployment
