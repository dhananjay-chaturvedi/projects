---
title: monitor
description: Poll DB and OS metrics, evaluate thresholds, and dispatch alerts.
sidebar:
  order: 7
---

The `monitor` command requires the **Server Monitor** module. For
long-running background monitoring use [`daemon`](/cli/daemon/) instead.

## Single poll

```bash
python dbtool.py monitor --conn prod --once
```

Output:

```text
[prod] PostgreSQL — connections=14/100, active=3, txn/s=120, cache_hit=99.4%
[prod] OS — cpu=42% mem=68% disk_root=78% load=1.45

Thresholds OK
```

Multiple connections in one poll:

```bash
python dbtool.py monitor --conn prod,stage,dev --once
```

All saved connections:

```bash
python dbtool.py monitor --once
```

Save the snapshot:

```bash
python dbtool.py monitor --once --output metrics.json
```

## Foreground loop

```bash
python dbtool.py monitor --conn prod --interval 30
```

Polls every 30 seconds until `Ctrl+C`. Breached thresholds print
coloured alerts and dispatch a Teams notification (if
`ALERT_TEAMS_WEBHOOK_URL` is set).

Sample alert line:

```text
[ALERT/critical] prod  cpu_percent=92.4  threshold=90  (sustained 3/3 windows)
```

## Options

| Flag | Default | Effect |
|------|---------|--------|
| `--conn NAME[,NAME...]` | all saved | Connections to poll |
| `--interval SECONDS` | 30 | Polling interval (foreground loop) |
| `--once` | off | Single poll, exit immediately |
| `--output FILE` | — | Write metrics to JSON |
| `--no-alerts` | off | Don't dispatch notifications |
| `--format table\|json` | `table` | Output format |

## What is collected

| Source | Metrics |
|--------|---------|
| Database | Connections, active sessions, transactions/sec, cache-hit ratio, replication lag, lock waits, slow queries (engine-dependent) |
| Host OS | CPU, memory, disk, load average, process count |
| (Cloud) | Use [`cloud monitor`](/cli/cloud/) for AWS / Azure / GCP |

## Threshold evaluation

For each metric, the engine looks up the matching rule in
`monitoring/monitor_thresholds.ini` and applies the operator + level
(`critical` / `warning` / `info`). Sustained-breach logic requires the
threshold to be breached `window` consecutive polls before firing.

Manually evaluate one value without polling:

```bash
python dbtool.py thresholds check --source db --metric active_connections --value 250
```

## REST API equivalent

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/metrics/prod"
```

## See also

- [`daemon`](/cli/daemon/) — background loop with PID / log files
- [`thresholds`](/cli/thresholds/) — manage rules
- [`notify`](/cli/notify/) — test the alert channel
- [Server Monitor module](/modules/monitoring/)
