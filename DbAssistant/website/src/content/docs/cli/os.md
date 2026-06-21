---
title: os
description: Host OS metrics (CPU, memory, disk, load average).
sidebar:
  order: 11
---

The `os` command reads host metrics via `psutil`. Useful for spot-checks
or scripting.

## metrics

```bash
python dbtool.py os metrics
```

```text
CPU            user=18% sys=4% idle=76%
Load           1m=1.45 5m=1.12 15m=0.94
Memory         total=16.0 GB  used=10.8 GB (67.5%)  free=5.2 GB
Disk           /        used=190.4 GB / 250.0 GB (76%)
Processes      total=312  running=2
```

Disk usage for a specific mount:

```bash
python dbtool.py os metrics --disk /var
```

JSON output (handy for piping):

```bash
python dbtool.py os metrics --format json
```

```json
{
  "cpu": {"user": 18.0, "system": 4.1, "idle": 76.0},
  "load_avg": {"1m": 1.45, "5m": 1.12, "15m": 0.94},
  "memory": {
    "total_bytes": 17179869184,
    "used_bytes": 11597039616,
    "free_bytes": 5582829568,
    "percent": 67.5
  },
  "disk": {"mount": "/", "total_bytes": ..., "used_bytes": ...},
  "processes": {"total": 312, "running": 2}
}
```

## With thresholds

`os metrics` reads metric values only. To evaluate them against
threshold rules:

```bash
python dbtool.py thresholds check --source os --metric cpu_percent --value 99
```

The monitor loop and daemon do this automatically every cycle.

## REST API equivalent

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/os/metrics"

curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/os/metrics?disk=/var"
```

## Programmatic

```python
from app.headless.db_service import DBService
print(DBService().get_os_metrics(disk="/"))
```

## SSH server metrics

Local-only by default. To monitor a remote server's OS metrics over
SSH, register the server in the Monitor tab of the desktop UI — see
[Server Monitor module](/modules/monitoring/). The CLI flow currently
focuses on local + cloud metrics.
