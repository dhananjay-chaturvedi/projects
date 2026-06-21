---
title: thresholds
description: List, show, and check alert rules from monitor_thresholds.ini.
sidebar:
  order: 9
---

Threshold rules live in `monitoring/monitor_thresholds.ini`. Each rule
has a `(source, api, path, metric)` identity, a comparison operator,
and one or more levels (`critical`, `warning`, `info`).

## list

```bash
python dbtool.py thresholds list
python dbtool.py thresholds list --source aws
python dbtool.py thresholds list --source aws --api cloudwatch
python dbtool.py thresholds list --source aws --path cloudwatch.RDS
python dbtool.py thresholds list --source db --path mysql      # per-engine DB rules
python dbtool.py thresholds list --all                # include disabled rules
python dbtool.py thresholds list --format json
```

`--path` is dot-joined. For local DB engine overrides the path is a single
engine segment: `mysql`, `mariadb`, `oracle`, `postgresql`, or `sqlite`
(omit `--path` for the generic `[metric.db.*]` rules).

Output:

```text
┌─────────┬──────────────┬───────────────────┬───────────────────────┬──────────┬──────────┬─────────┐
│ source  │ api          │ path              │ metric                │ critical │ warning  │ enabled │
├─────────┼──────────────┼───────────────────┼───────────────────────┼──────────┼──────────┼─────────┤
│ db      │ —            │ —                 │ active_connections    │ 200      │ 100      │ yes     │
│ aws     │ cloudwatch   │ RDS               │ CPUUtilization        │ 90       │ 75       │ yes     │
│ aws     │ pi           │ RDS               │ db.SQL.tup_fetched.avg│ —        │ 10000    │ no      │
│ azure   │ azuremonitor │ DBforMySQL/flex.. │ cpu_percent           │ 90       │ 75       │ yes     │
│ gcp     │ cloudmonitor │ cloudsql/database │ cpu_utilization       │ 0.9      │ 0.75     │ yes     │
└─────────┴──────────────┴───────────────────┴───────────────────────┴──────────┴──────────┴─────────┘
```

Available `--source` values: `db`, `os`, `aws`, `azure`, `gcp`.

## show

Display a single rule with all fields including the underlying
`metric_name` (the verbatim string used in the cloud API call).

```bash
python dbtool.py thresholds show --source db --metric active_connections

# Engine-specific DB override
python dbtool.py thresholds show --source db --path postgresql --metric active_connections

python dbtool.py thresholds show \
    --source aws \
    --path cloudwatch.RDS \
    --metric CPUUtilization
```

```text
[metric.aws.cloudwatch.RDS.CPUUtilization]
  api          = cloudwatch
  namespace    = AWS/RDS
  metric_name  = CPUUtilization        ← exact string used in CloudWatch GetMetricStatistics
  critical     = 90
  warning      = 75
  operator     = gt
  unit         = percent
  window       = 3                     ← consecutive breaches required
  enabled      = true
```

## check

Evaluate a value against the rule without polling a real DB.

```bash
python dbtool.py thresholds check --source db --metric active_connections --value 250
```

```text
critical breach: active_connections = 250.0 > 200 (threshold)
```

With instance and path:

```bash
python dbtool.py thresholds check \
    --source aws \
    --path cloudwatch.RDS \
    --metric CPUUtilization \
    --value 88 \
    --instance prod-rds-01
```

```text
warning breach: prod-rds-01 CPUUtilization = 88.0 > 75 (threshold)
```

OK case:

```bash
python dbtool.py thresholds check --source db --metric active_connections --value 30
# OK: active_connections = 30.0 within thresholds (warning=100, critical=200)
exit code 0
```

## REST API equivalents

```bash
# List
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds?source=aws&api=cloudwatch"

# Show
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds/aws/CPUUtilization?path=cloudwatch.RDS"

# Check
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"source":"aws","metric":"CPUUtilization","value":88,"path":["cloudwatch","RDS"],"instance":"prod-rds-01"}' \
     "http://localhost:8000/api/thresholds/check"
```

## set / enable / disable

Edit a rule in place (comment-preserving, validated). Blank a level to turn it
off; `--window` sets the consecutive-breach count:

```bash
python dbtool.py thresholds set --source db --metric active_connections --critical 300 --warning 150
python dbtool.py thresholds set --source db --path oracle --metric database_size_mb --warning 500000
python dbtool.py thresholds set --source aws --path cloudwatch.RDS \
    --metric CPUUtilization --operator ">=" --window 5
python dbtool.py thresholds disable --source db --path sqlite --metric database_size_mb
python dbtool.py thresholds disable --source os --metric load_avg_5m
python dbtool.py thresholds enable  --source os --metric load_avg_5m
```

Editable fields: `critical`, `warning`, `info`, `operator`
(`> >= < <= == !=`), `window`, `description`, and enable/disable. Other fields
(`metric_name`, provider metadata) are structural and edited directly in the
INI. You can also edit rules from the **Monitor** tab via the **⚙ Alert
Settings** button.

These writes are also available through the desktop UI; they are **not**
exposed for writing over the REST API (the API is read-only for config).

## Editing rules by hand

You can still add or modify entries directly in
`monitoring/monitor_thresholds.ini`; rules are reloaded each poll cycle. Full
schema in [Threshold rules schema](/reference/threshold-rules/).

## See also

- [Settings & notifications](/guides/settings/)
