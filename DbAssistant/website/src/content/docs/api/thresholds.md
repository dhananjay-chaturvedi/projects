---
title: Thresholds
description: List, inspect, and evaluate threshold rules.
sidebar:
  order: 9
---

Requires the **Server Monitor** module.

## GET /api/thresholds

List threshold rules.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds"
```

Filter by source / API / path:

```bash
# All AWS rules
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds?source=aws"

# Only CloudWatch RDS rules
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds?source=aws&api=cloudwatch&path=RDS"

# Include disabled rules
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds?source=aws&all=true"
```

Response:

```json
[
  {
    "source": "aws",
    "api": "cloudwatch",
    "path": ["RDS"],
    "metric": "CPUUtilization",
    "metric_name": "CPUUtilization",
    "section": "metric.aws.cloudwatch.RDS.CPUUtilization",
    "namespace": "AWS/RDS",
    "critical": 90,
    "warning": 75,
    "operator": "gt",
    "unit": "percent",
    "window": 3,
    "enabled": true
  },
  ...
]
```

Query parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source` | string | (all) | `db`, `os`, `aws`, `azure`, `gcp` |
| `api` | string | (all) | `cloudwatch`, `pi`, `azuremonitor`, `cloudmonitoring` |
| `path` | string | (all) | Dot-joined path; e.g. `cloudwatch.RDS` |
| `all` | bool | `false` | Include disabled rules |

## GET /api/thresholds/{source}/{metric}

Show one rule.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds/db/active_connections"

# Per-engine DB override (path = engine name):
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds/db/active_connections?path=postgresql"

# With path disambiguation:
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/thresholds/aws/CPUUtilization?path=cloudwatch.RDS"
```

```json
{
  "source": "aws",
  "api": "cloudwatch",
  "path": ["RDS"],
  "metric": "CPUUtilization",
  "metric_name": "CPUUtilization",
  "namespace": "AWS/RDS",
  "critical": 90,
  "warning": 75,
  "operator": "gt",
  "unit": "percent",
  "window": 3,
  "enabled": true
}
```

`404` if the rule doesn't exist.

## POST /api/thresholds/check

Evaluate a value against a rule.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "source": "aws",
       "metric": "CPUUtilization",
       "value": 88,
       "path": ["cloudwatch", "RDS"],
       "instance": "prod-rds-01"
     }' \
     http://localhost:8000/api/thresholds/check
```

```json
{
  "breached": true,
  "level": "warning",
  "value": 88,
  "threshold": 75,
  "metric": "CPUUtilization",
  "instance": "prod-rds-01",
  "message": "prod-rds-01 CPUUtilization=88.0 above warning threshold 75 (cloudwatch)"
}
```

OK case:

```json
{
  "breached": false,
  "level": "ok",
  "value": 30,
  "metric": "cpu_percent"
}
```

### Body

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | yes | `db`, `os`, `aws`, `azure`, `gcp` |
| `metric` | string | yes | Metric name (`rule_id`) |
| `value` | number | yes | Value to evaluate |
| `path` | array | no | Path segments (e.g. `["cloudwatch", "RDS"]`) |
| `instance` | string | no | Logical instance name for the alert message |
| `api` | string | no | API hint (rarely needed; derivable from `path`) |

## Editing rules

Rules are file-driven (`monitoring/monitor_thresholds.ini`). The API
intentionally does **not** allow rule mutation over HTTP — to change a
rule, edit the INI and the next monitor cycle picks it up.

For the rule schema, see [Threshold rules schema](/reference/threshold-rules/).
