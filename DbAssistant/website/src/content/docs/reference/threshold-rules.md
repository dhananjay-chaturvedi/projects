---
title: Threshold rules schema
description: The complete schema for monitor_thresholds.ini including the variable-length section naming.
sidebar:
  order: 2
---

Each threshold rule lives in `monitoring/monitor_thresholds.ini` and is
identified by a structured section name plus a set of fields.

## Section naming

```ini
[metric.<source>.<api>.<...path...>.<rule_id>]
```

| Segment | Values | Notes |
|---------|--------|-------|
| `<source>` | `db`, `os`, `aws`, `azure`, `gcp` | Where the metric originated |
| `<api>` | `cloudwatch`, `pi`, `azuremonitor`, `cloudmonitoring` | Cloud API (omitted for `db` / `os`) |
| `<...path...>` | variable | Zero or more segments encoding resource hierarchy. For `db`, an optional engine segment (`mysql`, `mariadb`, `oracle`, `postgresql`, `sqlite`) |
| `<rule_id>` | string | Stable identifier used in code |

### Examples

```ini
[metric.db.active_connections]                 # generic DB default
[metric.db.mysql.database_size_mb]             # engine-specific override
[metric.os.memory_percent]
[metric.aws.cloudwatch.RDS.CPUUtilization]
[metric.aws.pi.RDS.db.SQL.tup_fetched.avg]
[metric.azure.azuremonitor.DBforMySQL.flexibleServers.cpu_percent]
[metric.azure.azuremonitor.sql.servers.dtu_consumption_percent]
[metric.gcp.cloudmonitoring.cloudsql.database.cpu_utilization]
```

### Per-engine DB thresholds

Local DB metrics support an **engine namespace** so each engine can be tuned
or disabled independently while sharing the same rule ids:

```ini
[metric.db.mysql.<rule_id>]
[metric.db.mariadb.<rule_id>]
[metric.db.oracle.<rule_id>]
[metric.db.postgresql.<rule_id>]
[metric.db.sqlite.<rule_id>]
```

At collection time the monitor looks up the engine-specific section first
(e.g. `[metric.db.postgresql.active_connections]`) and **falls back** to the
generic `[metric.db.active_connections]` when no engine override exists.
MySQL and MariaDB share the same collected metric set but keep separate
threshold namespaces. Set `enabled = false` on an engine section to silence a
metric for just that engine.

The `<rule_id>` is the **last** segment. It is what the code passes to
`ThresholdChecker.check_many(...)` as the metric key. The `metric_name`
field carries the exact cloud API metric string — which can differ from
`<rule_id>`.

## Rule fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `critical` | number | one of these | — | Critical-level threshold |
| `warning` | number | one of these | — | Warning-level threshold |
| `info` | number | one of these | — | Info-level threshold |
| `operator` | string | yes | `gt` | `gt`, `lt`, `ge`, `le`, `eq`, `ne` |
| `metric_name` | string | yes for cloud rules | rule_id | Exact metric string for the cloud API |
| `unit` | string | no | — | `percent`, `bytes`, `count`, `seconds`, `ratio`, `rows_per_sec` |
| `window` | int | no | 1 | Consecutive breaches required to fire |
| `enabled` | bool | no | true | Disable without deleting the rule |
| `description` | string | no | — | Free-text note |

## Operators

| Operator | Fires when |
|----------|------------|
| `gt` | value > threshold |
| `ge` | value ≥ threshold |
| `lt` | value < threshold |
| `le` | value ≤ threshold |
| `eq` | value == threshold |
| `ne` | value != threshold |

## Severity precedence

When multiple levels are set, the most severe matching one fires:

```text
critical > warning > info
```

So if `value = 95`, `warning = 70`, `critical = 90`, the alert level
is `critical`.

## Sustained-breach (`window`)

A rule with `window = 3` fires only after **3 consecutive** polls
breach the threshold. This prevents noisy alerts on transient spikes.

A single non-breaching sample resets the in-flight counter back to
zero immediately, so a recovered resource has to misbehave for a
full fresh window before alerts fire again. Non-numeric or NaN
samples (e.g. cloud API hiccups) leave the counter untouched and
are treated as a missed poll.

Counters are tracked per `(source, path, instance, metric, severity)`
in memory. Long-dormant entries are reclaimed automatically — see
`monitoring/monitoring_utils.py::STALE_KEY_TTL_SECONDS` (default
24 h).

## Examples

### DB connections

```ini
[metric.db.active_connections]
critical = 200
warning = 100
operator = gt
unit = count
window = 3
enabled = true
```

### Per-engine DB override (PostgreSQL connections)

```ini
# Generic default for all engines
[metric.db.active_connections]
warning = 100
critical = 200
operator = gt
unit = count

# PostgreSQL-specific override (used instead of the generic rule for PG)
[metric.db.postgresql.active_connections]
warning = 150
critical = 300
operator = gt
unit = count
enabled = true
```

### OS memory

```ini
[metric.os.memory_percent]
warning = 80
critical = 95
operator = gt
unit = percent
window = 2
```

### AWS CloudWatch — RDS CPU

```ini
[metric.aws.cloudwatch.RDS.CPUUtilization]
metric_name = CPUUtilization
critical = 90
warning = 75
operator = gt
unit = percent
window = 3
enabled = true
```

### AWS Performance Insights (disabled by default)

```ini
[metric.aws.pi.RDS.db.SQL.tup_fetched.avg]
metric_name = db.SQL.tup_fetched.avg
critical = 0
warning = 10000
operator = gt
unit = rows_per_sec
enabled = false
```

### Azure MySQL Flexible — CPU

```ini
[metric.azure.azuremonitor.DBforMySQL.flexibleServers.cpu_percent]
metric_name = cpu_percent
critical = 90
warning = 75
operator = gt
unit = percent
window = 3
enabled = true
```

### Azure SQL Database — DTU

```ini
[metric.azure.azuremonitor.sql.servers.dtu_consumption_percent]
metric_name = dtu_consumption_percent
critical = 90
warning = 80
operator = gt
unit = percent
enabled = true
```

### GCP — Cloud SQL CPU utilization

```ini
[metric.gcp.cloudmonitoring.cloudsql.database.cpu_utilization]
metric_name = cloudsql.googleapis.com/database/cpu/utilization
critical = 0.9
warning = 0.75
operator = gt
unit = ratio
window = 3
enabled = true
```

Note GCP CPU is reported as a `0..1` ratio, not a percent.

## Reload behaviour

The daemon reloads `monitor_thresholds.ini` on every poll cycle, so
changes take effect within one interval. The UI reloads on a manual
**Refresh** click or when re-opening the Monitor tab.

## Multiple rules per metric

Same metric name in two different API paths → two rules, evaluated
independently:

```ini
[metric.aws.cloudwatch.RDS.CPUUtilization]
metric_name = CPUUtilization
warning = 70
enabled = true

[metric.aws.pi.RDS.CPUUtilization]
metric_name = db.load.avg     # different actual PI metric
warning = 0
enabled = false
```

This is by design — the `(source, api, path, rule_id)` tuple is the
unique key.
