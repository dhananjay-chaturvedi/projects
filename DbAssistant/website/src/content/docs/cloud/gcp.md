---
title: GCP (Cloud SQL)
description: Set up Google Cloud SQL monitoring via Cloud Monitoring.
sidebar:
  order: 3
---

DbAssistant monitors GCP Cloud SQL through **Cloud Monitoring** (the
"System Insights" metric set). Cloud SQL Admin API is used for instance
metadata. Cloud Logging is used for recent log lines.

## Required IAM roles

Assign to the calling identity on the project:

- `roles/cloudsql.viewer` — instance metadata
- `roles/monitoring.viewer` — Cloud Monitoring metrics
- `roles/logging.viewer` — recent Cloud SQL logs (optional)

## Authentication

### Application Default Credentials (recommended)

```json
{
  "provider": "gcp",
  "project_id": "my-gcp-project",
  "resource_name": "prod-cloudsql",
  "auth_mode": "sso"
}
```

Then run:

```bash
python dbtool.py cloud login --name prod-gcp
# launches: gcloud auth application-default login
```

### Service account key file

```json
{
  "provider": "gcp",
  "project_id": "my-gcp-project",
  "resource_name": "prod-cloudsql",
  "auth_mode": "keys",
  "service_account_json": "{\"type\":\"service_account\", ...}"
}
```

Or store the JSON file path:

```json
{
  "provider": "gcp",
  "project_id": "my-gcp-project",
  "resource_name": "prod-cloudsql",
  "auth_mode": "keys",
  "service_account_path": "/secure/path/to/sa.json"
}
```

## Add the connection

```bash
python dbtool.py cloud connections add \
    --name prod-gcp \
    --provider gcp \
    --json ./prod-gcp.json

python dbtool.py cloud connections test prod-gcp
python dbtool.py cloud metrics --name prod-gcp
```

## Metrics collected

All metric type URIs are stored in the **`metric_name`** field of each
threshold rule, so the cloud API call uses the exact string Google
expects.

| Friendly name (rule_id) | Metric type URI | Section |
|-------------------------|-----------------|---------|
| `cpu_utilization` | `cloudsql.googleapis.com/database/cpu/utilization` | `[metric.gcp.cloudmonitoring.cloudsql.database.cpu_utilization]` |
| `memory_utilization` | `.../database/memory/utilization` | `[metric.gcp.cloudmonitoring.cloudsql.database.memory_utilization]` |
| `disk_utilization` | `.../database/disk/utilization` | `[metric.gcp.cloudmonitoring.cloudsql.database.disk_utilization]` |
| `database_connections` | `.../database/network/connections` | `[metric.gcp.cloudmonitoring.cloudsql.database.database_connections]` |
| `io_read_ops` | `.../database/disk/read_ops_count` | `[metric.gcp.cloudmonitoring.cloudsql.database.io_read_ops]` |
| `io_write_ops` | `.../database/disk/write_ops_count` | `[metric.gcp.cloudmonitoring.cloudsql.database.io_write_ops]` |
| `network_receive_bytes` | `.../database/network/received_bytes_count` | same pattern |
| `network_transmit_bytes` | `.../database/network/sent_bytes_count` | same pattern |
| `replica_lag_seconds` | `.../database/replica_lag` | same pattern |

## Database ID matching

GCP Cloud Monitoring identifies Cloud SQL instances by a
**`database_id`** label of the form `<project>:<instance>`. DbAssistant
filters on both `<project>:<resource_name>` and bare `<resource_name>`,
so either works in the profile.

## Cost note

Cloud Monitoring is free for most metrics within fair-use limits. Set
the daemon interval to ≥ 60 seconds.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Permission denied (403)` for metrics | Missing `roles/monitoring.viewer` | Grant via IAM |
| `Permission denied (403)` for logs | Missing `roles/logging.viewer` | Grant or leave empty (logs are optional) |
| `Cloud Monitoring API has not been used` | API disabled | Enable in Console → APIs & Services |
| Metric `null` for all rules | Wrong `project_id` or `resource_name` | Verify with `gcloud sql instances describe <name>` |
| `ADC not available` | Never ran `gcloud auth application-default login` | Run `python dbtool.py cloud login --name <name>` |
