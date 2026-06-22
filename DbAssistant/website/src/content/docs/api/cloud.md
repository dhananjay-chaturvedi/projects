---
title: Cloud
description: Cloud DB connection CRUD, login, and metric endpoints (AWS / Azure / GCP).
sidebar:
  order: 11
---

Requires the **Server Monitor** module and the relevant cloud SDKs.

## GET /api/monitor/cloud/connections

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/monitor/cloud/connections
```

```json
[
  {"name": "prod-rds", "provider": "aws", "region": "us-east-1", "resource_name": "prod-postgres"},
  {"name": "prod-mysql-flex", "provider": "azure", "subscription_id": "...", "resource_name": "mysqlflex01"},
  {"name": "prod-gcp", "provider": "gcp", "project_id": "my-gcp-project", "resource_name": "prod-cloudsql"}
]
```

Secrets (access keys, client secrets) are **never** returned.

## POST /api/monitor/cloud/connections

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "prod-rds",
       "profile": {
         "provider": "aws",
         "region": "us-east-1",
         "resource_name": "prod-postgres",
         "auth_mode": "sso",
         "sso_profile": "myprofile"
       }
     }' \
     http://localhost:8000/api/monitor/cloud/connections
```

```json
{"status": "created", "name": "prod-rds"}
```

The `profile` object must include `provider` and the provider-specific
fields described below.

### AWS profile fields

| Field | Required | Notes |
|-------|----------|-------|
| `provider` | yes | `"aws"` |
| `region` | yes | e.g. `us-east-1` |
| `resource_name` | yes | RDS / Aurora instance identifier |
| `auth_mode` | yes | `keys`, `sso`, `profile` |
| `access_key_id`, `secret_access_key` | depends | Only for `auth_mode: keys` |
| `sso_start_url`, `sso_account_id`, `sso_role_name` | depends | For `auth_mode: sso` |
| `sso_profile` | optional | Named AWS profile to use for boto3 |

### Azure profile fields

| Field | Required | Notes |
|-------|----------|-------|
| `provider` | yes | `"azure"` |
| `subscription_id` | yes | |
| `resource_group` | yes | |
| `resource_name` | yes | Server / DB name |
| `resource_type` | yes | `Microsoft.Sql/servers`, `Microsoft.DBforMySQL/flexibleServers`, etc. |
| `auth_mode` | yes | `keys`, `sso` |
| `tenant_id`, `client_id`, `client_secret` | depends | Service principal for `auth_mode: keys` |

### GCP profile fields

| Field | Required | Notes |
|-------|----------|-------|
| `provider` | yes | `"gcp"` |
| `project_id` | yes | |
| `resource_name` | yes | Cloud SQL instance |
| `auth_mode` | yes | `keys`, `sso` |
| `service_account_json` | depends | For `auth_mode: keys` |

## POST /api/monitor/cloud/connections/{name}/test

Health-check.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/monitor/cloud/connections/prod-rds/test
```

```json
{"status": "ok", "message": "✓ AWS RDS prod-postgres reachable in us-east-1 (PostgreSQL 16.3)"}
```

Failure:

```json
{"status": "failed", "message": "✗ Azure credentials rejected (HTTP 401)"}
```

## POST /api/monitor/cloud/connections/{name}/login

Interactive cloud login (browser opens on the **API host**, not the
client). Use only for local/dev installs.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/monitor/cloud/connections/prod-rds/login
```

```json
{
  "status": "ok",
  "message": "gcloud ADC login completed and credentials verified. (project: my-gcp-project)"
}
```

## DELETE /api/monitor/cloud/connections/{name}

```bash
curl -X DELETE -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/monitor/cloud/connections/prod-rds
```

## GET /api/monitor/cloud/metrics/{name}

Fetch the latest cloud metrics — same data the monitor loop collects.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/monitor/cloud/metrics/prod-rds
```

```json
{
  "name": "prod-rds",
  "provider": "aws",
  "text": "CPUUtilization=42.1%, DatabaseConnections=18, FreeableMemory=7.3 GB, ...",
  "metrics": {
    "CPUUtilization": 42.1,
    "DatabaseConnections": 18,
    "FreeableMemory": 7864320000,
    "DiskQueueDepth": 0.4,
    "ReadIOPS": 120,
    "WriteIOPS": 85
  },
  "alerts": [],
  "graph_data": {
    "prod-rds_CPUUtilization": 42.1,
    "prod-rds_DatabaseConnections": 18
  }
}
```

Errors:

| Code | Cause |
|------|-------|
| `404` | Cloud connection name not found |
| `503` | Cloud SDK call failed (`detail` contains the error) |
| `503` | Module not installed |
