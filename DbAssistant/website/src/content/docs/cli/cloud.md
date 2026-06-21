---
title: cloud
description: Manage cloud DB connections, log in, and fetch cloud metrics for AWS / Azure / GCP.
sidebar:
  order: 10
---

The `cloud` command group requires the **Server Monitor** module and
the relevant cloud SDKs (installed via `requirements-cloud.txt`).

## connections list

```bash
python dbtool.py cloud connections list
python dbtool.py cloud connections list --format json
```

## connections add

```bash
python dbtool.py cloud connections add \
    --name prod-rds \
    --provider aws \
    --json ./rds.json
```

`rds.json` is a provider profile (must include `"provider"`):

```json
{
  "provider": "aws",
  "region": "ap-northeast-1",
  "resource_name": "my-rds-instance",
  "sso_profile": "myprofile"
}
```

Per-provider examples:

### AWS

```json
{
  "provider": "aws",
  "region": "us-east-1",
  "resource_name": "prod-postgres",
  "auth_mode": "sso",
  "sso_start_url": "https://my-org.awsapps.com/start",
  "sso_account_id": "123456789012",
  "sso_role_name": "DBOpsRole"
}
```

```json
{
  "provider": "aws",
  "region": "us-east-1",
  "resource_name": "prod-postgres",
  "auth_mode": "keys",
  "access_key_id": "AKIA...",
  "secret_access_key": "..."
}
```

### Azure

```json
{
  "provider": "azure",
  "subscription_id": "00000000-0000-0000-0000-000000000000",
  "resource_group": "rg-prod",
  "resource_name": "mysqlflex01",
  "resource_type": "Microsoft.DBforMySQL/flexibleServers",
  "auth_mode": "sso"
}
```

### GCP

```json
{
  "provider": "gcp",
  "project_id": "my-gcp-project",
  "resource_name": "prod-cloudsql",
  "auth_mode": "sso"
}
```

## connections test

Health-check a cloud connection (runs the cloud SDK with the saved
credentials):

```bash
python dbtool.py cloud connections test prod-rds
```

```text
✓ AWS RDS prod-postgres reachable in us-east-1 (engine PostgreSQL 16.3)
```

## connections remove

```bash
python dbtool.py cloud connections remove prod-rds
```

## login

Interactive authentication (opens a browser tab):

```bash
python dbtool.py cloud login --name prod-rds
```

Behind the scenes:

- **AWS** — `aws sso login --profile <sso_profile>` (or `aws login`)
- **Azure** — `az login` (device code)
- **GCP** — `gcloud auth application-default login`

The session is cached by the provider's CLI — subsequent
`cloud metrics` calls reuse it.

## metrics

Fetch the latest cloud metrics for a connection:

```bash
python dbtool.py cloud metrics --name prod-rds
```

```text
[prod-rds]  AWS / RDS  (us-east-1)

  CPUUtilization                42.1 %
  DatabaseConnections           18
  FreeableMemory                7.3 GB
  DiskQueueDepth                0.4
  ReadIOPS                      120
  WriteIOPS                     85
  ReadLatency                   1.4 ms
  WriteLatency                  2.1 ms
```

JSON output:

```bash
python dbtool.py cloud metrics --name prod-rds --format json
```

## monitor

Loop fetching cloud metrics every `interval` seconds, applying threshold
rules and dispatching notifications.

```bash
python dbtool.py cloud monitor --name prod-rds --interval 30
python dbtool.py cloud monitor --name prod-rds --once
```

## Auth chains

For AWS, when the profile uses `auth_mode: sso`, the tool uses boto3's
default credential chain — meaning a separate `aws login` /
`aws sso login` will work too. Static access keys take precedence when
both are present.

## See also

- [AWS cloud setup](/cloud/aws/)
- [Azure cloud setup](/cloud/azure/)
- [GCP cloud setup](/cloud/gcp/)
- [Cloud REST API](/api/cloud/)
