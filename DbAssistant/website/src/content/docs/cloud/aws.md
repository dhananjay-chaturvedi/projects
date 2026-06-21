---
title: AWS (RDS / Aurora / PI)
description: Set up AWS RDS / Aurora monitoring with CloudWatch and Performance Insights.
sidebar:
  order: 1
---

DbAssistant monitors AWS RDS / Aurora instances through:

- **CloudWatch** (primary) — instance-level metrics
- **Performance Insights** (fallback) — when CloudWatch returns no
  value for `CPU`, `Memory`, or `DiskQueueDepth`

## Required IAM permissions

The monitoring identity needs at minimum:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "rds:DescribeDBInstances",
        "rds:DescribeDBClusters",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:GetMetricData",
        "pi:GetResourceMetrics",
        "pi:DescribeDimensionKeys"
      ],
      "Resource": "*"
    }
  ]
}
```

## Authentication options

### Option 1 — IAM Identity Center (SSO)

Profile:

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

Login:

```bash
python dbtool.py cloud login --name prod-rds
```

### Option 2 — Named AWS profile

If you already use `aws configure sso` or `aws login` with named
profiles:

```json
{
  "provider": "aws",
  "region": "us-east-1",
  "resource_name": "prod-postgres",
  "auth_mode": "sso",
  "sso_profile": "my-aws-profile"
}
```

Run `aws sso login --profile my-aws-profile` first; boto3 picks up the
credential cache automatically.

### Option 3 — Static access keys

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

Stored encrypted under `~/.dbassistant/connections/cloud.json`. Prefer
SSO or instance-role auth whenever possible.

### Option 4 — Instance role / Container task role

Don't set any creds. Boto3's default chain picks up the EC2 / ECS task
role automatically.

```json
{
  "provider": "aws",
  "region": "us-east-1",
  "resource_name": "prod-postgres",
  "auth_mode": "default"
}
```

## Add the connection

```bash
python dbtool.py cloud connections add \
    --name prod-rds \
    --provider aws \
    --json ./prod-rds.json

python dbtool.py cloud connections test prod-rds
python dbtool.py cloud metrics --name prod-rds
```

## CloudWatch metrics collected

| Metric | Threshold key | Unit |
|--------|---------------|------|
| `CPUUtilization` | `[metric.aws.cloudwatch.RDS.CPUUtilization]` | percent |
| `DatabaseConnections` | `[metric.aws.cloudwatch.RDS.DatabaseConnections]` | count |
| `FreeableMemory` | `[metric.aws.cloudwatch.RDS.FreeableMemory]` | bytes |
| `FreeStorageSpace` | `[metric.aws.cloudwatch.RDS.FreeStorageSpace]` | bytes |
| `DiskQueueDepth` | `[metric.aws.cloudwatch.RDS.DiskQueueDepth]` | count |
| `ReadIOPS` / `WriteIOPS` | `[metric.aws.cloudwatch.RDS.ReadIOPS]` etc. | per second |
| `ReadLatency` / `WriteLatency` | same pattern | seconds |
| `ReplicaLag` | `[metric.aws.cloudwatch.RDS.ReplicaLag]` | seconds |

## Performance Insights metrics

PI metrics use a different section path:

```ini
[metric.aws.pi.RDS.db.SQL.tup_fetched.avg]
metric_name = db.SQL.tup_fetched.avg
critical = 0
warning = 10000
operator = gt
unit = rows_per_sec
enabled = false        # PI metrics are disabled by default
```

To enable, set `enabled = true` and ensure the RDS instance has PI
turned on.

## Cost note

- CloudWatch metrics are free at default 60-second granularity.
- Performance Insights is free for most engines for 7 days of
  retention.
- Set `interval` (in `dbtool daemon start --interval`) to ≥ 60s to
  avoid unnecessary GetMetricStatistics calls.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `ClientError: AccessDenied` | Missing IAM permission | Add the policy above |
| `ResourceNotFound: DBInstance` | Wrong `resource_name` or `region` | Check the RDS console |
| `ThrottlingException` | Too many concurrent calls | Increase poll interval |
| `ExpiredToken` | SSO session expired | `python dbtool.py cloud login --name prod-rds` |
