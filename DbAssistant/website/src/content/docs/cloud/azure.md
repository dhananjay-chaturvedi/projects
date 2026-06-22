---
title: Azure (SQL / MySQL / Postgres)
description: Set up Azure SQL Database, Azure Database for MySQL/PostgreSQL/MariaDB, Cosmos DB, and Azure Cache monitoring via Azure Monitor.
sidebar:
  order: 2
---

DbAssistant monitors these Azure services through **Azure Monitor**:

| Resource type | Section path |
|---------------|--------------|
| `Microsoft.Sql/servers` | `[metric.azure.azuremonitor.sql.servers.*]` |
| `Microsoft.DBforMySQL/flexibleServers` | `[metric.azure.azuremonitor.DBforMySQL.flexibleServers.*]` |
| `Microsoft.DBforMySQL/servers` | `[metric.azure.azuremonitor.DBforMySQL.servers.*]` |
| `Microsoft.DBforPostgreSQL/flexibleServers` | `[metric.azure.azuremonitor.DBforPostgreSQL.flexibleServers.*]` |
| `Microsoft.DBforPostgreSQL/servers` | `[metric.azure.azuremonitor.DBforPostgreSQL.servers.*]` |
| `Microsoft.DBforMariaDB/servers` | `[metric.azure.azuremonitor.DBforMariaDB.servers.*]` |
| `Microsoft.DocumentDB/databaseAccounts` | `[metric.azure.azuremonitor.DocumentDB.*]` |
| `Microsoft.Cache/Redis` | `[metric.azure.azuremonitor.Cache.Redis.*]` |

The path encodes the resource provider hierarchy so you can have
separate thresholds for, say, MySQL flexible vs single servers.

## Required Azure permissions

```text
Microsoft.Insights/metrics/read
Microsoft.Insights/metricDefinitions/read
Microsoft.Sql/servers/read                  (Azure SQL)
Microsoft.DBforMySQL/*/read                 (MySQL servers)
Microsoft.DBforPostgreSQL/*/read            (Postgres servers)
```

Easiest role assignment: **Monitoring Reader** on the subscription
or resource group.

## Authentication

### Service principal (keys)

```json
{
  "provider": "azure",
  "subscription_id": "00000000-0000-0000-0000-000000000000",
  "resource_group": "rg-prod",
  "resource_name": "mysqlflex01",
  "resource_type": "Microsoft.DBforMySQL/flexibleServers",
  "auth_mode": "keys",
  "tenant_id": "...",
  "client_id": "...",
  "client_secret": "..."
}
```

### `az login` (device code)

```json
{
  "provider": "azure",
  "subscription_id": "...",
  "resource_group": "rg-prod",
  "resource_name": "mysqlflex01",
  "resource_type": "Microsoft.DBforMySQL/flexibleServers",
  "auth_mode": "sso"
}
```

```bash
python dbtool.py cloud login --name prod-mysql-flex
```

Opens a browser to the Azure device-code page.

## Add the connection

```bash
python dbtool.py cloud connections add \
    --name prod-mysql-flex \
    --provider azure \
    --json ./prod-mysql-flex.json

python dbtool.py cloud connections test prod-mysql-flex
python dbtool.py cloud metrics --name prod-mysql-flex
```

## Common metrics

### Azure SQL Database

| Metric | Section |
|--------|---------|
| `cpu_percent` | `[metric.azure.azuremonitor.sql.servers.cpu_percent]` |
| `physical_data_read_percent` | `[metric.azure.azuremonitor.sql.servers.physical_data_read_percent]` |
| `log_write_percent` | `[metric.azure.azuremonitor.sql.servers.log_write_percent]` |
| `dtu_consumption_percent` | `[metric.azure.azuremonitor.sql.servers.dtu_consumption_percent]` |
| `connection_successful` | `[metric.azure.azuremonitor.sql.servers.connection_successful]` |
| `deadlock` | `[metric.azure.azuremonitor.sql.servers.deadlock]` |

### Azure Database for MySQL Flexible

| Metric | Section |
|--------|---------|
| `cpu_percent` | `[metric.azure.azuremonitor.DBforMySQL.flexibleServers.cpu_percent]` |
| `memory_percent` | `[metric.azure.azuremonitor.DBforMySQL.flexibleServers.memory_percent]` |
| `storage_used` | `[metric.azure.azuremonitor.DBforMySQL.flexibleServers.storage_used]` |
| `active_connections` | `[metric.azure.azuremonitor.DBforMySQL.flexibleServers.active_connections]` |
| `replication_lag` | `[metric.azure.azuremonitor.DBforMySQL.flexibleServers.replication_lag]` |

### Azure Database for PostgreSQL Flexible

| Metric | Section |
|--------|---------|
| `cpu_percent` | `[metric.azure.azuremonitor.DBforPostgreSQL.flexibleServers.cpu_percent]` |
| `memory_percent` | `[metric.azure.azuremonitor.DBforPostgreSQL.flexibleServers.memory_percent]` |
| `iops` | `[metric.azure.azuremonitor.DBforPostgreSQL.flexibleServers.iops]` |
| `active_connections` | `[metric.azure.azuremonitor.DBforPostgreSQL.flexibleServers.active_connections]` |

### Azure Cache for Redis

| Metric | Section |
|--------|---------|
| `percentProcessorTime` | `[metric.azure.azuremonitor.Cache.Redis.percentProcessorTime]` |
| `usedmemorypercentage` | `[metric.azure.azuremonitor.Cache.Redis.usedmemorypercentage]` |
| `cachemisses` | `[metric.azure.azuremonitor.Cache.Redis.cachemisses]` |

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `Forbidden (401/403)` | Identity lacks `Microsoft.Insights/metrics/read` | Assign **Monitoring Reader** |
| `ResourceNotFoundError` | Wrong `resource_group` / `resource_name` / `resource_type` | Verify in Azure Portal → resource → JSON view |
| `AuthenticationFailed` | Device-code session expired | Run `python dbtool.py cloud login --name <name>` again |
| Metric value `null` | Resource has no data in lookback window | Increase `[monitoring] metrics_lookback_minutes` |
