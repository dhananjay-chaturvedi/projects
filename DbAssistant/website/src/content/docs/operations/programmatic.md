---
title: Programmatic use (Python)
description: Embed DbAssistant in your own Python code by calling DBService directly.
sidebar:
  order: 2
---

Skip the CLI and REST layers — call the service directly from Python.
Same logic, no subprocesses.

## Quick example

```python
from app.headless.db_service import DBService

svc = DBService()

# Add and test
svc.add_connection(
    name="prod",
    db_type="PostgreSQL",
    host="db.example.com",
    port="5432",
    user="app",
    password="secret",
    database="appdb",
)
print(svc.test_connection("prod"))

# Query
result = svc.execute("prod", "SELECT version()")
print(result["columns"], result["rows"])

svc.disconnect_all()
```

## Imports

| Use case | Class | From |
|----------|-------|------|
| Full tool (all modules) | `DBService` | `app.headless.db_service` |
| Core only (no module dependencies) | `CoreDBService` | `common.headless.db_service` |
| Module-specific | e.g. `MonitorService` | `monitoring.service` |

## Connections

```python
svc.list_connections()                              # list of dicts (no passwords)
svc.add_connection(name="prod", db_type="PostgreSQL", host="...", ...)
svc.test_connection("prod")                         # {"status": "ok", "version": "..."}
svc.remove_connection("prod")
```

## Query

```python
res = svc.execute("prod", "SELECT id, name FROM users LIMIT 5")
# {"columns": [...], "rows": [...], "rowcount": 5, "elapsed_ms": 12}

res = svc.execute(
    "prod",
    "SELECT * FROM users WHERE status = :status",
    params={"status": "active"},
)
```

## Objects / schema

```python
svc.get_objects("prod", "tables")
svc.supported_object_types("PostgreSQL")
svc.get_table_schema("prod", "users")
ddl = svc.dump_schema("prod", table="users")["ddl"]

converted = svc.convert_schema(
    source_conn="prod",
    target_type="MySQL",
    table="users",
)
print(converted["ddl"], converted.get("warnings", []))
```

## Monitoring + alerts

```python
m = svc.get_metrics("prod")
print(m["metrics"], m["raw_floats"])
alerts = svc.check_alerts("prod", m["raw_floats"])
for a in alerts:
    print(a)
```

## AI

```python
print(svc.list_ai_backends())
result = svc.ai_query("prod", "count rows in users")
print(result["sql"], result["rows"], result["explanation"])
```

## Cloud

```python
svc.list_cloud_connections()
svc.add_cloud_connection("prod-rds", {
    "provider": "aws",
    "region": "us-east-1",
    "resource_name": "prod-postgres",
    "auth_mode": "sso",
    "sso_profile": "my-profile",
})
svc.test_cloud_connection("prod-rds")
svc.cloud_login("prod-rds")
metrics = svc.get_cloud_metrics("prod-rds")
print(metrics["text"], metrics["metrics"])
```

## Threshold rules

```python
svc.list_thresholds(source="aws", api="cloudwatch", path=("RDS",))
rule = svc.show_threshold("aws", "CPUUtilization", path=("cloudwatch", "RDS"))
result = svc.check_threshold(
    source="aws",
    metric="CPUUtilization",
    value=88,
    path=("cloudwatch", "RDS"),
    instance="prod-rds-01",
)
```

## Notifications

```python
svc.send_notification(severity="WARNING", message="Test alert")
```

## Path overrides for tests

```python
import os, tempfile
os.environ["DBASSISTANT_HOME"] = tempfile.mkdtemp(prefix="dba_")

from common.paths import bootstrap
bootstrap()

from app.headless.db_service import DBService
svc = DBService()
# operations here write to the temp home, not the real one
```

## Async / concurrency

`DBService` is synchronous. Wrap calls in `asyncio.to_thread` or a
thread pool if you embed it in an async app:

```python
import asyncio
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=4)

async def query_async(svc, conn, sql):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, svc.execute, conn, sql)
```

## See also

- [Architecture overview](/architecture/overview/) — how `DBService` is composed
- [CLI overview](/cli/overview/) — same surface, command-line driven
- [REST API overview](/api/overview/) — same surface, HTTP driven
