---
title: Query & objects
description: Run SQL and browse database objects.
sidebar:
  order: 5
---

## POST /api/query

Execute SQL (or a JSON document query for MongoDB / DocumentDB) and
return rows.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"connection":"prod","sql":"SELECT 1 AS n"}' \
     http://localhost:8000/api/query
```

Response:

```json
{
  "columns": ["n"],
  "rows": [[1]],
  "rowcount": 1,
  "elapsed_ms": 4
}
```

### Body

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `connection` | string | yes | Name of saved connection |
| `sql` | string | yes | SQL statement (or JSON doc query for Mongo/DocDB) |
| `params` | object/array | no | Parameterised query bindings |
| `limit` | integer | no | Cap on rows returned (default: configurable) |

Parameterised query:

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "connection": "prod",
       "sql": "SELECT * FROM users WHERE status = :status AND created > :since",
       "params": {"status": "active", "since": "2025-01-01"}
     }' \
     http://localhost:8000/api/query
```

DML returns rowcount:

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"connection":"prod","sql":"UPDATE t SET x=1 WHERE id=5"}' \
     http://localhost:8000/api/query
```

```json
{"rowcount": 1, "rows": [], "columns": [], "elapsed_ms": 2}
```

### MongoDB / DocumentDB

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "connection": "docdb",
       "sql": "{\"collection\":\"users\",\"operation\":\"find\",\"filter\":{\"status\":\"active\"},\"limit\":50}"
     }' \
     http://localhost:8000/api/query
```

Supported operations: `find`, `aggregate`, `count`.

### Errors

| Code | Cause |
|------|-------|
| `400` | Bad SQL syntax — `detail` contains the engine's error |
| `404` | Connection name not found |
| `408` | Statement exceeded `[database.connection] query_timeout` |
| `413` | SQL body > 1 MB |

## GET /api/objects/{conn}

List database objects.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/objects/prod?type=tables"
```

```json
{
  "columns": ["name"],
  "rows": [["accounts"], ["customers"], ["invoices"]]
}
```

Multi-column object types (e.g. `processlist`, `users`):

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/objects/prod?type=processlist"
```

```json
{
  "columns": ["id", "user", "host", "db", "state"],
  "rows": [
    [27, "app", "web1:53210", "appdb", "idle"],
    [45, "analytics", "etl1:48121", "appdb", "active query"]
  ]
}
```

### Query parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `type` | yes | Object type (`tables`, `views`, `procs`, etc.) |
| `filter` | no | Substring filter on names |

Valid `type` values depend on the engine — see
[`databases ops`](/cli/databases/) or `GET /api/databases/ops?type=...`.

### Errors

| Code | Cause |
|------|-------|
| `400` | Object type not supported by this engine |
| `404` | Connection name not found |

## GET /api/databases/types

List supported engines and capabilities.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/databases/types
```

```json
[
  {
    "type": "Oracle",
    "default_port": 1521,
    "uses_service": true,
    "query_language": "SQL",
    "schema_conversion": true,
    "transactions": true,
    "supported_objects": ["tables", "views", "procs", ...]
  },
  ...
]
```

## GET /api/databases/ops

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/databases/ops?type=MongoDB"
```

```json
{
  "type": "MongoDB",
  "object_types": ["collections", "indexes", "users", "databases"],
  "document_operations": ["find", "aggregate", "count"],
  "transactions": false,
  "schema_conversion": false
}
```
