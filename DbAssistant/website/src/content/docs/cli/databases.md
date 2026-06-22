---
title: databases
description: Inspect the supported engine list and per-engine capability metadata.
sidebar:
  order: 13
---

Inspect the engines DbAssistant supports and what operations each one
allows. Used by the UI to enable/disable features automatically.

## types

List engines:

```bash
python dbtool.py databases types
```

```text
┌──────────────┬──────────────┬──────────────────┬────────────────┐
│ type         │ query language│ schema conversion│ transactions  │
├──────────────┼──────────────┼──────────────────┼────────────────┤
│ Oracle       │ SQL          │ ✓                │ ✓              │
│ MySQL        │ SQL          │ ✓                │ ✓              │
│ MariaDB      │ SQL          │ ✓                │ ✓              │
│ PostgreSQL   │ SQL          │ ✓                │ ✓              │
│ SQL Server   │ SQL          │ ✓                │ ✓              │
│ SQLite       │ SQL          │ ✓                │ ✓              │
│ MongoDB      │ Document JSON│ —                │ —              │
│ DocumentDB   │ Document JSON│ —                │ —              │
└──────────────┴──────────────┴──────────────────┴────────────────┘
```

JSON form (full metadata):

```bash
python dbtool.py databases types --format json
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
    "supported_objects": [
      "tables", "views", "procs", "functions", "indexes", "triggers",
      "sequences", "constraints", "users", "schemas", "tablespaces",
      "roles", "synonyms", "packages", "types", "materializedviews",
      "databaselinks", "profiles", "sessions", "processlist"
    ]
  },
  ...
]
```

## ops

List operations supported by a specific engine:

```bash
python dbtool.py databases ops --type MongoDB
```

```text
MongoDB capabilities

Object types
  collections, indexes, users, databases

Document operations
  find, aggregate, count

Transactions
  not supported

Schema conversion
  not supported
```

JSON form:

```bash
python dbtool.py databases ops --type Oracle --format json
```

## REST API equivalents

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/databases/types"

curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/databases/ops?type=MySQL"
```

## Use cases

- Discover what object types you can browse on a given engine before
  calling `objects --type ...`
- Build a dynamic UI by listing engines from
  `GET /api/databases/types`
- Decide whether to surface Schema Conversion for a connection by
  checking `schema_conversion: true`

## See also

- [Supported databases reference](/reference/supported-databases/)
- [`objects`](/cli/objects/) — list objects of a specific type
