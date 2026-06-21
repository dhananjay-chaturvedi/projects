---
title: objects
description: Browse tables, views, procedures, indexes, triggers, sequences, and other database objects.
sidebar:
  order: 4
---

List database objects of a given type. The set of valid `--type` values
depends on the engine — see [Supported databases](/reference/supported-databases/).

## Syntax

```bash
python dbtool.py objects --conn NAME --type TYPE [--format ...]
```

## Universal types

| `--type` | Description | Engines |
|----------|-------------|---------|
| `tables` | Base tables | All SQL engines |
| `collections` | Collections (MongoDB alias) | MongoDB, DocumentDB |
| `views` | Views | All SQL engines |
| `procs` | Stored procedures | MySQL, MariaDB, Postgres, SQL Server, Oracle |
| `functions` | Functions | Same |
| `indexes` | Indexes | All SQL engines |
| `triggers` | Triggers | All SQL engines |
| `sequences` | Sequences | Postgres, Oracle, SQL Server |
| `constraints` | Constraints | All SQL engines |
| `databases` | Databases / schemas | All SQL engines |
| `users` | DB users | All SQL engines |
| `processlist` | Active sessions | MySQL, MariaDB, Postgres, SQL Server, Oracle |

## Engine-specific types

| `--type` | Engines |
|----------|---------|
| `tablespaces` | Oracle, SQL Server |
| `roles` | Oracle, Postgres, SQL Server |
| `synonyms` | Oracle |
| `packages` | Oracle |
| `types` | Oracle, Postgres |
| `materializedviews` | Oracle, Postgres |
| `databaselinks` | Oracle |
| `profiles` | Oracle |
| `sessions` | Oracle (v$session) |
| `activity` | Postgres (`pg_stat_activity`) |
| `extensions` | Postgres |
| `events` | MySQL, MariaDB |
| `engines` | MySQL, MariaDB |
| `charsets` | MySQL, MariaDB |
| `schemas` | SQL Server, Postgres |

Run this for the live list per engine:

```bash
python dbtool.py databases ops --type Oracle
```

## Examples

```bash
# Tables (compact)
python dbtool.py objects --conn prod --type tables

# Same as JSON
python dbtool.py objects --conn prod --type tables --format json

# Active sessions (multi-column tabular output)
python dbtool.py objects --conn prod --type processlist

# Mongo collections
python dbtool.py objects --conn docdb --type collections
```

## Output

Single-column types (tables, views, sequences):

```text
ACCOUNTS
CUSTOMERS
INVOICES
LINE_ITEMS
PAYMENTS
```

Multi-column types (`processlist`, `users`) render as a table:

```text
┌────┬──────────────┬──────────────┬─────────────┬──────────────┐
│ id │ user         │ host         │ db          │ state        │
├────┼──────────────┼──────────────┼─────────────┼──────────────┤
│ 27 │ app          │ web1:53210   │ appdb       │ idle         │
│ 45 │ analytics    │ etl1:48121   │ appdb       │ active query │
└────┴──────────────┴──────────────┴─────────────┴──────────────┘
```

## Filtering

Most engines support optional filtering by name:

```bash
python dbtool.py objects --conn prod --type tables --filter "USER%"
```

## Unsupported type

If a type isn't supported by the connection's engine, the CLI returns a
clear error:

```text
ERROR: object type 'packages' is not supported by PostgreSQL.
Try one of: tables, views, procs, functions, indexes, triggers,
sequences, constraints, databases, users, schemas, processlist,
extensions, roles, materializedviews, types, activity.
exit code 1
```

## Programmatic equivalent

```python
from app.headless.db_service import DBService

svc = DBService()
print(svc.supported_object_types("Oracle"))
print(svc.get_objects("prod", "tables"))
```
