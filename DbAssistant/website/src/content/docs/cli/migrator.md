---
title: migrator
description: Schema conversion, data transfer, and migration validation across database engines.
sidebar:
  order: 5
---

The `migrator` command group requires the **Data Migration** module
(package `schema_converter`).

## show

Display columns, types, nullability, defaults, and indexes for a table.

```bash
python dbtool.py migrator show --conn prod --table users
python dbtool.py migrator show --conn prod --table users --format json
```

## dump

Generate `CREATE TABLE`, index, and constraint DDL.

```bash
python dbtool.py migrator dump --conn prod --table users --output users.sql
python dbtool.py migrator dump --conn prod --output appdb_full.sql
```

## convert

Translate DDL from one engine to another.

```bash
python dbtool.py migrator convert \
    --source-conn prod \
    --target-type MySQL \
    --table users \
    --output users_mysql.sql
```

Supported `--target-type` values: `Oracle`, `MySQL`, `MariaDB`,
`PostgreSQL`, `SQL Server`, `SQLite`.

## apply

Execute a multi-statement DDL blob on a target connection.

```bash
python dbtool.py migrator apply \
    --target-conn target_mysql \
    --ddl-file users_mysql.sql
```

Use `--continue-on-error` to keep going after a failed statement.

## transfer-data

Copy rows from a source table into a target table (batched `executemany`).

```bash
python dbtool.py migrator transfer-data \
    --source-conn prod \
    --target-conn target_mysql \
    --table users \
    --batch-size 500
```

Use `--target-table` when the destination table name differs.

## compare-schema

Compare column definitions and indexes between source and target.

```bash
python dbtool.py migrator compare-schema \
    --source-conn prod \
    --target-conn target_mysql \
    --table users
```

## compare-data

Compare table data row-by-row.

```bash
# Sample first N rows (default from properties.ini)
python dbtool.py migrator compare-data \
    --source-conn prod \
    --target-conn target_mysql \
    --table users \
    --mode sample

# Full table comparison
python dbtool.py migrator compare-data \
    --source-conn prod \
    --target-conn target_mysql \
    --table users \
    --mode full
```

## row-counts / sample

Quick checks on a single connection:

```bash
python dbtool.py migrator row-counts --conn prod --tables users,orders
python dbtool.py migrator sample --conn prod --tables users --limit 5
```

## See also

- [Data Migration module](/modules/data-migration/) — full workflow and type mapping
- [Data Migration API](/api/migrator/) — REST equivalents
