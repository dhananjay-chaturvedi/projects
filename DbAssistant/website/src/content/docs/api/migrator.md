---
title: Data Migration
description: Schema introspection, DDL dump, conversion, data transfer, and migration validation.
sidebar:
  order: 6
---

Requires the **Data Migration** module (`schema_converter`).

## GET /api/migrator/{conn}/{table}

Return columns and indexes for a table.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/migrator/prod/users
```

## GET /api/migrator/{conn}/dump

Generate `CREATE TABLE` DDL.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/migrator/prod/dump?table=users"
```

## POST /api/migrator/convert

Translate DDL from one engine to another.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"source_conn":"prod","target_type":"MySQL","table":"users"}' \
     http://localhost:8000/api/migrator/convert
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_conn` | string | yes | Saved connection name |
| `target_type` | string | yes | Target engine name |
| `table` | string | yes | Table to convert |
| `type_map` | string | no | Custom rules, e.g. `varchar2:text,int:decimal` |
| `target_db` | string | no | Qualify target table names (e.g. `test`) |

## POST /api/migrator/apply

Execute DDL on a target connection.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `target_conn` | string | yes | Target connection |
| `ddl` | string | yes | Multi-statement DDL blob |
| `stop_on_error` | bool | no | Default `true` |

## POST /api/migrator/transfer-data

Copy rows source → target.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_conn` | string | yes | Source connection |
| `target_conn` | string | yes | Target connection |
| `table` | string | yes | Source table |
| `target_table` | string | no | Override target table name |
| `batch_size` | int | no | Rows per batch (default from config) |
| `where` | string | no | G1 row filter (SQL, no `WHERE` keyword) — single table only |
| `limit` | int | no | G1 max rows (applied per table) |
| `columns` | string | no | G2 comma-separated source column subset — single table only |
| `column_map` | string | no | G2 rename rules `src:tgt,...` — also valid on transfer-data-multi (all tables) |
| `continue_on_error` | bool | no | G3 keep going on row errors, report bad rows |
| `overflow_policy` | string | no | G4 `fail` / `truncate` / `skip` |
| `null_policy` | string | no | G6 `keep` / `empty_to_null` / `null_to_empty` |
| `bool_policy` | string | no | G6 `auto` / `int` / `true_false` |
| `timezone_policy` | string | no | G7 `preserve` / `naive` / `utc` / `target` |
| `target_timezone` | string | no | G7 named tz when `timezone_policy=target` |
| `reset_sequences` | bool | no | G8 reset target auto-increment after load |
| `checkpoint` | bool | no | G9 enable resume/checkpoint |
| `report_path` | string | no | G10 write JSON report to this file |

Fixed-value policies (`continue_on_error`, `overflow_policy`, `null_policy`,
`bool_policy`, `timezone_policy`, `target_timezone`, `reset_sequences`) default
to the saved `schema_converter/config.ini` values when omitted.

`POST /api/migrator/transfer-data-multi` takes `tables`, `parallel`, `workers`,
a per-table `limit`, `column_map` (applied to every selected table), and the
same fixed-value policy fields. Row filter (`where`) and `columns` are
**single-table only** and are not accepted by the multi endpoint.

## POST /api/migrator/validate

Pre-migration dry-run report (G5). No rows are moved.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_conn` | string | yes | Source connection |
| `target_conn` | string | yes | Target connection |
| `tables` | array | yes | Source tables to validate |
| `target_db` | string | no | Qualify target table names |
| `prefix` / `suffix` | string | no | Target name affixes |
| `type_map` | string | no | Type override rules |

Returns `{ok, tables:[{issues:[{severity,category,column,message}]}], summary}`.

## POST /api/migrator/compare-schema

Compare table schema between two connections.

## POST /api/migrator/compare-data

Compare table data between two connections.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_conn` | string | yes | Source connection |
| `target_conn` | string | yes | Target connection |
| `table` | string | yes | Table name |
| `mode` | string | no | `sample` (default) or `full` |
| `sample_size` | int | no | Rows when `mode=sample` |

## POST /api/migrator/convert-multi

Convert several tables in one request (`tables` array).

## POST /api/migrator/{conn}/row-counts

Row counts for many tables on one connection.

## POST /api/migrator/{conn}/sample-multi

Sample rows for many tables on one connection.

## Module config (`schema_converter/config.ini`)

### GET /api/migrator/config

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/migrator/config
```

### POST /api/migrator/config

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"section":"schema.conversion","key":"compare_sample_size","value":"20"}' \
     http://localhost:8000/api/migrator/config
```

### POST /api/migrator/config/restore

Restores `schema_converter/config.ini` from `config.ini.example`.

### Errors

| Code | Cause |
|------|-------|
| `400` | Engine doesn't support schema conversion or invalid body |
| `404` | Connection or table not found |
| `503` | Data Migration module not installed |
