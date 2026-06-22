---
title: Data Migration
description: Cross-engine migration — schema conversion, data transfer, and post-migration validation between Oracle, MySQL, PostgreSQL, SQL Server, and SQLite.
sidebar:
  order: 1
---

The **Data Migration** module (Python package `schema_converter`) helps you
move workloads between relational engines in three phases:

1. **Schema** — read source tables and generate target `CREATE TABLE` / index DDL
2. **Data** — copy rows from source to target in configurable batches
3. **Validation** — compare schema and data so you can confirm the migration

## Supported source / target engines

| Engine | Source | Target |
|--------|:------:|:------:|
| Oracle | ✓ | ✓ |
| MySQL | ✓ | ✓ |
| MariaDB | ✓ | ✓ |
| PostgreSQL | ✓ | ✓ |
| SQL Server | ✓ | ✓ |
| SQLite | ✓ | ✓ |
| MongoDB / DocumentDB | ✓ *(data copy)* | ✓ *(data copy)* |

Document-to-document **data transfer** (collection copy) is supported. Schema
conversion remains relational-only; RDBMS ↔ document migration is not supported yet.

## Type mapping overrides

Override default engine type mapping with rules in the form
`"source_type:target_type"` (comma-separated). Size/precision from the source
column is preserved when the target type accepts it.

Examples:

- `varchar2:text` — map all Oracle `VARCHAR2` columns to `TEXT`
- `int:decimal` — map integer types to `DECIMAL(10,0)` when no precision is present

Set per-run in the UI **Type mapping rules** field, CLI `--type-map`, or API
`type_map`. A default can be saved in `schema_converter/config.ini`
(`type_overrides`).

## Character set / multibyte data

`conversion_charset` in `schema_converter/config.ini` (default `utf-8`) controls
how text is transferred between engines. During data transfer the tool sets the
client encoding on source/target connections and decodes text-like byte values
using this charset. MySQL/MariaDB targets default to `utf8mb4` when converting
from UTF-8 sources so emoji and CJK characters survive.

## Advanced transfer options

Every data transfer (UI **Data Migration** tab, CLI `migrator transfer-data`,
API `POST /api/migrator/transfer-data[-multi]`) accepts the following options.

Fixed-value policies (`continue_on_error`, `overflow_policy`, `null_policy`,
`bool_policy`, `timezone_policy`, `target_timezone`, `reset_sequences`) are
configured in **⚙ Migration Settings** (`schema_converter/config.ini`) and act
as defaults; the CLI/API can still override them per run. The per-run row
filter (`where`) and column subset (`columns`) apply to **single-table
transfers only** — the UI greys them out when more than one table is selected,
and the multi endpoints/CLI reject them. Column rename (`column_map`) and the
row **limit** apply to **every selected table** (a rename is a no-op for tables
that lack a listed source column).

| Gap | Option | UI field | CLI flag | API field | Behaviour |
|-----|--------|----------|----------|-----------|-----------|
| G1 | Row filter | Row filter (WHERE) — single table | `--where` | `where` | SQL filter (no `WHERE` keyword); single-table transfers only |
| G1 | Row limit | Row limit (per table) | `--limit` | `limit` | Max rows applied to each selected table |
| G2 | Column subset | Columns (subset) — single table | `--columns` | `columns` | Comma-separated source columns; single-table transfers only |
| G2 | Column rename | Column rename — all tables | `--column-map` | `column_map` | `src:tgt,...` rename map; applied to every selected table |
| G3 | Continue on error | Migration Settings | `--continue-on-error` | `continue_on_error` | Falls back to per-row inserts; bad rows reported, transfer not aborted |
| G4 | Overflow policy | Migration Settings | `--overflow-policy` | `overflow_policy` | `fail` \| `truncate` \| `skip` when a value exceeds the target column |
| G5 | Dry-run | **Validate (Dry-run)** button | `migrator validate` | `POST /api/migrator/validate` | Read-only report: type incompatibilities, oversized columns, unsupported defaults |
| G6 | NULL/empty | Migration Settings | `--null-policy` | `null_policy` | `keep` \| `empty_to_null` \| `null_to_empty` |
| G6 | Boolean | Migration Settings | `--bool-policy` | `bool_policy` | `auto` \| `int` \| `true_false` |
| G7 | Timezone | Migration Settings | `--timezone-policy` / `--target-timezone` | `timezone_policy` / `target_timezone` | `preserve` \| `naive` \| `utc` \| `target` (named tz) |
| G8 | Sequence reset | Migration Settings | `--reset-sequences` | `reset_sequences` | Reset auto-increment/serial to `MAX(id)+1` after load |
| G9 | Checkpoint/resume | Checkpoint / resume | `--checkpoint` | `checkpoint` | Records committed rows so an interrupted transfer resumes where it stopped |
| G10 | Report artifact | Report file | `--report` | `report_path` | Writes a JSON report (rows, skipped, errors, durations, mismatches) |

### Pre-migration validation (dry-run)

```bash
python dbtool.py migrator validate \
    --source-conn prod --target-conn target_mysql \
    --tables users,orders --target-db test
```

## UI

Open the **Data Migration** tab. Use **⚙ Migration Settings** (top-right) to edit
`schema_converter/config.ini` (`compare_sample_size`, `zero_date_strategy`,
`parallel_workers`, `type_overrides`, `conversion_charset`, plus the fixed-value
transfer policies `continue_on_error`, `overflow_policy`, `null_policy`,
`bool_policy`, `timezone_policy`, `target_timezone`, `reset_sequences`).

1. Pick **source** and **target** connections and select table(s) or collection(s)
2. Optionally set **Type mapping rules** (e.g. `"varchar2:text,int:decimal"`)
3. **Preview / Convert schema** — review generated DDL and warnings *(relational only)*
4. **Apply DDL** on the target connection
5. **Transfer data** — batch copy rows/collections source → target
6. **Validate** — compare schema and data (row counts, sampled or full comparison)

MongoDB and DocumentDB appear in connection lists for **document-to-document data
transfer**. Schema conversion buttons are disabled for document-only pairs.
RDBMS ↔ MongoDB pairs show a clear “not supported yet” message.

## CLI

```bash
# Show schema for a table
python dbtool.py migrator show --conn prod --table users

# Dump DDL
python dbtool.py migrator dump --conn prod --table users --output users.sql

# Convert table to another engine (with custom type rules)
python dbtool.py migrator convert \
    --source-conn prod \
    --target-type MySQL \
    --table users \
    --type-map "varchar2:text,int:decimal" \
    --target-db test \
    --output users_mysql.sql

# Apply DDL on target
python dbtool.py migrator apply --target-conn target_mysql --ddl-file users_mysql.sql

# Transfer rows
python dbtool.py migrator transfer-data \
    --source-conn prod --target-conn target_mysql --table users

# Transfer with advanced options (filter, truncate overflow, resume, report)
python dbtool.py migrator transfer-data \
    --source-conn prod --target-conn target_mysql --table users \
    --where "status = 'active'" --columns "id,name,email" \
    --column-map "name:full_name" --overflow-policy truncate \
    --null-policy empty_to_null --timezone-policy utc \
    --continue-on-error --checkpoint --report /tmp/users_migrate.json

# Pre-migration dry-run (no rows moved)
python dbtool.py migrator validate \
    --source-conn prod --target-conn target_mysql --tables users,orders

# Validate migration
python dbtool.py migrator compare-schema \
    --source-conn prod --target-conn target_mysql --table users
python dbtool.py migrator compare-data \
    --source-conn prod --target-conn target_mysql --table users --mode sample
```

## REST API

```bash
# Schema of one table
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/migrator/prod/users"

# DDL dump
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/migrator/prod/dump?table=users"

# Convert
curl -X POST \
     -H "Content-Type: application/json" \
     -H "X-API-Key: $DBTOOL_API_KEY" \
     -d '{"source_conn":"prod","target_type":"MySQL","table":"users","type_map":"varchar2:text"}' \
     "http://localhost:8000/api/migrator/convert"

# Transfer data
curl -X POST \
     -H "Content-Type: application/json" \
     -H "X-API-Key: $DBTOOL_API_KEY" \
     -d '{"source_conn":"prod","target_conn":"target_mysql","table":"users"}' \
     "http://localhost:8000/api/migrator/transfer-data"

# Compare schema / data
curl -X POST \
     -H "Content-Type: application/json" \
     -H "X-API-Key: $DBTOOL_API_KEY" \
     -d '{"source_conn":"prod","target_conn":"target_mysql","table":"users","mode":"sample"}' \
     "http://localhost:8000/api/migrator/compare-data"
```

## Validation

| Check | CLI | API | What it does |
|-------|-----|-----|----------------|
| Pre-migration dry-run | `migrator validate` | `POST /api/migrator/validate` | Type incompatibilities, oversized columns, unsupported defaults (no rows moved) |
| Schema compare | `migrator compare-schema` | `POST /api/migrator/compare-schema` | Column types, nullability, keys |
| Data compare | `migrator compare-data` | `POST /api/migrator/compare-data` | Row-by-row (`--mode sample` or `full`) |
| Row counts | `migrator row-counts` | `POST /api/migrator/{conn}/row-counts` | Quick sanity check per table |

Use **sample** mode first on large tables; switch to **full** for a complete audit.

## Programmatic

```python
from schema_converter.bridge import make_service

svc = make_service()

# Convert
result = svc.convert_schema("prod", "MySQL", "users")
print(result["ddl"])

# Transfer
xfer = svc.transfer_data("prod", "target_mysql", "users")
print(xfer)

# Validate
schema_cmp = svc.compare_schema("prod", "target_mysql", "users")
data_cmp = svc.compare_data("prod", "target_mysql", "users", mode="sample")
```

## Type mapping (excerpt)

| Source type | MySQL | PostgreSQL | SQL Server | Oracle |
|-------------|-------|------------|------------|--------|
| `NUMBER(10)` | `BIGINT` | `BIGINT` | `BIGINT` | `NUMBER(10)` |
| `VARCHAR2(255)` | `VARCHAR(255)` | `VARCHAR(255)` | `NVARCHAR(255)` | `VARCHAR2(255)` |
| `CLOB` | `LONGTEXT` | `TEXT` | `NVARCHAR(MAX)` | `CLOB` |
| `BLOB` | `LONGBLOB` | `BYTEA` | `VARBINARY(MAX)` | `BLOB` |
| `DATE` | `DATETIME` | `TIMESTAMP` | `DATETIME2` | `DATE` |

## Conversion warnings

When a source type can't be mapped exactly, the closest match is used
and a warning is added to the result. Review warnings before applying
DDL or transferring data in production.
