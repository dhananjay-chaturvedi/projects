---
title: query
description: Run SQL or document queries against any saved connection.
sidebar:
  order: 3
---

Run a SQL statement (or a JSON document query for MongoDB / DocumentDB)
and print or save the results.

## Syntax

```bash
python dbtool.py query --conn NAME [--sql SQL | --file PATH] [--format ...]
```

Required: `--conn` plus exactly one of `--sql` or `--file`.

## Examples

Single statement:

```bash
python dbtool.py query --conn prod --sql "SELECT version()"
```

From a file:

```bash
python dbtool.py query --conn prod --file ./report.sql
```

JSON output (for piping):

```bash
python dbtool.py query --conn prod --sql "SELECT id, name FROM users LIMIT 5" \
    --format json
```

```json
{
  "columns": ["id", "name"],
  "rows": [
    [1, "alice"],
    [2, "bob"]
  ],
  "rowcount": 5,
  "elapsed_ms": 12
}
```

CSV output:

```bash
python dbtool.py query --conn prod --sql "SELECT * FROM users" \
    --format csv > users.csv
```

DML returns rowcount:

```bash
python dbtool.py query --conn prod --sql "UPDATE t SET x = 1 WHERE id = 5"
# rowcount: 1
```

Multi-statement scripts (semicolons):

```bash
python dbtool.py query --conn prod --file ./migration.sql
```

Each statement runs in order; failure aborts the rest.

## MongoDB / DocumentDB

Use JSON document queries:

```bash
python dbtool.py query --conn docdb --sql '{
  "collection": "users",
  "operation": "find",
  "filter": {"status": "active"},
  "limit": 50
}'
```

Supported operations: `find`, `aggregate`, `count`.

## Transactions

Autocommit for new connections is controlled by
`[database.connection] default_autocommit` in `config.ini` (default `true`),
applied consistently across all engines. For explicit transactions, run a
script:

```sql
-- transaction.sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;
COMMIT;
```

```bash
python dbtool.py query --conn prod --file transaction.sql
```

## Limits

By default, the CLI displays up to 1000 rows in table format. Adjust in
`properties.ini`:

```ini
[ui.limits]
cli_max_display_rows = 1000      # 0 = unlimited
table_export_max_rows = 0        # for --output / --format csv
```

JSON and CSV formats are not row-limited unless `table_export_max_rows`
is set.

## Programmatic equivalent

```python
from app.headless.db_service import DBService

svc = DBService()
result = svc.execute("prod", "SELECT 1")
print(result["columns"], result["rows"])
```

## See also

- [`objects`](/cli/objects/) — list tables, views, procedures, etc.
- [`migrator show`](/cli/migrator/) — display a table's full schema
- [SQL API endpoint](/api/query-objects/)
