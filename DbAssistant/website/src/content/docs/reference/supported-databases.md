---
title: Supported databases
description: Drivers, ports, query languages, capabilities, and per-engine object types.
sidebar:
  order: 1
---

| Engine | Driver | Default port | Uses service name | Query language | Schema conversion | Transactions |
|--------|--------|:----:|:----:|:------:|:----:|:----:|
| Oracle | `oracledb` | 1521 | ✓ | SQL | ✓ | ✓ |
| MySQL | `mysql-connector-python` | 3306 | — | SQL | ✓ | ✓ |
| MariaDB | `mysql-connector-python` | 3306 | — | SQL | ✓ | ✓ |
| PostgreSQL | `psycopg2-binary` | 5432 | — | SQL | ✓ | ✓ |
| SQL Server / Azure SQL | `pymssql` | 1433 | — | T-SQL | ✓ | ✓ |
| SQLite | built-in | — | — | SQL | ✓ | ✓ |
| MongoDB | `pymongo` | 27017 | — | JSON document | — | — |
| AWS DocumentDB | `pymongo` (TLS) | 27017 | — | JSON document | — | — |

## Connection examples

### Oracle

```bash
python dbtool.py connections add \
    --name ora1 \
    --type Oracle \
    --host oracle.example.com \
    --port 1521 \
    --user hr \
    --service ORCLPDB1
```

Set `oracle_client_path` in `config.ini` for thick mode; leave blank for
thin mode (Oracle 12.1+).

### MySQL / MariaDB

```bash
python dbtool.py connections add \
    --name app \
    --type MySQL \
    --host db.example.com \
    --port 3306 \
    --user app \
    --db appdb \
    --ssl-mode required
```

### PostgreSQL

```bash
python dbtool.py connections add \
    --name analytics \
    --type PostgreSQL \
    --host pg.example.com \
    --port 5432 \
    --user analytics \
    --db reporting \
    --ssl-mode verify_ca \
    --ssl-ca /etc/ssl/certs/rds-ca-bundle.pem
```

### SQL Server / Azure SQL

```bash
python dbtool.py connections add \
    --name reports \
    --type "SQL Server" \
    --host mssql.example.com \
    --port 1433 \
    --user reporter \
    --db ReportingDB
```

### MongoDB

```bash
python dbtool.py connections add \
    --name analytics-mongo \
    --type MongoDB \
    --host mongo.example.com \
    --port 27017 \
    --user app \
    --db appdb
```

### AWS DocumentDB

```bash
python dbtool.py connections add \
    --name docdb \
    --type DocumentDB \
    --host docdb-cluster.cluster-xxx.region.docdb.amazonaws.com \
    --port 27017 \
    --user app \
    --db appdb \
    --tls-ca /path/to/global-bundle.pem
```

TLS is enabled automatically for DocumentDB; the CA bundle must be the
AWS RDS combined bundle.

### SQLite

`host` is the file path.

```bash
python dbtool.py connections add \
    --name local \
    --type SQLite \
    --host /tmp/test.db
```

## Per-engine object types

| Object type | Oracle | MySQL | MariaDB | Postgres | SQL Server | SQLite | Mongo | DocumentDB |
|-------------|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| `tables` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| `collections` | — | — | — | — | — | — | ✓ | ✓ |
| `views` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| `procs` | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — |
| `functions` | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — |
| `indexes` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `triggers` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| `sequences` | ✓ | — | — | ✓ | ✓ | — | — | — |
| `constraints` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| `users` | ✓ | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ |
| `databases` | — | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ |
| `schemas` | ✓ | — | — | ✓ | ✓ | — | — | — |
| `tablespaces` | ✓ | — | — | — | ✓ | — | — | — |
| `engines` | — | ✓ | ✓ | — | — | — | — | — |
| `events` | — | ✓ | ✓ | — | — | — | — | — |
| `processlist` | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — |
| `roles` | ✓ | — | — | ✓ | ✓ | — | — | — |
| `synonyms` | ✓ | — | — | — | — | — | — | — |
| `packages` | ✓ | — | — | — | — | — | — | — |
| `types` | ✓ | — | — | ✓ | ✓ | — | — | — |
| `materializedviews` | ✓ | — | — | ✓ | — | — | — | — |
| `databaselinks` | ✓ | — | — | — | — | — | — | — |
| `extensions` | — | — | — | ✓ | — | — | — | — |
| `activity` | — | — | — | ✓ | — | — | — | — |

## Document operations (Mongo / DocumentDB)

| Operation | Example |
|-----------|---------|
| `find` | `{"collection":"users","operation":"find","filter":{...},"limit":50}` |
| `aggregate` | `{"collection":"users","operation":"aggregate","pipeline":[{"$match":{}}]}` |
| `count` | `{"collection":"users","operation":"count","filter":{...}}` |

## Capability metadata at runtime

```bash
python dbtool.py databases types --format json
python dbtool.py databases ops --type Oracle
```

Or REST:

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/databases/types
```
