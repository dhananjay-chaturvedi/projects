---
title: Connections
description: CRUD and test endpoints for saved database connection profiles.
sidebar:
  order: 4
---

## GET /api/connections

List all saved DB connection profiles.

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/connections
```

```json
[
  {
    "name": "prod",
    "db_type": "PostgreSQL",
    "host": "db.example.com",
    "port": "5432",
    "user": "app",
    "database": "appdb",
    "ssl_mode": "require"
  },
  {
    "name": "ora1",
    "db_type": "Oracle",
    "host": "ora.example.com",
    "port": "1521",
    "user": "hr",
    "service": "ORCLPDB1"
  }
]
```

Passwords are **never** returned.

## POST /api/connections

Create a new connection profile. Password is required and encrypted on
the server before being saved.

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "prod",
       "db_type": "PostgreSQL",
       "host": "db.example.com",
       "port": "5432",
       "user": "app",
       "password": "s3cret",
       "database": "appdb"
     }' \
     http://localhost:8000/api/connections
```

Success (`201 Created`):

```json
{"status": "created", "name": "prod"}
```

Validation error (`422`):

```json
{"detail": [{"loc": ["body", "name"], "msg": "field required", "type": "value_error.missing"}]}
```

### Body fields

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `name` | string | yes | Unique identifier |
| `db_type` | string | yes | `PostgreSQL`, `MySQL`, `MariaDB`, `Oracle`, `SQL Server`, `MongoDB`, `DocumentDB`, `SQLite` |
| `host` | string | yes | Hostname (or file path for SQLite) |
| `port` | string | yes | Port number |
| `user` | string | depends | Not required for SQLite |
| `password` | string | depends | Encrypted at rest |
| `database` | string | depends | For non-Oracle engines |
| `service` | string | depends | Oracle service name (use instead of `database`) |
| `ssl_mode` | string | no | `disable`, `prefer`, `require`, `verify_ca`, `verify_full` (engine-dependent) |
| `ssl_ca` | string | no | Path to CA bundle |
| `ssl_cert` | string | no | Path to client cert |
| `ssl_key` | string | no | Path to client key |
| `tls_ca` | string | no | Mongo / DocumentDB CA bundle |
| `ssh_tunnel` | object | no | Reach the DB through an SSH tunnel (see below) |

### Remote connections (`ssh_tunnel`)

Provide an `ssh_tunnel` object to register a connection that reaches the
database through an SSH bastion / jump host. With a tunnel, `host`/`port`
above are the database endpoint **as seen from the SSH host** (often
`localhost`).

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "prod_via_bastion",
       "db_type": "PostgreSQL",
       "host": "localhost",
       "port": "5432",
       "user": "app",
       "password": "s3cret",
       "database": "appdb",
       "ssh_tunnel": {
         "ssh_host": "bastion.example.com",
         "ssh_user": "ubuntu",
         "ssh_port": 22,
         "ssh_key_file": "/home/me/.ssh/id_rsa"
       }
     }' \
     http://localhost:8000/api/connections
```

| `ssh_tunnel` field | Type | Required | Notes |
|--------------------|------|----------|-------|
| `ssh_host` | string | yes | Bastion / SSH host |
| `ssh_user` | string | yes* | SSH username |
| `ssh_port` | int | no | Default `22` |
| `ssh_password` | string | no | Encrypted at rest; needs `sshpass` on the server |
| `ssh_key_file` | string | no | Private key path on the server |

The tunnel is established when the connection is opened/tested and torn down
on close.

## POST /api/connections/{name}/test

Test a saved connection (opens a connection, runs `SELECT 1` or
equivalent, then closes).

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/connections/prod/test
```

Success:

```json
{"status": "ok", "version": "PostgreSQL 16.1 (Debian 16.1-1.pgdg120+1)"}
```

Failure (`200` with error in body — the connection profile exists, but
the live connection failed):

```json
{
  "status": "failed",
  "error": "FATAL: password authentication failed for user \"app\"",
  "code": "AUTH_FAILED"
}
```

## DELETE /api/connections/{name}

Remove a saved connection profile.

```bash
curl -X DELETE -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/connections/prod
```

```text
204 No Content
```

If the connection does not exist:

```json
{"detail": "connection 'prod' not found"}
```

(`404 Not Found`)

## Bulk operations

There is no built-in bulk endpoint; loop on the client side:

```bash
for conn in prod stage dev; do
  curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
       http://localhost:8000/api/connections/$conn/test
done
```

## Python client example

```python
import os, requests

BASE = "http://localhost:8000/api"
HEADERS = {"X-API-Key": os.environ["DBTOOL_API_KEY"]}

# List
r = requests.get(f"{BASE}/connections", headers=HEADERS)
r.raise_for_status()
for c in r.json():
    print(c["name"], c["db_type"])

# Add
requests.post(f"{BASE}/connections", headers=HEADERS, json={
    "name": "prod",
    "db_type": "PostgreSQL",
    "host": "db.example.com",
    "port": "5432",
    "user": "app",
    "password": "s3cret",
    "database": "appdb",
}).raise_for_status()
```
