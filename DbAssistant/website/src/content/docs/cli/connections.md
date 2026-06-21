---
title: connections
description: Add, list, test, and remove encrypted database connection profiles.
sidebar:
  order: 2
---

Manage saved DB connection profiles. Passwords are encrypted with
Fernet and stored under `~/.dbassistant/connections/db.json`.

## list

```bash
python dbtool.py connections list
python dbtool.py connections list --format json
python dbtool.py connections list --format csv
```

Example output:

```text
┌──────┬────────────┬──────────────────┬──────┬──────┬───────┐
│ name │ db_type    │ host             │ port │ user │ database│
├──────┼────────────┼──────────────────┼──────┼──────┼───────┤
│ prod │ PostgreSQL │ db.example.com   │ 5432 │ app  │ appdb │
│ ora1 │ Oracle     │ ora.example.com  │ 1521 │ hr   │ —     │
└──────┴────────────┴──────────────────┴──────┴──────┴───────┘
```

## add

```bash
python dbtool.py connections add \
    --name prod \
    --type PostgreSQL \
    --host db.example.com \
    --port 5432 \
    --user app \
    --db appdb
# Password? ········  (prompted; not echoed)
```

Per-engine arguments:

| Engine | Use `--db` (database name) | Use `--service` (Oracle service name) |
|--------|:--:|:--:|
| Oracle | — | ✓ |
| MySQL / MariaDB / PostgreSQL / SQL Server / MongoDB | ✓ | — |
| SQLite | `--host` = file path | — |

Optional SSL flags (engine-dependent):

```bash
python dbtool.py connections add --name rds \
    --type MySQL --host rds.amazonaws.com --port 3306 --user app --db appdb \
    --ssl-mode verify_ca \
    --ssl-ca /path/to/global-bundle.pem
```

SSL modes: `disable`, `prefer`, `require`, `verify_ca`, `verify_full`
(PostgreSQL); `disabled`, `required`, `verify_ca`, `verify_identity`
(MySQL / MariaDB).

MongoDB / DocumentDB:

```bash
python dbtool.py connections add --name docdb \
    --type DocumentDB \
    --host docdb-cluster.cluster-xxx.region.docdb.amazonaws.com \
    --port 27017 \
    --user app \
    --db appdb \
    --tls-ca /path/to/global-bundle.pem
```

DocumentDB enables TLS automatically. Use the AWS RDS combined CA
bundle (`global-bundle.pem`).

## Remote connections (SSH tunnel)

Reach a database that is only accessible through a bastion / jump host by
adding SSH tunnel options. The tool opens an SSH **local port-forward** and
connects the driver to the local end automatically.

Important: with a tunnel, `--host` / `--port` are the database endpoint **as
seen from the SSH host** (commonly `localhost`).

Key-file auth:

```bash
python dbtool.py connections add --name prod_via_bastion \
    --type PostgreSQL --host localhost --port 5432 --user app --db appdb \
    --ssh-host bastion.example.com --ssh-user ubuntu \
    --ssh-key-file ~/.ssh/id_rsa
# DB Password? ········   (prompted)
```

Password auth (requires `sshpass` on `PATH`):

```bash
python dbtool.py connections add --name prod_via_bastion \
    --type MySQL --host 127.0.0.1 --port 3306 --user app --db appdb \
    --ssh-host bastion.example.com --ssh-user ubuntu
# SSH password? ········  (prompted when no key file is given)
# DB Password?  ········
```

| Flag | Meaning |
|------|---------|
| `--ssh-host` | Bastion/SSH host to tunnel through (enables a remote connection) |
| `--ssh-user` | SSH username |
| `--ssh-port` | SSH port (default `22`) |
| `--ssh-key-file` | Private key file for the tunnel |
| `--ssh-password` | SSH password (needs `sshpass`); omit to be prompted |

The tunnel is opened on `connect`/`test` and torn down on disconnect. The SSH
password (when saved) is encrypted at rest under the same Fernet key as DB
passwords.

## test

```bash
python dbtool.py connections test prod
```

Expected:

```text
connection ok — PostgreSQL 16.1 (Debian 16.1-1.pgdg120+1)
```

Failure:

```text
ERROR: connection to server at "db.example.com" failed:
  FATAL: password authentication failed for user "app"
exit code 2
```

## remove

```bash
python dbtool.py connections remove prod
```

Confirmation prompt:

```text
Remove connection 'prod'? [y/N]: y
removed
```

Skip the prompt for scripting:

```bash
python dbtool.py connections remove prod --yes
```

## Common patterns

Pipe into `jq`:

```bash
python dbtool.py connections list --format json | jq -r '.[].name'
# prod
# ora1
```

Add many connections from JSON:

```bash
jq -c '.[]' connections.json | while read profile; do
  name=$(echo "$profile" | jq -r .name)
  type=$(echo "$profile" | jq -r .db_type)
  host=$(echo "$profile" | jq -r .host)
  user=$(echo "$profile" | jq -r .user)
  db=$(echo   "$profile" | jq -r .database)
  python dbtool.py connections add --name "$name" --type "$type" \
       --host "$host" --user "$user" --db "$db" --password "$(get-secret "$name")"
done
```

## Where is it stored?

```text
~/.dbassistant/connections/db.json     # encrypted profiles
~/.dbassistant/keys/db.key             # Fernet key (chmod 600)
```

Never commit either to version control.
