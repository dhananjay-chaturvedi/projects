---
title: Security model
description: Encryption, secret storage, API authentication, and hardening guidance.
sidebar:
  order: 4
---

DbAssistant treats every saved credential as sensitive.

## Encryption at rest

All saved DB and cloud connection profiles are encrypted with
[Fernet](https://cryptography.io/en/latest/fernet/) (AES-128 in CBC
mode with HMAC-SHA256).

```text
~/.dbassistant/keys/db.key          ← randomly generated on first run
~/.dbassistant/keys/cloud.key
~/.dbassistant/keys/monitor.key
```

Each key file is `0600` (owner read/write only). Connection files are
also `0600`.

## Key rotation

To rotate the DB encryption key:

1. Export connection list (`dbtool connections list --format json`)
2. Remove `~/.dbassistant/keys/db.key`
3. Re-add each connection with `dbtool connections add ...`

A future release will ship an automated rotation command.

## REST API authentication

Authentication is enforced whenever **either** of these is true:

- `DBTOOL_API_KEY` is set in `.env` / the environment, or
- one or more keys exist in the built-in **Key Management System** (KMS).

```dotenv
DBTOOL_API_KEY=super-long-random-string-at-least-32-chars
```

Every non-public `/api/*` request must then present a key as either header:

```text
X-API-Key: <key>
Authorization: Bearer <key>
```

KMS keys are created/listed/revoked from the Settings panel of any UI, the
`dbtool apikey` CLI, or the API; only a salted PBKDF2-HMAC-SHA256 hash is stored
and the secret is shown once at creation. Token comparison uses
[`hmac.compare_digest`](https://docs.python.org/3/library/hmac.html#hmac.compare_digest)
to prevent timing attacks. Missing or wrong key → `401 Unauthorized`.

If neither a `DBTOOL_API_KEY` nor any KMS key is configured, the API runs in
open mode — only use that bound to `127.0.0.1` behind your own auth proxy.

## AI read-only enforcement

The **AI Query Assistant** and **App Builder** can never mutate a live database.
A shared guard (`common/sql_guard.py`) classifies every statement the AI is about
to execute and rejects anything that changes data or schema —
`DROP`, `DELETE`, `UPDATE`, `INSERT`, `TRUNCATE`, `ALTER`, `CREATE`, `MERGE`,
`GRANT`, `REVOKE`, and friends — including:

- statements hidden behind `--` / `/* */` comments,
- multiple statements separated by `;` (e.g. `SELECT 1; DROP TABLE x`),
- data-modifying CTEs (e.g. `WITH x AS (DELETE … RETURNING …) …`).

The guard runs at the single execution chokepoint
(`ai_query/sql_execution_service.execute_sql_after_gate`) plus the no-session
`POST /api/ai/execute-sql` path, so it applies uniformly across Tk, TUI, Web,
CLI, API, and headless. The App Builder's optional schema deploy is additive-only
(`CREATE TABLE IF NOT EXISTS`) and independently refuses destructive statements.
The general-purpose **SQL Editor** and **Data Migration** modules are *not*
restricted — they are the intended path for deliberate writes.

## Request size limits

The API rejects request bodies larger than 10 MB by default
(`[api] max_body_bytes`, or `DBTOOL_API_MAX_BODY_BYTES`). This prevents memory
exhaustion from oversized SQL strings or JSON payloads.

## CORS

By default the REST API only allows local origins
(`http://localhost:*` / `http://127.0.0.1:*`). Override with the `[api]`
`cors_origins` setting (comma-separated) or the `DBTOOL_API_CORS_ORIGINS`
environment variable; set it to `*` to allow all origins (development only):

```ini
[api]
cors_origins = https://your-frontend.example.com
```

## Daemon control

`/api/daemon/start` and `/api/daemon/stop` are intentionally **not
exposed over HTTP**. Lifecycle is CLI-only:

```bash
python dbtool.py daemon start
python dbtool.py daemon stop
```

`GET /api/daemon/status` is available for read-only monitoring.

## Cloud login

`POST /api/monitor/cloud/connections/{name}/login` shells out to `aws`, `az`,
or `gcloud` and opens a browser **on the host running the API**. It is
intended for local/developer use only. For production cloud auth,
configure environment-based credentials (IAM roles, service accounts,
Azure managed identity).

## Logging

`mask_sensitive_in_logs = true` in `config.ini` redacts passwords,
tokens, and connection strings from log lines.

## Recommended hardening checklist

- Set `DBTOOL_API_KEY` to a 64-character random string
- Run the API on `127.0.0.1` behind a reverse proxy (nginx, Caddy) with TLS
- Restrict CORS to your front-end origin
- Keep `~/.dbassistant/` on encrypted disk (FileVault / dm-crypt)
- Audit `~/.dbassistant/keys/` permissions (`0600`)
- Use OS-level auth (AWS profiles, `az login`, `gcloud auth`) instead
  of storing static cloud keys whenever possible
