---
title: Authentication
description: API key, environment variables, and hardening for remote access.
sidebar:
  order: 2
---

## Setting API keys

The API supports two authentication sources:

- the legacy single environment key (`DBTOOL_API_KEY`), and
- managed KMS keys created from the UI, CLI, or API.

Authentication is enforced when either `DBTOOL_API_KEY` is set or at least one
KMS key exists. Add the legacy environment key to `.env` in the project root:

```dotenv
DBTOOL_API_KEY=replace-with-a-long-random-string-at-least-32-chars
```

Generate a strong key:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Or set as a process environment variable:

```bash
export DBTOOL_API_KEY="...."
python dbtool.py api --port 8000
```

For managed keys, use the Settings panel (**Access Keys**) in Tk/TUI/Web, or:

```bash
python dbtool.py apikey create --name release-client
python dbtool.py apikey list
python dbtool.py apikey revoke <key-id>
python dbtool.py apikey regenerate <key-id>
```

KMS secrets are shown once at creation/regeneration. Only salted
PBKDF2-HMAC-SHA256 hashes are stored under `~/.dbassistant`.

## Using an API key

Pass it on every non-public request via either header:

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/health

curl -H "Authorization: Bearer $DBTOOL_API_KEY" \
     http://localhost:8000/api/health
```

Programmatically (Python):

```python
import os, requests
headers = {"X-API-Key": os.environ["DBTOOL_API_KEY"]}
r = requests.get("http://localhost:8000/api/health", headers=headers)
```

JavaScript / fetch:

```js
fetch("http://localhost:8000/api/health", {
  headers: { "X-API-Key": process.env.DBTOOL_API_KEY }
});
```

## Constant-time comparison

The API key check uses
[`hmac.compare_digest`](https://docs.python.org/3/library/hmac.html#hmac.compare_digest)
to defeat timing-attack probing. Any mismatch returns
`401 Unauthorized` with the same response time as a successful match.

## Open mode (no key)

If `DBTOOL_API_KEY` is unset **and** no KMS key exists, the API runs in
**open mode** — anyone who can reach the listener can call any endpoint. Only
use this:

- bound to `127.0.0.1` only, **and**
- behind an auth proxy you trust, **or**
- inside an isolated container network

## Multiple keys

Supported through the KMS. Keys are authentication-only; they do not grant
module licenses or per-key scopes.

## Rate limiting

Not currently enforced server-side. Put DbAssistant behind nginx,
Caddy, or a cloud API gateway for production rate limiting.

## CORS

By default the API only allows local origins. Configure `[api] cors_origins` in
`config.ini` or `DBTOOL_API_CORS_ORIGINS`:

```ini
[api]
cors_origins = https://your-frontend.example.com
```

## TLS

The built-in uvicorn server can do TLS:

```bash
python dbtool.py api --host 0.0.0.0 --port 8443 \
    --ssl-keyfile /etc/ssl/private/dbtool.key \
    --ssl-certfile /etc/ssl/certs/dbtool.crt
```

For production, prefer terminating TLS at a reverse proxy and running
the API on `127.0.0.1:8000`.

## Daemon control restriction

`POST /api/daemon/start` and `POST /api/daemon/stop` are intentionally
**not exposed**. Start/stop the daemon from the CLI on the host:

```bash
python dbtool.py daemon start
python dbtool.py daemon stop
```

`GET /api/daemon/status` is allowed.

## Cloud login restriction

`POST /api/monitor/cloud/connections/{name}/login` shells out to `aws`, `az`,
or `gcloud` and may open a browser **on the host running the API**.
Use it only for local/dev installs.

## Recommended hardening

- Run the API on `127.0.0.1` behind nginx/Caddy with TLS and IP allow-list.
- Set `DBTOOL_API_KEY` ≥ 32 random characters.
- Restrict CORS to your front-end origin.
- Disable `/docs` in production:
  ```python
  app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
  ```
- Audit `~/.dbassistant/keys/` mode = `0600`.
- Use cloud IAM roles instead of static AWS keys whenever possible.
