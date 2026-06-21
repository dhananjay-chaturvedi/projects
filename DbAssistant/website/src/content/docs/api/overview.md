---
title: REST API overview
description: Architecture, base URL, authentication, content types, error envelopes, and the complete endpoint index.
sidebar:
  order: 1
---

The REST API is a thin layer over `DBService`. Anything available in
the CLI is available here.

## Base URL

```text
http://<host>:<port>/api/...
```

Default: `http://127.0.0.1:8000/api/...`.

## Start the server

```bash
python dbtool.py api --host 0.0.0.0 --port 8000

# or as a module:
python -m monitoring api --port 8001    # core + monitoring routes only
```

Interactive Swagger UI:

```
http://localhost:8000/docs
```

OpenAPI spec:

```
http://localhost:8000/openapi.json
```

## Authentication

If `DBTOOL_API_KEY` is set in the environment, or if any managed KMS key exists,
every non-public request must include one of:

```text
X-API-Key: <your-key>
Authorization: Bearer <your-key>
```

Comparison is constant-time via `hmac.compare_digest`. Managed KMS keys store
only salted PBKDF2-HMAC-SHA256 hashes. See
[Authentication](/api/authentication/).

## Content type

All POST / PATCH / PUT bodies must be `application/json`:

```bash
curl -X POST http://localhost:8000/api/query \
     -H "Content-Type: application/json" \
     -H "X-API-Key: $DBTOOL_API_KEY" \
     -d '{"connection":"prod","sql":"SELECT 1"}'
```

Request body size is capped at 10 MB by default.

## Response envelope

Successful responses are bare JSON objects (no wrapping envelope):

```json
{"columns": ["n"], "rows": [[1]], "rowcount": 1, "elapsed_ms": 4}
```

Errors use FastAPI's default envelope:

```json
{"detail": "connection 'prod' not found"}
```

## HTTP status codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 201 | Created (new connection profile) |
| 204 | Deleted |
| 400 | Bad request (invalid args, malformed SQL) |
| 401 | Missing/invalid `X-API-Key` |
| 403 | Forbidden (e.g. daemon control via API) |
| 404 | Not found (no such connection, table, etc.) |
| 413 | Payload too large |
| 422 | Validation error |
| 500 | Server error |

## Module gating

Module-owned routes appear only when their module is installed. Check:

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/modules
```

A standalone module API (`python -m monitoring api`) serves the core
routes plus only that module's routes — useful for slim deployments.

## Endpoint index

### Core (always available)

| Method | Path | Page |
|--------|------|------|
| GET | `/api/health` | [Health & modules](/api/health-modules/) |
| GET | `/api/modules` | [Health & modules](/api/health-modules/) |
| GET / POST | `/api/connections` | [Connections](/api/connections/) |
| DELETE | `/api/connections/{name}` | [Connections](/api/connections/) |
| POST | `/api/connections/{name}/test` | [Connections](/api/connections/) |
| POST | `/api/query` | [Query & objects](/api/query-objects/) |
| GET | `/api/objects/{conn}?type=` | [Query & objects](/api/query-objects/) |
| GET | `/api/databases/types` | [Query & objects](/api/query-objects/) |
| GET | `/api/databases/ops?type=` | [Query & objects](/api/query-objects/) |
| GET | `/api/config?section=` | [Health & modules](/api/health-modules/) |
| GET | `/api/dashboard` | [Dashboard](/api/dashboard/) |

### Data Migration

| Method | Path | Page |
|--------|------|------|
| GET | `/api/migrator/{conn}/{table}` | [Data Migration](/api/migrator/) |
| GET | `/api/migrator/{conn}/dump?table=` | [Data Migration](/api/migrator/) |
| POST | `/api/migrator/convert` | [Data Migration](/api/migrator/) |
| POST | `/api/migrator/transfer-data` | [Data Migration](/api/migrator/) |
| POST | `/api/migrator/compare-schema` | [Data Migration](/api/migrator/) |
| POST | `/api/migrator/compare-data` | [Data Migration](/api/migrator/) |

### AI Query Assistant

| Method | Path | Page |
|--------|------|------|
| POST | `/api/ai/query` | [AI](/api/ai/) |
| GET | `/api/ai/backends` | [AI](/api/ai/) |
| GET / POST | `/api/ai/sessions` | [AI](/api/ai/) |
| PATCH / DELETE | `/api/ai/sessions/{id}` | [AI](/api/ai/) |
| POST | `/api/ai/sessions/{id}/messages` | [AI](/api/ai/) |
| POST | `/api/ai/sessions/{id}/cross-tab` | [AI](/api/ai/) |
| POST | `/api/ai/sessions/{id}/execute-sql` | [AI](/api/ai/) |
| POST | `/api/ai/sessions/save` | [AI](/api/ai/) |
| POST | `/api/ai/sessions/load` | [AI](/api/ai/) |

### Server Monitor

| Method | Path | Page |
|--------|------|------|
| GET | `/api/metrics` | [Metrics](/api/metrics/) |
| GET | `/api/metrics/{conn}` | [Metrics](/api/metrics/) |
| GET | `/api/os/metrics?disk=` | [OS metrics](/api/os/) |
| GET | `/api/thresholds?source=&api=&path=&all=` | [Thresholds](/api/thresholds/) |
| GET | `/api/thresholds/{source}/{metric}?path=` | [Thresholds](/api/thresholds/) |
| POST | `/api/thresholds/check` | [Thresholds](/api/thresholds/) |
| GET / POST | `/api/monitor/cloud/connections` | [Cloud](/api/cloud/) |
| DELETE | `/api/monitor/cloud/connections/{name}` | [Cloud](/api/cloud/) |
| POST | `/api/monitor/cloud/connections/{name}/test` | [Cloud](/api/cloud/) |
| POST | `/api/monitor/cloud/connections/{name}/login` | [Cloud](/api/cloud/) |
| GET | `/api/monitor/cloud/metrics/{name}` | [Cloud](/api/cloud/) |
| GET | `/api/monitor/cloud/providers/schema?provider=` | [Cloud](/api/cloud/) |
| GET | `/api/monitor/cloud/connections/{name}/rds-endpoint` | [Cloud](/api/cloud/) |
| POST | `/api/notify` | [Notifications](/api/notify/) |
| GET | `/api/daemon/status` | [Daemon status](/api/daemon/) |

## CORS

The API allows local origins by default. Set `[api] cors_origins` or
`DBTOOL_API_CORS_ORIGINS` to explicit trusted origins before exposing it on a
network — see [Security model](/architecture/security/).

## Next: pick an endpoint group from the sidebar.
