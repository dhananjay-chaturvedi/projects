---
title: App Builder
description: REST routes to generate apps from scratch, a codebase, or a database, run agentic build jobs, and manage the generated app lifecycle.
sidebar:
  order: 8
---

All routes require the `X-API-Key` header (see [Authentication](/api/authentication/))
and the **App Builder** module. Prefix: `/api/app-builder`. Tag: **app-builder**.

## Routes

| Method & path | Purpose |
|---------------|---------|
| `POST /api/app-builder/init` | Validate a blueprint + write workspace metadata. |
| `POST /api/app-builder/scaffold` | Scaffold minimal app infra (from_scratch). |
| `POST /api/app-builder/build` | Build an app (scratch / database / codebase). |
| `POST /api/app-builder/auto-build` | Autonomous build that iterates to a quality bar. |
| `POST /api/app-builder/package` | Approve + package into a shippable bundle. |
| `POST /api/app-builder/delete` | Erase a build's workspace and artifacts. |
| `GET /api/app-builder/services` | List service templates. |
| `POST /api/app-builder/start-app` · `/stop-app` | Run lifecycle. |
| `GET /api/app-builder/app-status` | Whether a generated app is running. |
| `GET /api/app-builder/pii` | PII masking status. |
| `GET /api/app-builder/llm-models` | Trained models + engines. |
| `POST /api/app-builder/train-llm` · `/build-train-llm` | Train the local LLM. |
| `GET /api/app-builder/rag-status` · `POST /index-rag` | RAG index for a connection. |
| `POST /api/app-builder/mine-training-pairs` | Preview validated NL→SQL pairs. |
| `POST /api/app-builder/jobs` | Start a background agentic build job. |
| `GET /api/app-builder/jobs/{id}` · `/events` · `/events/poll` | Job status / event stream. |
| `POST /api/app-builder/jobs/{id}/stop` · `/take-control` · `/answer` · `/message` | Job control. |

## Examples

```bash
# Build from a database
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"name":"shopapp","mode":"from_database","connections":["prod"],
          "build_profile":"full","variant":"application"}' \
     http://localhost:8000/api/app-builder/build

# Autonomous build
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"name":"shopapp","mode":"from_database","connections":["prod"],
          "use_ai":true,"max_rounds":4,"target_score":0.9}' \
     http://localhost:8000/api/app-builder/auto-build

# Run + status
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/app-builder/start-app?name=shopapp&port=8000"
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/app-builder/app-status?name=shopapp"

# Background job + live events
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"name":"shopapp","mode":"from_scratch","description":"todo API"}' \
     http://localhost:8000/api/app-builder/jobs
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/app-builder/jobs/<job_id>/events/poll?cursor=0"
```

## See also

- [App Builder module](/modules/app-builder/) · [App Builder CLI](/cli/app-builder/)
