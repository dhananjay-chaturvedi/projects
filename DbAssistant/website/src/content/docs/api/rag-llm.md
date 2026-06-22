---
title: RAG & LLM
description: REST routes for retrieval-augmented SQL (RAG) and the local trainable NL→SQL model (LLM), under /api/ai/rag and /api/ai/llm.
sidebar:
  order: 7
---

All routes require the `X-API-Key` header (see [Authentication](/api/authentication/))
and the **AI Query Assistant** module. Tags: **RAG**, **LLM**.

## RAG — `/api/ai/rag/*`

| Method & path | Purpose |
|---------------|---------|
| `POST /api/ai/rag/index` | Build/refresh the index (`{connection, rebuild}`). |
| `GET /api/ai/rag/status` | Index status (`?connection=`). |
| `GET /api/ai/rag/overview` · `GET /api/ai/rag/breakdown` | Overview / per-kind breakdown. |
| `POST /api/ai/rag/search` · `/context` · `/preview` | Raw hits / prompt block / ranked preview. |
| `POST /api/ai/rag/ask` | Generate RAG-augmented SQL. |
| `POST /api/ai/rag/example` · `/examples-file` | Add NL→SQL examples (single / bulk file). |
| `POST /api/ai/rag/glossary` | Add a glossary term. |
| `POST /api/ai/rag/document` · `GET /documents` · `POST /remove-document` | Manage documents. |
| `POST /api/ai/rag/add-codebase` | Index a source folder. |
| `GET /api/ai/rag/analytics` · `POST /seed-analytics` | Analytical query library. |
| `POST /api/ai/rag/search-multi` | Multi-scope search. |
| `POST /api/ai/rag/eval` | Retrieval evaluation harness. |
| `GET /api/ai/rag/drift` | Schema-drift check. |
| `POST /api/ai/rag/reindex-stale` | Refresh stale/drifted indexes. |
| `GET /api/ai/rag/reindex/schedule` | Scheduler status. |
| `POST /api/ai/rag/reindex/schedule/start` · `/stop` | Start/stop the daily re-index scheduler. |
| `DELETE /api/ai/rag` | Delete an index (`?connection=`). |

```bash
# Index, then ask
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"connection":"prod","rebuild":false}' \
     http://localhost:8000/api/ai/rag/index

curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"connection":"prod","question":"monthly active users"}' \
     http://localhost:8000/api/ai/rag/ask

# Evaluate retrieval and check drift
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"connection":"prod","per_case":true}' \
     http://localhost:8000/api/ai/rag/eval
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/ai/rag/drift?connection=prod"

# Re-index scheduler
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/ai/rag/reindex/schedule
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/ai/rag/reindex/schedule/start
```

## LLM — `/api/ai/llm/*`

| Method & path | Purpose |
|---------------|---------|
| `GET /api/ai/llm/engines` · `/models` | Engines / trained models. |
| `POST /api/ai/llm/train` | Train (or retrain) a model. |
| `GET /api/ai/llm/status` · `/model-dataset` · `/versions` | Status / dataset / snapshots. |
| `POST /api/ai/llm/restore` | Roll back to a snapshot. |
| `POST /api/ai/llm/generate` | Generate SQL with a model. |
| `POST /api/ai/llm/eval` | Accuracy meters. |
| `POST /api/ai/llm/export` · `/dataset` | Export dataset (JSONL). |
| `POST /api/ai/llm/train-llm` · `/train-multi` · `/train-pairs` | Rich / multi / pair training. |
| `POST /api/ai/llm/mine-training-pairs` | Preview mined pairs. |
| `GET /api/ai/llm/rag-status` · `POST /index-rag` | Training RAG index. |
| `POST /api/ai/llm/harvest` · `/harvest/stop` | Corpus harvest. |
| `GET /api/ai/llm/harvest/schedule` · `POST …/start` · `…/stop` | Nightly harvest scheduler. |
| `POST /api/ai/llm/enrich-templates` · `GET/DELETE /templates` | Template store. |
| `POST /api/ai/llm/jobs` · `GET /jobs/{id}` · `/events` · `/events/poll` · `POST /jobs/{id}/stop` | Background training jobs. |

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/ai/llm/engines

curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"name":"default","connection":"prod"}' \
     http://localhost:8000/api/ai/llm/train

curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"name":"default","question":"top 5 products by sales"}' \
     http://localhost:8000/api/ai/llm/generate
```

## See also

- [RAG Manager guide](/guides/rag/) · [Local LLM guide](/guides/local-llm/)
- [RAG & LLM CLI](/cli/ai-rag-llm/) · [AI REST API](/api/ai/)
