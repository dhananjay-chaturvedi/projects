---
title: RAG Manager (retrieval)
description: Index a database schema, glossary, examples, documents, and codebase into a local vector store and inject only the relevant context into NL→SQL prompts.
sidebar:
  order: 2
---

The **RAG Manager** is the retrieval layer behind the AI Query Assistant's
**Use RAG** toggle. It indexes a connection's schema (plus glossary, NL→SQL
examples, uploaded documents, and source code) into a local, offline vector
store, then injects **only the relevant objects** into the prompt. This directly
attacks the two biggest NL→SQL failure modes — hallucinated columns and unknown
joins.

Everything here is available identically across the Desktop UI, Terminal UI, Web
UI, CLI (`dbtool ai rag …`), and REST API (`/api/ai/rag/…`).

## Quick start

```bash
python dbtool.py ai rag index --conn prod
python dbtool.py ai rag overview --conn prod
python dbtool.py ai rag ask --conn prod "top customers by lifetime value"
```

…or flip **Use RAG** in any UI and ask normally.

## Semantic schema cards

Each table becomes a rich document:

- **Columns** with types/nullability and **comments/descriptions**
  (`column_comments`).
- **Relationships** — foreign keys are first-class join-graph docs (big win on
  multi-table questions).
- **Value profiles / enums** — low-cardinality columns (`≤ enum_max_distinct`)
  are indexed as enum docs; sampled values can be PII-masked (`mask_samples`,
  honoring global `[ai] mask_pii`).
- **Table purpose** — optional one-line AI summary per table (`table_purpose`).

## Retrieval pipeline

1. **Query understanding** — glossary-driven **query expansion** and **entity
   linking** (map question nouns → real table/column names).
2. **Hybrid search** — vector (cosine) + body-aware lexical, fused with
   **Reciprocal Rank Fusion** (`use_rrf`, `rrf_k`) or a linear blend
   (`lexical_alpha`).
3. **Reranking** (optional) — re-score the top `rerank_top_n` (heuristic always;
   cross-encoder `rerank_model` if `sentence-transformers` is installed).

Inspect each stage:

```bash
python dbtool.py ai rag search  --conn prod "orders that shipped late"
python dbtool.py ai rag context --conn prod "orders that shipped late"
python dbtool.py ai rag preview --conn prod "orders that shipped late"
```

## Teach the index

```bash
python dbtool.py ai rag add-example --conn prod --question "MAU" --sql "SELECT ..."
python dbtool.py ai rag add-examples-file --conn prod --file examples.jsonl
python dbtool.py ai rag add-glossary --conn prod --term "MAU" --definition "..."
python dbtool.py ai rag add-document --scope kb --file runbook.md --title Runbook
python dbtool.py ai rag add-codebase --scope code --path ./src
```

Documents are chunked structure-aware (headings/sentences, tables kept intact).
Bulk example import accepts JSONL, CSV, and free-form "Question … / SQL …" blocks.

## Multi-scope search

```bash
python dbtool.py ai rag search-multi --scopes prod,kb,code "how is churn computed"
```

## Evaluation harness

Tune retrieval with data, not guesswork — recall@k, MRR, context precision:

```bash
python dbtool.py ai rag eval --conn prod                 # seed gold from examples
python dbtool.py ai rag eval --conn prod --gold gold.jsonl --per-case
```

## Freshness & the re-index scheduler

```bash
python dbtool.py ai rag drift --conn prod
python dbtool.py ai rag reindex-stale --connections prod,stage --force
python dbtool.py ai rag reindex-schedule start     # daily incremental refresh
python dbtool.py ai rag reindex-schedule status
python dbtool.py ai rag reindex-schedule stop
```

```ini
[ai.rag.reindex.schedule]
enabled = false
start_time = 02:00
duration_hours = 1
connections =          ; blank = every indexed connection
force = false
```

The scheduler (`RagReindexScheduler`) runs `reindex-stale` once per day inside
the window. Because re-indexing is incremental, a run is cheap and never disrupts
a live index. In the UIs, the **RAG Manager** dropdown exposes *Scheduled
re-index: status / start / stop*.

## Scale & observability

- **ANN (FAISS).** Set `ann = true` (+ install `faiss`) for very large indexes;
  graceful fallback to brute-force when absent.
- **Retrieval logging.** `log_retrievals = true` appends per-ask hits + scores to
  `~/.dbassistant/session/ai/rag/retrievals.jsonl`.
- **PII governance.** `mask_samples` + `[ai] mask_pii` mask sampled values before
  they reach the index, prompt, or logs.

## Configuration

All keys live under `[ai.rag]` in `ai_query/config.ini`. Highlights:
`embedding_provider`, `use_rrf`, `query_expansion`, `entity_linking`,
`column_comments`, `enum_max_distinct`, `mask_samples`, `table_purpose`,
`rerank`/`rerank_top_n`/`rerank_model`, `ann`, `stale_after_days`,
`log_retrievals`, `top_k`. See the [RAG & LLM CLI](/cli/ai-rag-llm/) and
[RAG & LLM API](/api/rag-llm/) for the full surface matrix.
