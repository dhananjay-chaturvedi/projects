# RAG Manager — Retrieval-Augmented SQL

The **RAG Manager** is the retrieval layer behind the AI Query Assistant's
**Use RAG** toggle. It indexes a connection's schema (plus your glossary,
NL→SQL examples, uploaded documents, and even source code) into a local vector
store, then injects **only the relevant objects** into the prompt. This attacks
the two biggest NL→SQL failure modes — "the model used a column that doesn't
exist" and "the model didn't know how two tables join."

Everything described here is available identically across the **Desktop UI,
Terminal UI, Web UI, CLI (`dbtool ai rag …`), and REST API
(`/api/ai/rag/…`)**.

- [Concepts](#concepts)
- [Quick start](#quick-start)
- [Indexing](#indexing)
- [What gets indexed (semantic schema cards)](#what-gets-indexed-semantic-schema-cards)
- [Retrieval: hybrid search, expansion, reranking](#retrieval-hybrid-search-expansion-reranking)
- [Examples, glossary, documents, codebase](#examples-glossary-documents-codebase)
- [Multi-scope search](#multi-scope-search)
- [Evaluation harness](#evaluation-harness)
- [Freshness & the re-index scheduler](#freshness--the-re-index-scheduler)
- [Scale (ANN) & observability](#scale-ann--observability)
- [Configuration reference](#configuration-reference)
- [Full surface matrix](#full-surface-matrix)

---

## Concepts

| Term | Meaning |
|------|---------|
| **Scope** | A named index. A database connection name is a scope; so are standalone collections like `kb` or `code`. |
| **Embedder** | Turns text into vectors. `hash` (default, zero-dependency) or `sentence-transformers` (higher quality, optional). |
| **Vector store** | Local SQLite-backed store at `~/.dbassistant/session/ai/rag/`. FAISS can back it for scale. |
| **Hybrid retrieval** | Combines lexical (keyword/BM25-style) and vector similarity, fused with RRF or a linear blend. |
| **Reranking** | A second pass that re-scores the top-N candidates (heuristic always; cross-encoder if installed). |
| **Schema card** | A rich per-table document: columns, comments, relationships (FKs), enums/value profiles, and an optional AI "purpose" line. |

---

## Quick start

```bash
# 1) Build the index for a connection
python dbtool.py ai rag index --conn prod

# 2) Check what's in it
python dbtool.py ai rag overview --conn prod

# 3) Ask with retrieval-augmented context
python dbtool.py ai rag ask --conn prod "top customers by lifetime value"

# …or just flip "Use RAG" in any UI and ask normally.
```

---

## Indexing

```bash
python dbtool.py ai rag index --conn prod              # build or refresh (incremental)
python dbtool.py ai rag index --conn prod --rebuild    # full rebuild
python dbtool.py ai rag status --conn prod             # rows, embedder, staleness
python dbtool.py ai rag clear --conn prod              # delete the index
```

Indexing is **incremental** by default: a stable hash of each schema object is
stored, and a refresh re-embeds only the objects that changed. A full `--rebuild`
re-embeds everything.

**REST:**

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"connection":"prod","rebuild":false}' \
     http://localhost:8000/api/ai/rag/index

curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/ai/rag/status?connection=prod"
```

---

## What gets indexed (semantic schema cards)

Each table becomes a **semantic schema card** containing:

- **Columns** with types and nullability.
- **Column comments / descriptions** when the engine exposes them
  (`column_comments = true`).
- **Relationships** — foreign keys are first-class join-graph docs, which
  dramatically improves retrieval on multi-table questions.
- **Value profiles / enums** — columns with `≤ enum_max_distinct` distinct values
  are indexed as enum docs (so the model knows `status ∈ {active, churned}`).
  Sampled values can be PII-masked (`mask_samples = true`, also honoring the
  global `[ai] mask_pii`).
- **Table purpose** — an optional one-line, AI-generated summary per table
  (`table_purpose = true`; requires an AI backend during indexing).

Views and indexes are indexed too. Tuning lives in `[ai.rag]` (see
[Configuration reference](#configuration-reference)).

---

## Retrieval: hybrid search, expansion, reranking

The retrieval pipeline, in order:

1. **Query understanding**
   - **Query expansion** (`query_expansion = true`): glossary-term definitions
     are folded into lexical matching.
   - **Entity linking** (`entity_linking = true`): nouns in the question are
     mapped to real table/column names before retrieval.
2. **Hybrid search**
   - **Vector** similarity (cosine) + **lexical** (body-aware) scoring.
   - Fused with **Reciprocal Rank Fusion** (`use_rrf = true`, `rrf_k`) or a
     linear blend (`lexical_alpha` when `use_rrf = false`).
3. **Reranking** (optional quality tier, `rerank = true`)
   - The top `rerank_top_n` candidates are re-scored. A dependency-free
     **heuristic** reranker always runs; if `sentence-transformers` is installed,
     a **cross-encoder** (`rerank_model`) is used instead.

Inspect each stage:

```bash
python dbtool.py ai rag search  --conn prod "orders that shipped late"   # raw hits + scores
python dbtool.py ai rag context --conn prod "orders that shipped late"   # prompt-ready block
python dbtool.py ai rag preview --conn prod "orders that shipped late"   # ranked preview + context
```

---

## Examples, glossary, documents, codebase

Teach the index domain knowledge:

```bash
# NL->SQL examples (few-shot retrieval)
python dbtool.py ai rag add-example --conn prod \
    --question "monthly active users" \
    --sql "SELECT date_trunc('month', ts) m, count(distinct user_id) FROM events GROUP BY 1"

# Bulk import examples from a file (auto-detects format: jsonl, csv, md, q/sql blocks)
python dbtool.py ai rag add-examples-file --conn prod --file examples.jsonl

# Business glossary (drives query expansion)
python dbtool.py ai rag add-glossary --conn prod --term "MAU" \
    --definition "monthly active users = distinct user_id per calendar month"

# Reference documents (txt/md/sql/csv/json/rst; pdf via pypdf, docx via python-docx)
python dbtool.py ai rag add-document --scope kb --file runbook.md --title "Runbook"
python dbtool.py ai rag list-docs --scope kb
python dbtool.py ai rag remove-doc --scope kb --doc-id <id>

# Index a source-code folder (kind=code; respects ignore rules, code-aware chunking)
python dbtool.py ai rag add-codebase --scope code --path ./src

# Built-in analytical query library
python dbtool.py ai rag analytics
python dbtool.py ai rag seed-analytics --conn prod
```

Documents are chunked **structure-aware** (split on headings/sentences, markdown
tables kept intact) for better recall. Bulk example import accepts JSONL, CSV,
and free-form "Question … / SQL …" blocks.

---

## Multi-scope search

Search several scopes at once (e.g. a database schema + a docs collection + a
codebase) and merge/re-rank the results globally:

```bash
python dbtool.py ai rag search-multi --scopes prod,kb,code "how is churn computed"
```

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"scopes":["prod","kb","code"],"query":"how is churn computed","k":8}' \
     http://localhost:8000/api/ai/rag/search-multi
```

---

## Evaluation harness

Tune retrieval with data, not guesswork. The harness scores retrieval against a
gold set of (question → expected tables) and reports **recall@k**, **MRR**, and
**context precision**. Gold cases are seeded from your NL→SQL examples or
provided explicitly.

```bash
python dbtool.py ai rag eval --conn prod                 # seed gold from examples
python dbtool.py ai rag eval --conn prod --gold gold.jsonl --per-case
```

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"connection":"prod","per_case":true}' \
     http://localhost:8000/api/ai/rag/eval
```

Use the metrics to tune `lexical_alpha`, `top_k`, `use_rrf`, `embedding_provider`,
and `rerank`.

---

## Freshness & the re-index scheduler

The index can drift when the schema changes. The tool detects this and can
refresh automatically.

**On demand:**

```bash
python dbtool.py ai rag drift --conn prod            # has the schema changed since indexing?
python dbtool.py ai rag reindex-stale                # refresh every stale/drifted index
python dbtool.py ai rag reindex-stale --connections prod,stage --force
```

`status` reports staleness once an index is older than `stale_after_days`.

**Scheduled (daily, incremental):** a dedicated `RagReindexScheduler` (modeled on
the LLM harvest scheduler) runs `reindex-stale` once per day inside a configurable
window. Because re-indexing is incremental, a scheduled run is cheap and never
disrupts a live index.

```bash
python dbtool.py ai rag reindex-schedule status
python dbtool.py ai rag reindex-schedule start
python dbtool.py ai rag reindex-schedule stop
```

```bash
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/ai/rag/reindex/schedule
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/ai/rag/reindex/schedule/start
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/ai/rag/reindex/schedule/stop
```

Configure the window in `[ai.rag.reindex.schedule]`:

```ini
[ai.rag.reindex.schedule]
enabled = false
start_time = 02:00
duration_hours = 1
window_end =
connections =          ; blank = every indexed connection
force = false          ; re-index regardless of staleness/drift
```

In the UIs, the **RAG Manager** dropdown exposes *Scheduled re-index: status /
start / stop* (the Desktop UI also has dedicated buttons).

---

## Scale (ANN) & observability

- **ANN (FAISS).** The default SQLite brute-force scan is fine for typical
  schemas. For very large indexes, set `ann = true` and install `faiss`; vector
  scoring then runs through a FAISS approximate-nearest-neighbour index behind the
  same store interface (graceful fallback if faiss is absent).
- **Retrieval logging.** Set `log_retrievals = true` to append per-ask hits and
  scores to `~/.dbassistant/session/ai/rag/retrievals.jsonl` for auditing and
  tuning.
- **PII governance.** `mask_samples = true` (and the global `[ai] mask_pii`) mask
  sampled/enum values before they enter the index, prompt, or logs.

---

## Configuration reference

All keys live under `[ai.rag]` in `ai_query/config.ini` (see the full annotated
example in `ai_query/config.ini.example`).

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `true` | Master toggle for retrieval. |
| `embedding_provider` | `hash` | `hash` (zero-dep) or `sentence-transformers`. |
| `embedding_model` | `all-MiniLM-L6-v2` | Model for the ST provider. |
| `embedding_dim` | `256` | Vector dimension (changing it forces a re-index). |
| `max_tables` | `500` | Cap on tables indexed. |
| `sample_values` / `sample_limit` | `true` / `5` | Index sampled values. |
| `lexical_alpha` | `0.3` | Lexical vs vector blend (when `use_rrf=false`). |
| `use_rrf` / `rrf_k` | `true` / `60` | Reciprocal-rank fusion. |
| `query_expansion` | `true` | Fold glossary definitions into lexical match. |
| `entity_linking` | `true` | Map question nouns to schema names. |
| `column_comments` | `true` | Index column comments/descriptions. |
| `enum_max_distinct` | `12` | Distinct-count threshold for enum docs. |
| `mask_samples` | `false` | PII-mask sampled/enum values. |
| `table_purpose` | `false` | AI-generated one-line table purpose. |
| `rerank` / `rerank_top_n` / `rerank_model` | `false` / `20` / `…MiniLM…` | Cross-encoder/heuristic reranking. |
| `ann` | `false` | Use FAISS ANN when installed. |
| `stale_after_days` | `7` | Staleness threshold reported in status. |
| `log_retrievals` | `false` | Append retrieval hits/scores to JSONL. |
| `top_k` | `8` | Hits injected into the prompt. |
| `gate_by_complexity` | `true` | Skip/reduce RAG for simple questions. |
| `chunk_size` / `chunk_overlap` / `max_doc_chars` | `1000` / `150` / `2000000` | Document ingestion. |
| `codebase_max_files` / `codebase_max_file_bytes` | `500` / `512000` | Codebase ingestion caps. |

---

## Full surface matrix

| Capability | CLI (`dbtool ai rag …`) | REST (`/api/ai/rag/…`) | UI (RAG Manager) |
|------------|-------------------------|------------------------|------------------|
| Index / rebuild | `index [--rebuild]` | `POST /index` | Index / Rebuild |
| Status | `status` | `GET /status` | Status |
| Overview / breakdown | `overview`, `breakdown` | `GET /overview`, `GET /breakdown` | Overview |
| Search / context / preview | `search`, `context`, `preview` | `POST /search`, `/context`, `/preview` | Search / Preview |
| Ask (RAG SQL) | `ask` | `POST /ask` | Use RAG + Ask |
| Examples | `add-example`, `add-examples-file` | `POST /example`, `/examples-file` | Add example / file |
| Glossary | `add-glossary` | `POST /glossary` | Add glossary |
| Documents | `add-document`, `list-docs`, `remove-doc` | `POST /document`, `GET /documents`, `POST /remove-document` | Add / list / remove docs |
| Codebase | `add-codebase` | `POST /add-codebase` | Add codebase |
| Analytics | `analytics`, `seed-analytics` | `GET /analytics`, `POST /seed-analytics` | Analytics |
| Multi-scope | `search-multi` | `POST /search-multi` | Multi-scope search |
| Eval | `eval` | `POST /eval` | Evaluate |
| Drift / stale | `drift`, `reindex-stale` | `GET /drift`, `POST /reindex-stale` | Check drift / Re-index if stale |
| Scheduler | `reindex-schedule {status,start,stop}` | `GET /reindex/schedule`, `POST …/start`, `…/stop` | Scheduled re-index: status/start/stop |
| Clear | `clear` | `DELETE /api/ai/rag` | Clear index |

See also: [AI Query Assistant guide](../HOW_TO_USE.md), the annotated
`ai_query/config.ini.example`, and the website's
[AI Query module page](../website/src/content/docs/modules/ai-query.md).
