---
title: ai rag & ai llm
description: CLI for retrieval-augmented SQL (RAG) and the local trainable NL→SQL model (LLM), including the re-index and harvest schedulers.
sidebar:
  order: 7
---

These subcommand groups live under the `ai` command and require the **AI Query
Assistant** module. See the [RAG Manager guide](/guides/rag/) and
[Local LLM guide](/guides/local-llm/) for concepts.

## `ai rag` — retrieval-augmented SQL

```bash
python dbtool.py ai rag --help
```

| Command | Purpose |
|---------|---------|
| `index [--rebuild]` | Build/refresh the index (incremental by default). |
| `status` | Rows, embedder, staleness. |
| `overview` / `breakdown` | Status + breakdown + embedder check. |
| `search` / `context` / `preview` | Raw hits / prompt block / ranked preview. |
| `ask` | Generate RAG-augmented SQL. |
| `add-example` / `add-examples-file` | Add NL→SQL examples (single or bulk file). |
| `add-glossary` | Add a business glossary term. |
| `add-document` / `list-docs` / `remove-doc` | Manage uploaded documents. |
| `add-codebase` | Index a source folder (kind=code). |
| `analytics` / `seed-analytics` | Built-in analytical query library. |
| `search-multi` | Search across multiple scopes and merge/rerank. |
| `eval` | Retrieval evaluation harness (recall@k, MRR, precision). |
| `drift` | Detect schema drift since indexing. |
| `reindex-stale` | Refresh stale/drifted indexes. |
| `reindex-schedule {status,start,stop}` | Daily incremental re-index scheduler. |
| `clear` | Delete the index for a connection. |

```bash
python dbtool.py ai rag index --conn prod
python dbtool.py ai rag ask --conn prod "monthly active users by plan"
python dbtool.py ai rag add-examples-file --conn prod --file examples.jsonl
python dbtool.py ai rag eval --conn prod --per-case
python dbtool.py ai rag drift --conn prod
python dbtool.py ai rag reindex-stale --connections prod,stage --force
python dbtool.py ai rag reindex-schedule start
```

Tuning lives under `[ai.rag]` and `[ai.rag.reindex.schedule]` in
`ai_query/config.ini`. The full surface matrix (CLI ↔ API ↔ UI) is in the
[RAG Manager guide](/guides/rag/).

## `ai llm` — local trainable NL→SQL model

```bash
python dbtool.py ai llm --help
```

| Command | Purpose |
|---------|---------|
| `engines` / `list` | Available engines / trained models. |
| `train` | Train (or retrain) a model. |
| `status` | A model's status. |
| `generate` | Generate SQL with a model. |
| `eval` | Training-accuracy meters (execution accuracy, soft-F1, ESM). |
| `export` / `dataset` | Export the NL→SQL dataset to JSONL. |
| `verify` / `versions` / `restore` | Dataset check + model snapshot rollback. |
| `train-llm` / `train-multi` | Rich training (DB/codebase/scratch; one or many connections). |
| `mine-pairs` | Preview validated DB training pairs. |
| `rag-status` / `index-rag` | RAG training-index status / build. |
| `harvest` | Build a validated corpus and train. |
| `harvest-schedule {status,start,stop}` | Nightly harvest+train scheduler. |
| `enrich-templates` / `templates {status,clear}` | Template store management. |

```bash
python dbtool.py ai llm engines
python dbtool.py ai llm train --name default --conn prod
python dbtool.py ai llm generate --name default "top 5 products by sales"
python dbtool.py ai llm eval --name default
python dbtool.py ai llm harvest-schedule start
```

Tuning lives under `[ai.llm]`, `[ai.llm.eval]`, `[ai.llm.harvest]`,
`[ai.llm.capacity]`, `[ai.llm.versions]`, and `[ai.llm.harvest.schedule]`.

## See also

- [RAG Manager guide](/guides/rag/) · [Local LLM guide](/guides/local-llm/)
- [RAG & LLM REST API](/api/rag-llm/) · [AI CLI](/cli/ai/)
