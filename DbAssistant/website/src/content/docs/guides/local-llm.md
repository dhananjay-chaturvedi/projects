---
title: Local LLM training
description: Train an offline NL→SQL model on your schema and query history, evaluate it, harvest a corpus, and schedule nightly retraining.
sidebar:
  order: 3
---

The AI Query Assistant ships a **local, trainable NL→SQL model** you can use as
an offline backend (selected as `local-llm::<model>`). It needs no API keys and
the default engine is pure-Python.

## Engines

| Engine | Needs | Notes |
|--------|-------|-------|
| `python` | nothing | Zero-dependency, always available. |
| `numpy` | `numpy` | Vectorized MLP. |
| `pytorch` | `torch` | nano-GPT transformer; default when installed. |
| `ollama` | a running Ollama server | Uses a customized Ollama model. |

```bash
pip install -r ai_query/requirements-llm.txt    # numpy + torch + sqlglot
python dbtool.py ai llm engines                  # list engines + availability
```

Configure in `[ai.llm]` (`ai_query/config.ini`): `engine`, `engine_fallback`,
`active_model`, `template_mode` (`concrete`/`placeholder`/`both`),
`placeholder_resolution`, and the per-engine hyperparameters.

## Train, inspect, generate

```bash
python dbtool.py ai llm train --name default --conn prod
python dbtool.py ai llm status --name default
python dbtool.py ai llm list
python dbtool.py ai llm generate --name default "top 5 products by sales"
```

## Evaluate

```bash
python dbtool.py ai llm eval --name default      # execution accuracy, soft-F1, ESM
```

Configured in `[ai.llm.eval]` (`depth`, `dev_split`, `execution_accuracy`,
`soft_f1`, `esm`, `benchmark_path`).

## Rich training & corpus harvesting

```bash
# Train from a database, codebase, or scratch (one or many connections)
python dbtool.py ai llm train-llm --conn prod
python dbtool.py ai llm train-multi --connections prod,stage --name default

# Preview validated DB training pairs without training
python dbtool.py ai llm mine-pairs --conn prod

# Harvest a large validated corpus (curated + AI question bank + captures) then train
python dbtool.py ai llm harvest --conn prod
```

Harvest behavior lives in `[ai.llm.harvest]` (offline vs online `training_depth`,
question counts, workers, timeouts) and capacity scaling in `[ai.llm.capacity]`.
Model snapshots are kept per `[ai.llm.versions] keep` so a failed run can roll
back (`ai llm versions` / `ai llm restore`).

## Nightly harvest scheduler

A background scheduler runs harvest+train once per day inside a window, stopping
gracefully at safe checkpoints (a model write is never interrupted).

```bash
python dbtool.py ai llm harvest-schedule status
python dbtool.py ai llm harvest-schedule start
python dbtool.py ai llm harvest-schedule stop
```

```ini
[ai.llm.harvest.schedule]
enabled = false
start_time = 01:00
duration_hours = 4
connections =
train_mode = incremental
training_depth = offline      ; offline = no backend AI; online = also AI
```

## Auto-fix training

With `[ai.llm] auto_fix_train = true`, when the local LLM produces SQL that fails
(or is flagged) against the **connected** database, the fallback backend's
verified correction becomes a new training pair — the model improves from real
mistakes. The UI's *Auto-train on corrected queries* checkbox overrides this at
runtime.

## See also

- [RAG & LLM CLI](/cli/ai-rag-llm/) · [RAG & LLM API](/api/rag-llm/)
- [AI Query Assistant module](/modules/ai-query/)
