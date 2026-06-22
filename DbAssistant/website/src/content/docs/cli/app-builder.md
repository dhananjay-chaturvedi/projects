---
title: app-builder
description: CLI to generate apps from scratch, a codebase, or a database, run agentic build jobs, and manage the generated app lifecycle.
sidebar:
  order: 8
---

The `app-builder` command group requires the **App Builder** module
(`ai_assistant/app_builder/`). It is available as `dbtool app-builder …` or
standalone via `python -m ai_assistant.app_builder app-builder …`.

```bash
python dbtool.py app-builder --help
```

## Subcommands

| Command | Purpose |
|---------|---------|
| `init` | Validate a blueprint and write workspace metadata. |
| `scaffold` | Scaffold minimal app infra (from_scratch). |
| `build` | Build an app (scratch / database / codebase). |
| `auto-build` | Autonomous build that iterates until it passes a quality bar. |
| `package` | Approve + package a built app into a shippable bundle. |
| `delete` | Erase a build's workspace and all artifacts. |
| `start-app` / `stop-app` / `app-status` | Run lifecycle for a generated app. |
| `llm-models` | List trained LLM models and engines. |
| `train-llm` | Train LLM(s) without building an app. |
| `build-train-llm` | Train LLM(s) from an existing build's own data. |
| `mine-pairs` | Preview validated NL→SQL pairs mined from a DB. |
| `rag-status` / `index-rag` | RAG index status / build for a connection. |
| `jobs …` | Background agentic build jobs (start/status/events/message/take-control/answer/stop). |

## build

```bash
python dbtool.py app-builder build --name shopapp --mode from_database \
    --connections prod \
    --build-profile full \           # prototype | full
    --variant application \          # application | explorer
    --use-ai --backend claude \      # let an AI backend generate files
    --mask-pii
```

Key flags:

| Flag | Default | Meaning |
|------|---------|---------|
| `--mode` | `from_scratch` | `from_scratch` / `from_codebase` / `from_database` |
| `--description` | _(empty)_ | NL description (from_scratch) |
| `--connections` | _(empty)_ | Comma-separated DB connections |
| `--codebase-path` | _(empty)_ | Source folder (from_codebase) |
| `--build-profile` | `prototype` | `prototype` / `full` |
| `--variant` | `application` | `application` / `explorer` |
| `--db-app-variant` | `application` | `application` / `insights_admin` |
| `--codebase-variant` | `predicted_app` | `predicted_app` / `structure_metadata` |
| `--use-ai` / `--backend` | off | Generate files with an AI backend |
| `--train-llm` / `--train-new-name` / `--rich-train` / `--train-engine` | — | Train a local model after the build |
| `--use-rag` / `--index-rag` / `--rag-strategy` | off | RAG-augment the post-build training |

## auto-build

Same flags as `build`, plus:

```bash
python dbtool.py app-builder auto-build --name shopapp --mode from_database \
    --connections prod --use-ai --max-rounds 4 --target-score 0.9
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--max-rounds` | `[ai.app_builder] max_rounds` | Iteration cap |
| `--target-score` | `[ai.app_builder] target_score` | Quality bar to stop at |

## Run lifecycle

```bash
python dbtool.py app-builder start-app  --name shopapp --port 8000
python dbtool.py app-builder app-status --name shopapp
python dbtool.py app-builder stop-app   --name shopapp
python dbtool.py app-builder package    --name shopapp --port 8000   # add --no-archive to skip the zip
python dbtool.py app-builder delete      --name shopapp
```

## Background jobs

```bash
python dbtool.py app-builder jobs start  --body-file build.json
python dbtool.py app-builder jobs status --id <job_id>
python dbtool.py app-builder jobs events --id <job_id> --cursor 0
python dbtool.py app-builder jobs message --id <job_id> --text "use FastAPI"
python dbtool.py app-builder jobs take-control --id <job_id>
python dbtool.py app-builder jobs answer --id <job_id> --value "yes"
python dbtool.py app-builder jobs stop --id <job_id>
```

## See also

- [App Builder module](/modules/app-builder/) · [App Builder REST API](/api/app-builder/)
