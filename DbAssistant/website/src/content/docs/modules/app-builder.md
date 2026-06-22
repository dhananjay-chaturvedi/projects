---
title: App Builder
description: Generate runnable apps from scratch, an existing codebase, or a live database — with governed, agentic build jobs and a package/run lifecycle.
sidebar:
  order: 4
---

The **App Builder** generates runnable applications from one of three sources —
**scratch**, an existing **codebase**, or a live **database** — using a governed,
agentic build loop. It can train the local LLM from a build's own data, package
the result into a shippable bundle, and start/stop the generated app.

It surfaces in every interface: the **AI Query Assistant** tab launches the App
Builder in the Desktop UI, TUI, and Web UI; the CLI is `dbtool app-builder`; and
the REST routes live under `/api/app-builder/*`.

:::note[Bounded by design]
Profiling and codebase scanning are bounded (sample rows, table caps, timeouts,
file/depth caps) so a build never runs heavy queries against production. Limits
live in `[ai.app_builder]` in `ai_query/config.ini`.
:::

## Build modes

| Mode | Source | Typical use |
|------|--------|-------------|
| `from_scratch` | A natural-language description | Prototype a new app/service |
| `from_database` | A live connection's schema + bounded profiling | Build an app or insights view over existing data |
| `from_codebase` | An existing source folder | Extend/mirror an existing app |

Two axes shape the output:

- **`--build-profile`**: `prototype` (fast, demonstrative) or `full`
  (production-functional, more build rounds).
- **`--variant`**: `application` (a real user-facing app) or `explorer`
  (a metadata/insights view).

## Lifecycle at a glance

```bash
# 1) One-shot build
python dbtool.py app-builder build --name shopapp --mode from_database \
    --connections prod --build-profile full --variant application

# 2) Or an autonomous build that iterates until it passes a quality bar
python dbtool.py app-builder auto-build --name shopapp --mode from_database \
    --connections prod --use-ai --max-rounds 4 --target-score 0.9

# 3) Inspect / run the generated app
python dbtool.py app-builder app-status --name shopapp
python dbtool.py app-builder start-app  --name shopapp --port 8000
python dbtool.py app-builder stop-app   --name shopapp

# 4) Package a shippable bundle (install/run scripts + archive)
python dbtool.py app-builder package --name shopapp --port 8000

# 5) Remove everything for a build
python dbtool.py app-builder delete --name shopapp
```

## Agentic build jobs

Long builds run as **background jobs** with a live event stream. You can take
control, send messages to the build session, and answer the agent's questions —
the same governance the Desktop/TUI/Web UIs expose.

```bash
python dbtool.py app-builder jobs start  --body-file build.json
python dbtool.py app-builder jobs status --id <job_id>
python dbtool.py app-builder jobs events --id <job_id> --cursor 0
python dbtool.py app-builder jobs message --id <job_id> --text "use FastAPI, not Flask"
python dbtool.py app-builder jobs take-control --id <job_id>
python dbtool.py app-builder jobs answer --id <job_id> --value "yes"
python dbtool.py app-builder jobs stop --id <job_id>
```

Build governance is configured in `[ai.app_builder]`: `max_rounds` /
`full_max_rounds`, `target_score`, `target_coverage`, `max_wall_clock_seconds`,
`max_no_progress_rounds`, `agent_timeout`, plus retained-job TTL
(`job_ttl_seconds`) and `max_jobs`.

## Train the LLM from a build

The App Builder can train the local NL→SQL model from a build's **own**
generated schema/queries and DB insight (execution-validated), or mine a
connection directly:

```bash
python dbtool.py app-builder train-llm --connection prod --new-name shop
python dbtool.py app-builder build-train-llm --name shopapp --connection prod
python dbtool.py app-builder mine-pairs --connection prod --max-pairs 400
python dbtool.py app-builder rag-status --connection prod
python dbtool.py app-builder index-rag  --connection prod --rebuild
```

## REST API

```bash
curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" -H "Content-Type: application/json" \
     -d '{"name":"shopapp","mode":"from_database","connections":["prod"],
          "build_profile":"full","variant":"application"}' \
     http://localhost:8000/api/app-builder/build

curl -X POST -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/app-builder/start-app?name=shopapp&port=8000"

curl -H "X-API-Key: $DBTOOL_API_KEY" \
     "http://localhost:8000/api/app-builder/app-status?name=shopapp"
```

See the full route list in the [App Builder REST API](/api/app-builder/) and the
command reference in the [app-builder CLI](/cli/app-builder/).

## See also

- [App Builder CLI](/cli/app-builder/) · [App Builder REST API](/api/app-builder/)
- [AI Query Assistant](/modules/ai-query/) · [Local LLM training](/guides/local-llm/)
