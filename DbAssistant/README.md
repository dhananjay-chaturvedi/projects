# DbAssistant — Database Management Tool

A production-grade, multi-database management platform that exposes the **same
functionality across five surfaces**: a Tkinter **Desktop UI**, a Textual
**Terminal UI (TUI)**, a **Web UI**, a **CLI**, and a **REST API**. Built for
database engineers, application/full-stack developers, and the executives who
need a single, auditable view across fleets of databases.

[![CI](https://github.com/dhananjay-chaturvedi/dbassistant/actions/workflows/ci.yml/badge.svg)](https://github.com/dhananjay-chaturvedi/dbassistant/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-2196F3)](https://dhananjay-chaturvedi.github.io/dbassistant/)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Platforms](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey.svg)]()
[![Surfaces](https://img.shields.io/badge/surfaces-Desktop%20%7C%20TUI%20%7C%20Web%20%7C%20CLI%20%7C%20API-2196F3.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **UI / CLI / API parity is a hard guarantee.** Every feature is implemented
> once in a shared service layer and wired into all surfaces, so the Desktop UI,
> TUI, Web UI, CLI, and REST API never drift apart.

---

## Table of contents

- [Why DbAssistant](#why-dbassistant)
- [The five surfaces](#the-five-surfaces)
- [Modules](#modules)
- [Supported databases & clouds](#supported-databases--clouds)
- [Install](#install)
- [Quickstart](#quickstart)
- [Launch every surface](#launch-every-surface)
- [Configuration](#configuration)
- [Security model](#security-model)
- [Where data lives](#where-data-lives)
- [Documentation](#documentation)
- [Development & testing](#development--testing)

---

## Why DbAssistant

| For… | What you get |
|------|--------------|
| **Database engineers** | Cross-engine schema conversion + data migration with validation, checkpoint/resume, and JSON reports; live + cloud monitoring with thresholds, alerts, and a daemon. |
| **Application / full-stack developers** | Natural-language → SQL with retrieval-augmented context (RAG), a local trainable NL→SQL model, an SQL editor, an object browser, and an App Builder that scaffolds apps from a schema, a codebase, or scratch. |
| **Executives & SREs** | One auditable tool with read-only guardrails, encrypted-at-rest credentials, a REST API for dashboards, and headless automation for CI. |

Design principles:

- **Offline-first.** AI backends are CLI-based (Claude / Cursor / Codex) — no API
  keys to manage. The local LLM and the default RAG embedder are pure-Python and
  need no network.
- **Read-only by default.** The AI assistant can never run live mutations
  (`DROP`/`DELETE`/`UPDATE`/`INSERT`/`ALTER`/…) — they are rejected in the shared
  execution guard before reaching the database.
- **Modular.** Ship the full tool or any single module independently.

---

## The five surfaces

| Surface | Best for | Entry point |
|---------|----------|-------------|
| **Desktop UI** (Tkinter) | Interactive day-to-day work | `python dbtool.py ui` · `python conDbUi.py` |
| **Terminal UI** (Textual) | Remote/SSH sessions, keyboard-driven work | `python dbtool.py tui` |
| **Web UI** (FastAPI + SPA) | Shared/team access from a browser | `python dbtool.py webui` → `/ui/` |
| **CLI** | Scripting, CI, automation | `python dbtool.py <command>` |
| **REST API** | Integrations, dashboards, custom front-ends | `python dbtool.py api` → `/docs` |

All five dispatch into the same shared services (`common/headless/db_service.py`,
each module's `service.py`/`bridge.py`), guaranteeing feature parity.

---

## Modules

The tool ships as a **shared core** (connections, SQL editor, object browser,
engine registry, dashboard) plus **four optional modules**, each declaring its
own CLI commands, REST router, and UI tab via a `manifest.py`.

| Module | Folder | What it does |
|--------|--------|--------------|
| **Data Migration** | `schema_converter/` | Schema conversion + DDL dump/apply, data transfer (parallel, filtered, column-mapped), schema/data compare, dry-run validation, checkpoint/resume, JSON reports. |
| **AI Query Assistant** | `ai_query/` | NL→SQL via Claude/Cursor/Codex or a local trainable model; sessions, follow-ups, cross-tab; explain/optimize/review/correct; **RAG Manager** (schema + docs + codebase retrieval); local **LLM** train/eval/harvest with schedulers. |
| **Monitoring** | `monitoring/` | DB + OS + cloud (AWS/Azure/GCP) metrics, monitor-only connections, SSH targets, thresholds, alerts log, notifications, background daemon. |
| **App Builder** | `ai_assistant/app_builder/` | Generate apps from scratch, a codebase, or a database with governed, agentic build jobs; package/run/manage generated apps. |

See [`MODULES.md`](MODULES.md) for independent packaging and shipping.

---

## Supported databases & clouds

| Engine | Driver | Notes |
|--------|--------|-------|
| Oracle | `oracledb` | Thin mode (12.1+) or Instant Client for thick/11g |
| MySQL / MariaDB | `mysql-connector-python` | |
| PostgreSQL | `psycopg2-binary` | |
| SQL Server / Azure SQL | `pymssql` | |
| MongoDB | `pymongo` | Document queries in the SQL editor |
| AWS DocumentDB | `pymongo` | TLS required |
| SQLite | built-in | |

| Cloud | Services | Metrics API |
|-------|----------|-------------|
| AWS | RDS, Aurora | CloudWatch + Performance Insights |
| Azure | SQL, MySQL, PostgreSQL, MariaDB, Cosmos, Redis | Azure Monitor |
| GCP | Cloud SQL | Cloud Monitoring (System Insights) |

---

## Install

**Prerequisites:** Python 3.10+. Tkinter for the Desktop UI (`sudo apt-get
install python3-tk` on Debian/Ubuntu; bundled on macOS/Windows). For the AI
assistant, install at least one CLI backend: [Claude](https://claude.ai/download),
[Cursor](https://cursor.com), or [Codex](https://github.com/openai/codex).

### Option A — clone from Git (recommended)

```bash
git clone https://github.com/dhananjay-chaturvedi/dbassistant.git
cd dbassistant
bash install.sh                    # full tool: .venv + deps + config.ini
source .venv/bin/activate          # Windows: .venv\Scripts\activate
python dbtool.py modules           # verify modules
```

Single-module install (still includes the shared core):

```bash
bash install.sh --module ai        # core | migrator | ai | monitor | app_builder | full
bash install.sh --no-optional      # skip optional extras (e.g. local LLM)
```

The installer copies `config.ini` and `properties.ini` from
`common/config/*.ini.example`. Live config files are **not** in Git — see
[`.gitignore`](.gitignore).

Pre-built zip bundles (offline/air-gapped) are described in
[`website/src/content/docs/getting-started/installation.md`](website/src/content/docs/getting-started/installation.md)
and built with `bash shipper.sh` (attach releases to GitHub Releases).

### Option B — pip / editable (`pyproject.toml`)

```bash
git clone https://github.com/dhananjay-chaturvedi/dbassistant.git
cd dbassistant
python3 -m venv .venv && source .venv/bin/activate
pip install -e .                      # core only (pure-Python)
pip install -e ".[drivers]"             # + database drivers
pip install -e ".[api,ui]"              # + REST API/Web server + Textual TUI
pip install -e ".[cloud]"               # + AWS/Azure/GCP monitoring SDKs
pip install -e ".[llm]"                 # + local LLM engines & semantic RAG
pip install -e ".[all]"                 # everything except dev tooling
cp common/config/config.ini.example config.ini
cp common/config/properties.ini.example properties.ini
# Installs a `dbtool` console script on your PATH.
```

### Option C — manual pip requirements

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r setup/requirements-full.txt
# or a subset:
pip install -r setup/requirements-core.txt -r setup/requirements-drivers.txt
pip install -r setup/requirements-api.txt
pip install -r setup/requirements-ui.txt
cp common/config/config.ini.example config.ini
cp common/config/properties.ini.example properties.ini
```

---

## Quickstart

After `bash install.sh` (or copying the `*.ini.example` files as shown above):

```bash
source .venv/bin/activate               # if not already active

# 1) Verify install
python dbtool.py modules

# 2) Add a connection (interactive in any UI, or via CLI)
python dbtool.py connections add        # follow the prompts
python dbtool.py connections list

# 3) Run a query
python dbtool.py query --conn prod --sql "SELECT 1"

# 4) Ask in natural language (needs an AI backend)
python dbtool.py ai --conn prod "top 5 customers by revenue last 30 days"
```

---

## Launch every surface

```bash
# Desktop UI (full tool, or a single module tab)
python dbtool.py ui
python dbtool.py ui --module ai          # migrator | ai | monitor

# Terminal UI (TUI)
python dbtool.py tui
python dbtool.py tui --web --host 0.0.0.0 --port 8080   # serve the TUI over the web

# Web UI (SPA + API)
python dbtool.py webui --host 127.0.0.1 --port 8000     # open http://127.0.0.1:8000/ui/

# REST API only
python dbtool.py api --host 127.0.0.1 --port 8000       # open http://127.0.0.1:8000/docs

# Single module, standalone
python -m schema_converter migrator convert --help
python -m ai_query ai --conn prod "count rows in users"
python -m monitoring daemon status
python -m ai_assistant.app_builder app-builder --help
```

The REST API is protected by an API key — generate one with
`python dbtool.py apikey` and send it as the `X-API-Key` header.

---

## Configuration

Configuration is layered and **module-owned**:

| Scope | File | Owns |
|-------|------|------|
| Core engine & paths | `config.ini` (example: `common/config/config.ini.example`) | drivers, ports, timeouts |
| UI preferences | `properties.ini` (example: `common/config/properties.ini.example`) | window sizes, colors, logging |
| Data Migration | `schema_converter/config.ini` | type maps, charset, error policy |
| AI Query / RAG / LLM | `ai_query/config.ini` | backends, RAG, LLM, schedulers |
| Monitoring | `monitoring/monitor_config.ini`, `monitoring/monitor_thresholds.ini` | targets, thresholds, notifications |

Every module exposes its config through all surfaces, e.g.:

```bash
python dbtool.py config show                         # core
python dbtool.py ai config set ai default_backend claude
python dbtool.py migrator config show
python dbtool.py monitor-config show
```

See the [configuration reference](docs/CONFIG_REFERENCE.md) for every key.

---

## Security model

- **Encrypted at rest.** All credentials (DB, cloud, monitor) are encrypted with
  Fernet keys stored under `~/.dbassistant/keys/` with `0600` permissions.
- **Read-only AI.** Generated and AI-executed SQL is checked by a shared
  `sql_guard` that rejects any mutating statement, on every surface.
- **PII masking.** An optional toggle masks sampled values before they enter the
  AI prompt, the RAG index, or logs.
- **API key auth.** The REST API requires an `X-API-Key`; CORS and request
  guards are configured in the headless app factory.

---

## Where data lives

Everything the tool generates lives under `~/.dbassistant/` (override with
`DBASSISTANT_HOME`, which is essential for tests and isolated installs):

```
~/.dbassistant/
├── keys/             # Fernet encryption keys (db, cloud, monitor) — chmod 600
├── connections/      # encrypted connection profiles
├── runtime/          # daemon PID, metrics.json, logs, alerts log
├── session/          # AI sessions, RAG index, dashboard layout
└── version           # layout migration stamp
```

---

## Documentation

| Doc | Purpose |
|-----|---------|
| **[Documentation site](https://dhananjay-chaturvedi.github.io/dbassistant/)** | Full reference (CLI, API, config, guides) |
| [`docs/README.md`](docs/README.md) | Product overview |
| [`docs/QUICKSTART.md`](docs/QUICKSTART.md) | Short getting-started guide |
| [`docs/HOWTORUN.md`](docs/HOWTORUN.md) | Run UI / TUI / Web / CLI / API |
| [`HOW_TO_USE.md`](HOW_TO_USE.md) | Complete module-by-module user guide |
| [`docs/CONFIG_REFERENCE.md`](docs/CONFIG_REFERENCE.md) | Every config key, all modules |
| [`docs/RAG.md`](docs/RAG.md) | RAG Manager: schema cards, hybrid retrieval, reranking, eval, scheduler |
| [`docs/UI_ARCHITECTURE.md`](docs/UI_ARCHITECTURE.md) | Desktop / TUI / Web separation |
| [`docs/ADDING_FEATURES.md`](docs/ADDING_FEATURES.md) | UI/CLI/API parity guide for contributors |
| [`MODULES.md`](MODULES.md) | Independent module packaging & shipping |
| [`FIRST_COMMIT.md`](FIRST_COMMIT.md) | Public-repo checklist & first-commit guide |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | How to contribute (parity, PR checklist) |
| [`SECURITY.md`](SECURITY.md) | Vulnerability reporting & secure deployment |
| [`website/`](website/) | Full marketing + docs site (Astro + Starlight) |

The full reference site (every command, route, and config key) is in
[`website/`](website/) — run `cd website && npm ci && npm run dev`.

---

## Development

```bash
source .venv/bin/activate
bash install.sh                       # or: python setup/install.py --module full
python setup/install.py --verify-only --module full --skip-venv
python dbtool.py --help
```

**Contributing a feature?** Read [`docs/ADDING_FEATURES.md`](docs/ADDING_FEATURES.md):
put logic in the shared service, then wire the Desktop UI, TUI, Web UI, CLI,
and REST API in the same change so surfaces stay in parity.

**Publishing this repo?** See [`FIRST_COMMIT.md`](FIRST_COMMIT.md) for what to
include in the initial commit and what `.gitignore` excludes.

---

## License

**MIT License** — Copyright (c) 2026 Dhananjay Chaturvedi.

You may use, modify, and distribute this software under the terms in
[`LICENSE`](LICENSE). The copyright holder retains ownership of the codebase;
the MIT license grants permission to use it with minimal restrictions.

---

**Made for database engineers, application developers, and the teams who depend
on them. Enterprise-ready • Offline-capable • Secure • UI/CLI/API parity.**
