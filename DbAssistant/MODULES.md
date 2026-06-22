# Modular architecture & independent shipping

DbManagementTool splits **shared infrastructure** (`common/`) from **optional modules** and the **full-tool CLI** (`app/`).

## Product editions

Release builds are created at packaging time:

| Edition | Ships |
|---------|-------|
| Standard | Connections, Dashboard, SQL Editor, Database Objects, Data Migration / Schema Converter, Monitoring, AI Query Assistant |
| Advanced | Everything in Standard plus App Builder, Build & Train LLM, RAG Manager, and LLM Training (`ai_assistant/app_builder`, `ai_assistant/llm`, `ai_assistant/rag`) |

The Standard build physically omits the advanced folders using
`common/editions.py` and `scripts/build_edition.py`; it is not a runtime license
switch. AI Query Assistant stays available in Standard and degrades gracefully
when advanced LLM/RAG modules are absent.

```
DbManagementTool/
├── common/                         ← SHIP WITH EVERY MODULE
│   ├── shell/menu_lib.sh           # bash menu helpers (run_*.sh)
│   ├── cloud/                      # cloud connection profiles (shared UI wizard)
│   │   ├── connection_manager.py   # cloud_connections.json (encrypted)
│   │   ├── schemas.py              # AWS/Azure/GCP form fields
│   │   └── validation.py           # profile validation (DB id required for Connections)
│   ├── dashboard/                  # Dashboard tab + GET /api/dashboard (ships with core)
│   ├── ui/
│   │   ├── theme.py, widgets.py    # shared Tk styling
│   │   ├── master_shell.py         # Connections + Objects + SQL Editor + module tabs
│   │   └── launcher.py             # launch_desktop_ui(feature_module=…)
│   ├── core/
│   │   ├── modules.py              # discovery + ModuleManifest
│   │   ├── cli_handlers.py         # core CLI (connections, query, objects, …)
│   │   ├── cliutil.py
│   │   └── standalone_runner.py    # python -m <module> entry helper
│   └── headless/
│       ├── db_service.py           # CoreDBService
│       ├── app_factory.py          # FastAPI core + module routers
│       └── composite.py            # core + module service wrapper
├── app/                            ← FULL TOOL ONLY (master CLI + DBService composer)
│   ├── dbtool.py                   # re-exports common UI launcher for `dbtool ui`
│   └── headless/db_service.py      # DBService = CoreDBService + module delegation
├── schema_converter/               # module: service.py, bridge.py, cli, api, standalone
├── ai_query/                       # module: service.py, cli, api, standalone
└── monitoring/                     # module: service.py, cli, api, daemon
```

## Module-owned configuration

Each optional module ships its own `*.ini.example` and creates a live `config.ini`
(or `monitor_config.ini`) on first save. These files are **not** edited from the
core Settings tab.

| Module | Config file | UI | CLI | API |
|--------|-------------|-----|-----|-----|
| Monitoring | `monitoring/monitor_config.ini` | Monitor Settings | `monitor-config` | `/api/monitor/config` |
| Monitoring | `monitoring/monitor_thresholds.ini` | Alert Thresholds | `thresholds` | `/api/thresholds` |
| AI Query | `ai_query/config.ini` | AI Settings | `ai config` | `/api/ai/config` |
| Data Migration | `schema_converter/config.ini` | Migration Settings | `migrator config` | `/api/migrator/config` |

Notification secrets stay encrypted under `~/.dbassistant` (not in INI files).

Headless live verification:

```bash
PYTHONPATH=. .venv/bin/python scripts/live_smoke_matrix.py
```

## Per-module surfaces (no `app/` required)

Each module ships with:

| Surface | Entry |
|---------|--------|
| **Module CLI** | `python -m <module> migrator\|ai\|monitor …` |
| **Core CLI** | `python -m <module> connections list`, `query`, `objects`, … |
| **Shell UI** | `bash <module>/run_*.sh` or `python -m <module> --shell-ui` (bash menu, no tkinter) |
| **Module API** | `python -m <module> api` → core + module routes |
| Module | Canonical UI file | `launch_ui()` used by |
|--------|-------------------|------------------------|
| Data Migration | `schema_converter/schema_converter_ui.py` | `python -m schema_converter --ui`, direct script |
| AI | `ai_query/ai_query_ui.py` | `python -m ai_query --ui`, direct script |
| Monitor | `monitoring/monitoring_ui.py` | `python -m monitoring --ui`, direct script |

Registered in `common/core/standalone_runner.py` as ``_MODULE_UI_ENTRY``. Each
``launch_ui()`` opens the shared shell in ``common/ui/master_shell.py`` (Connections,
Dashboard, Objects, SQL Editor) plus that module's tab.

Shared logic lives in `common/` — module packages only add `service.py` (feature logic) plus CLI/API/UI wiring.

## Ship a single module

Each module is **self-contained**: copy `common/` plus the module folder (and `app/` only for the full tool). Run the installer for that module only:

```bash
bash setup/install.sh --module migrator    # Linux / macOS
bash install.sh --module migrator          # wrapper at repo root
python setup/install.py --module migrator  # any OS with Python only
```

Windows:

```batch
install.bat --module migrator
run.bat
```

Example — Data Migration only (minimum files):

```
common/
schema_converter/
config.ini          # created from example on install
properties.ini      # created from example on install
setup/
install.sh
install.bat
run.sh
run.bat
conDbUi.py          # optional; use python -m schema_converter --ui
```

Requirement bundles (see `setup/module_manifest.py`):

| Module | pip requirements |
|--------|------------------|
| `core` | `requirements-core.txt` + `requirements-drivers.txt` |
| `migrator` | core + drivers + API + `schema_converter/requirements.txt` |
| `ai` | core + drivers + API + `ai_query/requirements.txt` |
| `monitor` | core + drivers + cloud + API + `monitoring/requirements.txt` |
| `full` | `requirements-full.txt` (everything) |

Example — Data Migration usage after install:
bash schema_converter/run_schema_converter.sh   # bash menu (no tkinter)
python -m schema_converter --shell-ui           # same menu
python -m schema_converter migrator convert ...
python -m schema_converter connections list
python -m schema_converter api
```

Same pattern for `ai_query` and `monitoring` (replace module name and commands).

## One-click install

**Linux / macOS** (Python 3.10+ required; script installs venv, pip deps, tkinter hints, config):

```bash
bash install.sh                  # full tool
bash install.sh --module monitor # monitoring module only
```

**Windows** (cmd.exe / double-click):

```batch
install.bat
install.bat --module full
run.bat
```

**Python-only** (all platforms):

```bash
python setup/install.py --module full
```

## Full tool

Ship everything including `app/`. Entry points at the repo root:

```bash
python dbtool.py ui          # → app/dbtool.py
python dbtool.py api           # → app/headless/api.py via uvicorn
python conDbUi.py              # → common/ui/master_shell.py
```

## Canonical import paths

All implementation code lives under `common/`, `app/`, and module packages.
Legacy root shims (`core/`, `ui/`, `drivers/`, `headless/`, `config_loader.py`, …)
were removed — import from the paths below:

| Concern | Import from |
|---------|-------------|
| Config | `common.config_loader` |
| Saved DB connections | `common.connection_manager` |
| DatabaseManager | `common.db_manager` |
| Engine registry | `common.database_registry` |
| Module discovery / CLI util | `common.core.*` |
| Core API service | `common.headless.db_service.CoreDBService` |
| Full-tool API service | `app.headless.db_service.DBService` |
| FastAPI app (full tool) | `app.headless.api.app` |
| Cloud profiles | `common.cloud.connection_manager` |
| Cloud provider plugins | `monitoring.cloud_providers.*` |
| Monitor UI | `monitoring.server_monitor.ServerMonitorUI` |
| Stop monitors / PID helpers | `monitoring.stop` |

## Module registry

`common/core/modules.py` — `migrator` → `schema_converter`, `ai` → `ai_query`, `monitor` → `monitoring`.
