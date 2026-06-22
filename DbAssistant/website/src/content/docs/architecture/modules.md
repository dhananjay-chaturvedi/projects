---
title: Modules & shipping
description: How modular architecture works and how to ship the full tool or a single module.
sidebar:
  order: 3
---

DbAssistant ships as a **shared core** + **four optional modules**.
Each module is self-contained and ships independently.

## Module table

| Module | Folder | CLI commands | API prefix | UI tab |
|--------|--------|--------------|------------|--------|
| Data Migration | `schema_converter/` | `migrator` | `/api/migrator/*` | Data Migration |
| AI Query Assistant | `ai_query/` | `ai` (incl. `ai rag`, `ai llm`) | `/api/ai/*` | AI Query Assistant |
| Monitoring | `monitoring/` | `monitor`, `daemon`, `thresholds`, `os`, `notify`, `cloud` | `/api/metrics`, `/api/thresholds`, `/api/os`, `/api/notify`, `/api/cloud`, `/api/daemon` | Monitor |
| App Builder | `ai_assistant/app_builder/` | `app-builder` | `/api/app-builder/*` | App Builder |

Always-available core commands: `connections`, `query`, `objects`,
`databases`, `config`, `api`, `ui`, `modules`.

## Ship a single module

Each module folder + `common/` + `setup/` is enough to install that
module standalone.

```bash
unzip dbassistant-schema-1.0.0.zip
cd dbassistant-schema-1.0.0
./install.sh          # installer auto-detects the module
```

Module-only contents:

```text
common/                # shared core (required by every module)
schema_converter/      # the module folder
setup/                 # install.sh / install.bat / install.py
config.ini.example
properties.ini.example
install.sh
install.bat
run.sh
run.bat
conDbUi.py             # optional UI launcher
```

Running it:

```bash
python -m schema_converter --help           # module CLI
python -m schema_converter --ui             # desktop UI with this module's tab
python -m schema_converter --shell-ui       # text-mode menu (no Tk)
python -m schema_converter api --port 8001  # core + module API routes
```

## Ship the full tool

Include everything above plus `app/`, `dbtool.py`, `api.py`, and all
module folders.

```bash
unzip dbassistant-full-1.0.0.zip
cd dbassistant-full-1.0.0
./install.sh                # full tool
python dbtool.py modules    # confirm everything ready
```

## Requirement bundles

`setup/module_manifest.py` declares which pip requirements each module
needs:

| Module bundle | Includes |
|---------------|----------|
| `core` | `requirements-core.txt` + `requirements-drivers.txt` |
| `migrator` | core + API + `schema_converter/requirements.txt` |
| `ai` | core + API + `ai_query/requirements.txt` |
| `monitor` | core + cloud + API + `monitoring/requirements.txt` |
| `app_builder` | core + API + `ai_query/requirements.txt` (optional: `ai_query/requirements-llm.txt`) |
| `full` | `requirements-full.txt` (everything) |

## Adding a new module

1. Create `your_module/` with at minimum:
   ```text
   your_module/
   ├── __init__.py
   ├── manifest.py        # ModuleManifest(...)
   ├── service.py         # business logic
   ├── cli.py             # add_subparsers(subparsers)
   ├── api.py             # build_router() -> APIRouter
   ├── ui_panel.py        # optional Tk panel
   └── requirements.txt   # optional extra deps
   ```
2. Implement `manifest.py`:
   ```python
   from common.core.modules import ModuleManifest
   from . import cli, api
   MANIFEST = ModuleManifest(
       name="your_module",
       display_name="Your Module",
       folder="your_module",
       cli_register=cli.register,
       api_router=api.build_router(),
       required_paths=["your_module"],
       requirement_files=["your_module/requirements.txt"],
   )
   ```
3. Add the module to `common/core/modules.py`'s registry.
4. Drop a sidebar entry in this website's `astro.config.mjs`.

The core discovers the module on next start. `dbtool modules` shows it
as **installed** and **ready** once dependencies are satisfied.

## Shipper

Build any bundle with the shipper:

```bash
./shipper.sh
# or:
python setup/shipper.py --module full
python setup/shipper.py --module ai
python setup/shipper.py --module monitor --offline   # bundle wheels for 4 platforms × 3 Pythons
```

See [Shipper / packaging](/operations/shipper/) for full options.
