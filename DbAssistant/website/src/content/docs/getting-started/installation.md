---
title: Installation
description: Install DbAssistant from a lean bundle, an offline bundle, or directly from source.
sidebar:
  order: 2
---

DbAssistant requires **Python 3.10 or newer**. Tkinter is needed only for
the desktop UI (already bundled with Python on macOS and Windows; install
`python3-tk` on Debian/Ubuntu).

## Option 1 — Lean bundle (recommended)

Lean bundles are roughly **500 KB**. The installer creates a virtual
environment and resolves dependencies from PyPI.

### macOS / Linux

```bash
unzip dbassistant-full-1.0.0.zip
cd dbassistant-full-1.0.0
./install.sh
./run.sh
```

### Windows

```batch
:: extract dbassistant-full-1.0.0.zip
cd dbassistant-full-1.0.0
install.bat
run.bat
```

The installer:

1. Checks for Python 3.10+
2. Creates `.venv/`
3. Installs the appropriate requirement bundle (core / drivers / cloud / API)
4. Copies `config.ini` and `properties.ini` from their `.example` files
5. Generates `run.sh` / `run.bat` shortcuts
6. Bootstraps `~/.dbassistant/` and migrates any legacy `~/.dbmanager/` content

## Option 2 — Offline bundle

Use when the target machine has no internet access. Offline bundles are
about **1 GB** because they include wheels for macOS (arm64, x86_64),
Linux (x86_64), and Windows (x86_64) across Python 3.10, 3.11, and 3.12.

```bash
unzip dbassistant-full-1.0.0-offline.zip
cd dbassistant-full-1.0.0-offline
./install.sh --offline
```

The `--offline` flag tells `pip` to install from the bundled
`wheels/<platform>` directory only — no network calls are made.

## Option 3 — Per-module bundle

You can also install only one module. Lean and offline variants exist
for each.

```bash
# Data Migration only
unzip dbassistant-migrator-1.0.0.zip
cd dbassistant-migrator-1.0.0
./install.sh

# AI Query Assistant only
./install.sh --module ai

# Monitor only (includes cloud SDKs)
./install.sh --module monitor
```

Module names: `migrator | ai | monitor | core | full`.

## Option 4 — From source

```bash
git clone <repo-url>
cd DbManagementTool

# Full tool
bash install.sh

# Or just verify an existing venv has everything
python setup/install.py --verify-only --module full --skip-venv
```

## Option 5 — Manual

```bash
python3.12 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate.bat

pip install -r setup/requirements-full.txt

# Or per-module:
pip install -r setup/requirements-core.txt -r setup/requirements-drivers.txt
pip install -r setup/requirements-api.txt        # REST API only
pip install -r setup/requirements-cloud.txt      # AWS / Azure / GCP only
```

## Verifying the install

```bash
python dbtool.py modules
```

You should see a table listing which modules are installed and ready:

```
┌─────────┬──────────────────────┬───────────┬───────┐
│ Module  │ Display name         │ Installed │ Ready │
├─────────┼──────────────────────┼───────────┼───────┤
│ migrator│ Data Migration       │ yes       │ yes   │
│ ai      │ AI Query Assistant   │ yes       │ yes   │
│ monitor │ Server Monitor       │ yes       │ yes   │
└─────────┴──────────────────────┴───────────┴───────┘
```

## macOS Gatekeeper note

If you double-click `install.command` (the macOS Finder launcher) and
see *"unidentified developer"*, allow it once:

```bash
xattr -d com.apple.quarantine install.command
```

Or open System Settings → Privacy & Security → click **Open Anyway**.

## Next steps

- [Quickstart](/getting-started/quickstart/) — connect your first DB
- [Configuration](/getting-started/configuration/) — tweak `config.ini`
- [CLI reference](/cli/overview/)
