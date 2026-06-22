---
title: Shipper / packaging
description: Build distributable ZIP bundles (lean or offline, full or per-module) for sharing the tool.
sidebar:
  order: 3
---

The shipper builds installable ZIP archives ready to send to users.

## Interactive (recommended)

```bash
./shipper.sh
```

The script prompts:

1. Which module? `full`, `core`, `migrator`, `ai`, `monitor`
2. Lean (~500 KB) or offline (~1 GB, all wheels)?
3. Output directory (default `./dist`)

## Non-interactive

```bash
python setup/shipper.py --module full
python setup/shipper.py --module ai
python setup/shipper.py --module monitor --offline
python setup/shipper.py --module full --offline --output /tmp/dist
```

## Modes

### Lean (default)

- Strips `tests/`, `__pycache__/`, `.git/`, `.venv/`, logs, runtime files.
- Receiver runs `./install.sh` (or `install.bat`) — Python deps installed from PyPI.
- Bundle size: ~250–520 KB depending on module.

### Offline (`--offline`)

- Same source layout as lean.
- Includes `wheels/<platform>/` directories for:
  - macOS arm64
  - macOS x86_64
  - Linux x86_64
  - Windows x86_64
- For Python 3.10, 3.11, and 3.12 — pip picks the matching wheel
  automatically on the receiver.
- Bundle size: ~400 MB (`core`) to ~1.3 GB (`full`).

## What gets stripped

| Path pattern | Reason |
|--------------|--------|
| `tests/`, `**/tests/`, `**/test_*.py` | Not needed by users |
| `.venv/` | Receiver creates their own |
| `__pycache__/`, `*.pyc` | Build artifacts |
| `.git/`, `.gitignore` | Source control |
| `*.log`, `runtime/`, `logs/` | Generated at runtime |
| `dist/`, `build/` | Previous build outputs |
| `.DS_Store`, `Thumbs.db` | OS noise |
| `node_modules/` (in website/) | Website build artifacts |

Override exclusions in `setup/shipper.py` (`EXCLUDED_DIR_NAMES`,
`EXCLUDED_FILE_GLOBS`).

## Bundle layout

```text
dbassistant-full-1.0.0/                 (lean)
├── conDbUi.py
├── dbtool.py
├── api.py
├── VERSION
├── config.ini.example
├── properties.ini.example
├── README_INSTALL.md
├── install.sh                # POSIX launcher → setup/install.sh
├── install.command           # macOS Finder launcher
├── install.bat               # Windows launcher → setup\install.bat
├── run.sh
├── run.bat
├── setup/
│   ├── install.py            # cross-platform Python installer
│   ├── install.sh
│   ├── install.bat
│   ├── module_manifest.py
│   ├── requirements-*.txt
│   └── shipper.py
├── common/
├── app/
├── schema_converter/
├── ai_query/
└── monitoring/
```

Offline bundles add:

```text
wheels/
├── linux-x86_64/    *.whl  (cp310, cp311, cp312)
├── macos-arm64/     *.whl
├── macos-x86_64/    *.whl
└── windows-x86_64/  *.whl
```

## Versioning

The shipper reads `VERSION` at the repository root. Bump that file
before building a release.

```bash
echo "1.1.0" > VERSION
./shipper.sh
# produces dbassistant-full-1.1.0.zip
```

## Receiver install flow

After unzipping:

```bash
# macOS / Linux
./install.sh           # lean
./install.sh --offline # offline

# Windows
install.bat            # lean
install.bat --offline  # offline
```

`README_INSTALL.md` inside the bundle documents the install steps,
macOS Gatekeeper note, and offline troubleshooting.

## macOS Gatekeeper note

If the receiver double-clicks `install.command` and sees *"unidentified
developer"*:

```bash
xattr -d com.apple.quarantine install.command
```

Or right-click → **Open** → confirm.

## Per-module bundles

Each module bundle includes the shared `common/` core plus only that
module's folder:

```bash
python setup/shipper.py --module ai
# → dbassistant-ai-1.0.0.zip (~290 KB)

python setup/shipper.py --module monitor --offline
# → dbassistant-monitor-1.0.0-offline.zip (~900 MB-1 GB)
```

A module-only install yields a working CLI / API / UI for that single
module plus all core commands.

## CI integration

```yaml
# .github/workflows/release.yml
name: Build release bundles
on:
  push:
    tags: ["v*.*.*"]
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install -r setup/requirements-full.txt
      - run: |
          mkdir dist
          python setup/shipper.py --module full --output dist
          python setup/shipper.py --module full --offline --output dist
          python setup/shipper.py --module ai --output dist
      - uses: actions/upload-artifact@v4
        with: { name: bundles, path: dist/*.zip }
```

## See also

- [Modules & shipping](/architecture/modules/) — what each module
  bundle contains
- [Installation](/getting-started/installation/) — the receiver's view
