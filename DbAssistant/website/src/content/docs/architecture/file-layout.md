---
title: File layout (~/.dbassistant)
description: Where DbAssistant stores keys, connections, runtime state, and session data on disk.
sidebar:
  order: 2
---

Everything DbAssistant generates lives under a single root directory:

```text
~/.dbassistant/
├── version                       # layout version stamp (e.g. "2")
├── keys/
│   ├── db.key                    # Fernet — DB connection passwords
│   ├── cloud.key                 # Fernet — cloud profile secrets
│   └── monitor.key               # Fernet — monitor connection secrets
├── connections/
│   ├── db.json                   # encrypted DB profiles
│   ├── cloud.json                # encrypted cloud profiles
│   └── monitor.json              # encrypted monitoring targets
├── runtime/                      # configurable via config.ini
│   ├── daemon.pid                # background daemon PID
│   ├── daemon.log
│   └── metrics.json              # last poll snapshot (served by /api/metrics)
├── session/                      # configurable via config.ini
│   ├── ai/
│   │   └── sessions.json         # saved AI Query Assistant tabs
│   └── dashboard/
│       └── layout.json           # user-customised card layout
└── .migrate.lock                 # transient — held during migration
```

## Overriding the base path

Set `DBASSISTANT_HOME` to relocate everything:

```bash
export DBASSISTANT_HOME=/var/lib/dbassistant
```

This is critical for:

- **Tests** — `tests/conftest.py` points it at a temp path for hermetic runs
- **Containers** — mount a persistent volume at this path
- **Multi-tenant installs** — give each user a distinct root

## Configurable subpaths

Only `runtime/` and `session/` are configurable in `config.ini`:

```ini
[paths]
runtime_dir = ~/.dbassistant/runtime
session_dir = ~/.dbassistant/session
```

`keys/` and `connections/` are intentionally fixed under
`~/.dbassistant/` for predictable security audits.

## Migration from legacy layouts

Older builds wrote to `~/.dbmanager/` and `~/.dbtool/`. On first start
after upgrade, `common.layout_migration` performs a safe,
copy-then-rename migration:

1. Acquire a file lock at `~/.dbassistant/.migrate.lock`
2. Copy each legacy file to its new location
3. Re-encrypt cloud profiles with the new `cloud.key`
4. Write `~/.dbassistant/version`
5. Rename the legacy folders to `~/.dbmanager.legacy/` / `~/.dbtool.legacy/`
6. Release the lock

The migration is **idempotent** — running the tool repeatedly never
double-migrates or corrupts state.

## File permissions

Keys are written with mode `0600` (read/write by owner only).
Connection files are also `0600`. The base directory itself is `0700`.

## Cleaning up

Remove everything the tool stored:

```bash
rm -rf ~/.dbassistant ~/.dbmanager ~/.dbtool ~/.dbmanager.legacy
```

Or use the official uninstaller, which also stops daemons and removes
systemd units — see [Uninstall](/getting-started/uninstall/).
