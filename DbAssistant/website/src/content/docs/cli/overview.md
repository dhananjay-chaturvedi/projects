---
title: CLI overview
description: The dbtool CLI — global flags, output formats, and command index.
sidebar:
  order: 1
---

The CLI is the headless surface of DbAssistant. Anything you can do in
the UI you can script here, plus a few extras (daemon control, threshold
inspection without a live DB) that aren't exposed in the UI.

## Invocation

```bash
python dbtool.py <command> [options]
# or, if .venv/bin is on PATH:
dbtool <command> [options]
```

After running `./install.sh`, the project root contains a `run.sh` /
`run.bat` shortcut that launches the UI; the CLI binary is at
`.venv/bin/python dbtool.py`.

## Global flags

| Flag | Default | Effect |
|------|---------|--------|
| `--format {table,json,csv}` | `table` | Output format for any data command |
| `--no-color` | off | Disable ANSI colors |
| `--help`, `-h` | — | Show command help |

Environment variables:

| Variable | Purpose |
|----------|---------|
| `DBTOOL_DEBUG=1` | Print full tracebacks on error |
| `DBASSISTANT_HOME` | Override `~/.dbassistant/` base path |
| `DBTOOL_API_KEY` | Required for REST API when set |

## Command index

### Core (always available)

| Command | Purpose |
|---------|---------|
| [`connections`](/cli/connections/) | Manage saved DB connection profiles |
| [`query`](/cli/query/) | Run SQL or document queries |
| [`objects`](/cli/objects/) | List tables, views, procedures, etc. |
| [`databases`](/cli/databases/) | List engines and capabilities |
| [`config`](/cli/config/) | Show effective configuration |
| `api` | Start the REST API server |
| `ui` | Launch desktop UI |
| `modules` | List installed modules |

### Data Migration module

| Command | Purpose |
|---------|---------|
| [`migrator show`](/cli/migrator/) | Display table columns and indexes |
| [`migrator dump`](/cli/migrator/) | Generate `CREATE TABLE` DDL |
| [`migrator convert`](/cli/migrator/) | Translate DDL to another engine |
| [`migrator apply`](/cli/migrator/) | Execute DDL on target connection |
| [`migrator transfer-data`](/cli/migrator/) | Copy rows source → target |
| [`migrator compare-schema`](/cli/migrator/) | Validate schema after migration |
| [`migrator compare-data`](/cli/migrator/) | Validate data after migration |

### AI Query Assistant module

| Command | Purpose |
|---------|---------|
| [`ai`](/cli/ai/) | One-shot natural-language query |
| `ai session new/ask/follow-up/list/save/load/close/cross/execute-sql/set-mode` | Multi-turn sessions |

### Server Monitor module

| Command | Purpose |
|---------|---------|
| [`monitor`](/cli/monitor/) | Poll DB metrics |
| [`daemon`](/cli/daemon/) | Manage background monitor process |
| [`thresholds`](/cli/thresholds/) | List / show / check threshold rules |
| [`cloud`](/cli/cloud/) | Cloud DB connections and metrics |
| [`os`](/cli/os/) | Host OS metrics |
| [`notify`](/cli/notify/) | Send a test notification |

## Module gating

Module-owned commands (`migrator`, `ai`, `monitor`, `daemon`,
`thresholds`, `os`, `notify`, `cloud`) print a clear *"Module … is not
installed"* message — including the folder to copy in — when their
owning module is missing. Non-zero exit code.

Check what's installed:

```bash
python dbtool.py modules
```

## Output formats

Every data command honors `--format`:

```bash
python dbtool.py query --conn prod --sql "SELECT 1" --format table
python dbtool.py query --conn prod --sql "SELECT 1" --format json
python dbtool.py query --conn prod --sql "SELECT 1" --format csv
```

JSON is the most useful for piping into `jq`, `python`, or other tools:

```bash
python dbtool.py connections list --format json | jq '.[].name'
```

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Invalid arguments / usage |
| 2 | Connection / engine error |
| 3 | Module not installed |
| 4 | Authentication / encryption failure |
| 5 | Timeout |

## Next: dive into a command

Pick any from the sidebar — every page has full syntax, every flag, and
worked examples.
