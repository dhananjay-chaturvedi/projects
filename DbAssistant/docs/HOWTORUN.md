# DbManagementTool — How to Run & Use

This document covers every way to run and interact with DbManagementTool:
the **GUI desktop application**, the **headless CLI**, the **REST API**, and
the **background monitoring daemon**.

---

## Table of Contents

1. [Installation](#1-installation)
2. [GUI Desktop Application](#2-gui-desktop-application)
3. [Command-Line Interface (CLI)](#3-command-line-interface-cli)
   - [Connection Management](#31-connection-management)
   - [Running Queries](#32-running-queries)
   - [Browsing Database Objects](#33-browsing-database-objects)
   - [Data Migration](#34-data-migration)
   - [AI Query Assistant](#35-ai-query-assistant)
   - [Live Monitoring](#36-live-monitoring-blocking)
4. [Background Monitoring Daemon](#4-background-monitoring-daemon)
5. [REST API Server](#5-rest-api-server)
   - [API Endpoints Reference](#api-endpoints-reference)
6. [systemd (Linux Auto-Start)](#6-systemd-linux-auto-start)
7. [Configuration Files](#7-configuration-files)
8. [Output Formats](#8-output-formats)
9. [Environment Variables](#9-environment-variables)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Installation

### Automated (recommended)

```bash
# macOS / Linux
bash install.sh
```

`install.sh` detects your OS, installs system packages, creates `.venv`, and
installs all Python dependencies.

### Manual

```bash
# Python 3.10+ required
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r setup/requirements.txt

# Only needed for the REST API:
pip install -r setup/requirements-api.txt
```

Or from project root: `bash install.sh` (runs `setup/install.sh`).

---

## 2. GUI Desktop Application

Requires a display (Tkinter). This is the standard desktop mode.

```bash
source .venv/bin/activate
python conDbUi.py                  # full tool (Dashboard tab on startup)
python dbtool.py ui                # same via master CLI
python dbtool.py ui --module ai    # single module: migrator | ai | monitor

# Module canonical launchers
python -m schema_converter --ui
python -m ai_query --ui
python -m monitoring --ui
```

### Tabs in the GUI (full tool)

| Tab | What you can do |
|-----|-----------------|
| **Welcome** | In-app guide: features, CLI/API entry points, supported databases |
| **Connections** | Add / edit / test direct and cloud database connections |
| **Dashboard** | Operational overview cards; drag to rearrange; auto-refresh while visible |
| **Database Objects** | Browse tables, views, indexes, procedures (engine-aware) |
| **SQL Editor** | SQL or MongoDB/DocumentDB JSON queries; export results |
| **Data Migration** | Schema convert, data transfer, and validation *(module)* |
| **AI Query Assistant** | Natural language → SQL; multi-tab sessions *(module)* |
| **Monitor** | OS, DB, and cloud resource metrics; thresholds *(module)* |
| **Clear Cache** | Clear AI caches; reload credentials from disk |

Standalone module UI (`dbtool.py ui --module …`) includes Connections, Dashboard,
Database Objects, SQL Editor, plus one module tab.

Check installed modules:

```bash
python dbtool.py modules
```

See [`MODULES.md`](../MODULES.md) for independent module packaging.

### Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `F5` or `Ctrl+Enter` | Execute query |
| `Ctrl+Tab` | Cycle through tabs |
| `Escape` | Close dialog |

---

## 2b. Terminal UI (TUI) and Web UI

Both surfaces share the same backend services as Tk. Install UI extras first:

```bash
pip install -r setup/requirements-ui.txt
```

**Textual TUI** (terminal, no browser):

```bash
python dbtool.py tui
python dbtool.py tui --module ai
python -m schema_converter --tui
```

**Web SPA** (browser; loopback-only by default — set `DBTOOL_WEBUI_API_KEY` when exposing beyond localhost):

```bash
python dbtool.py webui
python dbtool.py webui --module monitor --host 127.0.0.1 --port 8090
python -m ai_query --web-ui
```

See [`docs/UI_ARCHITECTURE.md`](UI_ARCHITECTURE.md) for the full surface matrix.

---

## 3. Command-Line Interface (CLI)

The CLI (`dbtool.py`) requires no display and no Tkinter. All commands use the
same saved connection profiles as the GUI.

```bash
source .venv/bin/activate

# General help
python dbtool.py --help

# Help for any sub-command
python dbtool.py connections --help
python dbtool.py daemon start --help
```

### Global flags (apply to all commands)

| Flag | Description |
|------|-------------|
| `--format table\|json\|csv` | Output format (default: `table`) |
| `--no-color` | Strip ANSI colour codes (useful for piping) |

---

### 3.1 Connection Management

```bash
# List all saved connections
python dbtool.py connections list
python dbtool.py connections list --format json

# Add a new connection (password is prompted securely — never passed as an arg)
python dbtool.py connections add \
    --name my_mysql \
    --type MySQL \
    --host localhost \
    --port 3306 \
    --user root \
    --db mydb

# Add a PostgreSQL connection
python dbtool.py connections add \
    --name prod_pg \
    --type PostgreSQL \
    --host db.example.com \
    --port 5432 \
    --user admin \
    --db orders

# Add an Oracle connection (uses --service instead of --db)
python dbtool.py connections add \
    --name oracle_prod \
    --type Oracle \
    --host orahost \
    --port 1521 \
    --user system \
    --service ORCL

# Test a connection (opens it, fetches version, reports latency)
python dbtool.py connections test my_mysql

# Remove a saved connection
python dbtool.py connections remove my_mysql
```

**Supported DB types:** `MySQL`, `MariaDB`, `PostgreSQL`, `Oracle`, `SQLite`

---

### 3.2 Running Queries

```bash
# Inline SQL
python dbtool.py query --conn my_mysql --sql "SELECT 1"

# SQL from a file
python dbtool.py query --conn my_mysql --file report.sql

# Output as JSON (pipe-friendly)
python dbtool.py query --conn my_mysql --sql "SELECT * FROM users LIMIT 5" \
    --format json

# Output as CSV (redirect to file)
python dbtool.py query --conn prod_pg \
    --sql "SELECT id, name, email FROM customers" \
    --format csv > customers.csv

# Multi-statement file (executed sequentially)
python dbtool.py query --conn my_mysql --file migrations/v2.sql
```

---

### 3.3 Browsing Database Objects

```bash
# List tables (default)
python dbtool.py objects --conn my_mysql

# List views
python dbtool.py objects --conn prod_pg --type views

# List stored procedures
python dbtool.py objects --conn my_mysql --type procs

# All available types
python dbtool.py objects --conn my_mysql \
    --type tables|views|procs|functions|indexes|triggers

# JSON output for scripting
python dbtool.py objects --conn prod_pg --type tables --format json
```

---

### 3.4 Data Migration

Migrate tables across database engines: convert DDL, transfer data, and validate the result.

```bash
# Convert 'orders' table from MySQL to PostgreSQL DDL (printed to stdout)
python dbtool.py migrator convert \
    --source-conn my_mysql \
    --target-type PostgreSQL \
    --table orders

# Save output to a file, apply on target, transfer rows, validate
python dbtool.py migrator convert \
    --source-conn prod_pg \
    --target-type MySQL \
    --table customers \
    --output customers_mysql.sql
python dbtool.py migrator apply --target-conn target_mysql --ddl-file customers_mysql.sql
python dbtool.py migrator transfer-data --source-conn prod_pg --target-conn target_mysql --table customers
python dbtool.py migrator compare-schema --source-conn prod_pg --target-conn target_mysql --table customers
python dbtool.py migrator compare-data --source-conn prod_pg --target-conn target_mysql --table customers
```

---

### 3.5 AI Query Assistant

Convert a natural-language question into SQL using the AI agent.

```bash
# Ask a plain-English question
python dbtool.py ai --conn my_mysql "show all users who registered last month"

# Complex questions
python dbtool.py ai --conn prod_pg \
    "find top 10 customers by total order value in the last 90 days"

python dbtool.py ai --conn my_mysql \
    "which tables have more than 1 million rows"

# Japanese is supported
python dbtool.py ai --conn my_mysql \
    "2020年以降に入社した従業員を全て表示してください"
```

---

### 3.6 Live Monitoring (blocking)

Collect metrics and print them in the terminal. Press `Ctrl+C` to stop.

```bash
# Monitor a single connection (polls every 30s by default)
python dbtool.py monitor --conn my_mysql

# Monitor multiple connections
python dbtool.py monitor --conn my_mysql,prod_pg,oracle_prod

# One-shot (collect once and exit)
python dbtool.py monitor --conn my_mysql --once

# Custom poll interval (seconds)
python dbtool.py monitor --conn my_mysql --interval 60

# Save metrics snapshot to a JSON file on every poll
python dbtool.py monitor --conn my_mysql --output /tmp/metrics.json

# Monitor all saved connections, once, output JSON
python dbtool.py monitor --once --output /tmp/all_metrics.json
```

---

## 4. Background Monitoring Daemon

The daemon polls metrics silently in the background, evaluates thresholds, sends
alerts, and writes a metrics snapshot to `~/.dbassistant/runtime/metrics.json` (consumed by
the REST API `/api/metrics`).

```bash
# Start daemon (background, uses all saved connections)
python dbtool.py daemon start

# Start for specific connections only
python dbtool.py daemon start --connections my_mysql,prod_pg

# Custom interval and custom paths
python dbtool.py daemon start \
    --interval 60 \
    --pid-file /var/run/dbtool.pid \
    --log-file /var/log/dbtool.log \
    --output   /var/lib/dbtool/metrics.json

# Check if daemon is running
python dbtool.py daemon status

# Stop the daemon gracefully
python dbtool.py daemon stop

# Run in foreground (for Docker / systemd / debugging)
python dbtool.py daemon start --foreground
```

Default file locations (when not overridden):

| File | Default path |
|------|--------------|
| PID file | `~/.dbassistant/runtime/daemon.pid` |
| Log file | `~/.dbassistant/runtime/daemon.log` |
| Metrics JSON | `~/.dbassistant/runtime/metrics.json` |

**Tail the daemon log:**
```bash
tail -f ~/.dbassistant/runtime/daemon.log
```

---

## 5. REST API Server

Requires `fastapi` and `uvicorn` (install from `requirements-api.txt`).

```bash
pip install -r requirements-api.txt

# Start API server (default: http://127.0.0.1:8000)
python dbtool.py api

# Listen on all interfaces (LAN / server deployment)
python dbtool.py api --host 0.0.0.0 --port 8000

# Development mode (auto-reload on code changes)
python dbtool.py api --reload

# Or start directly with uvicorn
uvicorn app.headless.api:app --host 0.0.0.0 --port 8000 --reload
```

Interactive docs are served automatically:

| URL | Description |
|-----|-------------|
| `http://localhost:8000/docs` | Swagger UI (try endpoints in browser) |
| `http://localhost:8000/redoc` | ReDoc docs |
| `http://localhost:8000/openapi.json` | OpenAPI schema |

---

### API Endpoints Reference

#### Health & modules

```
GET  /api/health
GET  /api/modules                        Installed module status
GET  /api/dashboard                        Operational snapshot (same data as Dashboard tab)
```
```json
{ "status": "ok", "timestamp": "2026-05-24T22:00:00" }
```

**Module-only API** (core + one module):

```bash
python -m monitoring api --port 8001
python -m ai_query api --port 8002
python -m schema_converter api --port 8003
```

---

#### Connections

```
GET    /api/connections                  List all connections
POST   /api/connections                  Create connection
DELETE /api/connections/{name}           Remove connection
POST   /api/connections/{name}/test      Test connection
```

**Create connection:**
```bash
curl -X POST http://localhost:8000/api/connections \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_mysql",
    "db_type": "MySQL",
    "host": "localhost",
    "port": "3306",
    "user": "root",
    "password": "secret",
    "database": "mydb"
  }'
```

**Test connection:**
```bash
curl -X POST http://localhost:8000/api/connections/my_mysql/test
```

---

#### Query Execution

```
POST  /api/query
```
```bash
curl -X POST http://localhost:8000/api/query \
  -H "Content-Type: application/json" \
  -d '{"connection": "my_mysql", "sql": "SELECT 1 AS val"}'
```
```json
{
  "columns": ["val"],
  "rows": [["1"]],
  "rowcount": 1,
  "time_ms": 2.3,
  "error": null
}
```

---

#### Database Objects

```
GET  /api/objects/{connection}?type=tables|views|procs|functions|indexes|triggers
```
```bash
curl http://localhost:8000/api/objects/my_mysql?type=tables
curl http://localhost:8000/api/objects/prod_pg?type=views
```

---

#### Metrics

```
GET  /api/metrics               All connections (from daemon snapshot if running)
GET  /api/metrics/{connection}  Live metrics for one connection
```
```bash
curl http://localhost:8000/api/metrics/my_mysql
curl http://localhost:8000/api/metrics
```

---

#### Data Migration

```
POST  /api/migrator/convert
POST  /api/migrator/transfer-data
POST  /api/migrator/compare-schema
POST  /api/migrator/compare-data
```
```bash
curl -X POST http://localhost:8000/api/migrator/convert \
  -H "Content-Type: application/json" \
  -d '{
    "source_conn": "my_mysql",
    "target_type": "PostgreSQL",
    "table": "orders"
  }'
```

---

#### AI Query

```
POST  /api/ai/query
```
```bash
curl -X POST http://localhost:8000/api/ai/query \
  -H "Content-Type: application/json" \
  -d '{
    "connection": "my_mysql",
    "question": "show top 5 customers by order total"
  }'
```
```json
{
  "sql": "SELECT customer_id, SUM(total) ...",
  "explanation": "This query joins ...",
  "error": null
}
```

---

## 6. systemd (Linux Auto-Start)

Two unit files are provided in `setup/systemd/`.

```bash
# Edit the unit files to set your WorkingDirectory and venv path:
nano setup/systemd/dbtool-monitor.service
nano setup/systemd/dbtool-api.service

# Install and enable
sudo cp setup/systemd/dbtool-monitor.service /etc/systemd/system/
sudo cp setup/systemd/dbtool-api.service     /etc/systemd/system/
sudo systemctl daemon-reload

# Enable on boot
sudo systemctl enable dbtool-monitor
sudo systemctl enable dbtool-api

# Start now
sudo systemctl start dbtool-monitor
sudo systemctl start dbtool-api

# Check status
sudo systemctl status dbtool-monitor
sudo systemctl status dbtool-api

# Follow logs
journalctl -u dbtool-monitor -f
journalctl -u dbtool-api     -f
```

---

## 7. Configuration Files

| File | Purpose |
|------|---------|
| `config.ini` | DB paths, Oracle client, timeouts |
| `properties.ini` | UI fonts, colours, window sizes, limits |
| `monitor_thresholds.ini` | Metric thresholds, severities, enable/disable |

### monitor_thresholds.ini — enable or disable individual metrics

```ini
# Generic DB default (applies to any engine without an override)
[metric.db.total_connections]
enabled  = true
warning  = 600
critical = 900
unit     = count

# Per-engine override: mysql | mariadb | oracle | postgresql | sqlite.
# Looked up first; falls back to the generic [metric.db.*] rule above.
[metric.db.postgresql.total_connections]
enabled  = true
warning  = 400
critical = 700
unit     = count

[metric.os.cpu_utilization]
enabled  = false        # set true to show CPU in monitoring
warning  = 80
critical = 95
unit     = pct
```

Changes take effect immediately when you click **Refresh** in the GUI, or on
the next daemon poll cycle. List/edit engine rules from the CLI with
`--source db --path <engine>` (see `dbtool.py thresholds --help`).

---

## 8. Output Formats

The `--format` flag controls CLI output. All three formats are machine-readable.

| Format | Best for |
|--------|----------|
| `table` (default) | Human reading in terminal |
| `json` | Scripting, piping, log ingestion |
| `csv` | Spreadsheets, ETL pipelines |

```bash
# JSON — pretty-printed
python dbtool.py query --conn my_mysql \
    --sql "SELECT * FROM orders LIMIT 3" \
    --format json

# CSV — pipe to file
python dbtool.py objects --conn my_mysql \
    --type tables --format csv > tables.csv

# No-colour for CI/CD logs
python dbtool.py connections list --no-color
```

---

## 9. Environment Variables

| Variable | Effect |
|----------|--------|
| `DBTOOL_DEBUG=1` | Print full Python traceback on errors (CLI) |

---

## 10. Troubleshooting

### GUI won't start — "No module named tkinter"

```bash
# macOS
brew install python-tk

# Ubuntu / Debian
sudo apt install python3-tk

# Fedora / RHEL
sudo dnf install python3-tkinter
```

### CLI error — "No module named tabulate"

```bash
pip install tabulate
```

### REST API error — "No module named fastapi"

```bash
pip install -r requirements-api.txt
```

### "Connection not found"

Connection profiles are stored encrypted in `~/.dbassistant/connections/`. If you
added a connection in the GUI, the CLI can use the same profile by the same name.
```bash
python dbtool.py connections list   # verify it appears
```

### psutil unavailable (Host / OS metrics show N/A)

```bash
source .venv/bin/activate
pip install psutil
```

### Oracle: "DPI-1047: Cannot locate a 64-bit Oracle Client library"

Set `oracle_client_path` in `config.ini` to your Instant Client directory,
or set the `LD_LIBRARY_PATH` / `DYLD_LIBRARY_PATH` environment variable.

### Daemon won't start — "Daemon already running"

```bash
python dbtool.py daemon status
python dbtool.py daemon stop
# If the process is truly gone, remove the stale PID file:
rm ~/.dbassistant/runtime/daemon.pid
```

### Check what the daemon is collecting

```bash
# Tail live log
tail -f ~/.dbassistant/runtime/daemon.log

# Inspect latest metrics snapshot
python3 -c "import json; d=json.load(open('$HOME/.dbassistant/runtime/metrics.json')); print(list(d.keys()))"
```

---

## Quick-reference cheat sheet

```
INSTALL     bash install.sh
GUI         python conDbUi.py
CLI help    python dbtool.py --help

CONNECTIONS
  list      python dbtool.py connections list
  add       python dbtool.py connections add --name X --type MySQL --host H --user U
  test      python dbtool.py connections test X
  remove    python dbtool.py connections remove X

QUERY       python dbtool.py query --conn X --sql "SELECT 1"
            python dbtool.py query --conn X --file q.sql --format json

OBJECTS     python dbtool.py objects --conn X [--type tables|views|procs|...]

MIGRATOR    python dbtool.py migrator convert --source-conn X --target-type PostgreSQL --table T

AI          python dbtool.py ai --conn X "natural language question"

MONITOR     python dbtool.py monitor --conn X [--once] [--interval 60]

DAEMON      python dbtool.py daemon start [--foreground]
            python dbtool.py daemon status
            python dbtool.py daemon stop

API         python dbtool.py api [--host 0.0.0.0] [--port 8000]
            → http://localhost:8000/docs
```
