# 🚀 Quick Start Guide

## Installation (3 minutes)

```bash
# 1. Navigate to project directory
cd DbManagementTool

# 2. Run setup script (recommended)
bash install.sh

# Or manual:
python3 -m venv .venv
source .venv/bin/activate
pip install -r setup/requirements.txt

# 3. Configure (edit with your credentials)
nano config.ini

# 4. Launch
python conDbUi.py          # full desktop UI (opens Dashboard tab)
# or
python dbtool.py ui        # same UI via master CLI
# Terminal UI (Textual) — pip install -r setup/requirements-ui.txt first
python dbtool.py tui
# Browser UI — loopback by default; set DBTOOL_WEBUI_API_KEY when exposing beyond localhost
python dbtool.py webui
```

## First-Time Setup Checklist

- [ ] Python 3.10+ installed
- [ ] Run `bash install.sh` or manual venv + pip install
- [ ] Edit `config.ini` (optional: `oracle_client_path` for Oracle thick mode)
- [ ] Install Oracle Instant Client only if you need thick mode or Oracle 11g
- [ ] Install an AI CLI backend if using AI module (`claude`, `cursor`, or `codex`)
- [ ] Check modules: `python dbtool.py modules`
- [ ] Launch: `python conDbUi.py` or `python dbtool.py ui`

## Three ways to use the tool

| Surface | Command | Notes |
|---------|---------|-------|
| **Desktop UI** | `python conDbUi.py` | Full tool; Dashboard opens first |
| **CLI** | `python dbtool.py <command>` | Headless; scriptable |
| **REST API** | `python dbtool.py api` | Open `/docs` for Swagger UI |

**Single module only:**

```bash
python dbtool.py ui --module migrator    # Data Migration
python dbtool.py ui --module ai        # AI Query Assistant
python dbtool.py ui --module monitor   # Monitoring

python -m schema_converter --ui
python -m ai_query api --port 8001
python -m monitoring daemon start
```

## Common Commands

### Installation
```bash
bash install.sh
pip install -r setup/requirements-api.txt   # REST API extras
python dbtool.py modules                    # installed / ready status
```

### Run Application
```bash
python conDbUi.py
python dbtool.py ui
python dbtool.py ui --module monitor
```

### CLI examples
```bash
python dbtool.py connections list
python dbtool.py query --conn mydb --sql "SELECT 1"
python dbtool.py migrator convert --help
python dbtool.py ai --conn mydb "count employees"
python dbtool.py monitor --conn mydb
```

### REST API
```bash
python dbtool.py api --host 127.0.0.1 --port 8000
curl http://127.0.0.1:8000/api/health
curl http://127.0.0.1:8000/api/dashboard
```

### Configuration
```bash
nano config.ini
nano properties.ini
ls -la ~/.dbassistant/connections/    # encrypted saved connections
```

## Quick Tips

### Keyboard Shortcuts
- **F5** - Execute SQL query
- **Ctrl+Enter** - Execute query (alternate)
- **Ctrl+Tab** - Cycle through tabs
- **Escape** - Close dialog

### Supported databases
Oracle, MySQL, MariaDB, PostgreSQL, SQLite, SQL Server, MongoDB, DocumentDB

### AI Query examples

**English:**
```
Show all employees hired after 2020
Calculate average salary by department
Find top 10 customers by revenue
```

### Connection profiles

1. **Add database connection** on Connections tab
2. **Test** then **Save**
3. Next time: **Load Saved** → connect

## Troubleshooting Quick Fixes

| Problem | Solution |
|---------|----------|
| "tkinter not found" | `sudo apt-get install python3-tk` (Linux) |
| Oracle DPI-1047 at startup | Leave `oracle_client_path` blank for thin mode, or set Instant Client path |
| "AI backend not found" | Install Claude/Cursor/Codex CLI; run `dbtool.py modules` |
| Module tab missing | Copy module folder + `pip install -r <module>/requirements.txt`; restart |
| Scrolling not working | Restart application |

## Next Steps

1. **Connect** — Connections tab → Add database connection
2. **Dashboard** — Overview cards; drag headers to rearrange
3. **Browse objects** — Database Objects tab
4. **Run SQL** — SQL Editor (F5)
5. **Optional modules** — AI, Data Migration, Monitor tabs

## Getting Help

- **HOW_TO_USE.md** — Full UI / CLI / API reference
- **MODULES.md** — Packaging and independent module shipping
- **Welcome tab** — In-app guide (full tool)
- **docs/HOWTORUN.md** — CLI and API command reference
