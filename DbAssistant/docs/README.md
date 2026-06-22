# 🗄️ Database Management Tool

A comprehensive, multi-database management application with AI-powered query assistance, real-time monitoring, and schema conversion capabilities.

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Platform](https://img.shields.io/badge/platform-macOS%20%7C%20Linux%20%7C%20Windows-lightgrey.svg)]()
[![Surfaces](https://img.shields.io/badge/surfaces-Desktop%20%7C%20TUI%20%7C%20Web%20%7C%20CLI%20%7C%20API-2196F3.svg)]()

> See the root [`README.md`](../README.md) for the production overview and the
> [`website/`](../website/) for the full reference site. The tool ships a shared
> core plus **four optional modules**: Data Migration, AI Query Assistant,
> Monitoring, and App Builder.

## 📋 Table of Contents

- [Features](#-features)
- [Supported Databases](#-supported-databases)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Project Structure](#-project-structure)
- [Documentation](#-documentation)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

## ✨ Features

### 🔌 **Universal Database Connectivity**
- Connect to multiple database systems simultaneously
- Encrypted credential storage with Fernet encryption
- Save and manage connection profiles
- Test connections before connecting

### 🤖 **AI-Powered Query Assistant**
- Natural language to SQL conversion via CLI backends (Claude, Cursor, or Codex)
- A **local, trainable** NL→SQL model (zero-dependency `python` engine; optional
  numpy/pytorch/ollama engines) usable as an offline backend
- **RAG** (retrieval-augmented generation): schema cards, relationships, glossary,
  NL→SQL examples, documents, and codebase indexed into a local vector store —
  see [RAG.md](RAG.md)
- Multi-language support (English, Japanese, and more)
- Context-aware query generation with schema understanding
- Query explanation, optimization, review, and auto-correction
- Conversational follow-up questions, multi-tab sessions, cross-tab references

### 🏗️ **App Builder**
- Generate runnable apps from scratch, an existing codebase, or a live database
- Governed, agentic build jobs with package/run/manage lifecycle

### 📊 **Database Operations**
- Browse database objects (tables, views, indexes, triggers, procedures)
- Execute SQL queries with syntax highlighting
- Export data to CSV, JSON formats
- Multi-tab result viewer
- Query history and favorites

### 🔄 **Data Migration**
- Convert schemas between different database platforms
- Intelligent type mapping (Oracle ↔ MySQL ↔ PostgreSQL ↔ SQLite)
- **Custom type override rules** (`varchar2:text`, `int:decimal`, etc.) via UI, CLI, or config
- **Configurable conversion charset** for multibyte text (Japanese, Chinese, Hindi, emoji)
- Data migration with progress tracking and optional parallel table transfer
- Document-to-document collection copy (MongoDB ↔ DocumentDB)
- **Partial migration**: per-table row filter / limit, column subset & rename
- **Continue-on-error** with per-row error reporting, plus truncate/skip/fail
  overflow policy and NULL/empty/boolean/timezone normalization
- **Pre-migration dry-run** validation (type incompatibilities, oversized
  columns, unsupported defaults)
- **Sequence reset**, **checkpoint/resume** for interrupted transfers, and a
  JSON **migration report** artifact
- Validation and error reporting

### 📈 **Real-Time Monitoring**
- Server resource monitoring (CPU, memory, processes)
- Database performance metrics
- Real-time graphs and visualizations
- SSH-based remote monitoring

### 🎨 **Modern UI**
- Clean, intuitive interface built with Tkinter
- Responsive design with mouse wheel scrolling
- Customizable themes and fonts via configuration
- Tab-based navigation

### 🔒 **Security**
- All credentials encrypted at rest
- Secure key storage with proper file permissions
- No plaintext password storage
- Connection string sanitization

## 🗃️ Supported Databases

| Database | Version | Status | Notes |
|----------|---------|--------|-------|
| **Oracle** | 11g - 21c | ✅ Full Support | Thin mode (12.1+) or Instant Client for thick/11g |
| **MySQL** | 5.7, 8.0+ | ✅ Full Support | Native connector |
| **MariaDB** | 10.x | ✅ Full Support | MySQL-compatible |
| **PostgreSQL** | 10 - 16+ | ✅ Full Support | psycopg2 driver |
| **SQLite** | 3.x | ✅ Full Support | Built-in support |
| **SQL Server** | 2012+ | ✅ Full Support | pymssql driver |
| **MongoDB** | 4.x+ | ✅ Full Support | Document queries in SQL Editor |
| **DocumentDB** | AWS | ✅ Full Support | TLS + MongoDB-compatible API |

## 📦 Prerequisites

### Required
- **Python 3.10+** - [Download](https://www.python.org/downloads/)
- **Tkinter** - Usually included with Python
  - Linux: `sudo apt-get install python3-tk`
  - macOS/Windows: Included

### Optional (for specific features)
- **An AI CLI backend** - For AI Query Assistant: `claude` ([download](https://claude.ai/download)), `cursor` ([cursor.com](https://cursor.com)), or `codex` ([openai/codex](https://github.com/openai/codex))
- **Oracle Instant Client** - For Oracle connectivity ([Download](https://www.oracle.com/database/technologies/instant-client.html))
- **sshpass** - For SSH Monitoring
  - macOS: `brew install hudochenkov/sshpass/sshpass`
  - Linux: `sudo apt-get install sshpass`

## 🚀 Quick Start

### 1. Clone or Download

```bash
cd /path/to/DbManagementTool
```

### 2. Run Setup

```bash
# Recommended (macOS / Linux)
bash install.sh

# Or manual
python3 -m venv .venv
source .venv/bin/activate
pip install -r setup/requirements.txt
```

This will:
- ✅ Check Python version
- ✅ Install required dependencies
- ✅ Install optional database drivers
- ✅ Create configuration files
- ✅ Set up data directories

### 3. Configure

Edit `config.ini` with your database credentials and paths:

```ini
[paths]
oracle_client_path =          # optional — leave blank for oracledb thin mode

[database.ports]
oracle = 1521
mysql = 3306
postgresql = 5432
sqlserver = 1433
mongodb = 27017
```

### 4. Run the Application

**Desktop UI (full tool):**

```bash
python conDbUi.py
# or
python dbtool.py ui
```

**Headless CLI:**

```bash
python dbtool.py modules          # list installed modules
python dbtool.py connections list
python dbtool.py query --conn NAME --sql "SELECT 1"
```

**REST API:**

```bash
python dbtool.py api --host 127.0.0.1 --port 8000
# Open http://127.0.0.1:8000/docs
```

**Single module (UI / CLI / API):**

```bash
python dbtool.py ui --module migrator   # or ai, monitor
python -m monitoring --ui
python -m ai_query api --port 8001
```

See [`HOW_TO_USE.md`](../HOW_TO_USE.md) and [`MODULES.md`](../MODULES.md) for full details.

## 📥 Installation

### Method 1: Automated Setup (Recommended)

```bash
bash install.sh
```

### Method 2: Manual Installation

1. **Create virtual environment and install dependencies**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r setup/requirements.txt
   pip install -r setup/requirements-api.txt   # REST API only
   ```

2. **Install Database Drivers** (as needed — most are in requirements.txt)
   ```bash
   pip install oracledb pymssql pymongo mysql-connector-python psycopg2-binary
   ```

3. **Create Configuration Files**
   ```bash
   cp config.ini.example config.ini
   cp properties.ini.example properties.ini
   ```

4. **Edit Configuration**
   - Update `config.ini` with your database credentials
   - Customize `properties.ini` for UI preferences (optional)

## ⚙️ Configuration

### Configuration Files

The application uses two configuration files:

#### `config.ini` - Core engine & path settings
```ini
[paths]
oracle_client_path = /path/to/instantclient   # blank = oracledb thin mode

[database.ports]
oracle = 1521
mysql = 3306
postgresql = 5432

[database.connection]
connection_timeout = 30.0
query_timeout = 0
default_autocommit = true
max_connection_attempts = 8
```

> SSH/monitoring (`monitoring/monitor_config.ini`) and AI backend settings
> (`ai_query/config.ini`) are **module-owned** and no longer live in core
> `config.ini`. See [Configuration](#-configuration) and `MODULES.md`.

#### `properties.ini` - UI Settings
```ini
[ui.window]
main_window_width = 1150
main_window_height = 780

[ui.colors.primary]
primary = #2196F3
primary_dark = #1976D2

[monitoring.graphs]
metric_graph_width = 250
metric_graph_height = 52

[logging]
enable_stdout = false  # Suppress console output
```

### Data Storage

All application data is stored under `~/.dbassistant/` (override with the
`DBASSISTANT_HOME` environment variable):
- `connections/` - Encrypted connection profiles (DB, cloud, monitor)
- `keys/` - Fernet encryption keys (chmod 600)
- `runtime/` - Daemon PID, log, `metrics.json`, alerts log
- `session/` - AI sessions and dashboard layout

**Security Note:** Never commit these files to version control!

## 📖 Usage

### Connecting to a Database

1. **Open Connections Tab**
2. **Fill in connection details:**
   - Database Type (Oracle, MySQL, PostgreSQL, etc.)
   - Host, Port, Database/Service
   - Username, Password
3. **Test Connection** (optional)
4. **Click "Connect"**
5. **Save Connection** for future use (optional)

### Using AI Query Assistant

1. **Connect to a database**
2. **Open "AI Query Assistant" tab**
3. **Type your question in natural language:**
   
   **English:**
   ```
   Show all employees with salary greater than 50000
   ```
   
   **Japanese:**
   ```
   給料が5万円以上の従業員を全て表示してください
   ```

4. **Review generated SQL**
5. **Execute or modify the query**

### Executing SQL Queries

1. **Open "SQL Editor" tab**
2. **Write or paste your SQL**
3. **Press F5 or click "Execute"**
4. **View results in the results pane**
5. **Export data** if needed (CSV, JSON)

### Converting Schemas

1. **Open "Data Migration" tab**
2. **Select source connection and database**
3. **Load tables**
4. **Select tables to convert**
5. **Select target connection and database**
6. **Click "Convert Schema"**
7. **Review conversion results**
8. **Optionally migrate data**

### Monitoring Servers

1. **Open "Monitor" tab**
2. **Add server connection** (SSH credentials)
3. **Start monitoring**
4. **Toggle between text/graph view**
5. **Monitor OS metrics** (CPU, Memory, Processes)
6. **Monitor database metrics** (Connections, Cache, Sessions)

## 🏗️ Project Structure

```
DbManagementTool/
├── conDbUi.py                  # Desktop UI entry (shim → common.ui)
├── dbtool.py                   # Master CLI entry (shim → app.dbtool)
├── HOW_TO_USE.md               # Primary user guide (UI / CLI / API)
├── MODULES.md                  # Modular packaging & independent shipping
├── config.ini / properties.ini # Runtime configuration
│
├── common/                     # Shared core (always shipped)
│   ├── drivers/                # Database connectors
│   ├── dashboard/              # Dashboard tab + /api/dashboard
│   ├── headless/               # REST API factory, CoreDBService
│   └── ui/                     # master_shell.py (unified desktop UI)
│
├── app/                        # Full-tool layer (master CLI + DBService)
├── schema_converter/           # Optional module — Data Migration
├── ai_query/                   # Optional module — AI Query Assistant (RAG + LLM)
├── monitoring/                 # Optional module — Monitoring
└── ai_assistant/               # AI subpackages
    ├── app_builder/            #   Optional module — App Builder
    ├── rag/                    #   RAG Manager engine
    └── llm/                    #   Local trainable NL→SQL model
```

## 📚 Documentation

- **[HOW_TO_USE.md](../HOW_TO_USE.md)** — Complete guide: UI, CLI, REST API, all modules
- **[MODULES.md](../MODULES.md)** — Independent module packaging and entry points
- **[HOWTORUN.md](HOWTORUN.md)** — CLI, API, daemon quick reference
- **[QUICKSTART.md](QUICKSTART.md)** — Short getting-started guide
- **Welcome tab** — In-app overview (full tool only)

## 🔧 Troubleshooting

### Common Issues

#### **"tkinter not found"**
```bash
# Linux
sudo apt-get install python3-tk

# macOS/Windows - tkinter is included with Python
```

#### **"Oracle client not found"**
1. Download Oracle Instant Client
2. Update `oracle_client_path` in `config.ini`
3. Set `LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH` (macOS)

#### **"AI backend not available"**
- AI Query Assistant needs at least one CLI backend: `claude`, `cursor`, or `codex`
- Install one (e.g. Claude Code from https://claude.ai/download) and ensure it is on `PATH`
- If the app runs from a GUI and can't find an installed CLI, set `cli_path` under
  `[ai.claude]` / `[ai.codex]` in `ai_query/config.ini`

#### **"SSH connection timeout"**
- Install sshpass for password-based SSH
- Or use SSH keys for authentication
- Check firewall settings

#### **Mouse wheel scrolling not working**
- Should work automatically after latest updates
- Restart application if issues persist

#### **Slow performance with large databases**
- Adjust `max_tables_fetch` under `[ai.cache]` in `ai_query/config.ini`
- Use query filters to limit results

### Getting Help

1. Check the documentation in `CLAUDE.md`
2. Review configuration in `config.ini` and `properties.ini`
3. Check log files in application directory
4. Search GitHub issues (if applicable)

## 🛠️ Development

### Testing

[`tests/TEST_TYPE_REFERENCE.md`](../tests/TEST_TYPE_REFERENCE.md) is the single
source of truth for what "testing" means in this repo — scopes (full /
module-wise / changes-only), required test types, the surfaces to exercise
(CLI, API, UI), real connections/data, and the reporting format. Read it
**before** any testing task. When asked for "full testing / all modules / all
functionality", run its **Scope A** end-to-end.

```bash
source .venv/bin/activate
python -m pytest -q                     # automated suite
```

### Running from Source

```bash
# Clone the repository
git clone <repository-url>
cd DbManagementTool

# Run setup
bash install.sh

# Run the application
python dbtool.py ui            # or: python conDbUi.py
```

### Adding a New Database Type

1. Create connector module: `conNewDB.py`
2. Implement standard operations: `connect`, `disconnect`, `getTables`, etc.
3. Register in `database_registry.py`
4. Add type mappings in `schema_converter.py` (optional)
5. Test connectivity

See `CLAUDE.md` for detailed development guidelines.

## 🎯 Roadmap

- [ ] Query performance analyzer
- [ ] Automated backup/restore
- [ ] Plugin system for third-party extensions

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

See `CLAUDE.md` for coding standards and architecture details.

## 📄 License

**MIT License** — Copyright (c) 2026 Dhananjay Chaturvedi. See
[`LICENSE`](../LICENSE) in the project root for full terms.

## Acknowledgments

- **Anthropic** - Claude AI for natural language query generation
- **Python Software Foundation** - Python and Tkinter
- **Database vendors** - Oracle, MySQL, PostgreSQL, MariaDB, SQLite
- **Open source community** - All the amazing libraries used in this project

## 📞 Support

For support, please:
- Check the troubleshooting section above
- Review documentation in `CLAUDE.md`
- Check configuration examples

---

**Made for Database Administrators, Developers, and Data Engineers**

**Enterprise-ready • Multi-language • AI-powered • Secure**