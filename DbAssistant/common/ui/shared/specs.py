"""
Shared, declarative UI object specs — the "common objects" every UI renders.

These are framework-agnostic definitions (labels, control types, option lists,
defaults, grouping) drawn from the Tk desktop UI, which is the source of truth.
Tk, Textual and Web all read these so a change here shows up everywhere; each UI
keeps only its own render-specific values (geometry, fonts, CSS) locally.

Dependency-light: pure data + tiny helpers, no UI toolkit imports.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Connections tab. The Tk desktop tab has four collapsible sections, each with
# its own controls/buttons. All three UIs render the SAME sections, fields and
# button sets defined here. Engine-specific field visibility and SSL option
# lists still come from the live service metadata (/api/connections/metadata).
# ---------------------------------------------------------------------------
SSH_AUTH_METHODS = [
    {"value": "password", "label": "Password"},
    {"value": "key", "label": "Key file"},
]

# Section 1 — Active connections.
ACTIVE_CONNECTION_ACTIONS = [
    {"id": "disconnect", "label": "Disconnect Selected"},
    {"id": "disconnect_all", "label": "Disconnect All"},
]

# Saved connections — inline list of stored profiles (present in every UI).
SAVED_CONNECTION_ACTIONS = [
    {"id": "refresh", "label": "Refresh"},
    {"id": "load", "label": "Load"},
    {"id": "connect", "label": "Connect"},
    {"id": "test", "label": "Test"},
    {"id": "remove", "label": "Remove"},
]

# Section 2 — Direct (localhost) database connection.
DIRECT_FORM_FIELDS = [
    {"id": "name", "label": "Connection name", "type": "text", "required": True},
    {"id": "db_type", "label": "Database Type", "type": "enum", "source": "metadata"},
    {"id": "host", "label": "Host", "type": "text", "default": "localhost"},
    {"id": "port", "label": "Port", "type": "text", "source": "metadata"},
    {"id": "database", "label": "Database name", "type": "text",
     "dynamic_label": True, "note": "Oracle uses 'Service name'"},
    {"id": "user", "label": "Username", "type": "text"},
    {"id": "password", "label": "Password", "type": "secret"},
    {"id": "save_password", "label": "Save password (encrypted)", "type": "bool",
     "default": True},
]
DIRECT_ACTIONS = [
    {"id": "connect", "label": "Connect"},
    {"id": "test", "label": "Test Connection"},
    {"id": "load_saved", "label": "Load Saved"},
    {"id": "save", "label": "Save Connection"},
    {"id": "clear", "label": "Clear"},
]

# Section 3 — Remote database connection (reach DB via an SSH tunnel). DB
# host/port are the endpoint as seen FROM the SSH host (often localhost).
REMOTE_FORM_FIELDS = [
    {"id": "name", "label": "Connection name", "type": "text", "required": True},
    {"id": "db_type", "label": "Database type", "type": "enum", "source": "metadata"},
    {"id": "host", "label": "DB host", "type": "text", "default": "localhost"},
    {"id": "port", "label": "DB port", "type": "text", "source": "metadata"},
    {"id": "database", "label": "Database / Service", "type": "text"},
    {"id": "user", "label": "DB username", "type": "text"},
    {"id": "password", "label": "DB password", "type": "secret"},
    {"id": "save_password", "label": "Save passwords (encrypted)", "type": "bool",
     "default": True},
]
REMOTE_SSH_FIELDS = [
    {"id": "ssh_host", "label": "SSH host", "type": "text", "required": True},
    {"id": "ssh_port", "label": "SSH port", "type": "text", "default": "22"},
    {"id": "ssh_user", "label": "SSH username", "type": "text", "required": True},
    {"id": "ssh_auth", "label": "SSH auth", "type": "enum", "choices": SSH_AUTH_METHODS},
    {"id": "ssh_password", "label": "SSH password", "type": "secret", "when_auth": "password"},
    {"id": "ssh_key_file", "label": "SSH key file", "type": "file", "when_auth": "key"},
]
REMOTE_ACTIONS = [
    {"id": "connect", "label": "Connect"},
    {"id": "test", "label": "Test Connection"},
    {"id": "load_saved", "label": "Load Saved"},
    {"id": "save", "label": "Save"},
    {"id": "clear", "label": "Clear"},
]

# Tk security/TLS fields; shown per-engine capability (ssl_fields / supports_ssl).
# Only the Direct section renders these (the Tk remote panel has no SSL group).
CONNECTION_SSL_FIELDS = [
    {"id": "ssl_mode", "label": "SSL mode", "type": "enum", "source": "metadata"},
    {"id": "ssl_ca", "label": "SSL CA file", "type": "text", "cap": "ca"},
    {"id": "ssl_cert", "label": "SSL client cert", "type": "text", "cap": "cert"},
    {"id": "ssl_key", "label": "SSL client key", "type": "text", "cap": "key"},
    {"id": "wallet_location", "label": "Oracle wallet dir", "type": "text", "cap": "wallet"},
]
CONNECTION_TLS_FIELDS = [
    {"id": "tls", "label": "Use TLS (MongoDB / DocumentDB)", "type": "bool"},
    {"id": "tls_ca_file", "label": "TLS CA file", "type": "text"},
]

# Canonical Connections-tab layout shown by every UI. This list is the single
# source of truth for SECTION ORDER and the COLLAPSED-BY-DEFAULT state: only
# "Active connections" is expanded; all other sections start collapsed. Tk,
# Textual and Web all derive their section order and initial collapse state from
# here, so a layout change is made once and propagates to all three UIs.
CONNECTION_SECTIONS = [
    {"id": "active", "title": "Active connections", "collapsed": False,
     "actions": ACTIVE_CONNECTION_ACTIONS},
    {"id": "saved", "title": "Saved connections", "collapsed": True,
     "actions": SAVED_CONNECTION_ACTIONS},
    {"id": "direct", "title": "Add or select database connection", "collapsed": True,
     "fields": DIRECT_FORM_FIELDS, "ssl": True, "actions": DIRECT_ACTIONS},
    {"id": "remote", "title": "Add or select remote database connection",
     "collapsed": True,
     "fields": REMOTE_FORM_FIELDS, "ssh": REMOTE_SSH_FIELDS, "actions": REMOTE_ACTIONS},
    {"id": "cloud", "title": "Add or select cloud database connection",
     "collapsed": True, "providers": ["AWS", "Azure", "GCP", "Other"]},
]


def connection_section(section_id: str) -> dict:
    """Return the shared spec for one Connections-tab section (or ``{}``)."""
    for section in CONNECTION_SECTIONS:
        if section["id"] == section_id:
            return section
    return {}


def connection_section_collapsed(section_id: str) -> bool:
    """Whether the named Connections-tab section starts collapsed (shared)."""
    return bool(connection_section(section_id).get("collapsed", True))

# ---------------------------------------------------------------------------
# SQL Editor actions (toolbar parity with Tk).
# ---------------------------------------------------------------------------
SQL_EDITOR_ACTIONS = [
    {"id": "run_cursor", "label": "Execute at cursor (F5)", "shortcut": "F5"},
    {"id": "run_selected", "label": "Execute selected"},
    {"id": "run_all", "label": "Execute all"},
    {"id": "stop", "label": "Stop Query"},
    {"id": "format", "label": "Format SQL", "shortcut": "Ctrl+Shift+F"},
    {"id": "commit", "label": "Commit"},
    {"id": "rollback", "label": "Rollback"},
    {"id": "clear", "label": "Clear editor"},
    {"id": "load", "label": "Load query"},
    {"id": "save", "label": "Save query"},
    {"id": "history", "label": "Query history"},
    {"id": "export", "label": "Export Data"},
]
SQL_RESULT_MENU = [
    "Copy Cell", "Copy Row", "Copy Column", "Copy All Data",
    "Sort Ascending", "Sort Descending", "Filter Column...", "Clear Filter",
]

# Grouped SQL Editor layout shared by every UI (single source for labels/order).
# Stable action ids map to each UI's native widget ids; only labels are stamped
# from here so Tk, Textual and Web never drift (e.g. "Refresh connections").
SQL_CONNECTION_ACTIONS = [
    {"id": "refresh", "label": "Refresh connections"},
]
SQL_AUTOCOMMIT_LABEL = "Auto-commit"
SQL_EDITOR_TOOLBAR = [
    {"id": "run_cursor", "label": "Execute at cursor (F5)", "shortcut": "F5"},
    {"id": "run_selected", "label": "Execute selected"},
    {"id": "run_all", "label": "Execute all"},
    {"id": "stop", "label": "Stop Query"},
    {"id": "clear", "label": "Clear editor"},
    {"id": "load", "label": "Load query"},
    {"id": "save", "label": "Save query"},
    {"id": "format", "label": "Format SQL"},
    {"id": "autocomplete", "label": "Autocomplete: On", "labelOff": "Autocomplete: Off"},
    {"id": "commit", "label": "Commit"},
    {"id": "rollback", "label": "Rollback"},
]
SQL_RESULT_ACTIONS = [
    {"id": "copy_all", "label": "Copy All Data"},
    {"id": "sort_asc", "label": "Sort Ascending"},
    {"id": "sort_desc", "label": "Sort Descending"},
    {"id": "filter", "label": "Filter Column..."},
    {"id": "clear_filter", "label": "Clear Filter"},
    {"id": "clear_results", "label": "Clear All Results"},
    {"id": "export", "label": "Export Data"},
]
SQL_TAB_ACTIONS = [
    {"id": "new", "label": "+"},
]


def sql_editor_payload() -> dict:
    """SQL Editor layout shared by every UI (rendered natively by each)."""
    return {
        "connectionActions": SQL_CONNECTION_ACTIONS,
        "autocommitLabel": SQL_AUTOCOMMIT_LABEL,
        "editorActions": SQL_EDITOR_TOOLBAR,
        "resultActions": SQL_RESULT_ACTIONS,
        "resultMenu": SQL_RESULT_MENU,
        "tabActions": SQL_TAB_ACTIONS,
        # Back-compat: flat toolbar list kept for older consumers.
        "actions": SQL_EDITOR_ACTIONS,
    }

# ---------------------------------------------------------------------------
# Database Objects tab. Layout shared by every UI: a Tk-like header row
# (connection + refresh + import), then a horizontal paned layout with an
# "Object types" pane on the left and a "Results" pane on the right. Object
# TYPES stay engine-driven (service metadata: list_db_ops /
# supported_object_types), so they are NOT hard-coded here.
# ---------------------------------------------------------------------------
OBJECTS_LAYOUT = {
    "headerTitle": "Browse objects",
    "objectTypesTitle": "Object types",
    "objectTypesHint": "Choose an object type to browse.",
    "resultsTitle": "Results",
    "emptyResultsTitle": "No objects loaded",
    "emptyResultsHint": "Choose an object type on the left to list database objects.",
    "filterLabel": "Filter:",
    "filterClearLabel": "Clear",
    "tableCardHint": "▶ expands schema; Load Sample Data shows one row; Export Data saves rows.",
}
OBJECTS_TOOLBAR_ACTIONS = [
    {"id": "refresh", "label": "Refresh"},
    {"id": "import_jump", "label": "Import Data"},
]
OBJECTS_LIST_ACTIONS = [
    {"id": "clear_results", "label": "Clear results"},
]
# Actions for the currently-selected object (table/collection).
OBJECTS_DETAIL_ACTIONS = [
    {"id": "schema", "label": "Schema"},
    {"id": "sample", "label": "Load Sample Data"},
    {"id": "count", "label": "Row count"},
    {"id": "export_selected", "label": "Export Data"},
]
OBJECTS_EXPORT_FIELDS = [
    {"id": "exp_table", "label": "Export table", "type": "text"},
    {"id": "exp_path", "label": "Output path (server)", "type": "text",
     "placeholder": "/tmp/out.csv"},
    {"id": "exp_fmt", "label": "Format", "type": "enum", "choices": ["csv", "json"],
     "default": "csv"},
    {"id": "exp_limit", "label": "Limit (optional)", "type": "int"},
]
OBJECTS_IMPORT_FIELDS = [
    {"id": "imp_path", "label": "Import CSV path (server)", "type": "text",
     "placeholder": "/tmp/in.csv"},
    {"id": "imp_table", "label": "Target table (optional)", "type": "text"},
    {"id": "imp_create", "label": "Create table if missing", "type": "bool",
     "default": True},
]


def objects_payload() -> dict:
    """Database Objects layout shared by every UI (rendered natively by each)."""
    return {
        "layout": OBJECTS_LAYOUT,
        "toolbarActions": OBJECTS_TOOLBAR_ACTIONS,
        "listActions": OBJECTS_LIST_ACTIONS,
        "detailActions": OBJECTS_DETAIL_ACTIONS,
        "exportFields": OBJECTS_EXPORT_FIELDS,
        "importFields": OBJECTS_IMPORT_FIELDS,
    }

# ---------------------------------------------------------------------------
# Data Migration — conversion/transfer options (the Tk "G1..G10" set).
# ---------------------------------------------------------------------------
MIGRATION_OPTIONS = [
    {"id": "create_indexes", "label": "Create Indexes (with schema)", "type": "bool",
     "default": True},
    {"id": "drop_if_exists", "label": "Drop Table If Exists (before schema conversion)",
     "type": "bool", "default": False},
    {"id": "batch_size", "label": "Batch Size (for data transfer)", "type": "int",
     "default": 1000},
    {"id": "parallel", "label": "Run data transfer in parallel", "type": "bool",
     "default": False},
    {"id": "workers", "label": "Parallel Workers", "type": "int", "default": 1},
    {"id": "type_map", "label": "Type mapping rules (e.g. varchar2:text,int:decimal)",
     "type": "text", "default": ""},
    {"id": "where", "label": "Row filter (WHERE, single table)", "type": "text",
     "single_table": True},
    {"id": "limit", "label": "Row limit (per table)", "type": "int"},
    {"id": "columns", "label": "Columns (subset, single table)", "type": "text",
     "single_table": True},
    {"id": "column_map", "label": "Column rename (src:tgt,..., all tables)", "type": "text"},
    {"id": "checkpoint", "label": "Checkpoint / resume", "type": "bool", "default": False},
    {"id": "report", "label": "Report file", "type": "text"},
    {"id": "overflow", "label": "Overflow policy", "type": "enum",
     "choices": ["", "fail", "truncate", "skip"]},
    {"id": "null_policy", "label": "NULL/empty policy", "type": "enum",
     "choices": ["", "keep", "empty_to_null", "null_to_empty"]},
    {"id": "bool_policy", "label": "Boolean policy", "type": "enum",
     "choices": ["", "auto", "int", "true_false"]},
    {"id": "tz_policy", "label": "Timezone policy", "type": "enum",
     "choices": ["", "preserve", "naive", "utc", "target"]},
    {"id": "target_tz", "label": "Target timezone (when policy=target)", "type": "text"},
    {"id": "reset_seq", "label": "Reset target sequences after load", "type": "bool"},
]
MIGRATION_ACTIONS = [
    {"id": "preview", "label": "Preview Schema"},
    {"id": "row_counts", "label": "Row Counts"},
    {"id": "sample", "label": "Sample Data"},
    {"id": "validate", "label": "Validate (Dry-run)"},
    {"id": "convert", "label": "Convert Schema"},
    {"id": "apply", "label": "Apply DDL"},
    {"id": "transfer", "label": "Transfer Data"},
    {"id": "compare", "label": "Compare Data"},
    {"id": "clear", "label": "Clear Preview"},
]

# ---------------------------------------------------------------------------
# AI Query — actions, SQL modes, settings (parity with Tk AI tab).
# ---------------------------------------------------------------------------
AI_ACTIONS = [
    {"id": "generate", "label": "Generate SQL"},
    {"id": "execute", "label": "Execute query"},
    {"id": "stop", "label": "Stop Query"},
    {"id": "explain", "label": "Explain query"},
    {"id": "optimize", "label": "Optimize"},
    {"id": "review", "label": "Run Review"},
    {"id": "clear", "label": "Clear all"},
]
# Inline controls beside Generate (parity with the Tk question toolbar): load a
# batch of natural-language questions from a file and iterate them, RAG toggle,
# index RAG, and train the local LLM. Kept separate from AI_ACTIONS so the core
# action set/order stays stable.
AI_QUESTION_TOOLS = [
    {"id": "questions_file", "label": "Questions from file"},
    {"id": "index_rag", "label": "Index RAG"},
    {"id": "train_llm", "label": "Train LLM"},
]
AI_USE_RAG_LABEL = "Use RAG"

# RAG Manager — shared labels/actions across Tk, TUI, and Web.
RAG_MANAGER_TITLE = "RAG Manager"
RAG_MANAGER_ACTIONS = [
    {"id": "overview", "label": "Overview (status + breakdown)"},
    {"id": "index", "label": "Index schema"},
    {"id": "reindex", "label": "Re-index schema"},
    {"id": "codebase", "label": "Add codebase folder"},
    {"id": "document", "label": "Add document"},
    {"id": "docs", "label": "List documents"},
    {"id": "preview", "label": "Preview search"},
    {"id": "eval", "label": "Evaluate retrieval quality"},
    {"id": "drift", "label": "Check schema drift"},
    {"id": "reindex_stale", "label": "Re-index if stale"},
    {"id": "schedule_status", "label": "Scheduled re-index: status"},
    {"id": "schedule_start", "label": "Scheduled re-index: start"},
    {"id": "schedule_stop", "label": "Scheduled re-index: stop"},
    {"id": "seed", "label": "Seed analytical patterns"},
    {"id": "analytics", "label": "Show analytical library"},
    {"id": "example", "label": "Add NL→SQL example"},
    {"id": "examples_file", "label": "Import examples from file"},
    {"id": "glossary", "label": "Add glossary term"},
    {"id": "help", "label": "How to use RAG"},
    {"id": "clear", "label": "Clear collection"},
]
RAG_MANAGER_SCOPE_LABELS = {
    "database": "Active database",
    "standalone": "Standalone collection",
    "collection_name": "Collection name",
}
RAG_MANAGER_BUTTONS = [
    {"id": "index_schema", "label": "Index Schema"},
    {"id": "reindex", "label": "Re-index"},
    {"id": "overview", "label": "Overview"},
    {"id": "add_codebase", "label": "Add Codebase"},
    {"id": "add_document", "label": "Add Document"},
    {"id": "paste_content", "label": "Paste Content"},
    {"id": "breakdown", "label": "Breakdown"},
    {"id": "preview", "label": "Preview"},
]

AI_SQL_ACTIONS = [
    {"id": "copy", "label": "Copy SQL"},
    {"id": "edit", "label": "Edit SQL"},
    {"id": "send_editor", "label": "Send to SQL Editor"},
    {"id": "exec_rules", "label": "SQL execution rules"},
]
AI_SQL_MODES = [
    {"value": "strict_summary", "label": "Strict summary mode"},
    {"value": "summary", "label": "Summary mode"},
    {"value": "open", "label": "Open mode"},
]
# The Tk AI tab shows results in a notebook with these tabs (in this order).
# All three UIs render the SAME tab set so Explain → Explanation, Optimize →
# Optimization, Execute → Query results, Run Review → Review, and follow-ups →
# Chat all land in the matching pane.
AI_RESULT_TABS = ["Query results", "Explanation", "Optimization", "RAG context",
                  "Chat", "Review"]

# Chat / follow-up controls live inside the Chat result tab (parity with the Tk
# "Send Follow-up Message" pane). The two flag buttons mark a generated query as
# wrong (syntax/logic) or wrongly interpreted; both route through the fallback /
# primary backend to repair it (svc.correct_sql).
AI_CHAT_ACTIONS = [
    {"id": "send_followup", "label": "Send Follow-up"},
    {"id": "clear_chat", "label": "Clear Chat"},
    {"id": "flag_query", "label": "Flag incorrect query"},
    {"id": "flag_interpretation", "label": "Flag incorrect interpretation"},
]
AI_UNINTERRUPTED_LABEL = "Uninterrupted follow-ups"

# Fallback backend (Tk status row): a second backend that takes over when the
# primary is unreachable and repairs wrong/failed SQL. Single-sourced label.
AI_FALLBACK_LABEL = "Fallback backend"
AI_FALLBACK_HINT = "(failover + corrects wrong/failed SQL)"

# Build Apps — grouped entry before schema-cache controls in the AI toolbar.
# Houses the AppBuilderAssistant (ai_assistant package). The local LLM trainer
# and RAG manager live in the Generate-SQL surface, not here.
AI_BUILD_APPS_ACTIONS = [
    {"id": "app_builder", "label": "App Builder"},
]

AI_SETTINGS = [
    {"id": "default_backend", "label": "Default backend", "type": "enum",
     "choices": ["auto", "claude", "cursor", "codex"], "default": "auto"},
    {"id": "mask_pii", "label": "Mask PII in prompts", "type": "bool", "default": True},
    {"id": "max_sessions", "label": "Max in-memory sessions", "type": "int", "default": 20},
    {"id": "claude_timeout", "label": "Claude timeout (s)", "type": "int", "default": 120},
    {"id": "cursor_model", "label": "Cursor model", "type": "text", "default": "auto"},
    {"id": "cursor_timeout", "label": "Cursor timeout (s)", "type": "int", "default": 60},
    {"id": "codex_model", "label": "Codex model (blank=default)", "type": "text", "default": ""},
    {"id": "codex_timeout", "label": "Codex timeout (s)", "type": "int", "default": 120},
    {"id": "auto_execute_ai_loop", "label": "Auto-execute AI loop", "type": "bool",
     "default": False},
    {"id": "auto_execute_summary_sql", "label": "Auto-run summary SQL", "type": "bool",
     "default": False},
    {"id": "auto_loop_max", "label": "Auto-loop max iterations", "type": "int", "default": 5},
    {"id": "default_sql_mode", "label": "Default SQL mode", "type": "enum",
     "choices": ["strict_summary", "summary", "open"], "default": "summary"},
]

# ---------------------------------------------------------------------------
# Cloud providers (for the Connections cloud panel + Monitoring).
# ---------------------------------------------------------------------------
CLOUD_PROVIDERS = ["AWS", "Azure", "GCP", "Other"]
CLOUD_AUTH_TABS = ["Access keys / tokens", "Username / password", "SSO"]

# ---------------------------------------------------------------------------
# Monitoring tab. The Tk desktop tab is the reference: a status bar (Monitor
# Settings / Alert Thresholds) over THREE sections — Server, Database and Cloud —
# each with its OWN saved-targets list and target controls (Add / Select / Remove)
# plus its own metrics panel. "Add" creates a saved target; "Select" starts
# monitoring it (adds it to that section's active set — many can run at once);
# "Remove" stops monitoring an active target or deletes a saved one. A shared
# refresh tick polls every active target across the three sections concurrently.
# All three UIs single-source these section titles, the per-section target-action
# labels, the view toolbar and the top settings here, so a label/section change is
# made once and shows up everywhere.
# ---------------------------------------------------------------------------
MONITOR_TOP_ACTIONS = [
    {"id": "settings", "label": "\u2699 Monitor Settings"},
    {"id": "thresholds_settings", "label": "\u2699 Alert Thresholds"},
]
MONITOR_SECTIONS = [
    {"id": "server", "title": "Server monitoring", "metricsTitle": "OS metrics",
     "targetActions": [
         {"id": "add", "label": "Add Connection"},
         {"id": "select", "label": "Select Server"},
         {"id": "remove", "label": "Remove Server"},
     ]},
    {"id": "database", "title": "Database Monitoring", "metricsTitle": "Database Metrics",
     "targetActions": [
         {"id": "add", "label": "Add Database"},
         {"id": "select", "label": "Select Database"},
         {"id": "remove", "label": "Remove Database"},
     ]},
    {"id": "cloud", "title": "Cloud Resource Monitoring",
     "metricsTitle": "Cloud Resource Metrics",
     "targetActions": [
         {"id": "add", "label": "Add Cloud Resource"},
         {"id": "select", "label": "Select Resource"},
         {"id": "remove", "label": "Remove Resource"},
     ]},
]
# Shared metrics view toolbar — the Tk per-section right toolbar.
MONITOR_VIEW_ACTIONS = [
    {"id": "show_graphs", "label": "\U0001F4CA Show Graphs"},
    {"id": "show_text", "label": "\U0001F4DD Show Text"},
    {"id": "clear_graphs", "label": "Clear Graphs"},
    {"id": "refresh", "label": "Refresh"},
    {"id": "alerts", "label": "Alerts"},
]
# Threshold / alert management (Tk uses a separate editor window; TUI and Web
# expose these inline). Single-sourced so the labels stay in step.
MONITOR_THRESHOLD_ACTIONS = [
    {"id": "load", "label": "Thresholds"},
    {"id": "edit", "label": "Edit threshold"},
    {"id": "check", "label": "Check threshold"},
    {"id": "clear_alerts", "label": "Clear alerts"},
]


def monitoring_section(section_id: str) -> dict:
    """Return one Monitoring section spec by id (server/database/cloud)."""
    for section in MONITOR_SECTIONS:
        if section["id"] == section_id:
            return section
    return {}


def monitoring_payload() -> dict:
    """Monitoring-tab layout/labels shared by every UI (rendered natively).

    Mirrors the Tk Monitoring tab: top actions, the three sections (each with a
    title, metrics title and Add/Select/Remove target actions), the shared view
    toolbar and the threshold actions.
    """
    return {
        "topActions": MONITOR_TOP_ACTIONS,
        "sections": MONITOR_SECTIONS,
        "viewActions": MONITOR_VIEW_ACTIONS,
        "thresholdActions": MONITOR_THRESHOLD_ACTIONS,
    }

# ---------------------------------------------------------------------------
# Keyboard shortcuts (Tk Help → Keyboard shortcuts).
# ---------------------------------------------------------------------------
KEYBOARD_SHORTCUTS = [
    {"keys": "F5", "action": "Execute query at cursor"},
    {"keys": "Ctrl+Enter", "action": "Execute query at cursor"},
    {"keys": "Ctrl+Shift+F", "action": "Format SQL"},
    {"keys": "Ctrl+Space", "action": "Autocomplete"},
    {"keys": "Ctrl+Tab", "action": "Next tab"},
    {"keys": "Escape", "action": "Close popup / cancel"},
]


# ---------------------------------------------------------------------------
# Welcome tab — documentation/help content. Single source of truth for the Tk,
# Textual and Web Welcome screens; each UI renders these natively (colours,
# fonts, layout stay local). Drawn from the Tk desktop Welcome tab.
# ---------------------------------------------------------------------------
WELCOME_TAGLINE = "Multi-Database Control Center | Modular | UI · CLI · REST API"

WELCOME_OVERVIEW = [
    "Oracle, MySQL, MariaDB, PostgreSQL, SQLite, SQL Server, MongoDB, and DocumentDB",
    "Core + optional modules: Data Migration, AI Query Assistant, Monitoring",
    "Three surfaces: desktop UI, headless CLI (dbtool.py), REST API",
    "Ship the full tool or any single module independently",
    "Dashboard overview of in-tool activity across tabs and modules",
    "Encrypted connection profiles, SSL/TLS, cloud resource monitoring",
]

# Per-tab "what it is / how to use it" guide.
WELCOME_TAB_GUIDE = [
    {"title": "Connections", "lines": [
        "Purpose: Manage direct and cloud database connection profiles",
        "Usage: Add, test, edit, connect, and remove saved profiles",
        "Features: Encrypted storage, SSL/TLS options, cloud API credentials",
        "How to use: Add database connection → choose engine → Test → Save → Connect",
    ]},
    {"title": "Dashboard", "lines": [
        "Purpose: Operational overview of activity across tabs and modules",
        "Usage: See status cards for Connections, Monitor, AI, Schema, and core tabs",
        "Features: Draggable 2-column layout, auto-refresh while visible, Go to tab links",
        "How to use: Opens on startup; use Refresh for manual update; drag headers to rearrange",
    ]},
    {"title": "Database Objects", "lines": [
        "Purpose: Browse and explore database structure and objects",
        "Usage: View tables, views, indexes, triggers, procedures, functions, and more",
        "Features: Engine-aware object types, tree navigation, DDL generation",
        "How to use: Select connection → expand tree → right-click or use toolbar actions",
    ]},
    {"title": "SQL Editor", "lines": [
        "Purpose: Write and execute SQL or document queries (MongoDB/DocumentDB)",
        "Usage: Multiple editor tabs; run statements with multi-tab results",
        "Features: inline + / × tabs, F5 execute, export, query history; JSON document mode",
        "How to use: Write SQL or JSON query → F5 or Execute → export results if needed",
    ]},
    {"title": "AI Query Assistant (module)", "lines": [
        "Purpose: Generate SQL from natural language with schema awareness",
        "Usage: Multi-tab sessions, follow-ups, SQL modes (strict/summary/open)",
        "Features: Claude, Cursor, or Codex backends; auto-execute and execution rules",
        "How to use: Select connection → ask a question → review SQL → execute or refine",
    ]},
    {"title": "Data Migration (module)", "lines": [
        "Purpose: Migrate databases across SQL platforms — schema + data",
        "Usage: Convert table DDL, transfer rows, then validate the migration",
        "Features: Type mapping, constraint preservation, schema/data comparison, CLI and API access",
        "How to use: Source connection → target engine → choose tables → convert → review",
    ]},
    {"title": "Monitor (module)", "lines": [
        "Purpose: OS, database, and cloud resource performance monitoring",
        "Usage: SSH servers, local DB metrics, AWS/Azure/GCP cloud resources",
        "Features: Thresholds, alerts, daemon mode, Teams notifications",
        "How to use: Register targets → Select Database / Select Resource → view metrics",
    ]},
]

# CLI / REST API / modular build entry points.
WELCOME_ACCESS = [
    "Full desktop UI: python conDbUi.py  or  python dbtool.py ui",
    "Single module UI: python dbtool.py ui --module migrator|ai|monitor",
    "Headless CLI: python dbtool.py connections|query|objects|schema|ai|monitor|…",
    "REST API: python dbtool.py api  →  http://127.0.0.1:8000/docs",
    "Module-only CLI/API: python -m schema_converter|ai_query|monitoring …",
    "Check installed modules: python dbtool.py modules",
]

# Supported database engines + version coverage.
WELCOME_PLATFORMS = [
    {"name": "Oracle", "versions": "11g - 21c"},
    {"name": "MySQL", "versions": "5.7, 8.0+"},
    {"name": "MariaDB", "versions": "10.x"},
    {"name": "PostgreSQL", "versions": "10 - 16+"},
    {"name": "SQLite", "versions": "3.x"},
    {"name": "SQL Server", "versions": "2012+"},
    {"name": "MongoDB", "versions": "4.x+"},
    {"name": "DocumentDB", "versions": "AWS"},
]

WELCOME_TIPS = [
    "App opens on Dashboard — use cards to jump to tabs",
    "Credentials encrypted under ~/.dbassistant/",
    "Optional modules ship independently — see MODULES.md",
    "AI backends: Claude CLI, Cursor, or Codex on PATH",
    "Oracle thin mode works without Instant Client (12.1+)",
    "Full guide: HOW_TO_USE.md in project root",
]

WELCOME_FOOTER = (
    "Need help? See HOW_TO_USE.md and MODULES.md in the project folder"
)


def ai_payload() -> dict:
    """AI Query Assistant layout/labels shared by every UI (rendered natively).

    Drawn from the Tk AI tab: the same action buttons, SQL toolbar, SQL modes,
    the five result tabs (Query results / Explanation / Optimization / Chat /
    Review) and the Chat follow-up controls. A label/order change here shows up
    in Tk, Textual and Web at once.
    """
    from common.editions import advanced_modules_installed

    advanced = advanced_modules_installed()
    return {
        "actions": AI_ACTIONS,
        "questionTools": AI_QUESTION_TOOLS,
        "useRagLabel": AI_USE_RAG_LABEL,
        "sqlActions": AI_SQL_ACTIONS,
        "sqlModes": AI_SQL_MODES,
        "resultTabs": AI_RESULT_TABS,
        "chatActions": AI_CHAT_ACTIONS,
        "uninterruptedLabel": AI_UNINTERRUPTED_LABEL,
        "fallbackLabel": AI_FALLBACK_LABEL,
        "fallbackHint": AI_FALLBACK_HINT,
        "buildAppsActions": list(AI_BUILD_APPS_ACTIONS) if advanced else [],
        "advancedModules": advanced,
        "settings": AI_SETTINGS,
    }


def welcome_payload() -> dict:
    """Welcome-tab content shared by every UI (rendered natively by each)."""
    return {
        "tagline": WELCOME_TAGLINE,
        "overview": WELCOME_OVERVIEW,
        "tabGuide": WELCOME_TAB_GUIDE,
        "access": WELCOME_ACCESS,
        "shortcuts": KEYBOARD_SHORTCUTS,
        "platforms": WELCOME_PLATFORMS,
        "tips": WELCOME_TIPS,
        "footer": WELCOME_FOOTER,
    }


def as_payload() -> dict:
    """Bundle every spec for the Web UI's /ui/config endpoint."""
    return {
        "connection": {
            "sections": CONNECTION_SECTIONS,
            "sshAuth": SSH_AUTH_METHODS,
            "activeActions": ACTIVE_CONNECTION_ACTIONS,
            "savedActions": SAVED_CONNECTION_ACTIONS,
            "directFields": DIRECT_FORM_FIELDS,
            "directActions": DIRECT_ACTIONS,
            "remoteFields": REMOTE_FORM_FIELDS,
            "remoteSshFields": REMOTE_SSH_FIELDS,
            "remoteActions": REMOTE_ACTIONS,
            "sslFields": CONNECTION_SSL_FIELDS,
            "tlsFields": CONNECTION_TLS_FIELDS,
        },
        "sqlEditor": sql_editor_payload(),
        "objects": objects_payload(),
        "migration": {"options": MIGRATION_OPTIONS, "actions": MIGRATION_ACTIONS},
        "monitoring": monitoring_payload(),
        "ai": ai_payload(),
        "cloud": {"providers": CLOUD_PROVIDERS, "authTabs": CLOUD_AUTH_TABS},
        "shortcuts": KEYBOARD_SHORTCUTS,
        "welcome": welcome_payload(),
    }
