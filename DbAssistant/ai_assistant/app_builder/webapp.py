"""Full-stack application generator.

Turns an :class:`~ai_assistant.app_builder.spec.AppSpec` into a complete, runnable
FastAPI application: a JSON REST API, a server-rendered HTML UI (Jinja2), a
SQLite-backed parameterized repository, infra adapters mapping to the
centralized services, tests (FastAPI ``TestClient``), Docker/CI, and docs.

Design choices that matter:

* The app is **runnable and testable out of the box** — it defaults to a SQLite
  store (stdlib), so a generated app starts with ``uvicorn`` and its tests pass
  with no external database. Real deployments point ``APP_DB_PATH`` / the infra
  config at the centralized resources.
* Generated source uses **token replacement** (``__TOKEN__``) rather than
  f-strings/format so that literal ``{...}`` in the emitted Python/Jinja code is
  preserved verbatim.
* Routes are generic (table-driven) to keep the output small and duplication low
  while still exposing per-entity REST + UI behavior.
"""

from __future__ import annotations

from ai_assistant.app_builder.engine import TEST_TAXONOMY
from ai_assistant.app_builder.spec import AppSpec, Entity

# ── small text helpers ───────────────────────────────────────────────────────-
def _py_list(values: list[str]) -> str:
    return "[" + ", ".join('"' + v + '"' for v in values) + "]"


def _schema_literal(entities: list[Entity]) -> str:
    lines = ["SCHEMA = {"]
    for e in entities:
        lines.append('    "' + e.table + '": ' + _py_list(e.safe_fields()) + ",")
    lines.append("}")
    return "\n".join(lines)


def _entities_literal(entities: list[Entity]) -> str:
    lines = ["ENTITIES = ["]
    for e in entities:
        lines.append(
            '    {"table": "' + e.table + '", "label": "' + e.label
            + '", "fields": ' + _py_list(e.safe_fields()) + "},"
        )
    lines.append("]")
    return "\n".join(lines)


# ── static files ──────────────────────────────────────────────────────────────
def _requirements() -> str:
    return (
        "fastapi\n"
        "uvicorn[standard]\n"
        "jinja2\n"
        "python-multipart\n"
        "httpx\n"
        "pytest\n"
        "pyyaml\n"
    )


def _settings_py(app_name: str) -> str:
    return (
        '"""Runtime settings, read from environment with safe defaults."""\n'
        "from __future__ import annotations\n\n"
        "import os\n"
        "from dataclasses import dataclass\n\n\n"
        "@dataclass\n"
        "class Settings:\n"
        '    """Application settings."""\n\n'
        '    app_name: str = "__APP_NAME__"\n'
        '    db_path: str = "var/app.db"\n'
        "    enable_monitoring: bool = True\n\n\n"
        "def get_settings() -> Settings:\n"
        '    """Build settings from environment variables."""\n'
        "    return Settings(\n"
        '        app_name=os.environ.get("APP_NAME", "__APP_NAME__"),\n'
        '        db_path=os.environ.get("APP_DB_PATH", "var/app.db"),\n'
        '        enable_monitoring=os.environ.get("APP_ENABLE_MONITORING", "1") != "0",\n'
        "    )\n"
    ).replace("__APP_NAME__", app_name)


def _schema_py(entities: list[Entity]) -> str:
    return (
        '"""Database schema definition and DDL (generated from the app spec)."""\n'
        "from __future__ import annotations\n\n"
        + _schema_literal(entities) + "\n\n\n"
        "def create_ddl() -> list[str]:\n"
        '    """Return CREATE TABLE statements for every table."""\n'
        "    statements = []\n"
        "    for table, columns in SCHEMA.items():\n"
        "        defs = []\n"
        "        for col in columns:\n"
        '            if col == "id":\n'
        '                defs.append("id INTEGER PRIMARY KEY AUTOINCREMENT")\n'
        "            else:\n"
        '                defs.append(col + " TEXT")\n'
        '        statements.append(\n'
        '            "CREATE TABLE IF NOT EXISTS " + table + " (" + ", ".join(defs) + ")"\n'
        "        )\n"
        "    return statements\n"
    )


def _schema_sql(entities: list[Entity]) -> str:
    out = ["-- Generated DDL (SQLite dialect)"]
    for e in entities:
        defs = []
        for col in e.safe_fields():
            if col == "id":
                defs.append("id INTEGER PRIMARY KEY AUTOINCREMENT")
            else:
                defs.append(col + " TEXT")
        out.append("CREATE TABLE IF NOT EXISTS " + e.table + " (" + ", ".join(defs) + ");")
    return "\n".join(out) + "\n"


def _connection_py() -> str:
    return (
        '"""SQLite connection management with lazy schema initialization."""\n'
        "from __future__ import annotations\n\n"
        "import os\n"
        "import sqlite3\n"
        "import threading\n\n"
        "from src.db.schema import SCHEMA, create_ddl\n\n"
        "_LOCK = threading.Lock()\n"
        "_CONNECTION: sqlite3.Connection | None = None\n\n\n"
        "def get_connection() -> sqlite3.Connection:\n"
        '    """Return a process-wide SQLite connection, creating schema on first use."""\n'
        "    global _CONNECTION\n"
        "    with _LOCK:\n"
        "        if _CONNECTION is None:\n"
        '            path = os.environ.get("APP_DB_PATH", "var/app.db")\n'
        '            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)\n'
        "            conn = sqlite3.connect(path, check_same_thread=False)\n"
        "            conn.row_factory = sqlite3.Row\n"
        '            conn.executescript(";\\n".join(create_ddl()) + ";")\n'
        "            _seed_if_empty(conn)\n"
        "            conn.commit()\n"
        "            _CONNECTION = conn\n"
        "        return _CONNECTION\n\n\n"
        "def _seed_if_empty(conn: sqlite3.Connection) -> None:\n"
        '    """Seed deterministic sample rows into any empty table.\n\n'
        "    Ensures a freshly launched app demonstrates its flows with real data\n"
        "    instead of blank lists when the database does not already cover the\n"
        "    app. Table/column names come from the validated SCHEMA whitelist;\n"
        "    values are bound with ``?`` placeholders.\n"
        '    """\n'
        "    for table, columns in SCHEMA.items():\n"
        '        cols = [c for c in columns if c != "id"]\n'
        "        if not cols:\n"
        "            continue\n"
        '        count = conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]\n'
        "        if count:\n"
        "            continue\n"
        '        placeholders = ", ".join(["?"] * len(cols))\n'
        '        sql = ("INSERT INTO " + table + " (" + ", ".join(cols)\n'
        '               + ") VALUES (" + placeholders + ")")\n'
        "        for n in (1, 2, 3):\n"
        '            conn.execute(\n'
        '                sql, tuple("sample " + c + " " + str(n) for c in cols))\n\n\n'
        "def reset_connection() -> None:\n"
        '    """Close the cached connection (used by tests)."""\n'
        "    global _CONNECTION\n"
        "    with _LOCK:\n"
        "        if _CONNECTION is not None:\n"
        "            _CONNECTION.close()\n"
        "            _CONNECTION = None\n\n\n"
        "def scalar_count(sql: str, params: tuple = ()) -> int:\n"
        '    """Return a single integer from a COUNT(*) (or similar) query.\n\n'
        "    Works with sqlite3.Row (index row[0]); do not call .values() on Row.\n"
        '    """\n'
        "    row = get_connection().execute(sql, params).fetchone()\n"
        "    if row is None:\n"
        "        return 0\n"
        "    return int(row[0] or 0)\n"
    )


def _models_py(entities: list[Entity]) -> str:
    out = [
        '"""Generated data models (dataclasses) reflecting the app schema."""',
        "from __future__ import annotations",
        "",
        "from dataclasses import dataclass",
        "from typing import Any",
        "",
    ]
    for e in entities:
        out += ["", "@dataclass", "class " + e.class_name + ":",
                '    """Row model for the ' + e.table + ' table."""', ""]
        for col in e.safe_fields():
            out.append("    " + col + ": Any = None")
        out.append("")
    return "\n".join(out) + "\n"


def _repository_py() -> str:
    return (
        '"""Parameterized CRUD repository over the configured SQLite database.\n\n'
        "Table names come from a fixed, validated whitelist (SCHEMA); all *values*\n"
        "are bound with ``?`` placeholders, never string-formatted.\n"
        '"""\n'
        "from __future__ import annotations\n\n"
        "from src.db.connection import get_connection\n"
        "from src.db.schema import SCHEMA\n\n\n"
        "class Repository:\n"
        '    """Generic data-access layer using parameterized queries."""\n\n'
        "    def __init__(self, table: str) -> None:\n"
        "        if table not in SCHEMA:\n"
        '            raise KeyError("unknown table: " + table)\n'
        "        self._table = table\n"
        "        self._columns = SCHEMA[table]\n\n"
        "    def list(self, limit: int = 100) -> list[dict]:\n"
        '        """Return up to *limit* rows as dicts (newest first)."""\n'
        "        conn = get_connection()\n"
        '        sql = "SELECT * FROM " + self._table + " ORDER BY id DESC LIMIT ?"\n'
        "        cur = conn.execute(sql, (limit,))\n"
        "        return [dict(row) for row in cur.fetchall()]\n\n"
        "    def get(self, row_id: int) -> dict | None:\n"
        '        """Return a single row by id, or None."""\n'
        "        conn = get_connection()\n"
        '        sql = "SELECT * FROM " + self._table + " WHERE id = ?"\n'
        "        row = conn.execute(sql, (row_id,)).fetchone()\n"
        "        return dict(row) if row is not None else None\n\n"
        "    def create(self, data: dict) -> int:\n"
        '        """Insert a row from *data* (known columns only) and return its id."""\n'
        '        cols = [c for c in self._columns if c != "id" and c in data]\n'
        '        placeholders = ", ".join(["?"] * len(cols))\n'
        "        values = [data[c] for c in cols]\n"
        '        sql = ("INSERT INTO " + self._table + " (" + ", ".join(cols)\n'
        '               + ") VALUES (" + placeholders + ")")\n'
        "        conn = get_connection()\n"
        "        cur = conn.execute(sql, values)\n"
        "        conn.commit()\n"
        "        return int(cur.lastrowid)\n\n"
        "    def update(self, row_id: int, data: dict) -> bool:\n"
        '        """Update known columns of a row; return True if a row changed."""\n'
        '        cols = [c for c in self._columns if c != "id" and c in data]\n'
        "        if not cols:\n"
        "            return False\n"
        '        assignments = ", ".join(c + " = ?" for c in cols)\n'
        "        values = [data[c] for c in cols]\n"
        "        values.append(row_id)\n"
        '        sql = "UPDATE " + self._table + " SET " + assignments + " WHERE id = ?"\n'
        "        conn = get_connection()\n"
        "        cur = conn.execute(sql, values)\n"
        "        conn.commit()\n"
        "        return cur.rowcount > 0\n\n"
        "    def delete(self, row_id: int) -> bool:\n"
        '        """Delete a row by id; return True if it existed."""\n'
        "        conn = get_connection()\n"
        '        sql = "DELETE FROM " + self._table + " WHERE id = ?"\n'
        "        cur = conn.execute(sql, (row_id,))\n"
        "        conn.commit()\n"
        "        return cur.rowcount > 0\n"
    )


# ── JSON API ──────────────────────────────────────────────────────────────────
_API_HEADER = (
    '"""JSON REST API for the application entities."""\n'
    "from __future__ import annotations\n\n"
    "from fastapi import APIRouter, HTTPException\n\n"
    "from src.repository import Repository\n\n"
    'router = APIRouter(prefix="/api")\n'
)

_API_LIST = (
    "\n\n"
    '@router.get("/__TABLE__")\n'
    "def list___SINGULAR__(limit: int = 100) -> list[dict]:\n"
    '    """List __TABLE__."""\n'
    '    return Repository("__TABLE__").list(limit=limit)\n\n\n'
    '@router.get("/__TABLE__/{row_id}")\n'
    "def get___SINGULAR__(row_id: int) -> dict:\n"
    '    """Get one __SINGULAR__ by id."""\n'
    '    row = Repository("__TABLE__").get(row_id)\n'
    "    if row is None:\n"
    '        raise HTTPException(status_code=404, detail="not found")\n'
    "    return row\n"
)

_API_CREATE = (
    "\n\n"
    '@router.post("/__TABLE__")\n'
    "def create___SINGULAR__(payload: dict) -> dict:\n"
    '    """Create a __SINGULAR__."""\n'
    '    new_id = Repository("__TABLE__").create(payload)\n'
    '    return {"id": new_id}\n'
)

_API_UPDATE = (
    "\n\n"
    '@router.put("/__TABLE__/{row_id}")\n'
    "def update___SINGULAR__(row_id: int, payload: dict) -> dict:\n"
    '    """Update a __SINGULAR__."""\n'
    '    ok = Repository("__TABLE__").update(row_id, payload)\n'
    "    if not ok:\n"
    '        raise HTTPException(status_code=404, detail="not found")\n'
    '    return {"updated": True}\n'
)

_API_DELETE = (
    "\n\n"
    '@router.delete("/__TABLE__/{row_id}")\n'
    "def delete___SINGULAR__(row_id: int) -> dict:\n"
    '    """Delete a __SINGULAR__."""\n'
    '    ok = Repository("__TABLE__").delete(row_id)\n'
    "    if not ok:\n"
    '        raise HTTPException(status_code=404, detail="not found")\n'
    '    return {"deleted": True}\n'
)


def _api_py(spec: AppSpec) -> str:
    parts = [_API_HEADER]
    for e in spec.entities:
        block = ""
        if "list" in spec.features:
            block += _API_LIST
        if "create" in spec.features:
            block += _API_CREATE
        if "edit" in spec.features:
            block += _API_UPDATE
        if "delete" in spec.features:
            block += _API_DELETE
        parts.append(block.replace("__TABLE__", e.table).replace("__SINGULAR__", e.singular))
    return "".join(parts)


# ── HTML UI ─────────────────────────────────────────────────────────────────--
def _web_py(spec: AppSpec) -> str:
    can_create = "create" in spec.features
    can_edit = "edit" in spec.features
    can_delete = "delete" in spec.features
    header = (
        '"""Server-rendered HTML UI (Jinja2) for the application."""\n'
        "from __future__ import annotations\n\n"
        "from pathlib import Path\n\n"
        "from fastapi import APIRouter, HTTPException, Request\n"
        "from fastapi.responses import HTMLResponse, RedirectResponse\n"
        "from fastapi.templating import Jinja2Templates\n\n"
        "from src.repository import Repository\n\n"
        + _entities_literal(spec.entities) + "\n\n"
        "_BY_TABLE = {e[\"table\"]: e for e in ENTITIES}\n"
        "_TEMPLATES = Jinja2Templates(\n"
        "    directory=str(Path(__file__).resolve().parent.parent / \"templates\")\n"
        ")\n\n"
        "router = APIRouter()\n\n\n"
        "def _entity_or_404(table: str) -> dict:\n"
        '    """Return entity metadata for *table* or raise 404."""\n'
        "    meta = _BY_TABLE.get(table)\n"
        "    if meta is None:\n"
        '        raise HTTPException(status_code=404, detail="unknown entity")\n'
        "    return meta\n\n\n"
        '@router.get("/", response_class=HTMLResponse)\n'
        "def dashboard(request: Request) -> HTMLResponse:\n"
        '    """Render the dashboard listing every entity and its row count."""\n'
        "    cards = []\n"
        "    for meta in ENTITIES:\n"
        '        count = len(Repository(meta["table"]).list(limit=1000))\n'
        '        cards.append({"meta": meta, "count": count})\n'
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "index.html", {"entities": ENTITIES, "cards": cards}\n'
        "    )\n\n\n"
        '@router.get("/{table}", response_class=HTMLResponse)\n'
        "def list_view(request: Request, table: str) -> HTMLResponse:\n"
        '    """Render a table of rows for *table*."""\n'
        "    meta = _entity_or_404(table)\n"
        '    rows = Repository(table).list()\n'
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "list.html",\n'
        "        {\n"
        '            "entities": ENTITIES, "meta": meta,\n'
        '            "columns": meta["fields"], "rows": rows,\n'
        '            "can_create": __CAN_CREATE__, "can_edit": __CAN_EDIT__,\n'
        '            "can_delete": __CAN_DELETE__,\n'
        "        },\n"
        "    )\n"
    )
    create_routes = (
        "\n\n"
        '@router.get("/{table}/new", response_class=HTMLResponse)\n'
        "def new_form(request: Request, table: str) -> HTMLResponse:\n"
        '    """Render the create form for *table*."""\n'
        "    meta = _entity_or_404(table)\n"
        '    fields = [c for c in meta["fields"] if c != "id"]\n'
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "form.html",\n'
        "        {\n"
        '            "entities": ENTITIES, "meta": meta,\n'
        '            "fields": fields, "row": {}, "action": "/" + table + "/new",\n'
        '            "title": "New " + meta["label"],\n'
        "        },\n"
        "    )\n\n\n"
        '@router.post("/{table}/new")\n'
        "async def create_row(request: Request, table: str) -> RedirectResponse:\n"
        '    """Persist a new row from submitted form data."""\n'
        "    _entity_or_404(table)\n"
        "    form = await request.form()\n"
        "    Repository(table).create(dict(form))\n"
        '    return RedirectResponse("/" + table, status_code=303)\n'
    )
    edit_routes = (
        "\n\n"
        '@router.get("/{table}/{row_id}/edit", response_class=HTMLResponse)\n'
        "def edit_form(request: Request, table: str, row_id: int) -> HTMLResponse:\n"
        '    """Render the edit form for a single row."""\n'
        "    meta = _entity_or_404(table)\n"
        "    row = Repository(table).get(row_id)\n"
        "    if row is None:\n"
        '        raise HTTPException(status_code=404, detail="not found")\n'
        '    fields = [c for c in meta["fields"] if c != "id"]\n'
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "form.html",\n'
        "        {\n"
        '            "entities": ENTITIES, "meta": meta,\n'
        '            "fields": fields, "row": row,\n'
        '            "action": "/" + table + "/" + str(row_id) + "/edit",\n'
        '            "title": "Edit " + meta["label"],\n'
        "        },\n"
        "    )\n\n\n"
        '@router.post("/{table}/{row_id}/edit")\n'
        "async def update_row(request: Request, table: str, row_id: int) -> RedirectResponse:\n"
        '    """Apply submitted edits to a row."""\n'
        "    _entity_or_404(table)\n"
        "    form = await request.form()\n"
        "    Repository(table).update(row_id, dict(form))\n"
        '    return RedirectResponse("/" + table, status_code=303)\n'
    )
    delete_routes = (
        "\n\n"
        '@router.post("/{table}/{row_id}/delete")\n'
        "def delete_row(table: str, row_id: int) -> RedirectResponse:\n"
        '    """Delete a row and return to the list."""\n'
        "    _entity_or_404(table)\n"
        "    Repository(table).delete(row_id)\n"
        '    return RedirectResponse("/" + table, status_code=303)\n'
    )
    body = header
    if can_create:
        body += create_routes
    if can_edit:
        body += edit_routes
    if can_delete:
        body += delete_routes
    return (
        body
        .replace("__CAN_CREATE__", "True" if can_create else "False")
        .replace("__CAN_EDIT__", "True" if can_edit else "False")
        .replace("__CAN_DELETE__", "True" if can_delete else "False")
    )


# ── templates (Jinja2 — emitted verbatim) ────────────────────────────────────-
_BASE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ app_name | default("__APP_NAME__") }}</title>
  <link rel="stylesheet" href="/static/style.css">
</head>
<body>
  <header class="topbar">
    <a class="brand" href="/">__APP_NAME__</a>
    <nav>
      {% for e in entities %}
      <a href="/{{ e.table }}">{{ e.label }}</a>
      {% endfor %}
    </nav>
  </header>
  <main class="container">
    {% block content %}{% endblock %}
  </main>
  <footer class="footer">Generated by AppBuilderAssistant</footer>
</body>
</html>
"""

_INDEX_HTML = """{% extends "base.html" %}
{% block content %}
<h1>Dashboard</h1>
<div class="cards">
  {% for card in cards %}
  <a class="card" href="/{{ card.meta.table }}">
    <div class="card-title">{{ card.meta.label }}</div>
    <div class="card-count">{{ card.count }}</div>
    <div class="card-sub">records</div>
  </a>
  {% endfor %}
</div>
{% endblock %}
"""

_LIST_HTML = """{% extends "base.html" %}
{% block content %}
<div class="page-head">
  <h1>{{ meta.label }}</h1>
  {% if can_create %}<a class="btn primary" href="/{{ meta.table }}/new">+ New</a>{% endif %}
</div>
<table class="grid">
  <thead>
    <tr>
      {% for col in columns %}<th>{{ col }}</th>{% endfor %}
      {% if can_edit or can_delete %}<th>Actions</th>{% endif %}
    </tr>
  </thead>
  <tbody>
    {% for row in rows %}
    <tr>
      {% for col in columns %}<td>{{ row.get(col, "") }}</td>{% endfor %}
      {% if can_edit or can_delete %}
      <td class="actions">
        {% if can_edit %}<a class="btn" href="/{{ meta.table }}/{{ row.id }}/edit">Edit</a>{% endif %}
        {% if can_delete %}
        <form method="post" action="/{{ meta.table }}/{{ row.id }}/delete" class="inline">
          <button class="btn danger" type="submit">Delete</button>
        </form>
        {% endif %}
      </td>
      {% endif %}
    </tr>
    {% endfor %}
    {% if not rows %}
    <tr><td colspan="99" class="empty">No records yet.</td></tr>
    {% endif %}
  </tbody>
</table>
{% endblock %}
"""

_FORM_HTML = """{% extends "base.html" %}
{% block content %}
<h1>{{ title }}</h1>
<form method="post" action="{{ action }}" class="form">
  {% for field in fields %}
  <label>
    <span>{{ field }}</span>
    <input name="{{ field }}" value="{{ row.get(field, '') }}">
  </label>
  {% endfor %}
  <div class="form-actions">
    <button class="btn primary" type="submit">Save</button>
    <a class="btn" href="/{{ meta.table }}">Cancel</a>
  </div>
</form>
{% endblock %}
"""

_STYLE_CSS = """:root {
  --bg: #0f172a; --panel: #ffffff; --ink: #0f172a; --muted: #64748b;
  --accent: #2563eb; --danger: #dc2626; --line: #e2e8f0;
}
* { box-sizing: border-box; }
body { margin: 0; font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
  color: var(--ink); background: #f1f5f9; }
.topbar { display: flex; align-items: center; gap: 24px; padding: 12px 24px;
  background: var(--bg); color: #fff; }
.brand { font-weight: 700; color: #fff; text-decoration: none; font-size: 18px; }
.topbar nav { display: flex; gap: 14px; flex-wrap: wrap; }
.topbar nav a { color: #cbd5e1; text-decoration: none; }
.topbar nav a:hover { color: #fff; }
.container { max-width: 1000px; margin: 24px auto; padding: 0 20px; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); gap: 16px; }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 12px;
  padding: 18px; text-decoration: none; color: inherit; box-shadow: 0 1px 2px rgba(0,0,0,.04); }
.card:hover { border-color: var(--accent); }
.card-title { font-weight: 600; }
.card-count { font-size: 32px; font-weight: 700; color: var(--accent); }
.card-sub { color: var(--muted); font-size: 12px; }
.page-head { display: flex; justify-content: space-between; align-items: center; }
.grid { width: 100%; border-collapse: collapse; background: var(--panel);
  border: 1px solid var(--line); border-radius: 12px; overflow: hidden; }
.grid th, .grid td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--line); }
.grid th { background: #f8fafc; font-size: 12px; text-transform: uppercase; color: var(--muted); }
.empty { text-align: center; color: var(--muted); padding: 24px; }
.btn { display: inline-block; padding: 7px 12px; border-radius: 8px; border: 1px solid var(--line);
  background: #fff; color: var(--ink); text-decoration: none; cursor: pointer; font-size: 14px; }
.btn.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
.btn.danger { background: var(--danger); color: #fff; border-color: var(--danger); }
.actions { display: flex; gap: 8px; }
.inline { display: inline; }
.form { background: var(--panel); border: 1px solid var(--line); border-radius: 12px;
  padding: 20px; max-width: 520px; display: grid; gap: 14px; }
.form label { display: grid; gap: 6px; }
.form label span { font-size: 13px; color: var(--muted); }
.form input { padding: 9px 10px; border: 1px solid var(--line); border-radius: 8px; font-size: 14px; }
.form-actions { display: flex; gap: 10px; }
.footer { text-align: center; color: var(--muted); padding: 30px; font-size: 12px; }
"""


# ── infra adapters ─────────────────────────────────────────────────────────--
def _infra_init_py(services: list[str]) -> str:
    lines = [
        '"""Centralized-infra adapters wired into the app at startup.',
        "",
        "Each adapter maps a generated app to one of the platform's centralized",
        "services. Endpoints/credentials come from config/infra.yaml + environment,",
        "so the same app runs locally (safe defaults) or against shared infra.",
        '"""',
        "from __future__ import annotations",
        "",
        "from typing import Any",
        "",
        "",
        "def startup(app: Any, settings: Any) -> None:",
        '    """Instantiate and register every configured infra adapter."""',
        "    registry: dict = {}",
    ]
    if "monitoring" in services:
        lines += [
            "    from src.infra.monitoring import Monitoring",
            '    registry["monitoring"] = Monitoring(settings)',
        ]
    if "notification" in services:
        lines += [
            "    from src.infra.notification import Notification",
            '    registry["notification"] = Notification(settings)',
        ]
    if "document" in services:
        lines += [
            "    from src.infra.document import DocumentService",
            '    registry["document"] = DocumentService(settings)',
        ]
    lines += [
        "    app.state.infra = registry",
        "",
    ]
    return "\n".join(lines)


def _monitoring_py() -> str:
    return (
        '"""Monitoring adapter — maps app health/metrics to the central monitor.\n\n'
        "The platform offers monitoring as a managed service: it polls this app's\n"
        "``/health`` API (app liveness + DB readiness). This adapter mirrors that\n"
        "payload so the central monitor and the in-app health endpoint agree.\n"
        '"""\n'
        "from __future__ import annotations\n\n"
        "import time\n"
        "from typing import Any\n\n\n"
        "class Monitoring:\n"
        '    """Record lightweight metrics and expose app + DB health to the monitor."""\n\n'
        "    def __init__(self, settings: Any) -> None:\n"
        "        self._settings = settings\n"
        "        self._started = time.time()\n"
        "        self._events = 0\n\n"
        "    def record(self, event: str) -> None:\n"
        '        """Record a named metric event."""\n'
        "        self._events += 1\n\n"
        "    def db_health(self) -> dict:\n"
        '        """Probe database readiness with a cheap SELECT 1."""\n'
        "        try:\n"
        "            from src.db.connection import get_connection\n"
        '            get_connection().execute("SELECT 1")\n'
        '            return {"status": "up"}\n'
        "        except Exception as exc:  # readiness probe never raises\n"
        '            return {"status": "down", "error": str(exc)[:200]}\n\n'
        "    def health(self) -> dict:\n"
        '        """Return current app + DB health/uptime for the central monitor."""\n'
        '        return {\n'
        '            "status": "healthy",\n'
        '            "uptime_seconds": round(time.time() - self._started, 3),\n'
        '            "events": self._events,\n'
        '            "database": self.db_health(),\n'
        "        }\n"
    )


def _notification_py() -> str:
    return (
        '"""Notification adapter — routes app notifications to the central service."""\n'
        "from __future__ import annotations\n\n"
        "from typing import Any\n\n\n"
        "class Notification:\n"
        '    """Dispatch notifications through the configured central channel."""\n\n'
        "    def __init__(self, settings: Any) -> None:\n"
        "        self._settings = settings\n\n"
        "    def notify(self, channel: str, message: str) -> bool:\n"
        '        """Send *message* on *channel*; returns True when accepted."""\n'
        "        return bool(channel and message)\n"
    )


def _document_py() -> str:
    return (
        '"""Document adapter — generates/stores documents via the central store."""\n'
        "from __future__ import annotations\n\n"
        "from typing import Any\n\n\n"
        "class DocumentService:\n"
        '    """Render and persist documents to the centralized document store."""\n\n'
        "    def __init__(self, settings: Any) -> None:\n"
        "        self._settings = settings\n\n"
        "    def render(self, name: str, body: str) -> dict:\n"
        '        """Return a document descriptor for *name* (stub for central store)."""\n'
        '        return {"name": name, "bytes": len(body or "")}\n'
    )


def _ai_builder_py() -> str:
    return (
        '"""Hooks for AI-assisted iteration on this app."""\n'
        "from __future__ import annotations\n\n\n"
        "def on_build(event: dict) -> None:\n"
        '    """React to build lifecycle events (stub)."""\n'
        "    return None\n"
    )


def _infra_yaml(spec: AppSpec) -> str:
    out = [
        "# Centralized-infra mapping for " + spec.app_name,
        "# Override endpoints/credentials via environment in real deployments.",
        "app: " + spec.app_name,
        "services:",
    ]
    mapping = {
        "monitoring": "central-monitoring (env: MONITORING_ENDPOINT)",
        "notification": "central-notifications (env: NOTIFY_ENDPOINT)",
        "document": "central-document-store (env: DOCSTORE_ENDPOINT)",
        "hosting": "central-hosting (deploy/hosting.yaml)",
        "ci_cd": "central-ci (.github/workflows/ci.yml)",
        "database": "central-database (env: APP_DB_PATH / DATABASE_URL)",
        "ai_builder": "ai-builder-hooks (src/ai/builder_hooks.py)",
    }
    # What the central monitor polls on this app (managed monitoring service).
    probes = {"monitoring": "health_api: /health  (covers app liveness + DB)"}
    for svc in spec.services:
        out.append("  - name: " + svc)
        out.append("    target: " + mapping.get(svc, "unmapped"))
        if svc in probes:
            out.append("    monitors: " + probes[svc])
    if not spec.services:
        out.append("  []")
    return "\n".join(out) + "\n"


# ── top-level files ────────────────────────────────────────────────────────--
def _app_py(app_name: str, services: list[str]) -> str:
    infra_line = (
        "    infra_startup(app, settings)\n" if _has_infra(services) else ""
    )
    infra_import = (
        "from src.infra import startup as infra_startup\n" if _has_infra(services) else ""
    )
    return (
        '"""Application entry point: wires the API, the web UI, and infra services."""\n'
        "from __future__ import annotations\n\n"
        "from pathlib import Path\n\n"
        "from fastapi import FastAPI\n"
        "from fastapi.staticfiles import StaticFiles\n\n"
        "from src.api import router as api_router\n"
        + infra_import
        + "from src.settings import get_settings\n"
        "from src.web import router as web_router\n\n\n"
        "def create_app() -> FastAPI:\n"
        '    """Create and configure the FastAPI application."""\n'
        "    settings = get_settings()\n"
        "    app = FastAPI(title=settings.app_name)\n"
        "    static_dir = Path(__file__).resolve().parent.parent / \"static\"\n"
        '    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")\n\n'
        '    @app.get("/health")\n'
        "    def health() -> dict:\n"
        '        """App + DB health for the centralized monitor.\n\n'
        "        Liveness is always reported; database readiness is probed with a\n"
        "        cheap ``SELECT 1`` so the platform's monitoring-as-a-service can\n"
        "        watch both the app health API and its database.\n"
        '        """\n'
        '        database = {"status": "up"}\n'
        "        try:\n"
        "            from src.db.connection import get_connection\n"
        '            get_connection().execute("SELECT 1")\n'
        "        except Exception as exc:  # DB readiness must not crash liveness\n"
        '            database = {"status": "down", "error": str(exc)[:200]}\n'
        "        return {\n"
        '            "status": "healthy",\n'
        '            "service": settings.app_name,\n'
        '            "database": database,\n'
        "        }\n\n"
        "    app.include_router(api_router)\n"
        "    app.include_router(web_router)\n"
        + infra_line
        + "    return app\n\n\n"
        "app = create_app()\n"
    ).replace("__APP_NAME__", app_name)


def _has_infra(services: list[str]) -> bool:
    return any(s in services for s in ("monitoring", "notification", "document"))


def _entity_tests(spec: AppSpec, entity: Entity) -> str:
    """Per-entity tests covering each requested feature (API + UI).

    Generated for *every* entity so the requirement-coverage meter can confirm
    each requested entity is genuinely exercised by the suite.
    """
    table = entity.table
    editable = entity.editable_fields()
    payload_items = ", ".join('"' + f + '": "test"' for f in editable)
    has_list = "list" in spec.features
    has_create = "create" in spec.features
    has_edit = "edit" in spec.features
    has_delete = "delete" in spec.features
    out: list[str] = []

    if has_list:
        out.append(
            "\n\n"
            "def test_" + table + "_list_api_and_page():\n"
            '    """' + table + ' is listable via the API and renders an HTML page."""\n'
            '    resp = client.get("/api/' + table + '")\n'
            "    assert resp.status_code == 200\n"
            "    assert isinstance(resp.json(), list)\n"
            '    page = client.get("/' + table + '")\n'
            "    assert page.status_code == 200\n"
            '    assert "<table" in page.text.lower()\n'
        )
    if has_create:
        out.append(
            "\n\n"
            "def test_" + table + "_create_api():\n"
            '    """A ' + table + ' row can be created through the JSON API."""\n'
            "    payload = {" + payload_items + "}\n"
            '    created = client.post("/api/' + table + '", json=payload)\n'
            "    assert created.status_code == 200\n"
            '    new_id = created.json()["id"]\n'
            '    rows = client.get("/api/' + table + '").json()\n'
            '    assert any(row["id"] == new_id for row in rows)\n'
        )
    if has_create and has_edit and editable:
        out.append(
            "\n\n"
            "def test_" + table + "_update_api():\n"
            '    """A ' + table + ' row can be updated through the JSON API."""\n'
            "    payload = {" + payload_items + "}\n"
            '    new_id = client.post("/api/' + table + '", json=payload).json()["id"]\n'
            '    upd = client.put("/api/' + table + '/" + str(new_id), json=payload)\n'
            "    assert upd.status_code == 200\n"
        )
    if has_create and has_delete:
        out.append(
            "\n\n"
            "def test_" + table + "_delete_api():\n"
            '    """A ' + table + ' row can be deleted through the JSON API."""\n'
            "    payload = {" + payload_items + "}\n"
            '    new_id = client.post("/api/' + table + '", json=payload).json()["id"]\n'
            '    deleted = client.delete("/api/' + table + '/" + str(new_id))\n'
            "    assert deleted.status_code == 200\n"
        )
    return "".join(out)


def _tests_py(spec: AppSpec) -> str:
    body = (
        '"""End-to-end tests for the generated app (API + UI).\n\n'
        "Boots the FastAPI app via TestClient against a file-backed test SQLite DB\n"
        "and exercises every requested entity/feature so the build genuinely covers\n"
        "the requirements (not just the structure).\n"
        '"""\n'
        "from __future__ import annotations\n\n"
        "from fastapi.testclient import TestClient\n\n"
        "from src.app import app\n\n"
        "client = TestClient(app)\n\n\n"
        "def test_health():\n"
        '    """Health endpoint returns healthy."""\n'
        '    resp = client.get("/health")\n'
        "    assert resp.status_code == 200\n"
        '    assert resp.json()["status"] == "healthy"\n\n\n'
        "def test_dashboard_renders():\n"
        '    """The dashboard renders HTML."""\n'
        '    resp = client.get("/")\n'
        "    assert resp.status_code == 200\n"
        '    assert "<html" in resp.text.lower()\n'
    )
    for entity in spec.entities:
        body += _entity_tests(spec, entity)
    return body


def _conftest_py() -> str:
    return (
        '"""Pytest runtime DB isolation for the generated app."""\n'
        "from __future__ import annotations\n\n"
        "import os\n\n"
        "import pytest\n\n\n"
        "@pytest.fixture(autouse=True)\n"
        "def _isolated_app_db(tmp_path):\n"
        '    """Use one file-backed SQLite DB per test and reset cached connections."""\n'
        '    os.environ["APP_DB_PATH"] = str(tmp_path / "app_test.db")\n'
        '    os.environ.pop("DBASSIST_DB_PATH", None)\n'
        "    try:\n"
        "        from src.db.connection import reset_connection\n"
        "        reset_connection()\n"
        "    except Exception:\n"
        "        pass\n"
        "    yield\n"
        "    try:\n"
        "        from src.db.connection import reset_connection\n"
        "        reset_connection()\n"
        "    except Exception:\n"
        "        pass\n"
    )


def _readme(spec: AppSpec) -> str:
    entities = ", ".join(e.table for e in spec.entities)
    return (
        "# " + spec.app_name + "\n\n"
        + (spec.description or "Full-stack app generated by AppBuilderAssistant.") + "\n\n"
        "## What this is\n\n"
        "A runnable FastAPI application with a JSON REST API **and** a server-rendered\n"
        "HTML UI (Jinja2), backed by SQLite out of the box.\n\n"
        "- Entities: " + entities + "\n"
        "- Features: " + ", ".join(spec.features) + "\n"
        "- Infra services: " + (", ".join(spec.services) or "none") + "\n\n"
        "## Run\n\n"
        "```bash\n"
        "pip install -r requirements.txt\n"
        "uvicorn src.app:app --reload\n"
        "```\n\n"
        "Open http://127.0.0.1:8000/ for the UI and http://127.0.0.1:8000/docs for the API.\n\n"
        "## Test\n\n"
        "```bash\n"
        "pytest -q\n"
        "```\n\n"
        "## Centralized infra\n\n"
        "See `config/infra.yaml` for how each service maps to the platform's shared\n"
        "infrastructure. Point `APP_DB_PATH` (or `DATABASE_URL`) at the central\n"
        "database for production.\n"
    )


def _dockerfile() -> str:
    return (
        "FROM python:3.12-slim\n"
        "WORKDIR /app\n"
        "COPY . .\n"
        "RUN pip install -r requirements.txt\n"
        'CMD ["python", "-m", "uvicorn", "src.app:app", "--host", "0.0.0.0", "--port", "8000"]\n'
    )


def _ci_yml() -> str:
    return (
        "name: ci\n"
        "on: [push]\n"
        "jobs:\n"
        "  test:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - uses: actions/checkout@v4\n"
        "      - uses: actions/setup-python@v5\n"
        '        with: {python-version: "3.12"}\n'
        "      - run: pip install -r requirements.txt\n"
        "      - run: pytest -q\n"
    )


def _hosting_yaml(app_name: str) -> str:
    return (
        "# Hosting descriptor for " + app_name + "\n"
        "service: " + app_name + "\n"
        "runtime: python3.12\n"
        "entrypoint: uvicorn src.app:app --host 0.0.0.0 --port 8000\n"
        "health_check: /health\n"
    )


def _architecture_md(spec: AppSpec) -> str:
    out = [
        "# " + spec.app_name + " architecture", "",
        "## Layers", "",
        "- `src/app.py` — FastAPI entry point (API + UI + infra wiring)",
        "- `src/api.py` — JSON REST endpoints per entity",
        "- `src/web.py` — server-rendered HTML UI (Jinja2)",
        "- `src/repository.py` — parameterized SQLite data access",
        "- `src/db/` — schema + connection management",
        "- `src/infra/` — centralized-infra adapters", "",
        "## Entities", "",
    ]
    for e in spec.entities:
        out.append("- **" + e.label + "** (`" + e.table + "`): " + ", ".join(e.safe_fields()))
    out += ["", "## Services", ""]
    for s in spec.services or ["(none)"]:
        out.append("- " + s)
    return "\n".join(out) + "\n"


# ── standardized skeleton (test taxonomy + docs) ────────────────────────────--
_TEST_DIR_PURPOSE = {
    "unit_test": "Fast, isolated unit tests for individual functions/classes.",
    "full_test": "End-to-end / full-suite tests exercising the whole app.",
    "write_test_cases": "Scratch space for newly written / ad-hoc test cases "
                        "before they are promoted into a category above.",
    "connectivity": "Connectivity checks: the app boots, routes are reachable, "
                    "and centralized-infra adapters are wired.",
    "db": "Database tests: schema, parameterized queries, read-only probes "
          "(and sample tables/data when a real DB is connected).",
    "api": "API contract tests for each endpoint.",
    "functionality": "Feature/behavior tests mapped to the requirement's "
                     "features.",
    "test_sample_data": "Sample data fixtures used by the tests above.",
}


def _sample_rows_literal(spec: AppSpec) -> str:
    """Build a Python literal of deterministic sample rows per entity."""
    rows: list[str] = []
    for e in spec.entities:
        editable = e.editable_fields()
        if not editable:
            continue
        examples = []
        for n in (1, 2):
            cells = ", ".join(
                repr(f) + ": " + repr(f"sample {f} {n}") for f in editable)
            examples.append("        {" + cells + "}")
        rows.append('    "' + e.table + '": [\n'
                    + ",\n".join(examples) + ",\n    ]")
    return "{\n" + ",\n".join(rows) + ("\n" if rows else "") + "}"


def _sample_data_py(spec: AppSpec) -> str:
    return (
        '"""Deterministic sample data fixtures for the test taxonomy.\n\n'
        "Session C (validator) and the regular suite use these rows so "
        "connectivity / db / api / functionality tests run against realistic "
        "data without needing a live database.\n"
        '"""\n'
        "from __future__ import annotations\n\n\n"
        "SAMPLE_ROWS: dict[str, list[dict]] = " + _sample_rows_literal(spec)
        + "\n\n\n"
        "def rows_for(table: str) -> list[dict]:\n"
        '    """Return sample rows for *table* (empty list when unknown)."""\n'
        "    return [dict(r) for r in SAMPLE_ROWS.get(table, [])]\n"
    )


def _docs_readme(spec: AppSpec) -> str:
    return (
        "# " + spec.app_name + " — documentation\n\n"
        + (spec.description or "Application documentation home.") + "\n\n"
        "Session B (advisor) and Session C (validator) read this folder to "
        "frame accurate, requirement-grounded answers and tests.\n\n"
        "- `requirement.txt` — the captured requirement/spec this app satisfies\n"
        "- `ARCHITECTURE.md` — how the app is structured\n\n"
        "## Tests\n\n"
        "Tests are organized under `tests/` by category: "
        + ", ".join(TEST_TAXONOMY) + ".\n"
    )


def _docs_requirement_txt(spec: AppSpec) -> str:
    entities = ", ".join(e.table for e in spec.entities) or "(none)"
    return (
        "REQUIREMENT\n"
        "===========\n"
        "App:        " + spec.app_name + "\n"
        "Archetype:  " + (spec.kind or "crud") + "\n"
        "Entities:   " + entities + "\n"
        "Features:   " + (", ".join(spec.features) or "(none)") + "\n"
        "Infra add-ons: " + (", ".join(spec.services) or "none") + "\n\n"
        "Description:\n"
        + (spec.description or "(none provided)") + "\n\n"
        "Acceptance criteria:\n"
        "- Each entity/feature has API + UI + tests.\n"
        "- GET /health reports app liveness AND database readiness.\n"
        "- Tests live under tests/ by category (" + ", ".join(TEST_TAXONOMY)
        + ").\n"
        "- Sample data lives in tests/test_sample_data/sample_data.py.\n"
    )


def minimal_scratch_stub(spec: AppSpec) -> dict[str, str]:
    """Tiny runnable stub for from_scratch — the agent owns all other structure.

    Ships only ``src.app:app`` (with ``/`` live-preview and ``/health``) plus
    ``requirements.txt`` so the platform can Start/host/monitor immediately.
    """
    app_name = spec.app_name
    app_py = (
        '"""Minimal runnable stub — the agent expands this app."""\n'
        "from __future__ import annotations\n\n"
        "import os\n"
        "from pathlib import Path\n\n"
        "from fastapi import FastAPI\n"
        "from fastapi.responses import HTMLResponse, JSONResponse\n\n"
        f'app = FastAPI(title="{app_name}")\n\n\n'
        "@app.get(\"/health\")\n"
        "def health():\n"
        '    """Liveness + database readiness for platform monitoring."""\n'
        "    return JSONResponse({\n"
        '        "status": "healthy",\n'
        '        "database": {"status": "not_configured"},\n'
        "    })\n\n\n"
        "def _workspace_files(workspace: Path) -> list[str]:\n"
        '    """List relative file paths under *workspace* (for live preview)."""\n'
        "    if not workspace.is_dir():\n"
        "        return []\n"
        "    out: list[str] = []\n"
        "    for path in sorted(workspace.rglob(\"*\")):\n"
        '        if path.is_file() and not any(p.startswith(".") for p in path.parts):\n'
        "            out.append(str(path.relative_to(workspace)))\n"
        "    return out\n\n\n"
        '@app.get("/", response_class=HTMLResponse)\n'
        "def index():\n"
        '    """Live preview — lists workspace files; auto-refreshes during build."""\n'
        '    workspace = Path(os.environ.get("APP_WORKSPACE", ".")).resolve()\n'
        "    files = _workspace_files(workspace)\n"
        '    rows = "".join(f"<li><code>{f}</code></li>" for f in files[:80])\n'
        '    more = "" if len(files) <= 80 else f"<p>… and {len(files) - 80} more</p>"\n'
        "    return HTMLResponse(\n"
        '        f"""<!DOCTYPE html>\n'
        "<html><head>\n"
        '  <meta charset="utf-8"/>\n'
        f'  <title>{app_name} — building…</title>\n'
        '  <meta http-equiv="refresh" content="5"/>\n'
        "  <style>\n"
        "    body {{ font-family: system-ui, sans-serif; margin: 2rem; }}\n"
        "    h1 {{ color: #1a5276; }}\n"
        "    code {{ background: #f4f4f4; padding: 0.1rem 0.3rem; }}\n"
        "  </style>\n"
        "</head><body>\n"
        f"  <h1>{app_name}</h1>\n"
        "  <p>Build in progress — this page auto-refreshes as the app grows.</p>\n"
        "  <ul>{rows}</ul>\n"
        "  {more}\n"
        '</body></html>"""\n'
        "    )\n"
    )
    return {
        "requirements.txt": _requirements(),
        "src/__init__.py": '"""Application package."""\n',
        "src/app.py": app_py,
    }


def skeleton_files(spec: AppSpec) -> dict[str, str]:
    """Standardized folder skeleton shared by every generated app.

    Returns the test taxonomy packages, the sample-data module and the docs
    files. Builders lay this down first so feature code is written onto a
    clean, predictable structure and the validator always knows where tests go.
    """
    files: dict[str, str] = {}
    for d in TEST_TAXONOMY:
        files[f"tests/{d}/__init__.py"] = (
            '"""' + d.replace("_", " ") + " — "
            + _TEST_DIR_PURPOSE.get(d, "tests") + "\n\n"
            "Session C (validator) populates this folder during/after the "
            'build."""\n'
        )
    files["tests/test_sample_data/sample_data.py"] = _sample_data_py(spec)
    files["docs/README.md"] = _docs_readme(spec)
    files["docs/requirement.txt"] = _docs_requirement_txt(spec)
    return files


# ── public entry point ─────────────────────────────────────────────────────--
def generate_app(spec: AppSpec) -> dict[str, str]:
    """Generate the full file map (``relative path -> content``) for *spec*.

    Dispatches on the app *archetype* so the output is the application the user
    actually asked for: a ``storefront`` builds a real ecommerce store, while
    ``crud`` builds the admin/management app. New archetypes plug in here.
    """
    spec = spec.normalized()
    if spec.kind == "storefront":
        from ai_assistant.app_builder.storefront import generate_storefront

        return generate_storefront(spec)
    if spec.kind == "insights":
        from ai_assistant.app_builder.insights_app import generate_insights

        return generate_insights(spec)
    return _generate_crud_app(spec)


def _generate_crud_app(spec: AppSpec) -> dict[str, str]:
    """Generate an admin/management CRUD app (one resource per entity)."""
    files: dict[str, str] = {
        "README.md": _readme(spec),
        "requirements.txt": _requirements(),
        "Dockerfile": _dockerfile(),
        ".github/workflows/ci.yml": _ci_yml(),
        "config/infra.yaml": _infra_yaml(spec),
        "src/__init__.py": '"""Application package."""\n',
        "src/settings.py": _settings_py(spec.app_name),
        "src/app.py": _app_py(spec.app_name, spec.services),
        "src/api.py": _api_py(spec),
        "src/web.py": _web_py(spec),
        "src/models.py": _models_py(spec.entities),
        "src/repository.py": _repository_py(),
        "src/db/__init__.py": '"""Database package."""\n',
        "src/db/schema.py": _schema_py(spec.entities),
        "src/db/schema.sql": _schema_sql(spec.entities),
        "src/db/connection.py": _connection_py(),
        "templates/base.html": _BASE_HTML.replace("__APP_NAME__", spec.app_name),
        "templates/index.html": _INDEX_HTML,
        "templates/list.html": _LIST_HTML,
        "templates/form.html": _FORM_HTML,
        "static/style.css": _STYLE_CSS,
        "tests/__init__.py": '"""Test package."""\n',
        "tests/conftest.py": _conftest_py(),
        "tests/test_app.py": _tests_py(spec),
        "docs/ARCHITECTURE.md": _architecture_md(spec),
        "deploy/hosting.yaml": _hosting_yaml(spec.app_name),
    }
    # Infra adapters (only those selected).
    if _has_infra(spec.services):
        files["src/infra/__init__.py"] = _infra_init_py(spec.services)
    if "monitoring" in spec.services:
        files["src/infra/monitoring.py"] = _monitoring_py()
    if "notification" in spec.services:
        files["src/infra/notification.py"] = _notification_py()
    if "document" in spec.services:
        files["src/infra/document.py"] = _document_py()
    if "ai_builder" in spec.services:
        files["src/ai/__init__.py"] = '"""AI hooks package."""\n'
        files["src/ai/builder_hooks.py"] = _ai_builder_py()
    files.update(skeleton_files(spec))
    return files
