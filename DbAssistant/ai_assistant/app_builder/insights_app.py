"""DB insights / admin dashboard generator.

Builds a read-only FastAPI app for DBAs: schema catalog, column profiling,
relationship graph, data-quality signals, and sample-data viewer — all driven
by live SQLite introspection (PRAGMA + catalog queries), not static CRUD.
"""

from __future__ import annotations

from ai_assistant.app_builder.spec import AppSpec
from ai_assistant.app_builder.webapp import (
    _ci_yml,
    _conftest_py,
    _dockerfile,
    _has_infra,
    _hosting_yaml,
    _infra_init_py,
    _infra_yaml,
    _monitoring_py,
    _notification_py,
    _document_py,
    _requirements,
    _settings_py,
    skeleton_files,
)


def generate_insights(spec: AppSpec) -> dict[str, str]:
    """Return the full file map for a DBA insights dashboard."""
    spec = spec.normalized()
    files: dict[str, str] = {
        "README.md": _readme(spec),
        "requirements.txt": _requirements(),
        "Dockerfile": _dockerfile(),
        ".github/workflows/ci.yml": _ci_yml(),
        "config/infra.yaml": _infra_yaml(spec),
        "src/__init__.py": '"""Application package."""\n',
        "src/settings.py": _settings_py(spec.app_name),
        "src/app.py": _app_py(spec.app_name, spec.services),
        "src/api.py": _api_py(),
        "src/web.py": _web_py(),
        "src/introspect.py": _introspect_py(),
        "src/models.py": '"""Insights models (runtime introspection only)."""\n',
        "src/repository.py": '"""No CRUD repository — insights app is read-only."""\n',
        "src/db/__init__.py": '"""Database package."""\n',
        "src/db/schema.py": '"""Schema is discovered at runtime via PRAGMA."""\n',
        "src/db/schema.sql": "-- Runtime introspection; no static DDL.\n",
        "src/db/connection.py": _connection_py(),
        "templates/base.html": _BASE_HTML.replace("__APP_NAME__", spec.app_name),
        "templates/index.html": _INDEX_HTML,
        "templates/table_detail.html": _TABLE_DETAIL_HTML,
        "templates/relationships.html": _RELATIONSHIPS_HTML,
        "templates/sample.html": _SAMPLE_HTML,
        "static/style.css": _STYLE_CSS,
        "tests/__init__.py": '"""Test package."""\n',
        "tests/conftest.py": _conftest_py(),
        "tests/test_app.py": _tests_py(spec),
        "docs/ARCHITECTURE.md": _architecture_md(spec),
        "deploy/hosting.yaml": _hosting_yaml(spec.app_name),
    }
    if _has_infra(spec.services):
        files["src/infra/__init__.py"] = _infra_init_py(spec.services)
    if "monitoring" in spec.services:
        files["src/infra/monitoring.py"] = _monitoring_py()
    if "notification" in spec.services:
        files["src/infra/notification.py"] = _notification_py()
    if "document" in spec.services:
        files["src/infra/document.py"] = _document_py()
    files.update(skeleton_files(spec))
    return files


def _connection_py() -> str:
    return (
        '"""SQLite connection for live catalog introspection."""\n'
        "from __future__ import annotations\n\n"
        "import os\n"
        "import sqlite3\n"
        "import threading\n\n"
        "_LOCK = threading.Lock()\n"
        "_CONNECTION: sqlite3.Connection | None = None\n\n\n"
        "def get_connection() -> sqlite3.Connection:\n"
        '    """Return a process-wide SQLite connection (read-only introspection)."""\n'
        "    global _CONNECTION\n"
        "    with _LOCK:\n"
        "        if _CONNECTION is None:\n"
        '            path = os.environ.get("APP_DB_PATH", "var/app.db")\n'
        '            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)\n'
        "            conn = sqlite3.connect(path, check_same_thread=False)\n"
        "            conn.row_factory = sqlite3.Row\n"
        "            _CONNECTION = conn\n"
        "        return _CONNECTION\n\n\n"
        "def reset_connection() -> None:\n"
        '    """Close the cached connection (used by tests)."""\n'
        "    global _CONNECTION\n"
        "    with _LOCK:\n"
        "        if _CONNECTION is not None:\n"
        "            _CONNECTION.close()\n"
        "            _CONNECTION = None\n"
    )


def _introspect_py() -> str:
    return (
        '"""Live database introspection for the DBA insights dashboard.\n\n'
        "Uses SQLite PRAGMA and catalog queries to expose schema metadata,\n"
        "column statistics, relationships, and data-quality signals.\n"
        '"""\n'
        "from __future__ import annotations\n\n"
        "import re\n"
        "from typing import Any\n\n"
        "from src.db.connection import get_connection\n\n"
        "_ID_RE = re.compile(r\"(^id$|_id$|^.*_id$)\", re.I)\n"
        "_MONEY_RE = re.compile(r\"(price|amount|cost|fee|total|salary|balance)\", re.I)\n"
        "_TEMPORAL_RE = re.compile(r\"(date|time|timestamp|created|updated|modified)\", re.I)\n"
        "_PII_RE = re.compile(r\"(email|phone|ssn|password|secret|address|name)\", re.I)\n"
        "_STATUS_RE = re.compile(r\"(status|state|stage|phase)\", re.I)\n\n\n"
        "def list_tables() -> list[str]:\n"
        '    """Return user table names (excludes sqlite_ internals)."""\n'
        "    conn = get_connection()\n"
        '    rows = conn.execute(\n'
        '        "SELECT name FROM sqlite_master WHERE type=\'table\' "\n'
        '        "AND name NOT LIKE \'sqlite_%\' ORDER BY name"\n'
        "    ).fetchall()\n"
        "    return [str(r[0]) for r in rows]\n\n\n"
        "def table_row_count(table: str) -> int:\n"
        '    """Return row count for *table* (validated name only)."""\n'
        "    if table not in list_tables():\n"
        "        return 0\n"
        '    row = get_connection().execute(\n'
        '        "SELECT COUNT(*) FROM " + table\n'
        "    ).fetchone()\n"
        "    return int(row[0] or 0) if row else 0\n\n\n"
        "def table_columns(table: str) -> list[dict[str, Any]]:\n"
        '    """Return column metadata from PRAGMA table_info."""\n'
        "    if table not in list_tables():\n"
        "        return []\n"
        "    conn = get_connection()\n"
        '    info = conn.execute("PRAGMA table_info(" + table + ")").fetchall()\n'
        '    fks = {r[3]: r for r in conn.execute("PRAGMA foreign_key_list(" + table + ")").fetchall()}\n'
        '    idx_cols: set[str] = set()\n'
        '    for idx in conn.execute("PRAGMA index_list(" + table + ")").fetchall():\n'
        '        for ic in conn.execute("PRAGMA index_info(" + str(idx[1]) + ")").fetchall():\n'
        "            idx_cols.add(str(ic[2]))\n"
        "    out: list[dict[str, Any]] = []\n"
        "    for row in info:\n"
        "        cid, name, ctype, notnull, default, pk = row\n"
        "        tags = _semantic_tags(str(name))\n"
        "        out.append({\n"
        '            "name": str(name),\n'
        '            "type": str(ctype or ""),\n'
        '            "not_null": bool(notnull),\n'
        '            "default": default,\n'
        '            "primary_key": bool(pk),\n'
        '            "foreign_key": str(name) in fks,\n'
        '            "indexed": str(name) in idx_cols,\n'
        '            "semantic_tags": tags,\n'
        "        })\n"
        "    return out\n\n\n"
        "def column_stats(table: str, column: str) -> dict[str, Any]:\n"
        '    """Profile a single column: nulls, distinct, min/max/sample."""\n'
        "    cols = {c['name'] for c in table_columns(table)}\n"
        "    if column not in cols:\n"
        "        return {}\n"
        "    conn = get_connection()\n"
        "    total = table_row_count(table)\n"
        '    nulls = conn.execute(\n'
        '        "SELECT COUNT(*) FROM " + table + " WHERE " + column + " IS NULL"\n'
        "    ).fetchone()[0]\n"
        '    distinct = conn.execute(\n'
        '        "SELECT COUNT(DISTINCT " + column + ") FROM " + table\n'
        "    ).fetchone()[0]\n"
        "    stats: dict[str, Any] = {\n"
        '        "total_rows": total,\n'
        '        "null_count": int(nulls or 0),\n'
        '        "null_pct": round(100.0 * (nulls or 0) / total, 1) if total else 0.0,\n'
        '        "distinct_count": int(distinct or 0),\n'
        "    }\n"
        '    samples = conn.execute(\n'
        '        "SELECT DISTINCT " + column + " FROM " + table\n'
        '        + " WHERE " + column + " IS NOT NULL LIMIT 5"\n'
        "    ).fetchall()\n"
        '    stats["sample_values"] = [str(r[0])[:80] for r in samples]\n'
        "    try:\n"
        '        mn = conn.execute("SELECT MIN(" + column + ") FROM " + table).fetchone()[0]\n'
        '        mx = conn.execute("SELECT MAX(" + column + ") FROM " + table).fetchone()[0]\n'
        '        stats["min_value"] = str(mn)[:80] if mn is not None else None\n'
        '        stats["max_value"] = str(mx)[:80] if mx is not None else None\n'
        "    except Exception:\n"
        "        pass\n"
        "    return stats\n\n\n"
        "def table_profile(table: str) -> dict[str, Any]:\n"
        '    """Full table profile: columns, stats, role hint."""\n'
        "    columns = table_columns(table)\n"
        "    row_count = table_row_count(table)\n"
        "    fks_out = foreign_keys(table)\n"
        "    fks_in = incoming_foreign_keys(table)\n"
        "    role = _classify_role(table, columns, row_count, fks_out, fks_in)\n"
        "    col_stats = []\n"
        "    for col in columns:\n"
        "        cs = column_stats(table, col['name'])\n"
        "        col_stats.append({**col, **cs})\n"
        "    return {\n"
        '        "table": table,\n'
        '        "row_count": row_count,\n'
        '        "column_count": len(columns),\n'
        '        "role": role,\n'
        '        "columns": col_stats,\n'
        '        "outgoing_fks": fks_out,\n'
        '        "incoming_fks": fks_in,\n'
        "    }\n\n\n"
        "def foreign_keys(table: str) -> list[dict[str, str]]:\n"
        '    """Declared outgoing foreign keys from PRAGMA foreign_key_list."""\n'
        "    if table not in list_tables():\n"
        "        return []\n"
        '    rows = get_connection().execute(\n'
        '        "PRAGMA foreign_key_list(" + table + ")"\n'
        "    ).fetchall()\n"
        "    out = []\n"
        "    for r in rows:\n"
        "        out.append({\n"
        '            "from_table": table,\n'
        '            "from_column": str(r[3]),\n'
        '            "to_table": str(r[2]),\n'
        '            "to_column": str(r[4]),\n'
        '            "source": "declared",\n'
        "        })\n"
        "    return out\n\n\n"
        "def incoming_foreign_keys(table: str) -> list[dict[str, str]]:\n"
        '    """Find FKs from other tables pointing at *table*."""\n'
        "    out = []\n"
        "    for other in list_tables():\n"
        "        if other == table:\n"
        "            continue\n"
        "        for fk in foreign_keys(other):\n"
        "            if fk['to_table'] == table:\n"
        "                out.append(fk)\n"
        "    return out\n\n\n"
        "def all_relationships() -> list[dict[str, str]]:\n"
        '    """All declared FK edges in the database."""\n'
        "    rels: list[dict[str, str]] = []\n"
        "    for table in list_tables():\n"
        "        rels.extend(foreign_keys(table))\n"
        "    return rels\n\n\n"
        "def sample_rows(table: str, limit: int = 25) -> list[dict[str, Any]]:\n"
        '    """Return up to *limit* sample rows as dicts."""\n'
        "    if table not in list_tables():\n"
        "        return []\n"
        "    conn = get_connection()\n"
        '    cur = conn.execute("SELECT * FROM " + table + " LIMIT ?", (limit,))\n'
        "    return [dict(row) for row in cur.fetchall()]\n\n\n"
        "def dashboard_summary() -> dict[str, Any]:\n"
        '    """Overview metrics for the home dashboard."""\n'
        "    tables = list_tables()\n"
        "    rels = all_relationships()\n"
        "    total_rows = sum(table_row_count(t) for t in tables)\n"
        "    cards = []\n"
        "    for t in tables:\n"
        "        prof = table_profile(t)\n"
        "        cards.append({\n"
        '            "table": t,\n'
        '            "row_count": prof["row_count"],\n'
        '            "column_count": prof["column_count"],\n'
        '            "role": prof["role"],\n'
        "        })\n"
        "    quality = _quality_signals(tables)\n"
        "    return {\n"
        '        "table_count": len(tables),\n'
        '        "total_rows": total_rows,\n'
        '        "relationship_count": len(rels),\n'
        '        "tables": cards,\n'
        '        "relationships": rels,\n'
        '        "quality": quality,\n'
        "    }\n\n\n"
        "def _semantic_tags(name: str) -> list[str]:\n"
        "    tags: list[str] = []\n"
        "    if _ID_RE.search(name):\n"
        '        tags.append("identifier")\n'
        "    if _MONEY_RE.search(name):\n"
        '        tags.append("money")\n'
        "    if _TEMPORAL_RE.search(name):\n"
        '        tags.append("temporal")\n'
        "    if _PII_RE.search(name):\n"
        '        tags.append("pii")\n'
        "    if _STATUS_RE.search(name):\n"
        '        tags.append("status")\n'
        "    return tags\n\n\n"
        "def _classify_role(table, columns, row_count, fks_out, fks_in) -> str:\n"
        '    """Heuristic table role for DBA navigation."""\n'
        "    if len(fks_out) >= 2 and len(columns) <= 6:\n"
        '        return "junction"\n'
        "    if len(fks_in) >= 2 and row_count < 500:\n"
        '        return "master"\n'
        "    if len(fks_out) >= 1 and row_count > 100:\n"
        '        return "transaction"\n'
        "    if row_count < 50 and len(columns) <= 5:\n"
        '        return "lookup"\n'
        "    if any('audit' in c['name'].lower() or 'log' in table.lower() for c in columns):\n"
        '        return "audit"\n'
        '    return "general"\n\n\n'
        "def _quality_signals(tables: list[str]) -> list[dict[str, str]]:\n"
        '    """Data-quality advisories for the DBA."""\n'
        "    signals: list[dict[str, str]] = []\n"
        "    rel_tables = {r['from_table'] for r in all_relationships()}\n"
        "    for t in tables:\n"
        "        cols = table_columns(t)\n"
        "        id_cols = [c for c in cols if _ID_RE.search(c['name']) and not c['primary_key']]\n"
        "        if id_cols and t not in rel_tables:\n"
        "            signals.append({\n"
        '                "level": "info",\n'
        '                "table": t,\n'
        '                "message": f"Column(s) {[c[\'name\'] for c in id_cols]} look like FKs but no declared constraint",\n'
        "            })\n"
        "        for col in cols:\n"
        "            if col['primary_key']:\n"
        "                continue\n"
        "            st = column_stats(t, col['name'])\n"
        "            if st.get('null_pct', 0) > 50 and st.get('total_rows', 0) > 0:\n"
        "                signals.append({\n"
        '                    "level": "warning",\n'
        '                    "table": t,\n'
        '                    "message": f"{col[\'name\']}: {st[\'null_pct\']}% null values",\n'
        "                })\n"
        "    if not all_relationships() and len(tables) > 1:\n"
        "        signals.append({\n"
        '            "level": "info",\n'
        '            "table": "(schema)",\n'
        '            "message": "No declared foreign keys — relationships may be implicit only",\n'
        "        })\n"
        "    return signals[:20]\n"
    )


def _api_py() -> str:
    return (
        '"""JSON API for database insights (read-only)."""\n'
        "from __future__ import annotations\n\n"
        "from fastapi import APIRouter, HTTPException\n\n"
        "from src.introspect import (\n"
        "    all_relationships,\n"
        "    dashboard_summary,\n"
        "    list_tables,\n"
        "    sample_rows,\n"
        "    table_profile,\n"
        ")\n\n"
        'router = APIRouter(prefix="/api")\n\n\n'
        '@router.get("/summary")\n'
        "def api_summary() -> dict:\n"
        '    """Dashboard summary: tables, rows, relationships, quality."""\n'
        "    return dashboard_summary()\n\n\n"
        '@router.get("/tables")\n'
        "def api_tables() -> list[str]:\n"
        '    """List all user tables."""\n'
        "    return list_tables()\n\n\n"
        '@router.get("/tables/{table}")\n'
        "def api_table(table: str) -> dict:\n"
        '    """Full profile for one table."""\n'
        "    if table not in list_tables():\n"
        '        raise HTTPException(status_code=404, detail="table not found")\n'
        "    return table_profile(table)\n\n\n"
        '@router.get("/relationships")\n'
        "def api_relationships() -> list[dict]:\n"
        '    """All declared foreign-key relationships."""\n'
        "    return all_relationships()\n\n\n"
        '@router.get("/tables/{table}/sample")\n'
        "def api_sample(table: str, limit: int = 25) -> list[dict]:\n"
        '    """Sample rows from *table*."""\n'
        "    if table not in list_tables():\n"
        '        raise HTTPException(status_code=404, detail="table not found")\n'
        "    return sample_rows(table, limit=min(limit, 100))\n"
    )


def _web_py() -> str:
    return (
        '"""Server-rendered DBA insights UI."""\n'
        "from __future__ import annotations\n\n"
        "from pathlib import Path\n\n"
        "from fastapi import APIRouter, HTTPException, Request\n"
        "from fastapi.responses import HTMLResponse\n"
        "from fastapi.templating import Jinja2Templates\n\n"
        "from src.introspect import (\n"
        "    all_relationships,\n"
        "    dashboard_summary,\n"
        "    list_tables,\n"
        "    sample_rows,\n"
        "    table_profile,\n"
        ")\n\n"
        "_TEMPLATES = Jinja2Templates(\n"
        "    directory=str(Path(__file__).resolve().parent.parent / \"templates\")\n"
        ")\n\n"
        "router = APIRouter()\n\n\n"
        '@router.get("/", response_class=HTMLResponse)\n'
        "def dashboard(request: Request) -> HTMLResponse:\n"
        '    """DB insights home: schema overview + quality signals."""\n'
        "    summary = dashboard_summary()\n"
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "index.html", {"summary": summary, "tables": list_tables()}\n'
        "    )\n\n\n"
        '@router.get("/tables/{table}", response_class=HTMLResponse)\n'
        "def table_detail(request: Request, table: str) -> HTMLResponse:\n"
        '    """Column stats and metadata for one table."""\n'
        "    if table not in list_tables():\n"
        '        raise HTTPException(status_code=404, detail="table not found")\n'
        "    profile = table_profile(table)\n"
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "table_detail.html", {"profile": profile, "tables": list_tables()}\n'
        "    )\n\n\n"
        '@router.get("/relationships", response_class=HTMLResponse)\n'
        "def relationships(request: Request) -> HTMLResponse:\n"
        '    """Foreign-key relationship graph."""\n'
        "    rels = all_relationships()\n"
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "relationships.html",\n'
        '        {"relationships": rels, "tables": list_tables()},\n'
        "    )\n\n\n"
        '@router.get("/tables/{table}/sample", response_class=HTMLResponse)\n'
        "def sample_view(request: Request, table: str) -> HTMLResponse:\n"
        '    """Sample data viewer for *table*."""\n'
        "    if table not in list_tables():\n"
        '        raise HTTPException(status_code=404, detail="table not found")\n'
        "    rows = sample_rows(table)\n"
        "    cols = list(rows[0].keys()) if rows else []\n"
        "    return _TEMPLATES.TemplateResponse(\n"
        '        request, "sample.html",\n'
        '        {"table": table, "columns": cols, "rows": rows, "tables": list_tables()},\n'
        "    )\n"
    )


def _app_py(app_name: str, services: list[str]) -> str:
    infra_line = "    infra_startup(app, settings)\n" if _has_infra(services) else ""
    infra_import = (
        "from src.infra import startup as infra_startup\n" if _has_infra(services) else ""
    )
    return (
        '"""DB insights dashboard entry point."""\n'
        "from __future__ import annotations\n\n"
        "from pathlib import Path\n\n"
        "from fastapi import FastAPI\n"
        "from fastapi.staticfiles import StaticFiles\n\n"
        "from src.api import router as api_router\n"
        + infra_import
        + "from src.settings import get_settings\n"
        "from src.web import router as web_router\n\n\n"
        "def create_app() -> FastAPI:\n"
        '    """Create the DBA insights FastAPI application."""\n'
        "    settings = get_settings()\n"
        "    app = FastAPI(title=settings.app_name + \" — DB Insights\")\n"
        "    static_dir = Path(__file__).resolve().parent.parent / \"static\"\n"
        '    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")\n\n'
        '    @app.get("/health")\n'
        "    def health() -> dict:\n"
        '        """App + database health for monitoring."""\n'
        '        database = {"status": "up"}\n'
        "        try:\n"
        "            from src.db.connection import get_connection\n"
        '            get_connection().execute("SELECT 1")\n'
        "        except Exception as exc:\n"
        '            database = {"status": "down", "error": str(exc)[:200]}\n'
        "        return {\n"
        '            "status": "healthy",\n'
        '            "service": settings.app_name,\n'
        '            "mode": "db_insights",\n'
        '            "database": database,\n'
        "        }\n\n"
        "    app.include_router(api_router)\n"
        "    app.include_router(web_router)\n"
        + infra_line
        + "    return app\n\n\n"
        "app = create_app()\n"
    ).replace("__APP_NAME__", app_name)


_BASE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ app_name | default("__APP_NAME__") }} — DB Insights</title>
  <link rel="stylesheet" href="/static/style.css">
</head>
<body>
  <header class="topbar">
    <a class="brand" href="/">__APP_NAME__</a>
    <nav>
      <a href="/">Overview</a>
      <a href="/relationships">Relationships</a>
      {% for t in tables | default([]) %}
      <a href="/tables/{{ t }}">{{ t }}</a>
      {% endfor %}
    </nav>
  </header>
  <main class="container">
    {% block content %}{% endblock %}
  </main>
  <footer class="footer">DB Insights — generated by AppBuilderAssistant</footer>
</body>
</html>
"""

_INDEX_HTML = """{% extends "base.html" %}
{% block content %}
<h1>Database Overview</h1>
<div class="metrics">
  <div class="metric"><span class="metric-val">{{ summary.table_count }}</span><span class="metric-lbl">Tables</span></div>
  <div class="metric"><span class="metric-val">{{ summary.total_rows }}</span><span class="metric-lbl">Total rows</span></div>
  <div class="metric"><span class="metric-val">{{ summary.relationship_count }}</span><span class="metric-lbl">FK relationships</span></div>
</div>

<h2>Tables</h2>
<table class="grid">
  <thead><tr><th>Table</th><th>Role</th><th>Rows</th><th>Columns</th><th>Actions</th></tr></thead>
  <tbody>
  {% for card in summary.tables %}
  <tr>
    <td><strong>{{ card.table }}</strong></td>
    <td><span class="badge badge-{{ card.role }}">{{ card.role }}</span></td>
    <td>{{ card.row_count }}</td>
    <td>{{ card.column_count }}</td>
    <td class="actions">
      <a class="btn" href="/tables/{{ card.table }}">Profile</a>
      <a class="btn" href="/tables/{{ card.table }}/sample">Sample</a>
    </td>
  </tr>
  {% endfor %}
  </tbody>
</table>

{% if summary.quality %}
<h2>Data quality signals</h2>
<ul class="signals">
  {% for sig in summary.quality %}
  <li class="signal signal-{{ sig.level }}">
    <strong>{{ sig.table }}</strong>: {{ sig.message }}
  </li>
  {% endfor %}
</ul>
{% endif %}
{% endblock %}
"""

_TABLE_DETAIL_HTML = """{% extends "base.html" %}
{% block content %}
<h1>{{ profile.table }}</h1>
<p>
  <span class="badge badge-{{ profile.role }}">{{ profile.role }}</span>
  {{ profile.row_count }} rows · {{ profile.column_count }} columns
</p>

{% if profile.outgoing_fks or profile.incoming_fks %}
<h2>Relationships</h2>
<ul>
  {% for fk in profile.outgoing_fks %}
  <li>{{ fk.from_table }}.{{ fk.from_column }} → {{ fk.to_table }}.{{ fk.to_column }} <em>(declared)</em></li>
  {% endfor %}
  {% for fk in profile.incoming_fks %}
  <li>{{ fk.from_table }}.{{ fk.from_column }} → {{ fk.to_table }}.{{ fk.to_column }} <em>(incoming)</em></li>
  {% endfor %}
</ul>
{% endif %}

<h2>Columns</h2>
<table class="grid">
  <thead>
    <tr>
      <th>Column</th><th>Type</th><th>PK</th><th>FK</th><th>Indexed</th>
      <th>Nulls</th><th>Distinct</th><th>Tags</th><th>Samples</th>
    </tr>
  </thead>
  <tbody>
  {% for col in profile.columns %}
  <tr>
    <td><strong>{{ col.name }}</strong></td>
    <td>{{ col.type }}</td>
    <td>{% if col.primary_key %}✓{% endif %}</td>
    <td>{% if col.foreign_key %}✓{% endif %}</td>
    <td>{% if col.indexed %}✓{% endif %}</td>
    <td>{{ col.null_pct | default(0) }}%</td>
    <td>{{ col.distinct_count | default("—") }}</td>
    <td>{% for tag in col.semantic_tags %}<span class="tag">{{ tag }}</span>{% endfor %}</td>
    <td class="samples">{{ (col.sample_values or []) | join(", ") }}</td>
  </tr>
  {% endfor %}
  </tbody>
</table>
<p><a class="btn primary" href="/tables/{{ profile.table }}/sample">View sample data →</a></p>
{% endblock %}
"""

_RELATIONSHIPS_HTML = """{% extends "base.html" %}
{% block content %}
<h1>Relationship Graph</h1>
{% if relationships %}
<table class="grid">
  <thead><tr><th>From</th><th></th><th>To</th><th>Source</th></tr></thead>
  <tbody>
  {% for rel in relationships %}
  <tr>
    <td><a href="/tables/{{ rel.from_table }}">{{ rel.from_table }}</a>.{{ rel.from_column }}</td>
    <td>→</td>
    <td><a href="/tables/{{ rel.to_table }}">{{ rel.to_table }}</a>.{{ rel.to_column }}</td>
    <td>{{ rel.source }}</td>
  </tr>
  {% endfor %}
  </tbody>
</table>
{% else %}
<p class="empty">No declared foreign keys found. Check data-quality signals on the overview for inferred relationships.</p>
{% endif %}
{% endblock %}
"""

_SAMPLE_HTML = """{% extends "base.html" %}
{% block content %}
<h1>Sample: {{ table }}</h1>
{% if rows %}
<table class="grid">
  <thead><tr>{% for col in columns %}<th>{{ col }}</th>{% endfor %}</tr></thead>
  <tbody>
  {% for row in rows %}
  <tr>{% for col in columns %}<td>{{ row.get(col, "") }}</td>{% endfor %}</tr>
  {% endfor %}
  </tbody>
</table>
{% else %}
<p class="empty">No rows in this table.</p>
{% endif %}
<p><a class="btn" href="/tables/{{ table }}">← Back to profile</a></p>
{% endblock %}
"""

_STYLE_CSS = """:root {
  --bg: #0c1929; --panel: #ffffff; --ink: #0f172a; --muted: #64748b;
  --accent: #0891b2; --warn: #d97706; --info: #2563eb; --line: #e2e8f0;
}
* { box-sizing: border-box; }
body { margin: 0; font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif;
  color: var(--ink); background: #f0f4f8; }
.topbar { display: flex; align-items: center; gap: 24px; padding: 12px 24px;
  background: var(--bg); color: #fff; flex-wrap: wrap; }
.brand { font-weight: 700; color: #fff; text-decoration: none; font-size: 18px; }
.topbar nav { display: flex; gap: 12px; flex-wrap: wrap; }
.topbar nav a { color: #94a3b8; text-decoration: none; font-size: 13px; }
.topbar nav a:hover { color: #fff; }
.container { max-width: 1100px; margin: 24px auto; padding: 0 20px; }
.metrics { display: flex; gap: 20px; margin-bottom: 24px; flex-wrap: wrap; }
.metric { background: var(--panel); border: 1px solid var(--line); border-radius: 12px;
  padding: 16px 24px; text-align: center; min-width: 120px; }
.metric-val { display: block; font-size: 32px; font-weight: 700; color: var(--accent); }
.metric-lbl { font-size: 12px; color: var(--muted); text-transform: uppercase; }
.grid { width: 100%; border-collapse: collapse; background: var(--panel);
  border: 1px solid var(--line); border-radius: 12px; overflow: hidden; margin-bottom: 20px; }
.grid th, .grid td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--line); font-size: 14px; }
.grid th { background: #f8fafc; font-size: 11px; text-transform: uppercase; color: var(--muted); }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px;
  font-weight: 600; text-transform: uppercase; background: #e2e8f0; color: #475569; }
.badge-master { background: #dbeafe; color: #1d4ed8; }
.badge-transaction { background: #fef3c7; color: #92400e; }
.badge-junction { background: #ede9fe; color: #6d28d9; }
.badge-lookup { background: #d1fae5; color: #065f46; }
.badge-audit { background: #fee2e2; color: #991b1b; }
.tag { display: inline-block; padding: 1px 6px; margin: 1px; border-radius: 4px;
  font-size: 10px; background: #f1f5f9; color: #475569; }
.signals { list-style: none; padding: 0; }
.signal { padding: 10px 14px; margin-bottom: 8px; border-radius: 8px; background: var(--panel);
  border-left: 4px solid var(--info); }
.signal-warning { border-left-color: var(--warn); }
.btn { display: inline-block; padding: 6px 12px; border-radius: 8px; border: 1px solid var(--line);
  background: #fff; color: var(--ink); text-decoration: none; font-size: 13px; }
.btn.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
.actions { display: flex; gap: 6px; }
.samples { font-size: 12px; color: var(--muted); max-width: 200px; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; }
.empty { color: var(--muted); padding: 20px; }
.footer { text-align: center; color: var(--muted); padding: 30px; font-size: 12px; }
"""


def _tests_py(spec: AppSpec) -> str:
    return (
        '"""Tests for the DB insights dashboard."""\n'
        "from __future__ import annotations\n\n"
        "from fastapi.testclient import TestClient\n\n"
        "from src.app import app\n\n"
        "client = TestClient(app)\n\n\n"
        "def test_health():\n"
        '    resp = client.get("/health")\n'
        "    assert resp.status_code == 200\n"
        '    body = resp.json()\n'
        '    assert body["status"] == "healthy"\n'
        '    assert body["mode"] == "db_insights"\n\n\n'
        "def test_dashboard_renders():\n"
        '    resp = client.get("/")\n'
        "    assert resp.status_code == 200\n"
        '    assert "Database Overview" in resp.text\n\n\n'
        "def test_api_summary():\n"
        '    resp = client.get("/api/summary")\n'
        "    assert resp.status_code == 200\n"
        '    assert "table_count" in resp.json()\n\n\n'
        "def test_relationships_page():\n"
        '    resp = client.get("/relationships")\n'
        "    assert resp.status_code == 200\n"
    )


def _readme(spec: AppSpec) -> str:
    return (
        "# " + spec.app_name + " — DB Insights Dashboard\n\n"
        "A read-only DBA insights application generated by AppBuilderAssistant.\n\n"
        "## What this shows\n\n"
        "- **Schema overview** — all tables, row counts, inferred roles\n"
        "- **Column profiling** — null %, distinct counts, semantic tags, samples\n"
        "- **Relationship graph** — declared foreign keys\n"
        "- **Data quality signals** — missing FKs, high null columns\n"
        "- **Sample data viewer** — inspect live rows per table\n\n"
        "## Run\n\n"
        "```bash\n"
        "pip install -r requirements.txt\n"
        "uvicorn src.app:app --reload\n"
        "```\n\n"
        "Open http://127.0.0.1:8000/ for the dashboard.\n\n"
        "Point `APP_DB_PATH` at your SQLite database file.\n"
    )


def _architecture_md(spec: AppSpec) -> str:
    return (
        "# " + spec.app_name + " — DB Insights architecture\n\n"
        "## Purpose\n\n"
        "Read-only DBA dashboard that introspects the live SQLite database at "
        "runtime using PRAGMA catalog queries.\n\n"
        "## Layers\n\n"
        "- `src/introspect.py` — catalog + stats + relationship + quality logic\n"
        "- `src/web.py` — server-rendered HTML UI (overview, table profile, "
        "relationships, sample viewer)\n"
        "- `src/api.py` — JSON REST endpoints mirroring the UI data\n"
        "- `src/db/connection.py` — SQLite connection (no static schema DDL)\n\n"
        "## DBA surfaces\n\n"
        "1. Overview dashboard with table roles and quality advisories\n"
        "2. Per-table column profiling with semantic tags\n"
        "3. FK relationship graph\n"
        "4. Sample data viewer\n"
    )
