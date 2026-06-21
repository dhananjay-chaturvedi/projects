"""Modular AI channels + process meters for the App Builder.

Two strictly separate channels (per product spec):

* the **code agent** (direct chat backend) writes code/tests, and
* the **AI Query Assistant** (used as-is) understands the database's data.

These tests use a stub AI Query Assistant and a *real* in-memory SQLite manager
(so sample rows are genuine), plus a stub code-agent bridge, to exercise the
db-understanding channel, the three new meters, and the orchestrator journal —
no real model required.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from ai_assistant.app_builder.db_understanding import (
    DataInsight,
    DbUnderstandingClient,
    TableInsight,
)
from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator
from ai_assistant.meters import (
    DataUnderstandingMeter,
    ProcessAdherenceMeter,
    RequirementFidelityMeter,
)


# ── real sqlite-backed db manager (genuine sample data) ───────────────────────
class SqliteManager:
    db_type = "sqlite"

    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);"
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, total REAL);"
            "INSERT INTO products (name, price) VALUES ('Widget', 9.5), ('Gadget', 19.0);"
            "INSERT INTO orders (total) VALUES (28.5);"
        )
        self.conn.commit()

    def execute_query(self, sql: str):
        try:
            cur = self.conn.execute(sql)
            return [tuple(r) for r in cur.fetchall()], None
        except Exception as exc:  # noqa: BLE001
            return None, str(exc)


class RecordingManager:
    """Records every SQL it runs and answers like a catalog-aware engine."""

    def __init__(self, db_type: str = "MariaDB", table_rows: int = 42) -> None:
        self.db_type = db_type
        self.table_rows = table_rows
        self.queries: list[str] = []

    def execute_query(self, sql: str):
        self.queries.append(sql)
        low = sql.lower()
        if "information_schema.tables" in low or "table_rows" in low:
            return [(self.table_rows,)], None
        if "count(distinct" in low:
            return [(3,)], None
        if low.startswith("select sum(case when"):
            return [(0, 7)], None
        if "count(*)" in low:
            return [(7,)], None
        if low.startswith("select *"):
            return [(1, "Widget", 9.5)], None
        return [], None


class ColumnSampleCore:
    """Core stub whose sample_table returns real column names (+rows)."""

    db_type = "MariaDB"

    def open_connection(self, name, form=None):
        return {"ok": True}

    def get_manager(self, name, profile=None):
        return self

    def get_objects(self, name, obj_type="tables"):
        return []

    def get_table_schema(self, name, table):
        return {"columns": []}

    def sample_table(self, name, table, limit=5):
        return {"error": None, "table": table,
                "columns": ["id", "name", "price"],
                "rows": [[1, "Widget", 9.5], [2, "Gadget", None]],
                "rowcount": 2}

    def execute(self, name, sql):
        # No catalog stat available from this stub.
        return {"error": "no catalog", "rows": []}


class StubQueryAssistant:
    """Stands in for AIQueryAgent.start_new_conversation (understanding only)."""

    def __init__(self) -> None:
        self.questions: list[str] = []

    def start_new_conversation(self, question, db_manager, connection_name,
                               session_id=None):
        self.questions.append(question)
        return {"sql": "SELECT 1", "explanation": f"Understanding: {question[:40]}",
                "error": None}


class CleanFactsQueryAssistant:
    """Backend interpretation over deterministic profile facts."""

    def __init__(self) -> None:
        self.questions: list[str] = []
        self.backend_prompts: list[str] = []
        self.executed: list[str] = []

    def start_new_conversation(self, question, db_manager, connection_name,
                               session_id=None):
        self.questions.append(question)
        return {
            "sql": "SELECT name, price FROM products LIMIT 1",
            "explanation": (
                "Products are saleable catalog items.\n\n"
                "⚠️ SCHEMA VALIDATION WARNINGS:\n"
                "  • ignored token app_summary"
            ),
            "error": None,
        }

    def send_follow_up(self, question, db_manager, connection_name, session_id=None):
        return self.start_new_conversation(question, db_manager, connection_name,
                                           session_id=session_id)

    def execute_in_session(self, session_id, sql, db_manager):
        self.executed.append(sql)
        rows, err = db_manager.execute_query(sql)
        return {"result": rows, "error": err}

    def _call_ai(self, prompt, timeout=90):
        self.backend_prompts.append(prompt)
        if "app_summary" in prompt:
            return {
                "response": (
                    '{"app_summary": "an online store for product sales", '
                    '"data_flow": "products are listed, selected, and ordered"}'
                )
            }
        return {"response": '{"products": "Catalog items sold to customers."}'}


# ── requirement_fidelity_meter ────────────────────────────────────────────────
def test_fidelity_high_when_app_reflects_request():
    files = {
        "README.md": "# Staff shift scheduling for nurses\n",
        "src/api.py": '@router.get("/shifts")\n@router.get("/staff")\n',
        "src/web.py": "from fastapi.responses import HTMLResponse\n",
        "templates/schedule.html": "<html>nurse shift schedule</html>",
        "tests/test_app.py": "def test_x():\n    assert True\n",
    }
    m = RequirementFidelityMeter().measure(
        description="a staff shift scheduling app for nurses", files=files)
    assert m.score >= 0.8
    assert "shift" in m.evidence["matched"]
    assert "scheduling" in m.evidence["matched"]  # matched via 'schedule'


def test_fidelity_low_when_generic_crud_built_instead():
    files = {
        "src/api.py": '@router.get("/items")\n',
        "src/web.py": "from fastapi.responses import HTMLResponse\n",
        "templates/list.html": "<html>items</html>",
        "tests/test_app.py": "def test_x():\n    assert True\n",
    }
    m = RequirementFidelityMeter().measure(
        description="a staff shift scheduling app for nurses", files=files)
    assert m.score < 0.7
    assert "scheduling" in m.evidence["missing"]
    assert any("does not reflect" in i for i in m.issues)


# ── data_understanding_meter ──────────────────────────────────────────────────
def _full_insight() -> dict:
    return {
        "tables": [
            {"name": "products", "columns": ["id", "name", "price"],
             "row_count": 2, "sample_rows": [{"c0": 1}], "note": "items for sale"},
            {"name": "orders", "columns": ["id", "total"],
             "row_count": 1, "sample_rows": [{"c0": 1}], "note": "purchases"},
        ],
        "app_summary": "an online store", "data_flow": "products -> orders",
    }


def test_data_understanding_full_scores_high():
    m = DataUnderstandingMeter().measure(_full_insight())
    assert m.score >= 0.9 and not m.issues


def test_data_understanding_flags_missing_samples_and_summary():
    shallow = {
        "tables": [{"name": "products", "columns": ["id"], "sample_rows": [],
                    "note": ""}],
        "app_summary": "", "data_flow": "",
    }
    m = DataUnderstandingMeter().measure(shallow)
    assert m.score < 0.7
    assert any("sample data" in i for i in m.issues)
    assert any("app summary" in i for i in m.issues)


def test_data_understanding_zero_when_no_tables():
    m = DataUnderstandingMeter().measure({"tables": []})
    assert m.score == 0.0


# ── process_adherence_meter ───────────────────────────────────────────────────
def test_process_database_requires_query_assistant():
    good = ProcessAdherenceMeter().measure({
        "mode": "from_database",
        "channels": ["query_assistant", "code_agent"],
        "sample_data_created": True, "tests_run": True, "tests_passed": True,
        "verified_with_data": True,
    })
    assert good.score >= 0.9

    missing_qa = ProcessAdherenceMeter().measure({
        "mode": "from_database", "channels": ["code_agent"],
        "sample_data_created": True, "tests_run": True, "tests_passed": True,
    })
    assert missing_qa.score < good.score
    assert any("AI Query Assistant" in i for i in missing_qa.issues)


def test_process_scratch_does_not_require_query_assistant():
    m = ProcessAdherenceMeter().measure({
        "mode": "from_scratch", "channels": ["code_agent"],
        "sample_data_created": True, "tests_run": True, "tests_passed": True,
    })
    assert m.score >= 0.9
    assert not any("AI Query Assistant" in i for i in m.issues)


def test_process_scratch_flags_undeployed_schema_when_connection():
    m = ProcessAdherenceMeter().measure({
        "mode": "from_scratch", "channels": ["code_agent"], "connection": True,
        "sample_data_created": True, "tests_run": True, "tests_passed": True,
        "schema_deployed": False,
    })
    assert any("schema" in i for i in m.issues)


# ── DbUnderstandingClient (AI Query Assistant channel) ────────────────────────
def test_db_understanding_reads_real_samples_and_asks_assistant():
    qa = StubQueryAssistant()
    client = DbUnderstandingClient(
        query_assistant=qa, db_manager=SqliteManager(), connection_name="local")
    insight = client.understand({"products": ["id", "name", "price"],
                                 "orders": ["id", "total"]})
    assert {t.name for t in insight.tables} == {"products", "orders"}
    products = next(t for t in insight.tables if t.name == "products")
    assert products.row_count == 2
    assert products.sample_rows  # real rows read from sqlite
    assert products.note  # nature-of-data note from the assistant
    assert insight.app_summary and insight.data_flow
    # The assistant was consulted (understanding channel), not code-gen.
    assert any("products" in q for q in qa.questions)
    # And the meter rates this understanding as complete.
    from ai_assistant.meters import MeterSuite
    assert MeterSuite().evaluate_data_understanding(insight.as_dict())["score"] >= 0.9


def test_db_understanding_uses_profile_facts_without_aiq_execution():
    qa = CleanFactsQueryAssistant()
    client = DbUnderstandingClient(
        query_assistant=qa,
        db_manager=SqliteManager(),
        connection_name="local",
        tables_per_query=1,
    )
    insight = client.understand({"products": ["id", "name", "price"]})
    assert insight.app_summary == "an online store for product sales"
    assert insight.tables[0].note == "Catalog items sold to customers."
    assert qa.questions == []
    assert qa.executed == []
    handoff = "\n".join(qa.backend_prompts)
    assert "Widget" in handoff
    assert "SCHEMA VALIDATION WARNINGS" not in handoff
    assert "ignored token app_summary" not in handoff


class OneSessionCore:
    """Core stub that hands out a single cached manager and counts opens."""

    def __init__(self, manager):
        self._manager = manager
        self.opened = 0
        self.get_manager_calls = 0

    def open_connection(self, name, form=None):
        self.opened += 1
        return {"ok": True}

    def get_manager(self, name, profile=None):
        self.get_manager_calls += 1
        return self._manager

    def get_objects(self, name, obj_type="tables"):
        return []

    def get_table_schema(self, name, table):
        return {"columns": []}

    def sample_table(self, name, table, limit=5):
        rows, _ = self._manager.execute_query(f"SELECT * FROM {table} LIMIT {limit}")
        return {"error": None, "table": table, "columns": [], "rows": rows or [],
                "rowcount": len(rows or [])}


def test_db_understanding_opens_one_persistent_session():
    mgr = SqliteManager()
    core = OneSessionCore(mgr)
    client = DbUnderstandingClient(core=core, connection_name="local")
    client.understand({"products": ["id", "name", "price"],
                       "orders": ["id", "total"]})
    # Connection is resolved once and reused for the whole build (not per query).
    assert core.opened == 1
    assert core.get_manager_calls == 1
    # The profiler is bound to that exact single manager object.
    assert client._db is mgr
    assert client._profiler._db is mgr


def test_profiling_default_avoids_heavy_aggregation_and_uses_catalog():
    from ai_assistant.app_builder.db_profile import DbProfiler

    mgr = RecordingManager(db_type="MariaDB", table_rows=42)
    profiler = DbProfiler(
        db_manager=mgr, connection_name="c", deep_column_profiling=False)
    profile = profiler.profile({"products": ["id", "name", "price"]})
    joined = " ".join(q.lower() for q in mgr.queries)
    # No heavy per-column aggregation by default.
    assert "count(distinct" not in joined
    assert "sum(case when" not in joined
    # Row count came from the system catalog (no COUNT(*) scan).
    assert "information_schema.tables" in joined
    assert "count(*)" not in joined
    assert profile.tables[0].row_count_estimate == 42


def test_approx_column_stats_derived_from_sample_without_queries():
    from ai_assistant.app_builder.db_profile import DbProfiler

    core = ColumnSampleCore()
    profiler = DbProfiler(
        core=core, connection_name="c", deep_column_profiling=False)
    profile = profiler.profile({"products": ["id", "name", "price"]})
    cols = {c.name: c for c in profile.tables[0].columns}
    # price has one NULL across the two sampled rows → ~0.5 null ratio.
    assert cols["price"].null_ratio == 0.5
    # distinct estimate is derived from the sample (no DB aggregation).
    assert cols["name"].distinct_estimate == 2


def test_deep_profiling_runs_column_aggregations_when_enabled():
    from ai_assistant.app_builder.db_profile import DbProfiler

    mgr = RecordingManager(db_type="MariaDB", table_rows=42)
    profiler = DbProfiler(
        db_manager=mgr, connection_name="c", deep_column_profiling=True)
    profiler.profile({"products": ["id", "name", "price"]})
    joined = " ".join(q.lower() for q in mgr.queries)
    assert "count(distinct" in joined
    assert "sum(case when" in joined


def test_row_count_falls_back_to_exact_when_no_catalog_stat():
    from ai_assistant.app_builder.db_profile import DbProfiler

    # SQLite exposes no cheap catalog row stat → exact COUNT(*) fallback.
    profiler = DbProfiler(db_manager=SqliteManager(), connection_name="c")
    profile = profiler.profile({"products": ["id", "name", "price"],
                                "orders": ["id", "total"]})
    products = next(t for t in profile.tables if t.name == "products")
    assert products.row_count_estimate == 2


def test_row_count_none_when_no_catalog_and_exact_disabled():
    from ai_assistant.app_builder.db_profile import DbProfiler

    profiler = DbProfiler(
        db_manager=SqliteManager(), connection_name="c",
        exact_row_counts=False)
    profile = profiler.profile({"products": ["id", "name", "price"]})
    assert profile.tables[0].row_count_estimate is None


def test_db_understanding_degrades_without_assistant():
    client = DbUnderstandingClient(db_manager=SqliteManager())
    insight = client.understand({"products": ["id", "name", "price"]})
    p = insight.tables[0]
    assert p.sample_rows  # still reads real data
    assert p.note  # deterministic fallback note without the assistant
    assert insight.app_summary and insight.data_flow


# ── orchestrator: journal + grounding + meters ────────────────────────────────
class RecordingBridge:
    def __init__(self, responses):
        self._responses = list(responses)
        self.prompts: list[str] = []

    def available(self):
        return True

    def generate(self, prompt):
        self.prompts.append(prompt)
        return self._responses.pop(0) if self._responses else ""


class StubUnderstanding:
    def __init__(self, insight):
        self._insight = insight

    def available(self):
        return True

    def understand(self, schema):
        return self._insight


def _insight_obj() -> DataInsight:
    return DataInsight(
        connection="local",
        tables=[TableInsight(name="products", columns=["id", "name", "price"],
                             row_count=2, sample_rows=[{"name": "Widget"}],
                             note="items for sale")],
        app_summary="an online store", data_flow="products -> orders",
    )


def test_orchestrator_grounds_prompt_in_db_understanding(tmp_path):
    bridge = RecordingBridge([""])  # converge immediately, just capture prompt
    bp = AppBlueprint(name="store", mode=BuildMode.FROM_DATABASE,
                      connections=["local"], services=["database"],
                      description="an online store")
    orch = AppBuildOrchestrator(max_rounds=1)
    result = orch.run(
        bp, tmp_path / "ws",
        schema={"products": ["id", "name", "price"], "orders": ["id", "total"]},
        bridge=bridge, db_understanding=StubUnderstanding(_insight_obj()),
    )
    # The AI Query Assistant channel was recorded and fed into the build prompt.
    assert "query_assistant" in result.journal["channels"]
    assert result.data_understanding >= 0.9
    assert result.insight["app_summary"] == "an online store"
    assert bridge.prompts and "DATABASE UNDERSTANDING" in bridge.prompts[0]


def test_db_real_app_path_is_surfaced_and_enforced(tmp_path):
    insight = _insight_obj()
    insight.confident = True
    insight.variant = "application"
    insight.app_name = "ShopOps"
    insight.app_features = ["catalog dashboard", "order queue"]
    events: list[dict] = []
    bp = AppBlueprint(name="store", mode=BuildMode.FROM_DATABASE,
                      connections=["local"], services=["database"],
                      description="build from connected database")
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(
        bp, tmp_path / "ws",
        schema={"products": ["id", "name", "price"], "orders": ["id", "total"]},
        db_understanding=StubUnderstanding(insight),
        on_progress=lambda payload: events.append(payload),
    )
    assert result.build_path["path"] == "real_app"
    assert result.build_path["enforced_by"] == "requirement_fidelity"
    assert result.build_path["crud_contract"] == "disabled_for_raw_tables"
    assert result.as_dict()["build_path"]["path"] == "real_app"
    build_path_events = [
        p["agent_event"] for p in events
        if p.get("agent_event", {}).get("event", {}).get("type") == "build_path"
    ]
    assert build_path_events
    assert build_path_events[-1]["event"]["detail"]["path"] == "real_app"


def test_db_schema_admin_path_keeps_table_contract(tmp_path):
    insight = _insight_obj()
    insight.confident = False
    insight.variant = "application"
    bp = AppBlueprint(name="store", mode=BuildMode.FROM_DATABASE,
                      connections=["local"], services=["database"],
                      description="build from connected database")
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(
        bp, tmp_path / "ws",
        schema={"products": ["id", "name", "price"], "orders": ["id", "total"]},
        db_understanding=StubUnderstanding(insight),
    )
    assert result.build_path["path"] == "schema_admin"
    assert result.build_path["enforced_by"] == "per_table_schema_coverage"
    assert result.build_path["crud_contract"] == "enabled_for_raw_tables"


def test_db_fidelity_uses_clean_understanding_not_session_notes(tmp_path):
    polluted = (
        "Connection: MariaDB-pushdb-1. SQL mode: summary. "
        "Schema objects referenced: information_schema.TABLES. "
        "This database mos"
    )
    bp = AppBlueprint(name="store", mode=BuildMode.FROM_DATABASE,
                      connections=["local"], services=["database"],
                      description=polluted)
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(
        bp, tmp_path / "ws",
        schema={"products": ["id", "name", "price"], "orders": ["id", "total"]},
        db_understanding=StubUnderstanding(_insight_obj()),
    )
    gaps = " ".join(result.gaps + result.fidelity_gaps).lower()
    assert "information_schema" not in gaps
    assert "mariadb" not in gaps
    assert "pushdb" not in gaps


def test_orchestrator_runs_tests_and_records_journal(tmp_path):
    bp = AppBlueprint(name="notesapp", mode=BuildMode.FROM_DATABASE,
                      connections=["local"],
                      services=["ci_cd", "document", "hosting", "database"],
                      description="manage notes and tags")
    orch = AppBuildOrchestrator(max_rounds=0)
    result = orch.run(bp, tmp_path / "ws", run_tests=True)
    assert result.journal["tests_run"] is True
    assert result.journal["tests_passed"] is True
    assert result.journal["sample_data_created"] is True
    assert 0.0 <= result.process_adherence <= 1.0
    assert (Path(result.workspace) / "tests" / "test_app.py").is_file()
