"""Tests for the requirement-coverage meter and the coverage-driven loop.

The meter answers "did we build what was asked?" — for every requested entity
and feature it checks there is an API route, a UI page and a test. The
orchestrator uses it as the auto-build loop's *done* condition.
"""

from __future__ import annotations

from ai_assistant.app_builder.engine import AppBlueprint, BuildMode
from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator
from ai_assistant.app_builder.spec import AppSpec, Entity
from ai_assistant.app_builder.webapp import generate_app
from ai_assistant.meters import MeterSuite, RequirementCoverageMeter

FEATURES = ["list", "create", "edit", "delete"]


def _spec(*entities: Entity, services=None) -> AppSpec:
    return AppSpec(app_name="shop", entities=list(entities),
                   services=list(services or []))


# ── the meter ────────────────────────────────────────────────────────────────
def test_generated_app_is_fully_covered():
    spec = _spec(
        Entity("customers", ["id", "name", "email"]),
        Entity("orders", ["id", "total", "customer_id"]),
        services=["monitoring", "ci_cd", "database", "document"],
    )
    files = generate_app(spec)
    m = RequirementCoverageMeter().measure(
        entities=[e.table for e in spec.entities], features=FEATURES,
        files=files, services=spec.services,
    )
    assert m.score == 1.0
    assert m.passed is True
    assert m.evidence["fully_covered"] is True
    assert m.issues == []
    assert m.components == {
        "api_coverage": 1.0, "ui_coverage": 1.0,
        "test_coverage": 1.0, "service_coverage": 1.0,
    }


def test_missing_api_is_detected():
    spec = _spec(Entity("books", ["id", "title"]))
    files = generate_app(spec)
    files["src/api.py"] = files["src/api.py"].replace("@router.delete", "@router.xx")
    m = RequirementCoverageMeter().measure(
        entities=["books"], features=FEATURES, files=files,
    )
    assert m.components["api_coverage"] < 1.0
    assert any("missing API for delete" in g for g in m.issues)
    assert m.evidence["per_entity"]["books"]["api"]["delete"] is False


def test_missing_test_is_detected():
    spec = _spec(Entity("alphas", ["id", "x"]), Entity("betas", ["id", "y"]))
    files = generate_app(spec)
    # Strip every reference to entity "betas" from the test suite.
    files["tests/test_app.py"] = "\n".join(
        ln for ln in files["tests/test_app.py"].splitlines() if "betas" not in ln
    )
    m = RequirementCoverageMeter().measure(
        entities=["alphas", "betas"], features=FEATURES, files=files,
    )
    assert m.evidence["per_entity"]["alphas"]["tests"] is True
    assert m.evidence["per_entity"]["betas"]["tests"] is False
    assert m.components["test_coverage"] == 0.5
    assert any("no test exercises this entity" in g for g in m.issues)


def test_missing_service_is_detected():
    spec = _spec(Entity("items", ["id", "name"]))
    files = generate_app(spec)
    m = RequirementCoverageMeter().measure(
        entities=["items"], features=FEATURES, files=files,
        services=["notification"],  # never wired in
    )
    assert m.components["service_coverage"] == 0.0
    assert any("notification" in g for g in m.issues)


def test_no_entities_is_vacuously_complete():
    m = RequirementCoverageMeter().measure(
        entities=[], features=FEATURES, files={}, services=[],
    )
    assert m.score == 1.0
    assert m.issues == []


def test_literal_web_routes_are_credited():
    """If the AI rewrites generic web routes to literal per-entity routes,
    UI coverage is still detected (no over-reliance on the generic shape)."""
    files = {
        "src/api.py": (
            'from fastapi import APIRouter\n'
            'router = APIRouter(prefix="/api")\n'
            '@router.get("/widgets")\n'
            'def a():\n    return []\n'
            '@router.post("/widgets")\n'
            'def b():\n    return {}\n'
        ),
        "src/web.py": (
            'from fastapi import APIRouter\n'
            'from fastapi.responses import HTMLResponse\n'
            'router = APIRouter()\n'
            '@router.get("/widgets")\n'
            'def lv():\n    return HTMLResponse("x")\n'
            '@router.post("/widgets/new")\n'
            'def nf():\n    return HTMLResponse("x")\n'
        ),
        "tests/test_app.py": 'def test_w():\n    assert "/widgets"\n',
    }
    m = RequirementCoverageMeter().measure(
        entities=["widgets"], features=["list", "create"], files=files,
    )
    assert m.evidence["per_entity"]["widgets"]["ui"]["list"] is True
    assert m.evidence["per_entity"]["widgets"]["ui"]["create"] is True
    assert m.components["api_coverage"] == 1.0


def test_metadata_driven_generic_crud_is_credited():
    files = {
        "src/api.py": (
            'from fastapi import APIRouter\n'
            'router = APIRouter(prefix="/api")\n'
            'ENTITY_REGISTRY = {"customers": {"table": "customers"}, '
            '"orders": {"table": "orders"}}\n'
            '@router.get("/{entity}")\n'
            'def list_entity(entity: str):\n    return []\n'
            '@router.post("/{entity}")\n'
            'def create_entity(entity: str):\n    return {}\n'
            '@router.put("/{entity}/{item_id}")\n'
            'def update_entity(entity: str, item_id: int):\n    return {}\n'
            '@router.delete("/{entity}/{item_id}")\n'
            'def delete_entity(entity: str, item_id: int):\n    return {}\n'
        ),
        "src/web.py": (
            'from fastapi import APIRouter\n'
            'from fastapi.responses import HTMLResponse\n'
            'router = APIRouter()\n'
            'ENTITY_REGISTRY = {"customers": {}, "orders": {}}\n'
            '@router.get("/{entity}")\n'
            'def list_page(entity: str):\n    return HTMLResponse("x")\n'
            '@router.get("/{entity}/new")\n'
            'def new_page(entity: str):\n    return HTMLResponse("x")\n'
            '@router.get("/{entity}/{item_id}/edit")\n'
            'def edit_page(entity: str, item_id: int):\n    return HTMLResponse("x")\n'
            '@router.post("/{entity}/{item_id}/delete")\n'
            'def delete_page(entity: str, item_id: int):\n    return HTMLResponse("x")\n'
        ),
        "tests/test_app.py": (
            'from src.api import ENTITY_REGISTRY\n\n'
            'def test_all_entities(client):\n'
            '    for entity in ENTITY_REGISTRY:\n'
            '        assert client.get(f"/api/{entity}").status_code in (200, 404)\n'
            '        assert client.post(f"/api/{entity}", json={}).status_code in (200, 201, 400)\n'
        ),
    }
    m = RequirementCoverageMeter().measure(
        entities=["customers", "orders"], features=FEATURES, files=files,
    )
    assert m.score == 1.0
    assert m.issues == []
    assert m.evidence["per_entity"]["customers"]["api"]["delete"] is True
    assert m.evidence["per_entity"]["orders"]["tests"] is True


def test_suite_evaluate_requirements_wrapper():
    spec = _spec(Entity("tasks", ["id", "title"]))
    files = generate_app(spec)
    v = MeterSuite().evaluate_requirements(
        entities=["tasks"], features=FEATURES, files=files,
    )
    assert v["accepted"] is True
    assert v["fully_covered"] is True
    assert v["gaps"] == []
    assert v["measurement"]["meter"] == "requirement_coverage_meter"


# ── the loop driven by coverage ───────────────────────────────────────────────
def _bp():
    return AppBlueprint(name="auto", mode=BuildMode.FROM_SCRATCH,
                        services=["ci_cd", "document", "hosting", "database"],
                        description="manage notes")


class _Bridge:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def available(self):
        return True

    def generate(self, prompt):
        self.calls += 1
        return self._responses.pop(0) if self._responses else ""


_NEW_FILE = (
    "=== FILE: src/extra.py ===\n"
    '"""Extra module that closes the gap."""\n'
    "from __future__ import annotations\n\n\n"
    "def helper() -> int:\n"
    '    """Return one."""\n'
    "    return 1\n"
    "=== END FILE ===\n"
)


def test_orchestrator_reports_full_coverage_for_baseline(tmp_path):
    result = AppBuildOrchestrator().run(_bp(), tmp_path / "ws")
    assert result.requirement_coverage == 1.0
    assert result.coverage_ok is True
    assert result.gaps == []
    assert result.rounds[0].coverage == 1.0
    assert "coverage" in result.rounds[0].as_dict()


def test_orchestrator_uses_fidelity_coverage_only_when_ai_can_act(tmp_path):
    # With an available AI/code-agent loop, the same runnable baseline is not
    # treated as complete for a from_scratch requirement until the app reflects
    # the user's intent. This preserves real-app optimization without breaking
    # deterministic-only callers that expect structural baseline coverage.
    result = AppBuildOrchestrator(max_rounds=0).run(
        _bp(), tmp_path / "ws", bridge=_Bridge([]))
    assert result.requirement_coverage < 1.0
    assert result.rounds[0].coverage < 1.0


def test_orchestrator_loop_closes_coverage_gap(tmp_path):
    orch = AppBuildOrchestrator(max_rounds=4)
    # Simulate a baseline gap that is only closed once the AI adds src/extra.py.
    def fake_cov(files):
        done = "src/extra.py" in files
        return {
            "score": 1.0 if done else 0.5,
            "accepted": done,
            "gaps": [] if done else ["notes: no test exercises this entity"],
            "fully_covered": done,
            "measurement": {},
        }
    orch._coverage = fake_cov
    bridge = _Bridge([_NEW_FILE, ""])
    result = orch.run(_bp(), tmp_path / "ws", bridge=bridge)
    assert "src/extra.py" in result.files
    assert result.requirement_coverage == 1.0
    assert result.coverage_ok is True
    assert result.ok is True


def test_codebase_mode_is_not_blocked_by_coverage(tmp_path):
    """From-codebase is an analysis artifact (no CRUD entities): requirement
    coverage must be vacuously complete so it does not falsely fail the build."""
    cb = tmp_path / "cb"
    cb.mkdir()
    (cb / "m.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    bp = AppBlueprint(name="cbapp", mode=BuildMode.FROM_CODEBASE,
                      codebase_path=str(cb))
    result = AppBuildOrchestrator().run(bp, tmp_path / "out")
    assert result.ok is True
    assert result.requirement_coverage == 1.0
    assert result.gaps == []


def test_orchestrator_keeps_pushing_until_max_rounds_when_gap_persists(tmp_path):
    orch = AppBuildOrchestrator(max_rounds=3)
    # Coverage never completes; AI keeps returning nothing useful.
    orch._coverage = lambda files: {
        "score": 0.4, "accepted": False,
        "gaps": ["notes: missing API for delete"],
        "fully_covered": False, "measurement": {},
    }
    bridge = _Bridge(["", "", ""])
    result = orch.run(_bp(), tmp_path / "ws", bridge=bridge)
    assert result.coverage_ok is False
    assert result.ok is False
    assert result.gaps
