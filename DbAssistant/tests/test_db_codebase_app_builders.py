"""Tests for db/codebase app builder assistants, profilers, archetypes, meters."""

from __future__ import annotations

from ai_assistant.app_builder.archetypes import classify_archetype
from ai_assistant.app_builder.codebase_app_assistant import CodebaseAppBuilderAssistant
from ai_assistant.app_builder.codebase_profile import CodebaseProfiler
from ai_assistant.app_builder.db_app_assistant import build_design_brief
from ai_assistant.app_builder.db_profile import DbProfile, DbProfiler, TableProfile
from ai_assistant.app_builder.db_understanding import (
    DataInsight,
    DbUnderstandingClient,
    TableInsight,
)
from ai_assistant.app_builder.meters.registry import AppMeterRegistry


def test_archetype_classifies_credit_card_signals():
    profile = DbProfile(
        tables=[
            TableProfile(name="credit_cards", columns=[]),
            TableProfile(name="account_transactions", columns=[]),
        ],
    )
    match = classify_archetype(profile)
    assert match.id in ("credit_card_mgmt", "ledger", "crm")
    assert match.confidence > 0


def test_db_profiler_uses_provided_schema():
    profiler = DbProfiler(connection_name="test")
    profile = profiler.profile({"orders": ["id", "customer_id", "total"]})
    assert profile.tables
    assert profile.tables[0].name == "orders"
    assert "metadata" in profile.phases_completed


def test_db_understanding_without_assistant_still_builds_brief():
    client = DbUnderstandingClient(
        connection_name="c1",
        user_description="Customer portal",
        variant="application",
    )
    insight = client.understand({"customers": ["id", "name", "email"]})
    assert insight.design_brief
    assert "Customer portal" in insight.design_brief
    assert insight.phases_completed


def test_build_design_brief_insights_admin_variant():
    insight = DataInsight(
        variant="insights_admin",
        user_description="Show DB health",
        app_summary="Operations dashboard",
        archetype="generic_crud",
        archetype_confidence=0.5,
    )
    brief = build_design_brief(insight)
    assert "insights" in brief.lower() or "admin" in brief.lower()
    assert "Show DB health" in brief


def test_archetype_classifies_notification_sms_signals():
    profile = DbProfile(
        tables=[
            TableProfile(name="notifications", columns=[]),
            TableProfile(name="message_templates", columns=[]),
            TableProfile(name="delivery_log", columns=[]),
            TableProfile(name="recipients", columns=[]),
        ],
    )
    match = classify_archetype(profile)
    assert match.id == "messaging_notifications"
    assert match.confidence > 0


def test_build_design_brief_confident_targets_real_app_with_data_rules():
    insight = DataInsight(
        variant="application",
        app_name="BrokerAlert SMS Console",
        persona="Operations agent",
        app_features=["compose campaigns", "track delivery", "manage templates"],
        app_summary="A notification console for brokerage SMS alerts.",
        data_flow="messages are queued, sent, then logged with delivery status",
        archetype="messaging_notifications",
        archetype_confidence=0.8,
        confident=True,
        tables=[TableInsight(name="delivery_log",
                             columns=["id", "status", "sent_at"])],
    )
    brief = build_design_brief(insight)
    assert "BrokerAlert SMS Console" in brief
    assert "Operations agent" in brief
    assert "compose campaigns" in brief
    # Real-app goal + data requirements present; not a schema-mirror fallback.
    assert "real user-facing application" in brief.lower()
    assert "sample data" in brief.lower()
    assert "real event data" in brief.lower()
    assert "fallback" not in brief.lower()
    # The brief must steer AWAY from per-table CRUD and frame tables as the
    # data layer behind the predicted feature workflows.
    low = brief.lower()
    assert "data layer" in low
    assert "every table" in low  # "...for every table" prohibition


def test_build_design_brief_low_confidence_falls_back_to_schema_reflection():
    insight = DataInsight(
        variant="application",
        app_summary="Unclear back-office data store.",
        archetype="generic_crud",
        archetype_confidence=0.35,
        confident=False,
        tables=[TableInsight(name="misc", columns=["id", "blob"])],
    )
    brief = build_design_brief(insight)
    assert "fallback" in brief.lower()
    assert "schema" in brief.lower()
    # Does not advertise the confident real-app data requirements.
    assert "real event data" not in brief.lower()


def test_codebase_profiler_on_tmp_path(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "app.py").write_text(
        '@app.get("/health")\ndef health():\n    return {"ok": True}\n',
        encoding="utf-8",
    )
    (tmp_path / "requirements.txt").write_text("fastapi\n", encoding="utf-8")
    profile = CodebaseProfiler(str(tmp_path)).profile()
    assert profile.files >= 1
    assert profile.path == str(tmp_path)


def test_codebase_assistant_builds_brief(tmp_path):
    (tmp_path / "main.py").write_text("def main(): pass\n", encoding="utf-8")
    assistant = CodebaseAppBuilderAssistant(
        codebase_path=str(tmp_path),
        user_description="Inventory tool",
        variant="structure_metadata",
    )
    insight = assistant.understand()
    assert insight.design_brief
    assert insight.variant == "structure_metadata"
    assert "Inventory tool" in insight.design_brief


def test_db_meters_score_generated_app():
    reg = AppMeterRegistry()
    profile = DbProfile(
        metadata_kinds={"tables": True, "views": False, "indexes": True},
        tables=[TableProfile(name="orders", columns=[])],
    )
    files = {
        "src/web.py": "dashboard accounts cards transactions payments",
        "src/models.py": "class Order: pass  # orders table",
    }
    report = reg.evaluate_db_build(
        files, profile=profile.as_dict(), schema={"orders": ["id"]},
        archetype="credit_card_mgmt",
    )
    assert "overall" in report
    assert "meters" in report


def test_codebase_meters_relaxed_battery():
    reg = AppMeterRegistry()
    profile = {"routes": ["/health"], "services": ["app:HealthService"]}
    files = {"src/api.py": "health route service"}
    report = reg.evaluate_codebase_build(files, profile=profile, components=["api_routes"])
    assert report.get("overall") is not None
