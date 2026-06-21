"""AI Query Assistant parity tests — Tk (reference), Textual TUI and Web.

The Tk AI tab is the source of truth: action buttons, a SQL toolbar, SQL modes
and a "Results & AI insights" notebook with five tabs (Query results,
Explanation, Optimization, Chat, Review) plus a Chat follow-up pane. All three
UIs render the SAME set, single-sourced from ``common.ui.shared.specs`` so a
label/order change happens once.

These tests assert the shared structure AND behaviour (Explain → Explanation
pane, Optimize → Optimization pane, follow-ups land in Chat), not just that a
few tokens exist somewhere in the markup.
"""

from __future__ import annotations

import inspect

import pytest


def _client():
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from common.headless.app_factory import create_app
    return TestClient(create_app())


def _web_client():
    pytest.importorskip("starlette")
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend
    return TestClient(build_web_backend())


# --------------------------------------------------------------------------- #
# Shared spec is the single source of truth
# --------------------------------------------------------------------------- #
def test_shared_ai_payload_shape():
    from common.ui.shared import specs

    ai = specs.ai_payload()
    action_ids = [a["id"] for a in ai["actions"]]
    assert action_ids == ["generate", "execute", "stop", "explain", "optimize",
                          "review", "clear"]
    sql_ids = [a["id"] for a in ai["sqlActions"]]
    assert sql_ids == ["copy", "edit", "send_editor", "exec_rules"]
    # The six result tabs, in order (RAG context sits before Chat, as in Tk).
    assert ai["resultTabs"] == ["Query results", "Explanation", "Optimization",
                                "RAG context", "Chat", "Review"]
    chat_ids = [a["id"] for a in ai["chatActions"]]
    assert chat_ids == ["send_followup", "clear_chat", "flag_query",
                        "flag_interpretation"]
    assert ai["uninterruptedLabel"] == "Uninterrupted follow-ups"
    # Fallback backend + inline question tools (Questions from file, etc.).
    assert ai["fallbackLabel"] == "Fallback backend"
    assert [t["id"] for t in ai["questionTools"]] == [
        "questions_file", "index_rag", "train_llm"]
    # SQL modes carry both value + label.
    assert [m["value"] for m in ai["sqlModes"]] == ["strict_summary", "summary", "open"]


def test_ai_routes_registered():
    c = _client()
    paths = {r.path for r in c.app.routes}
    for p in ("/api/ai/query", "/api/ai/backends", "/api/ai/explain",
              "/api/ai/optimize", "/api/ai/backend", "/api/ai/cache", "/api/ai/pii"):
        assert p in paths, p


def test_ai_backends_listing():
    c = _client()
    r = c.get("/api/ai/backends")
    assert r.status_code == 200
    body = r.json()
    assert "all" in body and "ready" in body


def test_ai_pii_roundtrip():
    c = _client()
    assert c.put("/api/ai/pii", json={"enabled": False}).json()["ok"] is True


# --------------------------------------------------------------------------- #
# Tk desktop is the reference: it builds the five-tab results notebook
# --------------------------------------------------------------------------- #
def test_tk_ai_builds_the_shared_result_tabs():
    from common.ui.shared import specs
    from common.ui.tk.ai import ai_query_ui

    src = inspect.getsource(ai_query_ui.AIQueryUI.create_ui)
    # Every shared result-tab label is a real notebook tab in the Tk reference.
    for label in specs.AI_RESULT_TABS[:-1]:  # Review is added on demand
        assert f'text="{label}"' in src, f"Tk should have a '{label}' results tab"
    # Review tab is created lazily by run_sql_review.
    review_src = inspect.getsource(ai_query_ui.AIQueryUI.run_sql_review)
    assert 'text="Review"' in review_src
    # Tk also carries the Send Follow-up pane (Chat) — the shared chat action.
    chat_src = inspect.getsource(ai_query_ui.AIQueryUI.create_ui)
    assert "Send Follow-up" in chat_src
    # Auto-execute AI loop toggle (clarified label for the uninterrupted loop).
    assert "Auto-run AI follow-ups (until satisfied)" in chat_src


# --------------------------------------------------------------------------- #
# Web SPA: served HTML + /ui/config + app.js
# --------------------------------------------------------------------------- #
def test_web_ai_exposes_tk_controls():
    html = _web_client().get("/").text
    for token in (
        "ai-settings-open", "AI agent status", "ai-stop", "Execute query",
        "Explain query", "Clear all", "ai-copy-sql", "ai-send-editor",
        "ai-review-rules", "ai-import-review", "ai-review", "ai-exec-rules",
        "ai-refresh-conns", "ai-schema-clear", "ai-schema-show", "ai-auto-exec",
        "ai-sql-mode",
        # Newly mirrored Tk controls: fallback backend, Questions-from-file,
        # the RAG-context pane, and the two flag buttons.
        "ai-fallback", "ai-fallback-set", "ai-questions-file",
        "ai-rag-context", "ai-flag-query", "ai-flag-interp",
    ):
        assert token in html


def test_web_ai_has_result_tabs_and_chat():
    """The Web AI tab must carry the same five-pane notebook + Chat pane as Tk,
    not a single flat explanation+grid block."""
    html = _web_client().get("/").text
    # Tab bar + the five panes.
    assert 'id="ai-result-tabs"' in html
    for pane in ("ai-pane-results", "ai-pane-explanation", "ai-pane-optimization",
                 "ai-pane-rag", "ai-pane-chat", "ai-pane-review"):
        assert f'id="{pane}"' in html, pane
    # Distinct output targets per pane (not all dumping into ai-explanation).
    for out in ("ai-explanation", "ai-optimization", "ai-review-out", "ai-grid"):
        assert f'id="{out}"' in html, out
    # Chat follow-up controls.
    for cid in ("ai-chat-log", "ai-followup", "ai-followup-send", "ai-chat-clear",
                "ai-uninterrupted"):
        assert f'id="{cid}"' in html, cid
    # Conversation History header (parity with the Tk Chat pane).
    assert "Conversation History" in html


def test_web_ui_config_exposes_ai_spec():
    cfg = _web_client().get("/ui/config").json()
    ai = cfg["specs"]["ai"]
    assert ai["resultTabs"] == ["Query results", "Explanation", "Optimization",
                                "RAG context", "Chat", "Review"]
    assert [a["id"] for a in ai["chatActions"]] == [
        "send_followup", "clear_chat", "flag_query", "flag_interpretation"]


def test_web_ai_appjs_applies_labels_and_builds_tabs():
    js = _web_client().get("/ui/app.js").text
    # Labels + tabs are stamped from the shared spec, not hardcoded.
    assert "applyAiLabels" in js
    assert "SHARED_AI_ACTION_TO_DOM" in js
    assert "buildAiResultTabs" in js
    assert "showAiResultTab" in js
    assert "specs.ai" in js
    # Execute/Review/Chat switch to their own tabs (literal calls); Explain and
    # Optimize switch via a spec-order ternary, so assert both tab ids are wired.
    assert 'showAiResultTab("results")' in js
    assert 'showAiResultTab("review")' in js
    assert 'showAiResultTab("chat")' in js
    assert 'showAiResultTab("explanation")' in js
    assert '"optimization"' in js
    # Optimize writes to its own target; review to its own target (not all into
    # the shared explanation block).
    assert '"ai-optimization"' in js
    assert '"ai-review-out"' in js
    # Chat follow-up posts to the session messages endpoint.
    assert "ai-followup-send" in js and "/messages" in js


# --------------------------------------------------------------------------- #
# Textual TUI: widget tree + behaviour
# --------------------------------------------------------------------------- #
pytest.importorskip("textual")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_tui_ai_screen_composes_and_lists_backends():
    from textual.widgets import Button, Checkbox, Select

    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("ai")
        await pilot.pause()
        scr = app.screen
        bsel = scr.query_one("#ai-backend", Select)
        opts = [o for o in bsel._options] if hasattr(bsel, "_options") else []
        assert opts, "backend selector should be populated"
        for wid in (
            "#ai-settings-open", "#ai-ask", "#ai-exec", "#ai-stop", "#ai-explain",
            "#ai-optimize", "#ai-clear", "#ai-copy-sql", "#ai-edit-sql",
            "#ai-send-editor", "#ai-review-rules", "#ai-import-review", "#ai-review",
            "#ai-exec-rules", "#ai-refresh-conns", "#ai-schema-clear", "#ai-schema-show",
            # Newly mirrored Tk controls.
            "#ai-questions-file", "#ai-fallback-set", "#ai-flag-query",
            "#ai-flag-interp",
        ):
            assert scr.query_one(wid, Button) is not None
        assert scr.query_one("#ai-auto-exec", Checkbox) is not None
        assert scr.query_one("#ai-sql-mode", Select) is not None
        assert scr.query_one("#ai-fallback", Select) is not None


@pytest.mark.anyio
async def test_tui_ai_screen_mirrors_tk_result_tabs_and_chat():
    """TUI must render the same five result panes + Chat follow-up as Tk,
    with labels taken from the shared spec."""
    from textual.widgets import Button, Checkbox, Input, Static, TabbedContent, TabPane

    from common.ui.shared import specs
    from common.ui.textual.app import DbToolApp

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("ai")
        await pilot.pause()
        scr = app.screen

        assert scr.query_one("#ai-results-tabs", TabbedContent) is not None
        for pane in ("ai-tab-results", "ai-tab-explanation", "ai-tab-optimization",
                     "ai-tab-rag", "ai-tab-chat", "ai-tab-review"):
            assert scr.query_one("#" + pane, TabPane) is not None, pane
        # Distinct output widgets per pane.
        for out in ("ai-explanation", "ai-optimization", "ai-review-out",
                    "ai-rag-context", "ai-chat-log"):
            assert scr.query_one("#" + out, Static) is not None, out
        # Chat follow-up controls.
        assert scr.query_one("#ai-followup", Input) is not None
        assert scr.query_one("#ai-uninterrupted", Checkbox) is not None
        # Conversation History is a scrollable pane (parity with Tk's history
        # pane), and the chat log lives inside it.
        from textual.containers import VerticalScroll

        scroll = scr.query_one("#ai-chat-scroll", VerticalScroll)
        assert scroll is not None
        # The chat log lives inside the scrollable history pane.
        assert scroll.query_one("#ai-chat-log", Static) is not None

        # Labels come from the shared spec (single source).
        ai = specs.ai_payload()
        acts = {a["id"]: a["label"] for a in ai["actions"]}
        chat = {a["id"]: a["label"] for a in ai["chatActions"]}
        assert str(scr.query_one("#ai-ask", Button).label) == acts["generate"]
        assert str(scr.query_one("#ai-review", Button).label) == acts["review"]
        assert str(scr.query_one("#ai-followup-send", Button).label) == chat["send_followup"]
        assert str(scr.query_one("#ai-chat-clear", Button).label) == chat["clear_chat"]


@pytest.mark.anyio
async def test_tui_ai_explain_and_optimize_route_to_their_panes(monkeypatch):
    """Explain fills the Explanation pane and activates that tab; Optimize fills
    the Optimization pane and activates it — mirroring the Tk notebook routing."""
    from textual.widgets import Button, Static, TabbedContent, TextArea

    from common.ui.textual.app import DbToolApp

    class _FakeAi:
        def list_connections(self):
            return []

        def explain_sql(self, sql, **k):
            return {"explanation": "EXPLAINED-OK"}

        def optimize_sql(self, sql, **k):
            return {"optimization": "OPTIMIZED-OK"}

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("ai")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeAi()

        scr.query_one("#ai-sql", TextArea).text = "select 1"

        scr.query_one("#ai-explain", Button).press()
        await pilot.pause()
        assert "EXPLAINED-OK" in str(scr.query_one("#ai-explanation", Static).render())
        assert scr.query_one("#ai-results-tabs", TabbedContent).active == "ai-tab-explanation"

        scr.query_one("#ai-optimize", Button).press()
        await pilot.pause()
        assert "OPTIMIZED-OK" in str(scr.query_one("#ai-optimization", Static).render())
        assert scr.query_one("#ai-results-tabs", TabbedContent).active == "ai-tab-optimization"


@pytest.mark.anyio
async def test_tui_ai_use_rag_routes_generation_through_rag_ask():
    """Use RAG toggle makes Generate call rag_ask() instead of ai_query() (Tk
    parity with the inline Use RAG checkbox)."""
    from textual.widgets import Button, Checkbox, Input, Select, TextArea

    from common.ui.textual.app import DbToolApp

    calls = {"ai_query": 0, "rag_ask": 0}

    class _FakeAi:
        def list_connections(self):
            return [{"name": "c1", "db_type": "SQLite"}]

        def open_connection(self, *a, **k):
            return {"ok": True}

        def ai_query(self, conn, q, **k):
            calls["ai_query"] += 1
            return {"sql": "SELECT 1", "explanation": "plain"}

        def rag_ask(self, conn, q, **k):
            calls["rag_ask"] += 1
            return {"sql": "SELECT 2 /*rag*/", "explanation": "rag"}

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("ai")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeAi()
        sel = scr.query_one("#ai-conn", Select)
        sel.set_options([("c1", "c1")])
        sel.value = "c1"
        scr.query_one("#ai-question", Input).value = "show data"
        await pilot.pause()

        # Default: RAG off -> ai_query path.
        scr.query_one("#ai-ask", Button).press()
        await pilot.pause()
        assert calls == {"ai_query": 1, "rag_ask": 0}
        assert scr.query_one("#ai-sql", TextArea).text == "SELECT 1"

        # Enable Use RAG -> rag_ask path.
        scr.query_one("#ai-use-rag", Checkbox).value = True
        await pilot.pause()
        assert scr._use_rag is True
        scr.query_one("#ai-ask", Button).press()
        await pilot.pause()
        assert calls == {"ai_query": 1, "rag_ask": 1}
        assert "rag" in scr.query_one("#ai-sql", TextArea).text


@pytest.mark.anyio
async def test_tui_ai_index_rag_button_calls_service():
    """Index RAG beside Generate calls rag_index() for the selected connection."""
    from textual.widgets import Button, Select

    from common.ui.textual.app import DbToolApp

    seen = {}

    class _FakeAi:
        def list_connections(self):
            return [{"name": "c1", "db_type": "SQLite"}]

        def open_connection(self, *a, **k):
            return {"ok": True}

        def rag_index(self, connection, rebuild=False):
            seen["connection"] = connection
            seen["rebuild"] = rebuild
            return {"indexed": 7}

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("ai")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeAi()
        sel = scr.query_one("#ai-conn", Select)
        sel.set_options([("c1", "c1")])
        sel.value = "c1"
        await pilot.pause()

        scr.query_one("#ai-index-rag", Button).press()
        await pilot.pause()
        assert seen == {"connection": "c1", "rebuild": False}


@pytest.mark.anyio
async def test_tui_ai_followup_posts_to_chat_pane(monkeypatch):
    """Sending a follow-up appends to the Chat log and switches to the Chat tab."""
    from textual.widgets import Button, Input, Static, TabbedContent, TextArea

    from common.ui.textual.app import DbToolApp

    class _FakeAi:
        def list_connections(self):
            return []

        def ai_session_ask(self, sid, msg, **k):
            return {"explanation": "REFINED", "sql": "select 2"}

    app = DbToolApp()
    async with app.run_test() as pilot:
        app.push_screen_by_name("ai")
        await pilot.pause()
        scr = app.screen
        scr.svc = _FakeAi()
        monkeypatch.setattr(scr, "_ensure_session", lambda: "sid-123")

        scr.query_one("#ai-followup", Input).value = "add a where clause"
        scr.query_one("#ai-followup-send", Button).press()
        await pilot.pause()

        log = str(scr.query_one("#ai-chat-log", Static).render())
        assert "add a where clause" in log
        assert "REFINED" in log
        assert scr.query_one("#ai-results-tabs", TabbedContent).active == "ai-tab-chat"
        # The refined SQL flows into the Generated SQL box.
        assert scr.query_one("#ai-sql", TextArea).text == "select 2"
