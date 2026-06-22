"""AI Query Assistant screen.

Functional parity with the desktop AI Query tab: connection + backend
selection, natural-language → SQL generation, Explain / Optimize, Execute with
a results grid, query history, and AI settings (set backend, PII, cache).
Requires the AI module.
"""

from __future__ import annotations

from typing import Any

from textual.containers import Horizontal, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import (
    Button,
    Checkbox,
    Collapsible,
    DataTable,
    Input,
    Label,
    ListItem,
    ListView,
    Select,
    Static,
    TabbedContent,
    TabPane,
    TextArea,
)

from common.ui.shared import specs
from common.ui.textual.screens.base import BaseScreen
from common.ui.textual.screens.form_modal import FormModal


class AiqaTrainModal(ModalScreen):
    """Session training config for the AI Query Assistant.

    Pick the target model + engine once; the choice persists for the AI Query
    session (chat & follow-ups) and is reused by both "Train on current Q→SQL"
    and "Train from chat". RAG use follows the screen's Use RAG state. Returns
    ``(model, engine)`` so the parent screen can persist the session config.
    """

    DEFAULT_CSS = """
    AiqaTrainModal { align: center middle; }
    AiqaTrainModal > #box {
        width: 70; max-height: 90%; padding: 1 2;
        border: thick $accent; background: $surface;
    }
    AiqaTrainModal #title { text-style: bold; }
    AiqaTrainModal #hint { color: $text-muted; margin-bottom: 1; }
    AiqaTrainModal #actions { height: auto; margin-top: 1; }
    AiqaTrainModal #aiqa-out { margin-top: 1; }
    """
    BINDINGS = [("escape", "close", "Close")]

    def __init__(self, *, svc, conn, question, sql, use_rag, model, engine,
                 explanation=""):
        super().__init__()
        self._svc = svc
        self._conn = conn
        self._question = question
        self._sql = sql
        self._explanation = (explanation or "").strip()
        self._use_rag = bool(use_rag)
        self._model = model or "default"
        self._engine = engine  # None => config default

    def _pair_description(self, default: str) -> str:
        """Use the AI's explanation as the pair description when present so
        chat/follow-up turns train from the explanation too (it travels into
        RAG); fall back to a stable label otherwise."""
        exp = self._explanation
        if exp and not exp.lower().startswith("(no explanation"):
            return exp
        return default

    def compose(self):
        try:
            engines = self._svc.llm_engines().get("engines") or []
        except Exception:  # noqa: BLE001
            engines = []
        eng_opts = [("(config default)", "")] + [
            (f"{e['name']}{'' if e.get('available') else ' (unavailable)'}", e["name"])
            for e in engines
        ]
        with VerticalScroll(id="box"):
            yield Static("Train LLM — AI Query Assistant", id="title")
            yield Static(
                "Pick the target model + engine. They persist for this AI Query "
                "session — reused for current Q→SQL and chat/follow-up training. "
                f"Use RAG: {'on' if self._use_rag else 'off'} (toolbar toggle).",
                id="hint")
            yield Label("Target model (pick existing or type a new name)")
            yield Input(value=self._model, id="aiqa-model")
            yield Label("Engine")
            yield Select(eng_opts, id="aiqa-engine",
                         value=(self._engine or ""), allow_blank=False)
            with Horizontal(id="actions"):
                yield Button("Train on current Q→SQL", id="aiqa-train-current",
                             variant="primary")
                yield Button("Train from chat", id="aiqa-train-chat")
                yield Button("Verify in model", id="aiqa-verify")
                yield Button("Close", id="aiqa-close")
            yield Static("", id="aiqa-out")

    def _model_arg(self) -> str:
        return self.query_one("#aiqa-model", Input).value.strip() or "default"

    def _engine_arg(self):
        v = self.query_one("#aiqa-engine", Select).value
        return None if v in ("", Select.BLANK) else str(v)

    def action_close(self) -> None:
        self.dismiss((self._model_arg(), self._engine_arg()))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "aiqa-close":
            self.dismiss((self._model_arg(), self._engine_arg()))
            return
        model = self._model_arg()
        engine = self._engine_arg()
        self._model, self._engine = model, engine
        out = self.query_one("#aiqa-out", Static)
        if bid == "aiqa-verify":
            if not hasattr(self._svc, "llm_model_dataset"):
                out.update("LLM dataset inspection not available.")
                return
            r = self._svc.llm_model_dataset(name=model, query=self._question or "")
            if not r.get("ok"):
                out.update(r.get("error") or "Dataset lookup failed.")
            elif not r.get("available"):
                out.update(r.get("reason") or "No saved training data for this model.")
            elif self._question:
                out.update(
                    f"'{self._question}' IS in '{model}': {r.get('shown')} match(es) "
                    f"of {r.get('total')} pairs."
                    if r.get("matched") else
                    f"'{self._question}' is NOT in '{model}' "
                    f"({r.get('total')} pairs trained).")
            else:
                out.update(f"Model '{model}' trained on {r.get('total')} pair(s).")
            return
        if bid == "aiqa-train-current":
            if not self._question or not self._sql:
                out.update("Generate SQL first, then train on it.")
                return
            if not hasattr(self._svc, "llm_train_pairs"):
                out.update("LLM training service not available.")
                return
            r = self._svc.llm_train_pairs({
                "names": [model], "engine": engine or "",
                "connection": self._conn, "use_rag": self._use_rag,
                "pairs": [{"question": self._question, "sql": self._sql,
                           "description": self._pair_description(
                               "AI Query current turn")}],
            })
            out.update(
                f"Trained '{model}' from current Q→SQL ({r.get('pairs', 0)} pair(s))."
                if r.get("ok") else (r.get("error") or "LLM training failed."))
            return
        if bid == "aiqa-train-chat":
            if not self._conn:
                out.update("Select a connection first.")
                return
            if not hasattr(self._svc, "llm_train_rich"):
                out.update("LLM training service not available.")
                return
            body = {
                "mode": "from_database", "connections": [self._conn],
                "train_llm": [model], "train_engine": engine or "",
                "mine_db": False, "use_rag": self._use_rag, "include_sample": False,
            }
            if self._question and self._sql:
                body["extra_pairs"] = [{
                    "question": self._question,
                    "sql": self._sql,
                    "description": self._pair_description(
                        "AI Query current Generated SQL"),
                }]
            r = self._svc.llm_train_rich(body)
            out.update(
                f"Trained '{model}' from chat/follow-ups ({r.get('pairs', 0)} pair(s))."
                if r.get("ok") else (r.get("error") or "LLM training failed."))
            return


class AiQueryScreen(BaseScreen):
    """Natural-language to SQL (requires ai module)."""

    NAV_ID = "ai"

    DEFAULT_CSS = """
    AiQueryScreen #ai-chat-scroll {
        height: 12;
        border: round $panel;
        margin-bottom: 1;
    }
    AiQueryScreen .section { text-style: bold; margin-top: 1; }
    """

    # Result tab labels map to fixed TabPane ids in shared-spec order.
    _RESULT_TAB_IDS = [
        "ai-tab-results", "ai-tab-explanation", "ai-tab-optimization",
        "ai-tab-rag", "ai-tab-chat", "ai-tab-review",
    ]

    def __init__(self, svc: Any, **kwargs) -> None:
        super().__init__(svc, **kwargs)
        self._history: list[str] = []
        self._auto_execute = False
        self._sql_mode = "summary"
        self._sessions: list[dict] = []
        self._chat_lines: list[str] = []
        self._uninterrupted = False
        self._use_rag = False
        self._question_queue: list[str] = []

    def screen_title(self) -> str:
        return "AI Query Assistant"

    def compose_body(self):
        spec = specs.ai_payload()
        acts = {a["id"]: a["label"] for a in spec["actions"]}
        sqlacts = {a["id"]: a["label"] for a in spec["sqlActions"]}
        chat = {a["id"]: a["label"] for a in spec["chatActions"]}
        result_tabs = spec["resultTabs"]
        names = [c["name"] for c in self.svc.list_connections()]
        with Horizontal(classes="actions-row"):
            yield Label("AI agent status")
            yield Button("⚙ AI Settings", id="ai-settings-open")
        with Horizontal(classes="actions-row"):
            yield Label("Connection ")
            yield Select([(n, n) for n in names] or [("(none)", "")],
                         id="ai-conn", allow_blank=True)
            yield Label(" Backend ")
            yield Select([], id="ai-backend", allow_blank=True)
            yield Button("Refresh backends", id="ai-backends", classes="mini")
        with Horizontal(classes="actions-row"):
            yield Label(f"{spec.get('fallbackLabel', 'Fallback backend')} ")
            yield Select([], id="ai-fallback", allow_blank=True)
            yield Button("Set fallback", id="ai-fallback-set", classes="mini")
            yield Label(f" {spec.get('fallbackHint', '')}", classes="hint")
        yield Static("", id="ai-backend-state", classes="status")
        with Horizontal(classes="actions-row"):
            yield Label("Session ")
            yield Select([("(none)", "")], id="ai-session", allow_blank=True)
            yield Button("New session", id="ai-session-new", classes="mini")
            yield Button("Refresh sessions", id="ai-session-refresh", classes="mini")
            yield Button("Delete session", id="ai-session-delete", variant="error", classes="mini")
            yield Button("Ask follow-up", id="ai-session-followup", classes="mini")
            yield Button("Cross-tab action", id="ai-session-cross", classes="mini")

        yield Label("Question")
        yield Input(id="ai-question", placeholder="Show top 10 customers by revenue")
        qtools = {a["id"]: a["label"] for a in spec.get("questionTools", [])}
        with Horizontal(classes="actions-row"):
            yield Button(acts["generate"], id="ai-ask", variant="primary")
            yield Button(acts["execute"], id="ai-exec", variant="success")
            yield Button(qtools.get("questions_file", "Questions from file"),
                         id="ai-questions-file")
            yield Button(acts["stop"], id="ai-stop", variant="error")
            yield Checkbox(spec.get("useRagLabel", "Use RAG"), id="ai-use-rag")
            yield Button(qtools.get("index_rag", "Index RAG"), id="ai-index-rag")
            yield Button(qtools.get("train_llm", "Train LLM"), id="ai-train-current")
            yield Button(acts["explain"], id="ai-explain")
            yield Button(acts["optimize"], id="ai-optimize")
            yield Button(acts["clear"], id="ai-clear")
        yield Label("Generated SQL")
        yield TextArea("", id="ai-sql", language="sql")
        with Horizontal(classes="actions-row"):
            yield Button(sqlacts["copy"], id="ai-copy-sql")
            yield Button(sqlacts["edit"], id="ai-edit-sql")
            yield Button(sqlacts["send_editor"], id="ai-send-editor")
            yield Button("Write Review Rules", id="ai-review-rules")
            yield Button("Import SQL for Review", id="ai-import-review")
            yield Button(acts["review"], id="ai-review")
            yield Button(sqlacts["exec_rules"], id="ai-exec-rules")
        build_apps = {a["id"]: a["label"] for a in spec.get("buildAppsActions", [])}
        with Horizontal(classes="actions-row"):
            yield Button("Refresh Connections", id="ai-refresh-conns", classes="mini")
            if build_apps.get("app_builder"):
                yield Button(build_apps["app_builder"], id="ai-build-app", classes="mini")
            if spec.get("advancedModules"):
                yield Button("RAG Manager", id="ai-rag-manage", classes="mini")
                yield Button("Build or Train LLM", id="ai-train-llm", classes="mini")
            yield Button("Clear Schema Cache", id="ai-schema-clear", classes="mini")
            yield Button("Show Schema Sent to AI", id="ai-schema-show", classes="mini")
            yield Checkbox("Auto-execute SQL queries", id="ai-auto-exec")
            yield Label(" SQL mode ")
            yield Select(
                [
                    ("Strict summary mode", "strict_summary"),
                    ("Summary mode", "summary"),
                    ("Open mode", "open"),
                ],
                id="ai-sql-mode",
                value="summary",
                allow_blank=False,
            )
        # Results & AI insights notebook — the same five tabs as the Tk UI,
        # labelled from the shared spec. Explain/Optimize/Execute/Review each
        # surface in their matching pane; follow-ups land in Chat.
        with TabbedContent(id="ai-results-tabs"):
            with TabPane(result_tabs[0], id="ai-tab-results"):
                yield DataTable(id="ai-grid", zebra_stripes=True)
            with TabPane(result_tabs[1], id="ai-tab-explanation"):
                yield Static("", id="ai-explanation", classes="status")
            with TabPane(result_tabs[2], id="ai-tab-optimization"):
                yield Static("", id="ai-optimization", classes="status")
            with TabPane(result_tabs[3], id="ai-tab-rag"):
                yield Static(
                    "Retrieved context ranked by relevance "
                    "(only when 'Use RAG' is on):", classes="hint")
                yield Static("", id="ai-rag-context", classes="status")
            with TabPane(result_tabs[4], id="ai-tab-chat"):
                # Mirror the Tk Chat tab: a scrollable Conversation History pane
                # on top, then an always-visible Send Follow-up section below.
                yield Static("[b]Conversation History[/]", classes="section")
                with VerticalScroll(id="ai-chat-scroll"):
                    yield Static("", id="ai-chat-log", classes="status")
                yield Static("[b]Send Follow-up Message[/]", classes="section")
                yield Checkbox(spec["uninterruptedLabel"], id="ai-uninterrupted")
                yield Static(
                    "Examples: 'Add a WHERE clause for active users' · "
                    "'Sort by date descending' · 'The query failed with error X'",
                    classes="hint")
                yield Input(id="ai-followup",
                            placeholder="Add a WHERE clause for active users")
                with Horizontal(classes="actions-row"):
                    yield Button(chat["send_followup"], id="ai-followup-send",
                                 variant="primary")
                    yield Button(chat["clear_chat"], id="ai-chat-clear")
                    yield Button(chat["flag_query"], id="ai-flag-query",
                                 variant="warning")
                    yield Button(chat["flag_interpretation"], id="ai-flag-interp",
                                 variant="warning")
            with TabPane(result_tabs[5], id="ai-tab-review"):
                yield Static("", id="ai-review-out", classes="status")
        yield Static("", id="ai-status", classes="status")

        with Collapsible(title="AI settings", collapsed=True):
            with Horizontal(classes="actions-row"):
                yield Label("Default backend ")
                yield Select([], id="ai-set-backend", allow_blank=True)
                yield Checkbox("Verify", value=True, id="ai-verify")
                yield Button("Set backend", id="ai-set-backend-btn", variant="primary")
            with Horizontal(classes="actions-row"):
                yield Checkbox("Mask PII", id="ai-pii")
                yield Button("Save PII", id="ai-pii-btn")
                yield Button("Cache info", id="ai-cache-info", classes="mini")
                yield Button("Clear cache", id="ai-cache-clear", variant="error", classes="mini")
            yield TextArea("", id="ai-settings-out", read_only=True)

        with Collapsible(title="History", collapsed=True):
            yield ListView(id="ai-history")

        with Collapsible(title="SQL & review rules", collapsed=True):
            yield Label("SQL execution rules")
            yield TextArea("", id="ai-exec-rules-text")
            yield Label("Review rules")
            yield TextArea("", id="ai-review-rules-text")
            with Horizontal(classes="actions-row"):
                yield Button("Save sessions", id="ai-session-save", classes="mini")
                yield Button("Load sessions", id="ai-session-load", classes="mini")
                yield Button("Execute SQL (session rules)", id="ai-session-exec-sql", classes="mini")

    def on_mount(self) -> None:
        self._load_backends()
        self._load_sessions()

    # ------------------------------------------------------------------ #
    def _conn(self) -> str:
        return str(self.query_one("#ai-conn", Select).value or "")

    def _db_type(self) -> str:
        for c in self.svc.list_connections():
            if c.get("name") == self._conn():
                return c.get("db_type", c.get("type", ""))
        return ""

    @staticmethod
    def _session_id(sess: dict) -> str:
        return str(sess.get("session_id") or sess.get("id") or sess.get("name") or "")

    def _session_ref(self) -> str:
        return str(self.query_one("#ai-session", Select).value or "")

    def _load_sessions(self) -> None:
        sel = self.query_one("#ai-session", Select)
        if not hasattr(self.svc, "ai_session_list"):
            sel.set_options([("(none)", "")])
            return
        r = self.svc.ai_session_list()
        self._sessions = r.get("sessions") or []
        opts = []
        for s in self._sessions:
            sid = self._session_id(s)
            conn = s.get("connection_name") or s.get("connection") or ""
            opts.append((f"{sid[:8] or '(session)'}{(' · ' + conn) if conn else ''}", sid))
        sel.set_options(opts or [("(none)", "")])

    def _ensure_session(self) -> str:
        sid = self._session_ref()
        if sid:
            return sid
        if not hasattr(self.svc, "ai_session_create"):
            return ""
        r = self.svc.ai_session_create(
            self._conn(), str(self.query_one("#ai-backend", Select).value or "") or None,
            share_context=True, sql_mode=self._sql_mode,
        )
        sid = self._session_id(r.get("session") or {})
        self._load_sessions()
        if sid:
            try:
                self.query_one("#ai-session", Select).value = sid
            except Exception:  # noqa: BLE001
                pass
        return sid

    def _status(self, msg: str) -> None:
        self.query_one("#ai-status", Static).update(msg)

    def _import_sql_for_review(self) -> None:
        """Load SQL from a file into the Generated SQL box for review (Tk parity
        with ``import_sql_for_review``)."""
        def _done(v: dict | None) -> None:
            if not v:
                return
            path = (v.get("path") or "").strip()
            if not path:
                self._status("No file path given.")
                return
            try:
                import os
                with open(os.path.expanduser(path), "r", encoding="utf-8") as fh:
                    text = fh.read()
            except OSError as exc:
                self._status(f"Import failed: {exc}")
                return
            self.query_one("#ai-sql", TextArea).text = text
            self._status(f"Imported SQL from {path}. Run Review to check it.")

        self.app.push_screen(
            FormModal("Import SQL for review",
                      [{"name": "path", "label": "SQL file path",
                        "placeholder": "/path/to/query.sql"}],
                      submit_label="Import"),
            _done,
        )

    def _switch_result_tab(self, pane_id: str) -> None:
        try:
            self.query_one("#ai-results-tabs", TabbedContent).active = pane_id
        except Exception:  # noqa: BLE001
            pass

    def _render_rag_context(self, result: dict) -> None:
        """Show retrieved RAG context (parity with Tk's RAG context tab)."""
        items = (result or {}).get("rag_context") or (result or {}).get("context") \
            or (result or {}).get("retrieved") or []
        lines: list[str] = []
        if isinstance(items, list):
            for i, it in enumerate(items, 1):
                if isinstance(it, dict):
                    score = it.get("score") or it.get("relevance") or ""
                    text = it.get("text") or it.get("content") or it.get("document") or str(it)
                    lines.append(f"[{i}] {('(' + str(round(score, 3)) + ') ') if score else ''}{text}")
                else:
                    lines.append(f"[{i}] {it}")
        elif items:
            lines.append(str(items))
        try:
            self.query_one("#ai-rag-context", Static).update(
                "\n\n".join(lines) or "(no RAG context returned)")
        except Exception:  # noqa: BLE001
            pass

    def _set_fallback_backend(self) -> None:
        """Configure the fallback backend (failover + SQL repair)."""
        if not hasattr(self.svc, "configure_ai_fallback_backend"):
            self._status("Fallback backend is not available on this service.")
            return
        name = str(self.query_one("#ai-fallback", Select).value or "")
        r = self.svc.configure_ai_fallback_backend(name, verify=True)
        self._settings_out(r)
        self._status(r.get("message") or "Fallback backend updated.")

    def _flag_query(self, mode: str) -> None:
        """Flag the current query as wrong (syntax/logic) or wrongly interpreted.

        Routes through ``svc.correct_sql`` (fallback/primary backend) just like
        the Tk flag buttons, dropping the repaired SQL into the Generated SQL box
        and noting the exchange in the Chat pane.
        """
        if not hasattr(self.svc, "correct_sql"):
            self._status("Query correction is not available on this service.")
            return
        conn = self._conn()
        q = self.query_one("#ai-question", Input).value.strip()
        sql = self.query_one("#ai-sql", TextArea).text.strip()
        if not sql:
            self._status("Generate a query first, then flag it.")
            return
        self._switch_result_tab("ai-tab-chat")
        if mode == "syntax":
            self._add_chat("system", "Flagged as an incorrect query — asking the "
                                     "fallback backend to repair it.")
            error_text = ("User flagged this query as incorrect (syntax or logic — "
                          "e.g. wrong joins, subqueries, date handling).")
        else:
            self._add_chat("system", "Flagged as a wrong interpretation — asking "
                                     "the fallback backend to re-answer the question.")
            error_text = ""
        fb = str(self.query_one("#ai-fallback", Select).value or "")
        r = self.svc.correct_sql(
            q, sql, connection=conn, db_type=self._db_type(),
            error_text=error_text, mode=mode, backend=fb,
        )
        fixed = r.get("sql") or ""
        if fixed:
            self.query_one("#ai-sql", TextArea).text = fixed
            self._add_chat("assistant",
                           f"Corrected by {r.get('backend_used') or 'fallback'}:\n{fixed}")
            self._status("Corrected query placed in the Generated SQL box — "
                         "review and Execute.")
        else:
            self._add_chat("assistant", r.get("error") or "No correction produced.")
            self._status(r.get("error") or "Correction failed.")

    def _questions_from_file(self) -> None:
        """Load NL questions from a file and iterate them (parity with Tk).

        Each line becomes a question: Generate SQL, and when Auto-execute is on,
        run it. The remaining questions are kept and advanced by re-pressing
        Questions from file (or automatically under uninterrupted mode).
        """
        pending = getattr(self, "_question_queue", None)
        if pending:
            self._advance_question_queue()
            return

        def _done(v: dict | None) -> None:
            if not v:
                return
            path = (v.get("path") or "").strip()
            if not path:
                self._status("No file path given.")
                return
            try:
                import os
                with open(os.path.expanduser(path), "r", encoding="utf-8") as fh:
                    raw = fh.read()
            except OSError as exc:
                self._status(f"Import failed: {exc}")
                return
            questions = [ln.strip() for ln in raw.splitlines() if ln.strip()]
            if not questions:
                self._status("No questions found in the file.")
                return
            self._question_queue = questions
            self._status(f"Loaded {len(questions)} question(s) from {path}.")
            self._advance_question_queue()

        self.app.push_screen(
            FormModal("Questions from file",
                      [{"name": "path", "label": "Questions file path",
                        "placeholder": "/path/to/questions.txt"}],
                      submit_label="Load"),
            _done,
        )

    def _advance_question_queue(self) -> None:
        queue = getattr(self, "_question_queue", None) or []
        if not queue:
            self._status("No more questions in the queue.")
            return
        q = queue.pop(0)
        self._question_queue = queue
        self.query_one("#ai-question", Input).value = q
        remaining = len(queue)
        self._status(f"Question: {q}  ({remaining} remaining)")
        # Generate (and auto-execute when enabled) for this question.
        self.query_one("#ai-ask", Button).press()
        if (self._uninterrupted or self._auto_execute) and remaining:
            self.set_timer(0.1, self._advance_question_queue)

    def _add_chat(self, role: str, text: str) -> None:
        who = {"user": "You", "assistant": "AI", "system": "·"}.get(role, role)
        self._chat_lines.append(f"{who}: {text}")
        try:
            self.query_one("#ai-chat-log", Static).update("\n\n".join(self._chat_lines))
        except Exception:  # noqa: BLE001
            pass

    def _ensure_open(self) -> None:
        c = self._conn()
        if c and hasattr(self.svc, "open_connection"):
            try:
                self.svc.open_connection(c)
            except Exception:
                pass

    def _load_backends(self) -> None:
        if not hasattr(self.svc, "list_ai_backends"):
            self.query_one("#ai-backend-state", Static).update("AI module not available.")
            return
        info = self.svc.list_ai_backends()
        if not info.get("available"):
            self.query_one("#ai-backend-state", Static).update(
                info.get("error") or "AI not available.")
            return
        all_b = info.get("all") or []
        ready = info.get("ready") or []
        options = info.get("options") or []
        if options:
            # Local LLM is expanded into one entry per trained model
            # ("<model> (local <engine>)"); value carries the model selection.
            opts = [
                (o["label"] + (" ✓" if o.get("ready") else ""), o["value"])
                for o in options
            ]
        else:
            opts = [(b + (" ✓" if b in ready else ""), b) for b in all_b]
        self.query_one("#ai-backend", Select).set_options(opts)
        self.query_one("#ai-set-backend", Select).set_options(opts)
        # Fallback backend selector (parity with the Tk status-row combo).
        try:
            self.query_one("#ai-fallback", Select).set_options(
                [("(none)", "")] + opts)
        except Exception:  # noqa: BLE001
            pass
        state = f"ready: {', '.join(ready)}" if ready else "no backend verified"
        self.query_one("#ai-backend-state", Static).update(state)

    def _settings_out(self, obj: Any) -> None:
        import json
        try:
            self.query_one("#ai-settings-out", TextArea).text = json.dumps(obj, indent=2, default=str)
        except Exception:
            self.query_one("#ai-settings-out", TextArea).text = str(obj)

    def _add_history(self, q: str) -> None:
        q = q.strip()
        if not q:
            return
        self._history = [q] + [h for h in self._history if h != q]
        self._history = self._history[:50]
        lv = self.query_one("#ai-history", ListView)
        lv.clear()
        for h in self._history:
            lv.append(ListItem(Static(h)))

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if (event.list_view.id or "") != "ai-history":
            return
        idx = event.list_view.index
        if idx is not None and 0 <= idx < len(self._history):
            self.query_one("#ai-question", Input).value = self._history[idx]

    def _execute_sql(self) -> None:
        conn = self._conn()
        sql = self.query_one("#ai-sql", TextArea).text.strip()
        if not conn or not sql:
            self._status("Connection and SQL required.")
            return
        from common.sql_guard import assert_read_only

        guard_err = assert_read_only(sql)
        if guard_err:
            self._status(guard_err)
            return
        self._ensure_open()
        r = self.svc.execute(conn, sql)
        grid = self.query_one("#ai-grid", DataTable)
        grid.clear(columns=True)
        if r.get("error"):
            self._status(r["error"])
            return
        cols = r.get("columns") or []
        if cols:
            grid.add_columns(*[str(c) for c in cols])
        for row in r.get("rows") or []:
            grid.add_row(*[str(v) if v is not None else "" for v in row])
        self._switch_result_tab("ai-tab-results")
        self._status(f"OK — {r.get('rowcount', len(r.get('rows') or []))} rows")

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        cid = event.checkbox.id or ""
        if cid == "ai-auto-exec":
            self._auto_execute = event.value
            self._status("Auto-execute SQL enabled." if event.value else "Auto-execute SQL disabled.")
        elif cid == "ai-uninterrupted":
            self._uninterrupted = event.value
            self._status("Uninterrupted follow-ups enabled." if event.value else "Uninterrupted follow-ups disabled.")
        elif cid == "ai-use-rag":
            self._use_rag = event.value
            self._status(
                "Use RAG enabled — Generate will use retrieval-augmented context."
                if event.value else "Use RAG disabled."
            )

    def on_select_changed(self, event: Select.Changed) -> None:
        if (event.select.id or "") == "ai-sql-mode":
            self._sql_mode = str(event.value or "summary")
            self._status(f"SQL mode: {self._sql_mode.replace('_', ' ')}")

    # ------------------------------------------------------------------ #
    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "ai-backends":
            self._load_backends()
            return
        if bid == "ai-fallback-set":
            self._set_fallback_backend()
            return
        if bid == "ai-questions-file":
            self._questions_from_file()
            return
        if bid in ("ai-flag-query", "ai-flag-interp"):
            self._flag_query(
                "syntax" if bid == "ai-flag-query" else "interpretation")
            return
        if bid == "ai-settings-open":
            if hasattr(self.svc, "get_ai_config"):
                self._settings_out(self.svc.get_ai_config())
            else:
                self._status("AI settings section is below.")
            return
        if bid == "ai-refresh-conns":
            sel = self.query_one("#ai-conn", Select)
            names = [c["name"] for c in self.svc.list_connections()]
            sel.set_options([(n, n) for n in names])
            self._status("Connections refreshed.")
            return
        if bid == "ai-session-refresh":
            self._load_sessions()
            self._status("Sessions refreshed.")
            return
        if bid == "ai-session-new":
            if not hasattr(self.svc, "ai_session_create"):
                self._status("AI sessions are not available.")
                return
            r = self.svc.ai_session_create(
                self._conn(), str(self.query_one("#ai-backend", Select).value or "") or None,
                share_context=True, sql_mode=self._sql_mode,
            )
            if r.get("error"):
                self._status(r["error"])
                return
            sid = self._session_id(r.get("session") or {})
            self._load_sessions()
            if sid:
                self.query_one("#ai-session", Select).value = sid
            self._status("New AI session created.")
            return
        if bid == "ai-session-delete":
            sid = self._session_ref()
            if not sid:
                self._status("Select a session first.")
                return
            if hasattr(self.svc, "ai_session_delete"):
                r = self.svc.ai_session_delete(sid)
                self._status(r.get("error") or "Session deleted.")
                self._load_sessions()
            return
        if bid in {"ai-session-followup", "ai-session-cross"}:
            q = self.query_one("#ai-question", Input).value.strip()
            if not q:
                self._status("Question/instruction required.")
                return
            sid = self._ensure_session()
            if not sid:
                self._status("AI sessions are not available.")
                return
            if bid == "ai-session-cross":
                r = self.svc.ai_session_cross_tab(sid, q)
                self.query_one("#ai-explanation", Static).update(str(r))
                self._status(r.get("error") or "Cross-tab action routed.")
            else:
                r = self.svc.ai_session_ask(sid, q, mode="followup")
                self.query_one("#ai-sql", TextArea).text = r.get("sql") or r.get("summary_sql") or ""
                self.query_one("#ai-explanation", Static).update(r.get("explanation") or str(r))
                self._status(r.get("error") or "Follow-up response received.")
            return
        if bid == "ai-followup-send":
            msg = self.query_one("#ai-followup", Input).value.strip()
            if not msg:
                self._status("Enter a follow-up message.")
                return
            sid = self._ensure_session()
            if not sid:
                self._status("AI sessions are not available.")
                return
            self._switch_result_tab("ai-tab-chat")
            self._add_chat("user", msg)
            self.query_one("#ai-followup", Input).value = ""
            r = self.svc.ai_session_ask(sid, msg, mode="followup")
            sql = r.get("sql") or r.get("summary_sql") or ""
            if sql:
                self.query_one("#ai-sql", TextArea).text = sql
            self._add_chat("assistant", r.get("explanation") or sql or str(r))
            self._status(r.get("error") or "Follow-up processed.")
            return
        if bid == "ai-chat-clear":
            self._chat_lines = []
            self.query_one("#ai-chat-log", Static).update("")
            self.query_one("#ai-followup", Input).value = ""
            self._status("Chat cleared.")
            return
        if bid == "ai-clear":
            self.query_one("#ai-question", Input).value = ""
            self.query_one("#ai-sql", TextArea).text = ""
            self.query_one("#ai-explanation", Static).update("")
            self.query_one("#ai-optimization", Static).update("")
            self.query_one("#ai-review-out", Static).update("")
            try:
                self.query_one("#ai-rag-context", Static).update("")
            except Exception:  # noqa: BLE001
                pass
            self._chat_lines = []
            self.query_one("#ai-chat-log", Static).update("")
            self.query_one("#ai-grid", DataTable).clear(columns=True)
            self._switch_result_tab("ai-tab-results")
            return
        if bid == "ai-stop":
            workers = getattr(self, "workers", None)
            if workers is not None:
                try:
                    workers.cancel_all()
                    self._status("Stop requested; running AI work cancelled.")
                    return
                except Exception:  # noqa: BLE001
                    pass
            self._status("No AI query in progress.")
            return
        if bid == "ai-copy-sql":
            sql = self.query_one("#ai-sql", TextArea).text
            if not sql.strip():
                self._status("No SQL to copy.")
                return
            try:
                self.app.copy_to_clipboard(sql)
                self._status("SQL copied to clipboard.")
            except Exception:  # noqa: BLE001
                self._status("Select text in the Generated SQL panel to copy.")
            return
        if bid == "ai-edit-sql":
            self.query_one("#ai-sql", TextArea).focus()
            self._status("Generated SQL is editable.")
            return
        if bid == "ai-send-editor":
            sql = self.query_one("#ai-sql", TextArea).text
            if not sql.strip():
                self._status("No SQL to send.")
                return
            app = self.app
            app._pending_sql_editor = {"sql": sql, "conn": self._conn()}  # type: ignore[attr-defined]
            if hasattr(app, "push_screen_by_name"):
                app.push_screen_by_name("sql")
                self._status("SQL sent to the SQL Editor.")
            else:
                self._status("SQL Editor screen is not available in this build.")
            return
        if bid == "ai-review-rules":
            self._status("Edit review rules in the SQL & review rules section.")
            return
        if bid == "ai-import-review":
            self._import_sql_for_review()
            return
        if bid == "ai-review":
            sql = self.query_one("#ai-sql", TextArea).text.strip()
            if not sql:
                self._status("No SQL to review.")
                return
            if not hasattr(self.svc, "review_sql"):
                self._status("AI review not available.")
                return
            rules = self.query_one("#ai-review-rules-text", TextArea).text.strip()
            r = self.svc.review_sql(
                sql, rules=rules, connection=self._conn(), db_type=self._db_type(),
            )
            self.query_one("#ai-review-out", Static).update(
                r.get("review") or r.get("explanation") or r.get("error") or str(r))
            self._switch_result_tab("ai-tab-review")
            self._status(r.get("error") or "Review complete.")
            return
        if bid == "ai-exec-rules":
            self._status("Edit SQL execution rules in the SQL & review rules section.")
            return
        if bid == "ai-session-save":
            if hasattr(self.svc, "ai_session_save"):
                r = self.svc.ai_session_save()
                self._status(r.get("error") or f"Saved: {r.get('path', '')}")
            return
        if bid == "ai-session-load":
            if hasattr(self.svc, "ai_session_load"):
                r = self.svc.ai_session_load()
                self._load_sessions()
                self._status(r.get("error") or f"Loaded {len(r.get('sessions') or [])} session(s).")
            return
        if bid == "ai-session-exec-sql":
            sql = self.query_one("#ai-sql", TextArea).text.strip()
            if not sql:
                self._status("No SQL to execute.")
                return
            sid = self._ensure_session()
            if not sid:
                return
            rules = self.query_one("#ai-exec-rules-text", TextArea).text.strip()
            if rules and hasattr(self.svc, "ai_session_update"):
                self.svc.ai_session_update(sid, sql_execution_rules=rules)
            if hasattr(self.svc, "ai_session_execute_sql"):
                r = self.svc.ai_session_execute_sql(sid, sql)
                if r.get("error") or r.get("blocked"):
                    self._status(r.get("error") or "Blocked by rules.")
                    return
                res = r.get("result") or {}
                grid = self.query_one("#ai-grid", DataTable)
                grid.clear(columns=True)
                cols = res.get("columns") or []
                if cols:
                    grid.add_columns(*[str(c) for c in cols])
                for row in res.get("rows") or []:
                    grid.add_row(*[str(v) if v is not None else "" for v in row])
                self._switch_result_tab("ai-tab-results")
                self._status(f"OK — {res.get('rowcount', len(res.get('rows') or []))} rows (session rules)")
            return
        if bid == "ai-build-app":
            from common.ui.textual.screens.build_apps import AppBuilderScreen

            conns = []
            try:
                conns = [c["name"] for c in self.svc.list_connections()]
            except Exception:
                conns = []
            self.app.push_screen(AppBuilderScreen(connections=conns))
            self._status("App Builder opened")
            return
        if bid == "ai-index-rag":
            conn = self._conn()
            if not conn:
                self._status("Select a connection to index.")
                return
            if not hasattr(self.svc, "rag_index"):
                self._status("RAG indexing not available on this service.")
                return
            self._ensure_open()
            self._status(f"Indexing schema for {conn} into RAG…")
            try:
                r = self.svc.rag_index(conn, rebuild=False)
            except Exception as exc:  # noqa: BLE001
                self._status(f"RAG index failed: {exc}")
                return
            if r.get("error"):
                self._status(f"RAG index failed: {r['error']}")
            else:
                n = r.get("indexed") or r.get("count") or r.get("documents") or ""
                self._status(
                    f"RAG index updated for {conn}" + (f" ({n} item(s))." if n else ".")
                )
            return
        if bid == "ai-rag-manage":
            from common.ui.textual.screens.build_apps import RagManagerModal

            conns = []
            try:
                conns = [c["name"] for c in self.svc.list_connections()]
            except Exception:
                conns = []
            self.app.push_screen(RagManagerModal(connections=conns))
            self._status("RAG Manager opened")
            return
        if bid == "ai-train-llm":
            from common.ui.textual.screens.build_apps import LlmTrainerModal

            conns = []
            try:
                conns = [c["name"] for c in self.svc.list_connections()]
            except Exception:
                conns = []
            self.app.push_screen(LlmTrainerModal(connections=conns))
            self._status("LLM trainer opened")
            return
        if bid == "ai-train-current":
            conn = self._conn()
            q = self.query_one("#ai-question", Input).value.strip()
            sql = self.query_one("#ai-sql", TextArea).text.strip()
            use_rag = bool(getattr(self, "_use_rag", False))
            try:
                explanation = str(
                    self.query_one("#ai-explanation", Static).renderable or "").strip()
            except Exception:
                explanation = ""

            def _save(res):
                if res:
                    self._aiqa_train_model, self._aiqa_train_engine = res

            self.app.push_screen(
                AiqaTrainModal(
                    svc=self.svc, conn=conn, question=q, sql=sql, use_rag=use_rag,
                    model=getattr(self, "_aiqa_train_model", "default"),
                    engine=getattr(self, "_aiqa_train_engine", None),
                    explanation=explanation,
                ),
                _save,
            )
            self._status("Train LLM dialog opened")
            return
        if bid == "ai-schema-clear":
            if hasattr(self.svc, "clear_ai_cache"):
                self._settings_out(self.svc.clear_ai_cache(None))
            self._status("Schema cache cleared.")
            return
        if bid == "ai-schema-show":
            if hasattr(self.svc, "show_ai_cache"):
                self._settings_out(self.svc.show_ai_cache(self._conn()))
            elif hasattr(self.svc, "get_ai_cache_info"):
                self._settings_out(self.svc.get_ai_cache_info())
            self._status("Schema sent to AI shown.")
            return

        conn = self._conn()
        if bid == "ai-ask":
            if not hasattr(self.svc, "ai_query"):
                self._status("AI module not available.")
                return
            q = self.query_one("#ai-question", Input).value.strip()
            if not conn or not q:
                self._status("Connection and question required.")
                return
            self._ensure_open()
            backend = str(self.query_one("#ai-backend", Select).value or "") or None
            if self._use_rag and hasattr(self.svc, "rag_ask"):
                # Retrieval-augmented generation (mirrors Tk's "Use RAG" toggle).
                r = self.svc.rag_ask(conn, q, backend=backend)
                self._render_rag_context(r)
            else:
                r = self.svc.ai_query(
                    conn, q, backend=backend, sql_mode=self._sql_mode,
                )
            if r.get("error") and not r.get("sql"):
                self._status(r["error"])
            else:
                self.query_one("#ai-sql", TextArea).text = r.get("sql") or r.get("summary_sql") or ""
                self.query_one("#ai-explanation", Static).update(r.get("explanation") or "")
                self._switch_result_tab("ai-tab-explanation")
                if r.get("sql") or r.get("summary_sql"):
                    self._add_chat("user", q)
                    self._add_chat("assistant", r.get("explanation") or r.get("sql") or "")
                self._status("SQL generated.")
                if r.get("sql") or r.get("summary_sql"):
                    self._add_history(q)
                if self._auto_execute and (r.get("sql") or r.get("summary_sql")):
                    self._execute_sql()
        elif bid in ("ai-explain", "ai-optimize"):
            kind = "explain_sql" if bid == "ai-explain" else "optimize_sql"
            if not hasattr(self.svc, kind):
                self._status("AI module not available.")
                return
            sql = self.query_one("#ai-sql", TextArea).text.strip()
            if not sql:
                self._status("No SQL to analyse.")
                return
            r = getattr(self.svc, kind)(sql, connection=conn, db_type=self._db_type())
            if bid == "ai-optimize":
                self.query_one("#ai-optimization", Static).update(
                    r.get("optimization") or r.get("explanation") or r.get("error") or str(r))
                self._switch_result_tab("ai-tab-optimization")
            else:
                self.query_one("#ai-explanation", Static).update(
                    r.get("explanation") or r.get("optimization") or r.get("error") or str(r))
                self._switch_result_tab("ai-tab-explanation")
            self._status(r.get("error") or f"{bid.split('-')[1]} complete.")
        elif bid == "ai-exec":
            self._execute_sql()
        elif bid == "ai-set-backend-btn":
            if hasattr(self.svc, "configure_ai_backend"):
                r = self.svc.configure_ai_backend(
                    str(self.query_one("#ai-set-backend", Select).value or ""),
                    verify=self.query_one("#ai-verify", Checkbox).value)
                self._settings_out(r)
                self._load_backends()
        elif bid == "ai-pii-btn":
            if hasattr(self.svc, "set_pii_masking"):
                self._settings_out(self.svc.set_pii_masking(
                    self.query_one("#ai-pii", Checkbox).value))
        elif bid == "ai-cache-info":
            if hasattr(self.svc, "get_ai_cache_info"):
                self._settings_out(self.svc.get_ai_cache_info())
        elif bid == "ai-cache-clear":
            if hasattr(self.svc, "clear_ai_cache"):
                self._settings_out(self.svc.clear_ai_cache(None))
