"""Textual modal screen for the Build Apps suite (App Builder).

Parity with the Tk ``build_apps_dialogs`` and the Web modals: same fields, same
actions, wired to the same ``ai_assistant`` services. Service calls run in a
worker thread so the UI stays responsive.
"""

from __future__ import annotations

from typing import Any

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, Checkbox, Input, Label, RichLog, Select, Static, TextArea, SelectionList
from textual.widgets.selection_list import Selection

from ai_assistant.app_builder.engine import SERVICE_TEMPLATES
from ai_assistant.app_builder.spec import KNOWN_FEATURES

_MODAL_CSS = """
$bg: $surface;
ModalScreen, Screen { align: center middle; }
#box, #ab-screen {
    width: 95%;
    max-width: 120;
    height: 95%;
    padding: 1 2;
    border: round $accent;
    background: $surface;
}
#title { text-style: bold; padding-bottom: 1; }
#hint { color: $text-muted; padding-bottom: 1; }
.ab-log { height: 8; border: round $panel; margin-bottom: 1; }
#out { height: auto; max-height: 18; border: round $panel; padding: 1; margin-top: 1; }
.row { height: auto; }
Input, Select { margin-bottom: 1; }
#actions, #ab-actions { height: auto; align: left middle; }
"""


class AgentDecisionModal(ModalScreen[str]):
    """Blocking modal for agent decision questions during agentic builds."""

    DEFAULT_CSS = _MODAL_CSS

    def __init__(self, decision: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self._decision = decision

    def compose(self) -> ComposeResult:
        d = self._decision
        with VerticalScroll(id="box"):
            yield Static("Agent question", id="title")
            yield Static(d.get("question", ""))
            yield Static(str(d.get("detail", "")), id="hint")
            for opt in d.get("options") or []:
                yield Button(str(opt), id=f"opt-{opt}")
            yield Input(placeholder="Your answer", id="dec-answer")
            with Horizontal():
                yield Button("Send answer", id="dec-send", variant="primary")
                yield Button("Skip", id="dec-skip")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "dec-skip":
            self.dismiss("skip")
        elif bid == "dec-send":
            v = self.query_one("#dec-answer", Input).value.strip() or "skip"
            self.dismiss(v)
        elif bid.startswith("opt-"):
            self.dismiss(bid[4:])


class AppBuilderScreen(Screen):
    """Full App Builder screen — parity with Tk (agentic A/B/C sessions)."""

    BINDINGS = [("escape", "app_pop", "Back")]

    DEFAULT_CSS = """
    AppBuilderScreen { layout: vertical; }
    #ab-screen { width: 100%; height: 100%; border: none; }
    .ab-log { height: 10; border: round $panel; margin: 0 0 1 0; }
    #ab-form-scroll { height: 1fr; max-height: 40%; }
    #ab-panels { height: 2fr; }
    """

    def __init__(self, connections: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._connections = list(connections or [])
        self._svc = None
        self._cancel_event = None
        self._building = False
        self._workspace = ""
        self._decision_result: str | None = None

    def _service(self):
        if self._svc is None:
            from ai_assistant.app_builder.service import make_service
            self._svc = make_service()
        return self._svc

    def compose(self) -> ComposeResult:
        with Vertical(id="ab-screen"):
            yield Static("App Builder — AiAppEngine", id="title")
            with VerticalScroll(id="ab-form-scroll"):
                yield Input(value="myapp", id="ab-name")
                yield Select(
                    [("from_scratch", "from_scratch"), ("from_database", "from_database"),
                     ("from_codebase", "from_codebase")],
                    id="ab-mode", value="from_scratch", allow_blank=False)
                yield Input(placeholder="description", id="ab-desc")
                yield Input(placeholder="entities (comma-sep)", id="ab-entities")
                if self._connections:
                    yield Select([(c, c) for c in self._connections], id="ab-conn", allow_blank=True)
                else:
                    yield Input(placeholder="connection", id="ab-conn")
                yield Input(placeholder="codebase path", id="ab-codebase")
                yield Select([("application", "application"), ("insights_admin", "insights_admin")],
                             id="ab-db-variant", value="application", allow_blank=False)
                yield Select([("prototype", "prototype"), ("full", "full")],
                             id="ab-build-profile", value="prototype", allow_blank=False)
                yield Select([("uninterrupted", "uninterrupted"), ("auto", "auto"),
                              ("interactive", "interactive")],
                             id="ab-interaction", value="auto", allow_blank=False)
                yield Select([("low_token", "low_token"), ("thorough", "thorough")],
                             id="ab-validation", value="low_token", allow_blank=False)
                yield Input(value="8000", id="ab-port")
                for f in KNOWN_FEATURES:
                    yield Checkbox(f, value=True, id="ab-feat-" + f)
                for s in SERVICE_TEMPLATES:
                    yield Checkbox(s, value=s in {"ci_cd", "document", "hosting", "database", "monitoring"},
                                   id="ab-svc-" + s)
                yield Checkbox("Use AI", id="ab-useai", value=True)
                yield Checkbox("Mask PII data", id="ab-mask-pii", value=True)
                yield Static("Train LLM (multi-select)")
                yield SelectionList(id="ab-train-llm")
                yield Input(placeholder="New model name", id="ab-train-new")
                # Seeded with a safe default; on_mount replaces with live engines.
                # (Textual forbids an empty Select when allow_blank=False.)
                yield Select([("python", "python")], id="ab-train-engine",
                             value="python", allow_blank=False)
                yield Checkbox("Use RAG", id="ab-use-rag", value=False)
                yield Checkbox("Index RAG", id="ab-index-rag", value=False)
                yield Select([("index_first", "index_first"), ("parallel", "parallel")],
                             id="ab-rag-strategy", value="index_first", allow_blank=False)
                yield Checkbox("Mine DB queries (real data, validated)",
                               id="ab-mine-db", value=True)
                yield Input(value="5", placeholder="sample rows", id="ab-sample-limit")
                yield Checkbox(
                    "Train from this build's data (schema/queries/insight, validated)",
                    id="ab-rich-train", value=False)
                yield Checkbox("Deploy schema (from_scratch)", id="ab-deploy")
            with Horizontal(id="ab-actions"):
                yield Button("Build", id="ab-build", variant="primary")
                yield Button("Auto-build (AiQA)", id="ab-auto")
                yield Button("Agent build", id="ab-agent")
                yield Button("Train from build", id="ab-train-build")
                yield Button("Stop build", id="ab-stop", variant="error")
                yield Button("Take control", id="ab-takeover")
                yield Button("Start app", id="ab-start")
                yield Button("Stop app", id="ab-stopapp")
                yield Button("Approve & package", id="ab-package")
                yield Button("Delete build", id="ab-delete", variant="error")
                yield Button("Back", id="ab-back")
            with Vertical(id="ab-panels"):
                yield Static("Build status", classes="section-title")
                yield RichLog(id="ab-status-log", markup=True, wrap=True)
                yield Static("Session A — builder")
                yield RichLog(id="ab-log-a", markup=True, wrap=True)
                with Horizontal():
                    with Vertical():
                        yield Static("Session B — answerer")
                        yield RichLog(id="ab-log-b", markup=True, wrap=True)
                    with Vertical():
                        yield Static("Session C — validator")
                        yield RichLog(id="ab-log-c", markup=True, wrap=True)
                with Horizontal():
                    yield Select([("auto (B→A)", "auto"), ("builder", "builder"),
                                  ("answerer", "answerer"), ("validator", "validator")],
                                 id="ab-msg-target", value="auto", allow_blank=False)
                    yield Input(placeholder="Message to session…", id="ab-msg")
                    yield Button("Send", id="ab-msg-send")

    def _log(self, log_id: str, text: str) -> None:
        try:
            self.query_one(f"#{log_id}", RichLog).write(text)
        except Exception:
            pass

    def _conn_value(self) -> str:
        w = self.query_one("#ab-conn")
        if isinstance(w, Select):
            return "" if w.value is Select.BLANK else str(w.value)
        return w.value.strip()

    def _train_fields(self) -> dict:
        sl = self.query_one("#ab-train-llm", SelectionList)
        try:
            sample_limit = int(self.query_one("#ab-sample-limit", Input).value.strip() or 5)
        except ValueError:
            sample_limit = 5
        return {
            "mask_pii": self.query_one("#ab-mask-pii", Checkbox).value,
            "train_llm": list(sl.selected),
            "train_new_name": self.query_one("#ab-train-new", Input).value.strip(),
            "train_engine": str(self.query_one("#ab-train-engine", Select).value or ""),
            "use_rag": self.query_one("#ab-use-rag", Checkbox).value,
            "index_rag": self.query_one("#ab-index-rag", Checkbox).value,
            "rag_strategy": str(self.query_one("#ab-rag-strategy", Select).value or "index_first"),
            "mine_db": self.query_one("#ab-mine-db", Checkbox).value,
            "train_sample_limit": sample_limit,
            "rich_train": self.query_one("#ab-rich-train", Checkbox).value,
        }

    def _collect_body(self, **extra) -> dict:
        mode = self.query_one("#ab-mode", Select).value
        body = {
            "name": self.query_one("#ab-name", Input).value.strip() or "myapp",
            "mode": "from_scratch" if mode is Select.BLANK else str(mode),
            "description": self.query_one("#ab-desc", Input).value.strip(),
            "entities": [e.strip() for e in
                         self.query_one("#ab-entities", Input).value.split(",") if e.strip()],
            "connections": [c] if (c := self._conn_value()) else [],
            "codebase_path": self.query_one("#ab-codebase", Input).value.strip(),
            "db_app_variant": str(self.query_one("#ab-db-variant", Select).value or "application"),
            "build_profile": str(self.query_one("#ab-build-profile", Select).value or "prototype"),
            "interaction": str(self.query_one("#ab-interaction", Select).value or "auto"),
            "validation_depth": str(self.query_one("#ab-validation", Select).value or "low_token"),
            "deploy_schema": self.query_one("#ab-deploy", Checkbox).value,
            "use_ai": self.query_one("#ab-useai", Checkbox).value,
            "features": [f for f in KNOWN_FEATURES if self.query_one("#ab-feat-" + f, Checkbox).value],
            "services": [s for s in SERVICE_TEMPLATES if self.query_one("#ab-svc-" + s, Checkbox).value],
            **self._train_fields(),
            **extra,
        }
        return body

    def on_mount(self) -> None:
        svc = self._service()
        pii = svc.get_pii_masking()
        self.query_one("#ab-mask-pii", Checkbox).value = bool(pii.get("enabled", True))
        info = svc.llm_models()
        sl = self.query_one("#ab-train-llm", SelectionList)
        sl.clear_options()
        for m in info.get("models") or []:
            name = m.get("name", "")
            if name:
                sl.add_option(Selection(name, name))
        engines = [(e.get("name", ""), e.get("name", "")) for e in (info.get("engines") or []) if e.get("name")]
        eng_sel = self.query_one("#ab-train-engine", Select)
        if engines:
            eng_sel.set_options(engines)
            eng_sel.value = engines[0][0]

    def _handle_progress(self, payload: Any) -> None:
        if isinstance(payload, dict) and payload.get("agent_event"):
            ev = payload["agent_event"]
            session = ev.get("session", "builder")
            event = ev.get("event", {})
            etype = event.get("type", "")
            text = event.get("text", "")
            log_map = {"builder": "ab-log-a", "answerer": "ab-log-b",
                       "validator": "ab-log-c", "system": "ab-log-a"}
            lid = log_map.get(session, "ab-log-a")
            if etype == "baseline_ready":
                ws = (event.get("detail") or {}).get("workspace") or text
                if ws:
                    self._workspace = ws
                self._log("ab-log-a", "[system] baseline ready\n")
            elif text:
                self._log(lid, f"[{session}] {text}\n")
        elif isinstance(payload, dict):
            ptype = payload.get("type", "")
            if ptype.startswith("training_"):
                self._log("ab-status-log", f"[train] {ptype} {payload}\n")
            else:
                self._log("ab-status-log",
                          f"round {payload.get('index')} score={payload.get('score')}\n")

    def _agent_ask(self, decision: Any) -> str:
        import threading

        answer: list[str] = ["skip"]
        evt = threading.Event()
        d = {
            "question": getattr(decision, "question", ""),
            "detail": getattr(decision, "detail", ""),
            "options": list(getattr(decision, "options", []) or []),
        }

        def show() -> None:
            def on_done(result: str | None) -> None:
                answer[0] = result or "skip"
                evt.set()

            self.app.push_screen(AgentDecisionModal(d), on_done)

        self.app.call_from_thread(show)
        evt.wait(timeout=3600)
        return answer[0]

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "ab-back":
            self.action_app_pop()
            return
        if bid == "ab-stop":
            if self._cancel_event:
                self._cancel_event.set()
            self._log("ab-status-log", "Stopping build…\n")
            return
        if bid == "ab-takeover":
            dec = getattr(self._service(), "last_decider", None)
            if dec:
                dec.take_control()
                self.query_one("#ab-interaction", Select).value = "interactive"
            return
        if bid == "ab-msg-send":
            self._send_message()
            return
        if bid == "ab-start":
            self._start_app()
            return
        if bid == "ab-stopapp":
            self._stop_app()
            return
        if bid == "ab-package":
            self._package()
            return
        if bid == "ab-delete":
            self._delete_build()
            return
        if bid == "ab-train-build":
            self._train_from_build()
            return
        if bid not in ("ab-build", "ab-auto", "ab-agent"):
            return
        agentic = bid == "ab-agent"
        auto = bid in ("ab-auto", "ab-agent")
        body = self._collect_body(use_ai=auto or self.query_one("#ab-useai", Checkbox).value,
                                  agentic=agentic, run_tests=True)
        self._log("ab-status-log", f"Starting {'agent' if agentic else 'auto'} build…\n")
        import threading
        self._cancel_event = threading.Event()
        self._building = True

        def work():
            svc = self._service()
            return svc.run_agentic_build(
                body,
                on_progress=lambda p: self.app.call_from_thread(self._handle_progress, p),
                ask=self._agent_ask,
                cancel_event=self._cancel_event,
            )

        def done(r: dict[str, Any]) -> None:
            self._building = False
            self._show_result(r)

        def run():
            try:
                result = work()
            except Exception as exc:
                result = {"ok": False, "error": str(exc)}
            self.app.call_from_thread(done, result)

        self.run_worker(run, thread=True)

    def _train_from_build(self) -> None:
        """Train selected/new LLM model(s) from the build's OWN data."""
        body = self._collect_body(train_mode="full")
        if self._workspace:
            body["workspace"] = self._workspace
        if not (body.get("train_llm") or body.get("train_new_name")):
            self._log("ab-status-log",
                      "Select an existing model or enter a new model name first.\n")
            return
        self._log("ab-status-log", "Training LLM from this build's data…\n")

        def work():
            return self._service().build_train_llm(
                body,
                on_progress=lambda p: self.app.call_from_thread(self._handle_progress, p),
            )

        def done(r: dict[str, Any]) -> None:
            if not r.get("ok"):
                self._log("ab-status-log",
                          f"Train failed: {r.get('error') or r.get('reason')}\n")
                return
            cs = r.get("corpus_stats") or {}
            self._log(
                "ab-status-log",
                f"Trained {len(r.get('models') or [])} model(s) on {r.get('pairs')} "
                f"build-data pair(s) (validation={cs.get('validation')}, "
                f"rejected={cs.get('rejected', 0)})\n")

        def run():
            try:
                result = work()
            except Exception as exc:
                result = {"ok": False, "error": str(exc)}
            self.app.call_from_thread(done, result)

        self.run_worker(run, thread=True)

    def _show_result(self, r: dict[str, Any]) -> None:
        if r.get("workspace"):
            self._workspace = r["workspace"]
        self._log("ab-status-log",
                  f"{'READY' if r.get('ok') else 'INCOMPLETE'} score={r.get('score', '-')}\n")

    def _send_message(self) -> None:
        text = self.query_one("#ab-msg", Input).value.strip()
        if not text:
            return
        coord = getattr(self._service(), "last_coordinator", None)
        if not coord:
            return
        target = str(self.query_one("#ab-msg-target", Select).value or "auto")
        interactive = str(self.query_one("#ab-interaction", Select).value) == "interactive"
        try:
            if target == "auto":
                if interactive or not self._building:
                    reply = coord.route_user_request(text, interactive=interactive)
                else:
                    reply = coord.queue_user_message(text)
            elif target == "builder":
                reply = coord.builder.send(text) if coord.builder else ""
            elif target == "answerer":
                reply = coord.answerer.send(text) if coord.answerer else ""
            else:
                reply = coord.validator.send(text) if coord.validator else ""
            self._log("ab-log-b", f"[reply] {reply}\n")
        except Exception as exc:
            self._log("ab-status-log", f"Message error: {exc}\n")
        self.query_one("#ab-msg", Input).value = ""

    def _start_app(self) -> None:
        name = self.query_one("#ab-name", Input).value.strip()
        port = int(self.query_one("#ab-port", Input).value.strip() or "8000")
        r = self._service().start_app({"name": name, "port": port})
        if r.get("ok"):
            self._log("ab-status-log", f"Started: {r.get('url')}\n")
        else:
            self._log("ab-status-log", f"Start failed: {r.get('issues')}\n")

    def _stop_app(self) -> None:
        name = self.query_one("#ab-name", Input).value.strip()
        self._service().stop_app({"name": name})
        self._log("ab-status-log", "App stopped.\n")

    def _package(self) -> None:
        name = self.query_one("#ab-name", Input).value.strip()
        port = int(self.query_one("#ab-port", Input).value.strip() or "8000")
        r = self._service().package_app({"name": name, "port": port, "archive": True})
        self._log("ab-status-log", f"Package: {r.get('created', [])}\n")

    def _delete_build(self) -> None:
        name = self.query_one("#ab-name", Input).value.strip()
        if self._cancel_event:
            self._cancel_event.set()
        self._service().stop_app({"name": name})
        r = self._service().delete_app({"name": name})
        self._workspace = ""
        self._log("ab-status-log", f"Deleted: {r.get('workspace')}\n")

    def action_app_pop(self) -> None:
        self.app.pop_screen()


class AppBuilderModal(ModalScreen[None]):
    """Legacy modal — opens full App Builder screen."""

    DEFAULT_CSS = _MODAL_CSS
    BINDINGS = [("escape", "close", "Close")]

    def __init__(self, connections: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._connections = list(connections or [])

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="box"):
            yield Static("App Builder", id="title")
            yield Static("Opening full App Builder screen…", id="hint")
            yield Button("Open App Builder", id="ab-open", variant="primary")
            yield Button("Close", id="ab-close")

    def on_mount(self) -> None:
        pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "ab-open":
            self.app.push_screen(AppBuilderScreen(connections=self._connections))
            self.dismiss(None)
            return
        if bid == "ab-close":
            self.dismiss(None)

    def action_close(self) -> None:
        self.dismiss(None)


class LlmTrainerModal(ModalScreen[None]):
    """Build / train a local NL→SQL model (parity with the Tk LLM panel and the
    Web "Build or Train LLM" form). Wired to :class:`ai_assistant.llm.service.
    LlmService` — the same code path as ``dbtool ai llm`` and the REST API.
    Train / Status / Generate / Export run on a worker thread.
    """

    DEFAULT_CSS = _MODAL_CSS
    BINDINGS = [("escape", "close", "Close")]

    def __init__(self, connections: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._connections = list(connections or [])
        self._svc = None
        # Live harvest handle for graceful stop (set while a harvest runs).
        self._harvest_ai_svc = None
        self._harvest_id = ""

    def _service(self):
        if self._svc is None:
            from ai_assistant.llm.service import LlmService
            self._svc = LlmService()
        return self._svc

    def compose(self) -> ComposeResult:
        engines = []
        try:
            engines = self._service().engines().get("engines") or []
        except Exception:  # noqa: BLE001
            engines = []
        eng_opts = [("(config default)", "")] + [
            (f"{e['name']}{'' if e.get('available') else ' (unavailable)'}", e["name"])
            for e in engines
        ]
        with VerticalScroll(id="box"):
            yield Static("Local LLM — build or train your own NL→SQL model", id="title")
            yield Static(
                "Train a small NL→SQL model entirely on your machine. The python "
                "engine needs no extra deps; numpy/pytorch are optional. Optionally "
                "fold in the examples you've saved via RAG for a connection.", id="hint")
            yield Label("Model name")
            yield Input(value="default", id="llm-name")
            yield Label("Engine")
            yield Select(eng_opts, id="llm-engine", value="", allow_blank=False)
            yield Label("Action")
            yield Select(
                [("Build / train", "train"), ("Rich DB train", "rich_train"),
                 ("Train multi-connection (parallel)", "train_multi"),
                 ("Auto-harvest & train", "harvest"),
                 ("Enrich templates", "enrich"),
                 ("Preview mined DB queries", "mine"),
                 ("Show status", "status"), ("Generate SQL", "generate"),
                 ("Evaluate model", "eval"),
                 ("Export dataset", "export"),
                 ("Versions", "versions"), ("Restore version", "restore"),
                 ("Schedule training", "schedule_start"),
                 ("Discard scheduled training", "schedule_stop"),
                 ("Schedule status", "schedule_status")],
                id="llm-action", value="train", allow_blank=False)
            yield Checkbox("Include sample data", value=True, id="llm-sample")
            yield Checkbox("Mine DB queries (real data, validated)", value=True, id="llm-mine-db")
            yield Checkbox("Index RAG first", value=False, id="llm-index-rag")
            yield Input(value="5", placeholder="sample rows", id="llm-sample-limit")
            yield Label("AI-generated questions (harvest)")
            yield Input(value="40", placeholder="40", id="llm-harvest-q")
            yield Label("Training mode (full / incremental)")
            yield Select([("Full retrain", "full"), ("Incremental", "incremental")],
                         id="llm-train-mode", value="full", allow_blank=False)
            yield Label("Backend workers / timeout (seconds)")
            with Horizontal():
                yield Input(value="4", placeholder="workers", id="llm-gen-workers")
                yield Input(value="120", placeholder="timeout", id="llm-gen-timeout")
            yield Label("Train with RAG examples from connection")
            if self._connections:
                yield Select([(c, c) for c in self._connections], id="llm-rag-conn",
                             allow_blank=True)
            else:
                yield Input(placeholder="connection name", id="llm-rag-conn")
            yield Label("Multi-connection train: pick connections (parallel shards)")
            yield SelectionList(
                *[Selection(c, c) for c in self._connections], id="llm-multi-conns")
            yield Label("Question (for generate)")
            yield Input(placeholder="count the number of orders", id="llm-question")
            yield Label("Export path (for export)")
            yield Input(placeholder="/path/to/dataset.jsonl", id="llm-export-path")
            yield Label("Version id (for restore)")
            yield Input(placeholder="version id from Versions", id="llm-version")
            with Horizontal(id="actions"):
                yield Button("Run", id="llm-run", variant="primary")
                yield Button("Stop harvest", id="llm-stop", disabled=True)
                yield Button("Close", id="llm-close")
            yield Static("", id="out")

    def _rag_conn_value(self) -> str:
        w = self.query_one("#llm-rag-conn")
        if isinstance(w, Select):
            return "" if w.value is Select.BLANK else str(w.value)
        return w.value.strip()

    def _engine_arg(self) -> str | None:
        v = self.query_one("#llm-engine", Select).value
        return None if v in ("", Select.BLANK) else str(v)

    def _train_mode_value(self) -> str:
        v = str(self.query_one("#llm-train-mode", Select).value or "full")
        return v if v in ("full", "incremental") else "full"

    def _gen_int(self, sel: str, default: int) -> int:
        try:
            return int(self.query_one(sel, Input).value.strip() or default)
        except ValueError:
            return default

    def _harvest_extras(self) -> dict:
        return {
            "train_mode": self._train_mode_value(),
            "gen_workers": self._gen_int("#llm-gen-workers", 4),
            "gen_timeout": self._gen_int("#llm-gen-timeout", 120),
        }

    @staticmethod
    def _training_progress_msg(ev: dict) -> str | None:
        etype = ev.get("type")
        if etype == "training_capture":
            status = ev.get("status")
            if status == "collecting":
                return "Collecting training data…"
            if status == "captured":
                return (
                    f"Collected {ev.get('pairs', 0)} pair(s) "
                    f"({ev.get('source', '')}); training…"
                )
        elif etype == "training_rag":
            rag_status = ev.get("status")
            if rag_status == "indexing_parallel":
                return f"Indexing RAG for '{ev.get('connection', '')}'…"
            if rag_status == "indexed":
                return "RAG indexing complete."
            if rag_status == "index_failed":
                return "RAG indexing failed."
        elif etype == "training_progress":
            return f"Training {ev.get('model', 'model')}…"
        elif etype == "training_epoch":
            return (
                f"Training {ev.get('model', 'model')}: "
                f"epoch {ev.get('epoch', '?')}, loss {ev.get('loss', '?')}"
            )
        elif etype == "training_done":
            if ev.get("ok"):
                return (
                    f"Training complete — {ev.get('pairs', 0)} pair(s) "
                    f"({ev.get('source', '')})"
                )
            return "Training failed."
        elif etype == "harvest_offline_collected":
            return (
                f"Offline harvest collected {ev.get('pairs', 0)} validated pairs; "
                "training local model…"
            )
        elif etype == "harvest_train_done":
            phase = str(ev.get("phase") or "training").replace("_", " ")
            if ev.get("ok"):
                return f"{phase.title()} training complete."
            return str(ev.get("reason") or "Training failed.")
        elif etype == "harvest_backend_start":
            return "Offline model trained; starting optional backend enrichment…"
        elif etype == "harvest_question_bank":
            if ev.get("status") == "generating":
                return (
                    f"Asking AI to invent {ev.get('count', 0)} schema-grounded "
                    "questions… (this backend call can take a while)"
                )
            if ev.get("status") == "generated":
                return (
                    f"AI proposed {ev.get('questions', 0)} questions; "
                    "preparing backend generation…"
                )
        elif etype == "harvest_followup":
            q = (ev.get("question") or "").strip()
            tail = f": {q[:60]}" if q else ""
            return (
                f"Backend follow-up thread {ev.get('done', 0)}/{ev.get('total', 0)} "
                f"[{ev.get('category', '')}]{tail}…"
            )
        elif etype == "harvest_generate":
            if ev.get("status") == "planned":
                return (
                    f"Prepared {ev.get('total', 0)} backend question(s); generating SQL "
                    f"with {ev.get('workers', 1)} worker(s)…"
                )
            q = (ev.get("question") or "").strip()
            tail = f" — {q[:60]}" if q else ""
            return (
                f"Generating SQL with backend {ev.get('done', 0)}/{ev.get('total', 0)} "
                f"(kept {ev.get('kept', 0)}){tail}…"
            )
        elif etype == "harvest_collected":
            return f"Validated {ev.get('pairs', 0)} total pairs; finalizing training…"
        elif etype == "harvest_stopped":
            return "Stopping gracefully — keeping the trained model…"
        return None

    def _emit_progress(self, ev: dict) -> None:
        msg = self._training_progress_msg(ev)
        if msg:
            self.query_one("#out", Static).update(msg)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "llm-close":
            self.dismiss(None)
            return
        if bid == "llm-stop":
            if self._harvest_ai_svc is not None and self._harvest_id:
                self._harvest_ai_svc.llm_harvest_stop(self._harvest_id)
                self.query_one("#out", Static).update(
                    "Stop requested — finishing the current step, then saving the model…")
                self.query_one("#llm-stop", Button).disabled = True
            return
        if bid != "llm-run":
            return
        action = str(self.query_one("#llm-action", Select).value or "train")
        name = self.query_one("#llm-name", Input).value.strip() or "default"
        engine = self._engine_arg()
        include_sample = self.query_one("#llm-sample", Checkbox).value
        mine_db = self.query_one("#llm-mine-db", Checkbox).value
        index_rag = self.query_one("#llm-index-rag", Checkbox).value
        try:
            sample_limit = int(self.query_one("#llm-sample-limit", Input).value.strip() or 5)
        except ValueError:
            sample_limit = 5
        rag_conn = self._rag_conn_value()
        question = self.query_one("#llm-question", Input).value.strip()
        export_path = self.query_one("#llm-export-path", Input).value.strip()
        verb = {"train": "Training", "status": "Reading status",
                "generate": "Generating", "export": "Exporting",
                "eval": "Evaluating", "harvest": "Auto-harvesting"}.get(action, "Working")
        self.query_one("#out", Static).update(f"{verb}…")

        if action == "harvest":
            import time as _time

            from ai_query.service import make_service

            self._harvest_ai_svc = make_service()
            self._harvest_id = f"tui-{int(_time.time() * 1000)}"
            self.query_one("#llm-stop", Button).disabled = False

        def progress(ev: dict):
            self.app.call_from_thread(self._emit_progress, ev)

        def work():
            svc = self._service()
            if action == "status":
                return svc.status(name)
            if action == "generate":
                if not question:
                    return {"ok": False, "error": "Enter a question to generate SQL."}
                return svc.generate(question, name=name, engine=engine)
            if action == "eval":
                return svc.evaluate(
                    name=name, connection=rag_conn, include_sample=include_sample,
                    rag_connection=rag_conn)
            if action == "export":
                if not export_path:
                    return {"ok": False, "error": "Enter an export path."}
                return svc.export_dataset(
                    export_path, include_sample=include_sample, rag_connection=rag_conn)
            if action == "mine":
                from ai_query.service import make_service

                return make_service().llm_mine_pairs({
                    "connections": [rag_conn] if rag_conn else [],
                    "train_sample_limit": sample_limit,
                })
            if action == "enrich":
                from ai_query.service import make_service

                return make_service().llm_enrich_templates({
                    "connections": [rag_conn] if rag_conn else [],
                    "engine": engine or "",
                    "names": [name],
                })
            if action == "versions":
                from ai_query.service import make_service

                return make_service().llm_model_versions(name=name)
            if action == "restore":
                version = self.query_one("#llm-version", Input).value.strip()
                if not version:
                    return {"ok": False, "error": "Enter a version id to restore."}
                from ai_query.service import make_service

                return make_service().llm_model_restore(name=name, version=version)
            if action == "schedule_start":
                from ai_query.service import make_service

                return make_service().llm_harvest_schedule_start()
            if action == "schedule_stop":
                from ai_query.service import make_service

                return make_service().llm_harvest_schedule_stop()
            if action == "schedule_status":
                from ai_query.service import make_service

                return make_service().llm_harvest_schedule_status()
            if action == "train_multi":
                from ai_query.service import make_service

                selected = list(
                    self.query_one("#llm-multi-conns", SelectionList).selected)
                if not selected:
                    return {"ok": False,
                            "error": "Select one or more connections to train from."}
                return make_service().llm_train_multi({
                    "connections": selected,
                    "train_new_name": name,
                    "train_engine": engine or "",
                    "gen_workers": self._gen_int("#llm-gen-workers", 4),
                    "train_sample_limit": sample_limit,
                }, progress=progress)
            if action == "rich_train":
                from ai_query.service import make_service

                return make_service().llm_train_rich({
                    "mode": "from_database",
                    "connections": [rag_conn] if rag_conn else [],
                    "train_new_name": name,
                    "train_engine": engine or "",
                    "include_sample": include_sample,
                    "use_rag": bool(rag_conn),
                    "index_rag": index_rag,
                    "rag_strategy": "index_first",
                    "mine_db": mine_db,
                    "train_sample_limit": sample_limit,
                    "train_mode": self._train_mode_value(),
                }, progress=progress)
            if action == "harvest":
                if not rag_conn:
                    return {"ok": False, "error": "Select a connection to harvest from."}
                try:
                    qcount = int(self.query_one("#llm-harvest-q", Input).value.strip() or 0)
                except ValueError:
                    qcount = 0
                return self._harvest_ai_svc.llm_harvest({
                    "connection": rag_conn,
                    "train_new_name": name,
                    "train_engine": engine or "",
                    "generated_questions": qcount,
                    "use_rag": bool(rag_conn),
                    "sample_limit": sample_limit,
                    "do_train": True,
                    "harvest_id": self._harvest_id,
                    **self._harvest_extras(),
                }, progress=progress)
            return svc.train(name=name, engine=engine, include_sample=include_sample,
                             rag_connection=rag_conn, progress=progress)

        def run():
            r = work()
            self.app.call_from_thread(self._show_result, action, r)
        self.run_worker(run, thread=True)

    def _show_result(self, action: str, r: dict[str, Any]) -> None:
        if action == "harvest":
            self._harvest_ai_svc = None
            self._harvest_id = ""
            try:
                self.query_one("#llm-stop", Button).disabled = True
            except Exception:  # noqa: BLE001
                pass
        if not r.get("ok"):
            self.query_one("#out", Static).update(
                "ERROR: " + (r.get("error") or "operation failed"))
            return
        if action == "status":
            if not r.get("trained"):
                self.query_one("#out", Static).update(
                    f"Model '{r.get('name')}' is not trained yet.")
                return
            meta = r.get("meta") or {}
            self.query_one("#out", Static).update(
                f"name       : {r.get('name')}\n"
                f"engine     : {r.get('engine')}\n"
                f"trained_at : {meta.get('trained_at', '')}\n"
                f"pairs      : {meta.get('num_pairs', '')}\n"
                f"final_loss : {meta.get('final_loss', '')}\n"
                f"path       : {r.get('path')}")
            return
        if action == "generate":
            self.query_one("#out", Static).update(f"SQL:\n{r.get('sql')}")
            return
        if action == "eval":
            from ai_assistant.llm.eval import format_eval_summary

            summary = format_eval_summary(r) or "(no eval metrics)"
            self.query_one("#out", Static).update(
                f"{summary}\n"
                f"count      : {r.get('count', 0)}\n"
                f"parse_ok   : {r.get('parse_ok_rate')}\n"
                f"executable : {r.get('executable_rate')}\n"
                f"match      : {r.get('normalized_match_rate')}\n"
                f"EX         : {r.get('execution_exact_rate')}\n"
                f"soft_f1    : {r.get('soft_f1_avg')}")
            return
        if action == "export":
            self.query_one("#out", Static).update(
                f"Exported {r.get('count', 0)} pairs → {r.get('path')}")
            return
        if action == "mine":
            stats = r.get("stats") or {}
            self.query_one("#out", Static).update(
                f"Mined {stats.get('kept', 0)} pairs "
                f"({stats.get('validated', 0)}/{stats.get('candidates', 0)} passed)")
            return
        if action == "enrich":
            self.query_one("#out", Static).update(
                f"Enriched templates: {r.get('enriched', r.get('count', 0))} "
                f"item(s) generated.\n{r.get('message', '')}")
            return
        if action == "versions":
            versions = r.get("versions") or []
            if not versions:
                self.query_one("#out", Static).update("No saved versions for this model.")
                return
            lines = [f"{v.get('version', v.get('id', ''))}  "
                     f"{v.get('created', v.get('timestamp', ''))}  "
                     f"{v.get('reason', '')}" for v in versions]
            self.query_one("#out", Static).update(
                "Versions (newest first):\n" + "\n".join(lines))
            return
        if action == "restore":
            self.query_one("#out", Static).update(
                f"Restored '{r.get('name')}' to version "
                f"{r.get('version', '')}. {r.get('message', '')}")
            return
        if action in ("schedule_start", "schedule_stop", "schedule_status"):
            self.query_one("#out", Static).update(
                r.get("message")
                or f"Schedule: enabled={r.get('enabled')} window={r.get('window', '')} "
                   f"next={r.get('next_run', '')}")
            return
        if action == "harvest":
            srcs = r.get("sources") or {}
            self.query_one("#out", Static).update(
                ("Stopped — " if r.get("stopped") else "")
                + f"Harvested {r.get('pairs', 0)} validated pairs "
                f"(offline {r.get('offline_pairs', 0)}, "
                f"backend {r.get('backend_pairs', 0)}, "
                f"skipped-known {r.get('skipped_known', 0)}, "
                f"already={r.get('already_trained', 0)} new={r.get('new_pairs', 0)}, "
                f"rejected {r.get('rejected', 0)})\n"
                + "sources: " + ", ".join(f"{k}={v}" for k, v in srcs.items()) + "\n"
                + ("trained " + ", ".join(m.get("name", "")
                                          for m in (r.get("models") or []))
                   if r.get("trained") else "not trained"))
            return
        if action == "rich_train":
            self.query_one("#out", Static).update(
                f"Trained {len(r.get('models') or [])} model(s) on "
                f"{r.get('pairs')} pairs ({r.get('source', '')}); "
                f"already={r.get('already_trained', 0)} new={r.get('new_pairs', 0)}\n"
                f"{r.get('reason', '')}")
            return
        fb = ""
        if r.get("engine_fallback"):
            fb = (f"\n  (requested '{r.get('engine_requested')}' unavailable; "
                  f"used '{r.get('engine')}')")
        self.query_one("#out", Static).update(
            f"Trained '{r.get('name')}'  engine={r.get('engine')}  "
            f"pairs={r.get('num_pairs', '?')}  loss={r.get('final_loss', '?')}{fb}")

    def action_close(self) -> None:
        self.dismiss(None)


class RagManagerModal(ModalScreen[None]):
    """RAG Manager for Textual: schema, docs, glossary, examples and analytics."""

    DEFAULT_CSS = _MODAL_CSS
    BINDINGS = [("escape", "close", "Close")]

    def __init__(self, connections: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._connections = list(connections or [])
        self._svc = None

    def _service(self):
        if self._svc is None:
            from ai_query.service import make_service
            self._svc = make_service()
        return self._svc

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="box"):
            yield Static("RAG Manager", id="title")
            yield Static(
                "Index live database schema, upload documents, glossary, examples, "
                "analytical patterns, or a codebase folder. Enable Use RAG in "
                "Generate SQL to ground answers on the selected scope.",
                id="hint")
            yield Label("Active database")
            conn_opts = [(c, c) for c in self._connections] or [("(none — connect first)", "")]
            yield Select(conn_opts, id="rag-conn", allow_blank=False)
            yield Checkbox("Standalone collection", id="rag-standalone", value=False)
            yield Label("Collection name (standalone)")
            yield Input(value="docs", placeholder="e.g. docs, myapp-code", id="rag-scope")
            yield Label("Action")
            yield Select(
                [
                    ("Overview (status + breakdown)", "overview"),
                    ("Index schema", "index"),
                    ("Re-index schema", "reindex"),
                    ("Add codebase folder", "codebase"),
                    ("Add document", "document"),
                    ("List documents", "docs"),
                    ("Preview search", "preview"),
                    ("Evaluate retrieval quality", "eval"),
                    ("Check schema drift", "drift"),
                    ("Re-index if stale", "reindex_stale"),
                    ("Scheduled re-index: status", "schedule_status"),
                    ("Scheduled re-index: start", "schedule_start"),
                    ("Scheduled re-index: stop", "schedule_stop"),
                    ("Seed analytical patterns", "seed"),
                    ("Show analytical library", "analytics"),
                    ("Add NL→SQL example", "example"),
                    ("Import examples from file", "examples_file"),
                    ("Add glossary term", "glossary"),
                    ("Remove document", "remove"),
                    ("How to use RAG", "help"),
                    ("Clear collection", "clear"),
                ],
                id="rag-action", value="overview", allow_blank=False)
            yield Label("Query / question / term / document title")
            yield Input(id="rag-query")
            yield Label("Also search scopes (comma-separated; for Preview)")
            yield Input(placeholder="rag_code, docs", id="rag-extra-scopes")
            yield Label("File or folder path")
            yield Input(placeholder="/path/to/file.md or /path/to/repo", id="rag-file")
            yield Label("Text / SQL / definition / pasted document")
            yield TextArea("", id="rag-text")
            with Horizontal(id="actions"):
                yield Button("Run", id="rag-run", variant="primary")
                yield Button("Close", id="rag-close")
            yield Static("", id="out")

    def _scope_value(self) -> str:
        if self.query_one("#rag-standalone", Checkbox).value:
            return self.query_one("#rag-scope", Input).value.strip()
        sel = self.query_one("#rag-conn", Select).value
        return str(sel).strip() if sel not in (None, Select.BLANK) else ""

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "rag-close":
            self.dismiss(None)
            return
        if bid != "rag-run":
            return
        scope = self._scope_value()
        action = str(self.query_one("#rag-action", Select).value or "status")
        query = self.query_one("#rag-query", Input).value.strip()
        extra_scopes = [
            s.strip() for s in
            self.query_one("#rag-extra-scopes", Input).value.split(",") if s.strip()
        ]
        file_path = self.query_one("#rag-file", Input).value.strip()
        text = self.query_one("#rag-text", TextArea).text.strip()
        standalone = self.query_one("#rag-standalone", Checkbox).value
        _no_scope_actions = (
            "analytics", "help",
            "schedule_status", "schedule_start", "schedule_stop",
        )
        if not scope and action not in _no_scope_actions:
            self.query_one("#out", Static).update("ERROR: enter a collection name.")
            return
        self.query_one("#out", Static).update("Working...")

        def work():
            svc = self._service()
            if action == "overview":
                return svc.rag_scope_overview(scope)
            if action == "index":
                return svc.rag_index(scope, rebuild=False)
            if action == "reindex":
                return svc.rag_index(scope, rebuild=True)
            if action == "codebase":
                if not file_path:
                    return {"ok": False, "error": "Enter codebase folder path."}
                return svc.rag_add_codebase(
                    file_path, scope, standalone=standalone)
            if action == "preview":
                if not query:
                    return {"ok": False, "error": "Enter a search query."}
                if extra_scopes:
                    return svc.rag_preview_multi([scope] + extra_scopes, query, k=8)
                return svc.rag_preview(scope, query, k=8)
            if action == "eval":
                return svc.rag_eval(scope, k=8, per_case=True)
            if action == "drift":
                return svc.rag_drift(scope)
            if action == "reindex_stale":
                return svc.rag_reindex_stale([scope])
            if action == "schedule_status":
                return svc.rag_reindex_schedule_status()
            if action == "schedule_start":
                return svc.rag_reindex_schedule_start()
            if action == "schedule_stop":
                return svc.rag_reindex_schedule_stop()
            if action == "status":
                return {
                    "status": svc.rag_status(scope),
                    "breakdown": svc.rag_breakdown(scope),
                }
            if action == "search":
                if not query:
                    return {"ok": False, "error": "Enter a search query."}
                return svc.rag_search(scope, query, k=8)
            if action == "document":
                if not (file_path or text):
                    return {"ok": False, "error": "Enter a file path or pasted text."}
                return svc.rag_add_document(
                    scope, file_path=file_path or None, text=text or None,
                    title=query, source=query or file_path, standalone=standalone)
            if action == "docs":
                return svc.rag_documents(scope)
            if action == "seed":
                return svc.rag_seed_analytics(scope, standalone=standalone)
            if action == "analytics":
                return svc.rag_analytics_library()
            if action == "example":
                if not query or not text:
                    return {"ok": False, "error": "Question and SQL are required."}
                return svc.rag_add_example(scope, query, text)
            if action == "examples_file":
                if not file_path:
                    return {"ok": False, "error": "Enter the examples file path."}
                return svc.rag_add_examples_from_file(
                    scope, file_path, standalone=standalone)
            if action == "glossary":
                if not query or not text:
                    return {"ok": False, "error": "Term and definition are required."}
                return svc.rag_add_glossary(scope, query, text)
            if action == "clear":
                return svc.rag_clear(scope)
            if action == "remove":
                if not query:
                    return {"ok": False, "error": "Enter document title/id in query field."}
                return svc.rag_remove_document(scope, query)
            return {
                "ok": True,
                "help": [
                    "Select an active database (or Standalone collection).",
                    "Index schema to build table/relationship docs.",
                    "Add documents, glossary, examples, or a codebase folder.",
                    "Preview search shows ranked hits + context block.",
                    "Enable Use RAG in Generate SQL to ground answers.",
                ],
            }

        def run():
            r = work()
            self.app.call_from_thread(self._show_result, action, r)
        self.run_worker(run, thread=True)

    def _show_result(self, action: str, r: dict[str, Any]) -> None:
        import json

        if isinstance(r, dict) and r.get("error"):
            self.query_one("#out", Static).update("ERROR: " + str(r.get("error")))
            return
        if action == "analytics":
            lines = [
                f"[{q.get('category')}] {q.get('question')}\n{q.get('sql')}"
                for q in r.get("queries", [])
            ]
            self.query_one("#out", Static).update("\n\n".join(lines))
            return
        if action == "overview":
            st = r.get("status") or {}
            br = r.get("breakdown") or {}
            mm = st.get("embedder_mismatch") or br.get("embedder_mismatch") or {}
            lines = [
                f"scope      : {r.get('scope', '')}",
                f"indexed    : {st.get('indexed')}",
                f"doc_count  : {st.get('doc_count')}",
                f"provider   : {(st.get('meta') or {}).get('provider', '')}",
                f"dim        : {(st.get('meta') or {}).get('dim', '')}",
            ]
            if mm.get("mismatch"):
                lines.append(f"WARNING    : {mm.get('message')}")
            lines.append("\nbreakdown:")
            for k, v in sorted((br.get("counts") or {}).items()):
                lines.append(f"  {k:<12} {v}")
            self.query_one("#out", Static).update("\n".join(lines))
            return
        if action == "preview":
            parts = [r.get("preview") or "", "", "Context block:", r.get("context") or ""]
            self.query_one("#out", Static).update("\n".join(parts))
            return
        if action in ("index", "reindex", "codebase"):
            self.query_one("#out", Static).update(json.dumps(r, indent=2, default=str))
            return
        self.query_one("#out", Static).update(json.dumps(r, indent=2, default=str))

    def action_close(self) -> None:
        self.dismiss(None)
