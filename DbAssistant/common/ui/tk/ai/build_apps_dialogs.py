"""Tk dialog for the Build Apps suite: App Builder.

A functional window wired to the real ``ai_assistant.app_builder`` service.
Long-running service calls run on a worker thread and update the UI via
``after``. (The local LLM trainer and RAG manager live in the Generate-SQL
surface — see ``llm_panel`` / ``rag_panel``.)
"""

from __future__ import annotations

import re
import threading
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Any

from common.ui.tk import make_scrollable

_AUTO_APP_NAME_RE = re.compile(r"^dbassist_app_\d{8}_\d{6}$")


def _default_app_name() -> str:
    return f"dbassist_app_{datetime.now():%Y%m%d_%H%M%S}"


def _is_auto_app_name(name: str) -> bool:
    return bool(_AUTO_APP_NAME_RE.match((name or "").strip()))


def _ensure_build_name(name_var: tk.StringVar) -> str:
    """Return a build name; regenerate auto-pattern names so each build is fresh."""
    current = name_var.get().strip()
    if not current or _is_auto_app_name(current):
        fresh = _default_app_name()
        name_var.set(fresh)
        return fresh
    return current


def _stop_running_app(state: dict) -> None:
    """Terminate a started generated app before a new build touches its workspace."""
    proc = state.get("process")
    if proc is not None and proc.poll() is None:
        _stop_app_process(proc)
    state["process"] = None

from ai_assistant.app_builder.engine import SERVICE_TEMPLATES
from ai_assistant.app_builder.spec import KNOWN_FEATURES


def _root(owner: Any) -> tk.Misc:
    return getattr(owner, "root", None) or tk._get_default_root()


def _run_bg(owner: Any, work, on_done) -> None:
    """Run *work* off-thread; deliver result to *on_done* on the UI thread."""
    root = _root(owner)

    def runner():
        try:
            result = work()
            err = None
        except Exception as exc:  # noqa: BLE001
            result, err = None, str(exc)
        root.after(0, lambda: on_done(result, err))

    threading.Thread(target=runner, daemon=True).start()


def _introspect_live_schema(db_manager: Any, *, max_tables: int = 60) -> dict[str, list[str]]:
    """Introspect ``{table: [columns]}`` from an already-connected db manager.

    Uses the live connection the user is working with (not a fresh, possibly
    disconnected service), so ``from_database`` builds reflect the real schema.
    Runs DB I/O — call it from a worker thread.
    """
    from common.database_registry import DatabaseRegistry

    from ai_assistant.app_builder.service import _column_names

    db_type = getattr(db_manager, "db_type", "") or ""
    conn = getattr(db_manager, "conn", None)
    if not db_type or conn is None:
        return {}
    try:
        tables = DatabaseRegistry.execute_operation(db_type, "getTables", conn) or []
    except Exception:
        return {}
    out: dict[str, list[str]] = {}
    for row in list(tables)[:max_tables]:
        if isinstance(row, dict):  # error sentinel from some backends
            continue
        tname = row[0] if isinstance(row, (list, tuple)) and row else row
        tname = str(tname).strip()
        if not tname:
            continue
        cols: list[str] = []
        try:
            raw = DatabaseRegistry.execute_operation(
                db_type, "getTableSchema", conn, tname) or []
            cols = _column_names(raw)
        except Exception:
            cols = []
        out[tname.split(".")[-1]] = cols or ["id"]
    return out


class _ActiveConnectionCore:
    """CoreDBService adapter that can use Tk's already-open DB managers.

    The normal App Builder service is headless and resolves connections from
    saved profiles. In the Tk workspace, users can have an active connection
    that is not resolvable by a fresh ``CoreDBService`` instance. Manual Train
    LLM and RAG indexing must use that live manager instead of failing with
    "Connection '<name>' not found."
    """

    def __init__(self, owner: Any) -> None:
        from common.headless.db_service import CoreDBService

        self._owner = owner
        self._base = CoreDBService()

    def _active_manager(self, name: str) -> Any:
        live = getattr(self._owner, "active_connections", {}) or {}
        return live.get(name)

    def get_manager(self, name: str, profile: dict | None = None):
        mgr = self._active_manager(name)
        if mgr is not None and getattr(mgr, "conn", None) is not None:
            return mgr
        return self._base.get_manager(name, profile)

    def get_connection_profile(self, name: str) -> dict | None:
        profile = None
        try:
            profile = self._base.get_connection_profile(name)
        except Exception:
            profile = None
        mgr = self._active_manager(name)
        if mgr is None:
            return profile
        out = dict(profile or {})
        out.setdefault("name", name)
        out["db_type"] = getattr(mgr, "db_type", out.get("db_type", ""))
        return out

    def execute(self, name: str, sql: str) -> dict:
        mgr = self._active_manager(name)
        if mgr is None or getattr(mgr, "conn", None) is None:
            return self._base.execute(name, sql)
        try:
            raw, error = mgr.execute_query(sql)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "columns": [], "rows": [], "rowcount": 0,
                    "time_ms": 0, "message": None}
        if error:
            return {"error": error, "columns": [], "rows": [], "rowcount": 0,
                    "time_ms": 0, "message": None}
        raw = raw or {}
        rows = [
            [str(v) if v is not None else "" for v in row]
            for row in (raw.get("rows") or [])
        ]
        return {
            "error": None,
            "columns": raw.get("columns") or [],
            "rows": rows,
            "rowcount": raw.get("rowcount", len(rows)),
            "time_ms": round(raw.get("time", 0) * 1000, 1),
            "message": f"{raw.get('rowcount', len(rows))} row(s) returned.",
            "truncated": bool(raw.get("truncated", False)),
            "max_rows": raw.get("max_rows"),
        }

    def get_objects(self, name: str, obj_type: str = "tables") -> list:
        mgr = self._active_manager(name)
        if mgr is None or getattr(mgr, "conn", None) is None:
            return self._base.get_objects(name, obj_type)
        from common.database_registry import DatabaseRegistry

        op_map = {
            "tables": "getTables",
            "views": "getViews",
            "indexes": "getIndexes",
            "constraints": "getConstraints",
            "procedures": "getProcedures",
            "procs": "getProcedures",
            "functions": "getFunctions",
            "triggers": "getTriggers",
            "sequences": "getSequences",
        }
        key = obj_type.lower().replace(" ", "").replace("_", "")
        op = op_map.get(key)
        if not op:
            return self._base.get_objects(name, obj_type)
        db_type = getattr(mgr, "db_type", "")
        if db_type == "SQLite" and key in {"tables", "views", "indexes"}:
            kind = {"tables": "table", "views": "view", "indexes": "index"}[key]
            try:
                cur = mgr.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = ? "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY name",
                    (kind,),
                )
                return [str(r[0]) for r in cur.fetchall()]
            except Exception as exc:  # noqa: BLE001
                return [{"error": str(exc)}]
        try:
            result = DatabaseRegistry.execute_operation(db_type, op, mgr.conn) or []
        except Exception as exc:  # noqa: BLE001
            return [{"error": str(exc)}]
        flat: list = []
        for row in result:
            flat.append(row[0] if isinstance(row, (list, tuple)) and len(row) == 1 else row)
        return flat

    def get_table_schema(self, name: str, table: str) -> dict:
        mgr = self._active_manager(name)
        if mgr is None or getattr(mgr, "conn", None) is None:
            return self._base.get_table_schema(name, table)
        from common.database_registry import DatabaseRegistry

        db_type = getattr(mgr, "db_type", "")
        if db_type == "SQLite":
            try:
                cur = mgr.conn.execute(f'PRAGMA table_info("{table}")')
                cols = [(r[1], r[2]) for r in cur.fetchall()]
                return {"error": None, "table": table, "columns": cols, "indexes": []}
            except Exception as exc:  # noqa: BLE001
                return {"error": str(exc), "table": table, "columns": [], "indexes": []}
        try:
            raw = DatabaseRegistry.execute_operation(
                db_type, "getTableSchema", mgr.conn, table) or []
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "table": table, "columns": [], "indexes": []}
        return {"error": None, "table": table, "columns": raw, "indexes": []}

    def __getattr__(self, name: str) -> Any:
        return getattr(self._base, name)


def _open_in_file_browser(path: str) -> None:
    """Reveal *path* in the OS file browser (best-effort, cross-platform)."""
    import subprocess
    import sys

    if not path:
        return
    try:
        if sys.platform == "darwin":
            subprocess.Popen(["open", path])
        elif sys.platform.startswith("win"):
            import os
            os.startfile(path)  # type: ignore[attr-defined]  # noqa: S606
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception:
        pass


def _start_app_process(workspace: str, port: str):
    """Start a generated FastAPI app with uvicorn and return the process.

    Uses the same environment the build's import dry-run verified (local SQLite
    with seeded sample data, no ``DATABASE_URL``) so the prototype reliably
    boots and stays up. ``--reload`` is intentionally omitted: the reloader
    spawns a second process that dies silently on any import error, which is
    exactly the "crashes after build" symptom we are fixing.
    """
    import subprocess
    import sys
    from pathlib import Path

    from ai_assistant.app_builder import preflight

    safe_port = int(port or "8000")
    if safe_port <= 0 or safe_port > 65535:
        raise ValueError("Port must be between 1 and 65535.")
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "src.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(safe_port),
        ],
        cwd=workspace,
        env=preflight.launch_env(Path(workspace)),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )


def _preflight_app(workspace: str) -> tuple[bool, str]:
    """Compile/import/boot status before launch; never used as a launch blocker."""
    from pathlib import Path

    from ai_assistant.app_builder import preflight

    ws = Path(workspace)
    syntax = preflight.compile_check(ws)
    if syntax:
        return False, "syntax errors:\n  " + "\n  ".join(syntax[:8])
    ok, err = preflight.import_app_check(ws)
    if not ok:
        return False, err
    boot = preflight.boot_check(ws)
    if not boot.ok:
        return False, boot.digest()
    return True, ""


def _stop_app_process(proc: Any) -> None:
    """Stop a running generated-app process (best-effort)."""
    if proc is None:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
    except Exception:
        pass


def _interaction_dialog(owner: Any, decision: Any) -> Any:
    """Render an agent askQuestionInteractionQuery like the native agent UI.

    The user can pick from the agent's options, type a free-form answer, edit
    Session B's recommendation, send their answer to A, use B's suggestion, or
    skip. Returns the answer string, or ``'skip'``.
    """
    top = tk.Toplevel(_root(owner))
    top.title("App Builder — agent question")
    top.transient(_root(owner))
    top.geometry("620x520")
    top.minsize(480, 360)
    top.resizable(True, True)
    top.grab_set()

    chosen: dict[str, Any] = {"value": "skip"}
    recommendation = str(
        getattr(decision, "recommendation", None) or decision.detail or ""
    ).strip()
    agent_options: list[dict[str, str]] = list(
        getattr(decision, "agent_options", None) or []
    )
    allow_multiple = bool(getattr(decision, "allow_multiple", False))

    def _finish(value: Any) -> None:
        chosen["value"] = value
        top.destroy()

    btn_row = ttk.Frame(top, padding=10)
    btn_row.pack(side=tk.BOTTOM, fill=tk.X)
    ttk.Button(
        btn_row, text="Send to A",
        command=lambda: _finish(_compose_answer()),
    ).pack(side=tk.LEFT, padx=4)
    ttk.Button(
        btn_row, text="Use B's suggestion",
        command=lambda: _finish(_rec_text()),
    ).pack(side=tk.LEFT, padx=4)
    ttk.Button(btn_row, text="Skip", command=lambda: _finish("skip")).pack(
        side=tk.LEFT, padx=4)

    body = _make_scrollable(top)
    body.configure(padding=8)

    qbox = scrolledtext.ScrolledText(body, wrap=tk.WORD, height=5, padx=6, pady=6)
    qbox.pack(fill=tk.X)
    qbox.insert(tk.END, decision.question or "")
    qbox.config(state=tk.DISABLED)

    opt_frame = ttk.LabelFrame(body, text="Options", padding=6)
    opt_vars: list[tuple[str, tk.BooleanVar]] = []
    radio_var = tk.StringVar(value="")

    if agent_options:
        opt_frame.pack(fill=tk.X, pady=(8, 0))
        if allow_multiple:
            for item in agent_options:
                oid = str(item.get("id") or "").strip()
                label = str(item.get("label") or oid or "option").strip()
                var = tk.BooleanVar(value=False)
                opt_vars.append((oid or label, var))
                ttk.Checkbutton(
                    opt_frame, text=label, variable=var,
                ).pack(anchor=tk.W, pady=1)
        else:
            for item in agent_options:
                oid = str(item.get("id") or "").strip()
                label = str(item.get("label") or oid or "option").strip()
                ttk.Radiobutton(
                    opt_frame, text=label, variable=radio_var, value=oid or label,
                ).pack(anchor=tk.W, pady=1)

    free_row = ttk.Frame(body)
    free_row.pack(fill=tk.X, pady=(8, 0))
    ttk.Label(free_row, text="Your answer:").pack(side=tk.LEFT)
    free_var = tk.StringVar()
    ttk.Entry(free_row, textvariable=free_var).pack(
        side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 0))

    rec_lf = ttk.LabelFrame(body, text="Session B recommendation (editable)", padding=4)
    rec_lf.pack(fill=tk.BOTH, expand=True, pady=(8, 0))
    rec_box = scrolledtext.ScrolledText(rec_lf, wrap=tk.WORD, height=6, padx=6, pady=6)
    rec_box.pack(fill=tk.BOTH, expand=True)
    if recommendation:
        rec_box.insert(tk.END, recommendation)

    def _rec_text() -> str:
        return rec_box.get("1.0", tk.END).strip() or recommendation

    def _compose_answer() -> str:
        parts: list[str] = []
        if allow_multiple:
            for oid, var in opt_vars:
                if var.get():
                    parts.append(oid)
        elif radio_var.get():
            parts.append(radio_var.get())
        free = free_var.get().strip()
        if free:
            parts.append(free)
        if parts:
            return "\n".join(parts)
        return _rec_text()

    top.protocol("WM_DELETE_WINDOW", lambda: _finish("skip"))
    top.wait_window()
    return chosen["value"]


def _decision_dialog(owner: Any, decision: Any, options: list) -> Any:
    """Modal decision dialog: scrollable question/detail, pinned option buttons.

    The agent's question and proposed answer (``decision.detail``) can be long,
    so they go into a read-only scrolling text area that expands, while the
    option buttons stay pinned at the bottom and the window is resizable.
    """
    top = tk.Toplevel(_root(owner))
    top.title("App Builder — decision")
    top.transient(_root(owner))
    top.geometry("560x420")
    top.minsize(420, 260)
    top.resizable(True, True)
    top.grab_set()

    chosen = {"value": decision.default}

    def pick(opt):
        chosen["value"] = opt
        top.destroy()

    # Pinned button bar at the bottom (packed first with side=BOTTOM so it is
    # never pushed off-screen by long content).
    row = ttk.Frame(top, padding=10)
    row.pack(side=tk.BOTTOM, fill=tk.X)
    for opt in (options or ["apply", "skip", "stop"]):
        ttk.Button(row, text=str(opt).replace("_", " ").title(),
                   command=lambda o=opt: pick(o)).pack(side=tk.LEFT, padx=4)

    body = scrolledtext.ScrolledText(top, wrap=tk.WORD, height=10, padx=8, pady=8)
    body.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
    body.insert(tk.END, decision.question or "")
    if decision.detail:
        body.insert(tk.END, "\n\nProposed:\n" + str(decision.detail))
    body.config(state=tk.DISABLED)

    top.protocol("WM_DELETE_WINDOW", lambda: pick(decision.default))
    top.wait_window()
    return chosen["value"]


def _choice_dialog(owner: Any, decision: Any) -> Any:
    """Scrollable modal offering ``decision.options`` as buttons."""
    return _decision_dialog(
        owner, decision, decision.options or ["apply", "skip", "stop"])


def _ask_decision(owner: Any, decision: Any) -> Any:
    """Ask the user a build decision on the UI thread; block the worker thread.

    The orchestrator runs on a worker thread, so we marshal the dialog onto the
    Tk main thread via ``after`` and wait for the answer on a queue. Returns the
    decision default if anything goes wrong (so a build never hangs on the UI).
    """
    import queue as _queue

    root = _root(owner)
    answer_q: "_queue.Queue[Any]" = _queue.Queue(maxsize=1)

    def show():
        try:
            if decision.kind == "interaction":
                ans = _interaction_dialog(owner, decision)
            elif decision.kind == "choice":
                ans = _choice_dialog(owner, decision)
            else:
                # Yes/No, but rendered in the same scrollable dialog so long
                # questions/details stay readable (plain messagebox clips them).
                ans = _decision_dialog(owner, decision, ["yes", "no"]) == "yes"
        except Exception:  # noqa: BLE001
            ans = decision.default
        answer_q.put(ans)

    root.after(0, show)
    try:
        return answer_q.get(timeout=600)
    except Exception:  # noqa: BLE001
        return decision.default


def _make_scrollable(parent: tk.Misc) -> ttk.Frame:
    """Return a both-axis scrollable inner frame filling *parent*."""
    return make_scrollable(parent)


def _make_hscrollable(parent: tk.Misc) -> ttk.Frame:
    """Return a horizontally scrollable inner frame filling *parent*'s width.

    Used for the action button bar so no button is ever clipped/hidden on a
    narrow window. The horizontal scrollbar is always present (so the row is
    obviously scrollable), the canvas keeps a stable height matching the
    buttons, and Shift+MouseWheel scrolls the row sideways.
    """
    # The scrollbar is packed FIRST at the bottom so it is always reserved and
    # visible; the canvas fills the remaining width above it.
    hsb = ttk.Scrollbar(parent, orient="horizontal")
    hsb.pack(side=tk.BOTTOM, fill=tk.X)
    canvas = tk.Canvas(parent, highlightthickness=0, height=36)
    canvas.pack(side=tk.TOP, fill=tk.X, expand=True)
    canvas.configure(xscrollcommand=hsb.set)
    hsb.configure(command=canvas.xview)

    inner = ttk.Frame(canvas)
    canvas.create_window((0, 0), window=inner, anchor="nw")

    def _sync(_event=None):
        try:
            canvas.configure(scrollregion=canvas.bbox("all"))
            req_h = inner.winfo_reqheight()
            if req_h > 1:
                canvas.configure(height=req_h)
        except tk.TclError:
            return

    def _on_shift_wheel(event):
        if event.num == 5 or event.delta < 0:
            canvas.xview_scroll(1, "units")
        elif event.num == 4 or event.delta > 0:
            canvas.xview_scroll(-1, "units")

    def _bind_wheel(_event=None):
        canvas.bind_all("<Shift-MouseWheel>", _on_shift_wheel)

    def _unbind_wheel(_event=None):
        canvas.unbind_all("<Shift-MouseWheel>")

    inner.bind("<Configure>", _sync)
    canvas.bind("<Configure>", _sync)
    canvas.bind("<Enter>", _bind_wheel)
    canvas.bind("<Leave>", _unbind_wheel)
    inner.bind("<Destroy>", _unbind_wheel)
    canvas.after_idle(_sync)
    return inner


# ── App Builder ─────────────────────────────────────────────────────────────-
def _populate_app_builder(owner: Any, main: tk.Misc) -> None:
    """Build the App Builder UI into *main* (a frame/tab/container)."""
    from ai_assistant.app_builder.service import make_service

    svc = make_service(_ActiveConnectionCore(owner))

    ttk.Label(main, text="App Builder", font=("Arial", 14, "bold")).pack(anchor=tk.W)
    ttk.Label(
        main,
        text="Build an app from scratch, an existing codebase, or a database. "
        "Every build is validated by the AiAppEngine (code/meters), not prompts alone.",
        foreground="gray", wraplength=740, justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(0, 8))

    # Pin the action buttons to a bottom bar that's always visible, and put the
    # (tall) form into a scrollable body so every field stays reachable. The bar
    # scrolls horizontally so no button is hidden when the window is narrow.
    btn_bar = ttk.Frame(main)
    btn_bar.pack(side=tk.BOTTOM, anchor=tk.W, fill=tk.X, pady=(6, 0))
    btn_row = _make_hscrollable(btn_bar)

    # Middle is a draggable vertical split: the (scrollable) config form on top,
    # and the live status + the two agent-session panels below — the lower half
    # gets the larger share and lives OUTSIDE the scroll canvas so its own
    # scrollbars and mouse wheel work and it can be enlarged/dragged.
    middle = ttk.PanedWindow(main, orient=tk.VERTICAL)
    middle.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
    form_area = ttk.Frame(middle)
    middle.add(form_area, weight=1)
    lower = ttk.Frame(middle)
    middle.add(lower, weight=2)
    content = _make_scrollable(form_area)

    form = ttk.Frame(content)
    form.pack(fill=tk.X)

    ttk.Label(form, text="App name:").grid(row=0, column=0, sticky=tk.W, pady=2)
    name_var = tk.StringVar(value=_default_app_name())
    ttk.Entry(form, textvariable=name_var, width=30).grid(row=0, column=1, sticky=tk.W, padx=5)

    ttk.Label(form, text="Mode:").grid(row=0, column=2, sticky=tk.W, padx=(12, 0))
    mode_var = tk.StringVar(value="from_scratch")
    ttk.Combobox(
        form, textvariable=mode_var, width=16, state="readonly",
        values=["from_scratch", "from_database", "from_codebase"],
    ).grid(row=0, column=3, sticky=tk.W, padx=5)

    ttk.Label(form, text="Connection:").grid(row=1, column=0, sticky=tk.W, pady=2)
    conn_var = tk.StringVar()
    ttk.Combobox(form, textvariable=conn_var, width=27,
                 values=_connection_names(owner)).grid(
        row=1, column=1, sticky=tk.W, padx=5)

    ttk.Label(form, text="Codebase path:").grid(row=1, column=2, sticky=tk.W, padx=(12, 0))
    cb_var = tk.StringVar()
    cbrow = ttk.Frame(form)
    cbrow.grid(row=1, column=3, sticky=tk.W, padx=5)
    ttk.Entry(cbrow, textvariable=cb_var, width=18).pack(side=tk.LEFT)
    ttk.Button(cbrow, text="…", width=3,
               command=lambda: cb_var.set(filedialog.askdirectory() or cb_var.get())
               ).pack(side=tk.LEFT)

    ttk.Label(form, text="DB variant:").grid(row=2, column=0, sticky=tk.W, pady=2)
    db_variant_var = tk.StringVar(value="application")
    ttk.Combobox(
        form, textvariable=db_variant_var, width=27, state="readonly",
        values=["application", "insights_admin"],
    ).grid(row=2, column=1, sticky=tk.W, padx=5)

    ttk.Label(form, text="Codebase variant:").grid(row=2, column=2, sticky=tk.W, padx=(12, 0))
    codebase_variant_var = tk.StringVar(value="predicted_app")
    ttk.Combobox(
        form, textvariable=codebase_variant_var, width=18, state="readonly",
        values=["predicted_app", "structure_metadata"],
    ).grid(row=2, column=3, sticky=tk.W, padx=5)

    ttk.Label(form, text="Variant:").grid(row=3, column=0, sticky=tk.W, pady=2)
    variant_var = tk.StringVar(value="application")
    ttk.Combobox(
        form, textvariable=variant_var, width=18, state="readonly",
        values=["application", "explorer"],
    ).grid(row=3, column=1, sticky=tk.W, padx=5)

    ttk.Label(form, text="Build profile:").grid(
        row=3, column=2, sticky=tk.W, padx=(12, 0))
    build_profile_var = tk.StringVar(value="prototype")
    ttk.Combobox(
        form, textvariable=build_profile_var, width=18, state="readonly",
        values=["prototype", "full"],
    ).grid(row=3, column=3, sticky=tk.W, padx=5)

    # Step 1 — describe the app in plain language, then Analyze to draft a spec.
    _DESC_EXAMPLE = (
        "e.g. An app to manage customers and their orders, with the ability to "
        "add, edit and delete records."
    )
    desc_frame = ttk.LabelFrame(content, text="1. Describe the app", padding=6)
    desc_frame.pack(fill=tk.X, pady=(8, 4))
    desc_hint = ttk.Label(desc_frame, foreground="gray", wraplength=700,
                          justify=tk.LEFT, text="")
    desc_hint.pack(anchor=tk.W, pady=(0, 4))
    desc_text = tk.Text(desc_frame, height=3, wrap=tk.WORD)
    desc_text.pack(fill=tk.X)
    desc_text.insert("1.0", _DESC_EXAMPLE)
    analyze_row = ttk.Frame(desc_frame)
    analyze_row.pack(fill=tk.X, pady=(4, 0))
    ttk.Button(analyze_row, text="Analyze requirements",
               command=lambda: analyze()).pack(side=tk.LEFT)
    ttk.Label(analyze_row, foreground="gray",
              text="Drafts entities, features and infra from your description "
                   "(and the live schema for from_database).").pack(side=tk.LEFT, padx=8)

    def _update_desc_hint(*_args):
        """Mode-aware guidance for the description field.

        For 'build using database' the description is OPTIONAL — the app is built
        from the *intent of the data*. We clear the from-scratch example so the
        user can leave it empty when they don't know the app type.
        """
        current = desc_text.get("1.0", tk.END).strip()
        if mode_var.get() == "from_database":
            desc_hint.config(
                text="Optional: the app is built from the INTENT of the data in "
                     "the database (what kind of app would use this data), NOT a "
                     "copy of the table schema. Describe the app only if you know "
                     "what kind of app uses this database (a hint we'll match "
                     "against the data); if you don't, leave this empty.")
            if current == _DESC_EXAMPLE:
                desc_text.delete("1.0", tk.END)  # don't bias the data-driven build
        else:
            desc_hint.config(
                text="Describe the application you want; the builder designs and "
                     "builds it (not a table-per-word scaffold).")
            if not current:
                desc_text.insert("1.0", _DESC_EXAMPLE)

    mode_var.trace_add("write", _update_desc_hint)
    _update_desc_hint()

    # Step 2 — review/adjust the drafted requirements before building.
    review = ttk.LabelFrame(content, text="2. Review requirements", padding=6)
    review.pack(fill=tk.X, pady=(4, 4))

    ttk.Label(review, text="Entities (comma-separated):").grid(
        row=0, column=0, sticky=tk.W, pady=2)
    entities_var = tk.StringVar()
    ttk.Entry(review, textvariable=entities_var, width=58).grid(
        row=0, column=1, sticky=tk.W + tk.E, padx=5)
    ttk.Label(review, foreground="gray",
              text="(ignored for from_database — tables come from the connection)"
              ).grid(row=1, column=1, sticky=tk.W, padx=5)

    feat_frame = ttk.Frame(review)
    feat_frame.grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=(4, 0))
    ttk.Label(feat_frame, text="Features:").pack(side=tk.LEFT)
    feat_vars: dict[str, tk.BooleanVar] = {}
    for feat in KNOWN_FEATURES:
        fv = tk.BooleanVar(value=True)
        feat_vars[feat] = fv
        ttk.Checkbutton(feat_frame, text=feat, variable=fv).pack(side=tk.LEFT, padx=4)

    svc_frame = ttk.LabelFrame(content, text="Centralized infra services", padding=6)
    svc_frame.pack(fill=tk.X, pady=(4, 6))
    svc_vars: dict[str, tk.BooleanVar] = {}
    defaults = {"ci_cd", "document", "hosting", "database", "monitoring"}
    for i, s in enumerate(SERVICE_TEMPLATES):
        v = tk.BooleanVar(value=s in defaults)
        svc_vars[s] = v
        ttk.Checkbutton(svc_frame, text=s, variable=v).grid(
            row=i // 4, column=i % 4, sticky=tk.W, padx=6, pady=2)

    use_ai = tk.BooleanVar(value=False)
    ai_opts = ttk.Frame(content)
    ai_opts.pack(fill=tk.X, pady=(2, 0))
    ttk.Checkbutton(ai_opts, text="Ask AI backend to generate files (engine-validated)",
                    variable=use_ai).pack(side=tk.LEFT)
    pii_default = bool(svc.get_pii_masking().get("enabled", True))
    mask_pii_var = tk.BooleanVar(value=pii_default)
    ttk.Checkbutton(ai_opts, text="Mask PII data", variable=mask_pii_var).pack(
        side=tk.LEFT, padx=(12, 0))

    llm_info = svc.llm_models()
    llm_models = [m.get("name", "") for m in (llm_info.get("models") or []) if m.get("name")]
    llm_engines = [e.get("name", "") for e in (llm_info.get("engines") or []) if e.get("name")]
    train_frame = ttk.LabelFrame(content, text="Train LLM", padding=4)
    train_frame.pack(fill=tk.X, pady=(4, 4))
    train_list = tk.Listbox(train_frame, selectmode=tk.MULTIPLE, height=min(4, max(2, len(llm_models) or 1)),
                            exportselection=False, width=28)
    train_list.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 8))
    for name in llm_models:
        train_list.insert(tk.END, name)
    train_side = ttk.Frame(train_frame)
    train_side.pack(side=tk.LEFT, fill=tk.X, expand=True)
    ttk.Label(train_side, text="New model name:").pack(anchor=tk.W)
    train_new_var = tk.StringVar(value="")
    ttk.Entry(train_side, textvariable=train_new_var, width=24).pack(anchor=tk.W, pady=(0, 4))
    ttk.Label(train_side, text="Engine:").pack(anchor=tk.W)
    train_engine_var = tk.StringVar(value=llm_engines[0] if llm_engines else "python")
    ttk.Combobox(train_side, textvariable=train_engine_var, values=llm_engines or ["python"],
                 width=22, state="readonly").pack(anchor=tk.W)
    use_rag_var = tk.BooleanVar(value=False)
    index_rag_var = tk.BooleanVar(value=False)
    rag_strategy_var = tk.StringVar(value="index_first")
    rag_opts = ttk.Frame(train_side)
    rag_opts.pack(anchor=tk.W, pady=(4, 0))
    ttk.Checkbutton(rag_opts, text="Use RAG", variable=use_rag_var).pack(side=tk.LEFT)
    ttk.Checkbutton(rag_opts, text="Index RAG", variable=index_rag_var).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Label(rag_opts, text="Strategy:").pack(side=tk.LEFT, padx=(8, 0))
    rag_strategy_cb = ttk.Combobox(
        rag_opts, textvariable=rag_strategy_var, values=["index_first", "parallel"],
        width=12, state="readonly")
    rag_strategy_cb.pack(side=tk.LEFT, padx=(4, 0))
    mine_opts = ttk.Frame(train_side)
    mine_opts.pack(anchor=tk.W, pady=(4, 0))
    mine_db_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(
        mine_opts, text="Mine DB queries (real data, validated)",
        variable=mine_db_var).pack(side=tk.LEFT)
    ttk.Label(mine_opts, text="Sample rows:").pack(side=tk.LEFT, padx=(8, 0))
    sample_limit_var = tk.StringVar(value="5")
    ttk.Entry(mine_opts, textvariable=sample_limit_var, width=5).pack(side=tk.LEFT, padx=(4, 0))

    rich_opts = ttk.Frame(train_side)
    rich_opts.pack(anchor=tk.W, pady=(4, 0))
    rich_train_var = tk.BooleanVar(value=False)
    ttk.Checkbutton(
        rich_opts,
        text="Train from this build's data (schema/queries/insight, execution-validated)",
        variable=rich_train_var).pack(side=tk.LEFT)

    def _toggle_rag_strategy(*_args):
        state = "readonly" if index_rag_var.get() else "disabled"
        rag_strategy_cb.configure(state=state)

    index_rag_var.trace_add("write", _toggle_rag_strategy)
    _toggle_rag_strategy()

    def _selected_train_models() -> list[str]:
        return [train_list.get(i) for i in train_list.curselection()]

    def _train_body_extra() -> dict:
        try:
            sample_limit = int(sample_limit_var.get().strip() or 5)
        except ValueError:
            sample_limit = 5
        return {
            "mask_pii": bool(mask_pii_var.get()),
            "train_llm": _selected_train_models(),
            "train_new_name": train_new_var.get().strip(),
            "train_engine": train_engine_var.get().strip(),
            "use_rag": bool(use_rag_var.get()),
            "index_rag": bool(index_rag_var.get()),
            "rag_strategy": rag_strategy_var.get().strip() or "index_first",
            "mine_db": bool(mine_db_var.get()),
            "train_sample_limit": sample_limit,
            "rich_train": bool(rich_train_var.get()),
        }

    # Build options: how much the agent asks, and (from_scratch) whether to
    # deploy the app's tables into the selected connection.
    opts = ttk.LabelFrame(content, text="Build options", padding=6)
    opts.pack(fill=tk.X, pady=(4, 4))
    row1 = ttk.Frame(opts)
    row1.pack(fill=tk.X)
    ttk.Label(row1, text="Interaction:").pack(side=tk.LEFT)
    interaction_var = tk.StringVar(value="auto")
    ttk.Combobox(row1, textvariable=interaction_var, width=14, state="readonly",
                 values=["uninterrupted", "auto", "interactive"]).pack(
        side=tk.LEFT, padx=(4, 8))
    ttk.Label(
        row1, foreground="gray",
        text="uninterrupted = never asks · auto = asks only critical · "
             "interactive = approve each decision",
    ).pack(side=tk.LEFT)

    row2 = ttk.Frame(opts)
    row2.pack(fill=tk.X, pady=(4, 0))
    deploy_var = tk.BooleanVar(value=False)
    deploy_chk = ttk.Checkbutton(
        row2, text="Deploy app tables to the selected connection (from scratch)",
        variable=deploy_var, state=tk.DISABLED)
    deploy_chk.pack(side=tk.LEFT)
    deploy_hint = ttk.Label(
        row2, foreground="gray",
        text="(off by default — only when building from scratch with a connection)")
    deploy_hint.pack(side=tk.LEFT, padx=8)

    row3 = ttk.Frame(opts)
    row3.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(row3, text="Validation depth:").pack(side=tk.LEFT)
    validation_depth_var = tk.StringVar(value="low_token")
    ttk.Combobox(
        row3, textvariable=validation_depth_var, width=14, state="readonly",
        values=["low_token", "thorough"],
    ).pack(side=tk.LEFT, padx=(4, 8))
    ttk.Label(
        row3, foreground="gray",
        text="low-token = validate on file changes + PHASE-DONE · "
             "thorough = one phase at a time (api→db→web→tests)",
    ).pack(side=tk.LEFT)

    def _update_deploy_state(*_args):
        """Enable the deploy checkbox only for from_scratch + a chosen connection."""
        eligible = (mode_var.get() == "from_scratch"
                    and bool(conn_var.get().strip()))
        if eligible:
            deploy_chk.config(state=tk.NORMAL)
        else:
            deploy_var.set(False)
            deploy_chk.config(state=tk.DISABLED)

    mode_var.trace_add("write", _update_deploy_state)
    conn_var.trace_add("write", _update_deploy_state)
    _update_deploy_state()

    def _build_decider():
        """Construct a BuildDecider from the interaction setting (UI dialog-backed)."""
        from ai_assistant.app_builder.interaction import decider_from_options

        level = interaction_var.get()
        ask = (lambda d: _ask_decision(owner, d)) if level == "interactive" else None
        return decider_from_options(
            interaction=level, uninterrupted=(level == "uninterrupted"), ask=ask)

    def _description() -> str:
        return desc_text.get("1.0", tk.END).strip()

    def analyze():
        """Draft entities/features/services from the prompt (and live schema)."""
        from ai_assistant.app_builder.requirements import derive_spec

        desc = _description()
        schema: dict = {}
        if mode_var.get() == "from_database" and conn_var.get().strip():
            live = getattr(owner, "active_connections", {}) or {}
            dbm = live.get(conn_var.get().strip())
            if dbm is not None:
                schema = _introspect_live_schema(dbm)
        spec = derive_spec(
            app_name=name_var.get().strip() or _default_app_name(),
            description=desc, schema=schema or None,
            services=[s for s, v in svc_vars.items() if v.get()],
        )
        if not schema:
            entities_var.set(", ".join(e.name for e in spec.entities))
        else:
            entities_var.set(", ".join(sorted(schema.keys())) + "  (from connection)")
        for feat, fv in feat_vars.items():
            fv.set(feat in spec.features)
        kind_label = {
            "storefront": "ecommerce storefront (catalog + cart + checkout)",
        }.get(spec.kind, "CRUD / management app")
        results.delete(1.0, tk.END)
        results.insert(
            tk.END,
            "Drafted requirements — review above, then Build.\n"
            f"  app type: {kind_label}\n"
            f"  entities: {', '.join(e.name for e in spec.entities)}\n"
            f"  features: {', '.join(spec.features)}\n"
            f"  services: {', '.join(spec.services) or 'none'}\n"
            + (f"  schema tables: {len(schema)} from '{conn_var.get().strip()}'\n"
               if schema else "")
            + ("  → builds a real customer-facing store using this data; "
               "the catalog is seeded with sample products.\n"
               if spec.kind == "storefront" else ""),
        )

    runtime_frame = ttk.Frame(content)
    runtime_frame.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(runtime_frame, text="Run port:").pack(side=tk.LEFT)
    port_var = tk.StringVar(value="8000")
    ttk.Entry(runtime_frame, textvariable=port_var, width=8).pack(side=tk.LEFT, padx=(4, 10))

    # ── live status + dual agent sessions (in the lower split, OUTSIDE the
    # scroll canvas so their scrollbars/mouse-wheel work and they enlarge) ─────
    sessions_lf = ttk.LabelFrame(
        lower, text="Agent build sessions (App Builder only)", padding=6)
    sessions_lf.pack(fill=tk.BOTH, expand=True)
    ttk.Label(
        sessions_lf, foreground="gray", wraplength=760, justify=tk.LEFT,
        text="Live backend transcript — builder (Session A) writes & runs in the "
             "workspace; answerer (Session B) frames answers in ask mode; "
             "validator (Session C) checks completeness and reports issues via B. "
             "Every commit is validated by meters/managers. Does not use the AI "
             "Query Assistant chat button.",
    ).pack(anchor=tk.W, pady=(0, 4))

    # Resizable session panels: A on top (full width), B | C below.
    sessions_pane = ttk.PanedWindow(sessions_lf, orient=tk.VERTICAL)
    sessions_pane.pack(fill=tk.BOTH, expand=True)

    builder_lf = ttk.LabelFrame(
        sessions_pane, text="Session A — builder (writes & runs)")
    sessions_pane.add(builder_lf, weight=2)
    builder_box = scrolledtext.ScrolledText(builder_lf, wrap=tk.WORD, height=12)
    builder_box.pack(fill=tk.BOTH, expand=True)
    builder_box.config(state=tk.DISABLED)

    bc_pane = ttk.PanedWindow(sessions_pane, orient=tk.HORIZONTAL)
    sessions_pane.add(bc_pane, weight=1)
    answerer_lf = ttk.LabelFrame(
        bc_pane, text="Session B — answerer (ask mode)")
    bc_pane.add(answerer_lf, weight=1)
    answerer_box = scrolledtext.ScrolledText(answerer_lf, wrap=tk.WORD, height=10)
    answerer_box.pack(fill=tk.BOTH, expand=True)
    answerer_box.config(state=tk.DISABLED)
    validator_lf = ttk.LabelFrame(
        bc_pane, text="Session C — validator (tests & completeness)")
    bc_pane.add(validator_lf, weight=1)
    validator_box = scrolledtext.ScrolledText(validator_lf, wrap=tk.WORD, height=10)
    validator_box.pack(fill=tk.BOTH, expand=True)
    validator_box.config(state=tk.DISABLED)

    answer_row = ttk.Frame(sessions_lf)
    answer_row.pack(fill=tk.X, pady=(4, 0))
    ttk.Label(answer_row, text="Message to:").pack(side=tk.LEFT)
    # "auto (B→A)" frames the message through Session B before A; "builder"
    # talks to Session A DIRECTLY; answerer/validator chat with B/C directly.
    session_target_var = tk.StringVar(value="auto (B→A)")
    session_target = ttk.Combobox(
        answer_row, textvariable=session_target_var, width=12, state="readonly",
        values=["auto (B→A)", "builder", "answerer", "validator"])
    session_target.pack(side=tk.LEFT, padx=(2, 6))
    agent_answer_var = tk.StringVar()
    agent_answer_entry = ttk.Entry(answer_row, textvariable=agent_answer_var, width=40)
    agent_answer_entry.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)
    agent_send_btn = ttk.Button(answer_row, text="Send", state=tk.DISABLED)
    agent_send_btn.pack(side=tk.LEFT)

    def _send_agent_reply() -> None:
        text = agent_answer_var.get().strip()
        if not text:
            return
        target = session_target_var.get() or "auto (B→A)"
        coord = state.get("coord") or getattr(svc, "last_coordinator", None)
        interactive = interaction_var.get() == "interactive"
        is_auto = target.startswith("auto")

        # "auto (B→A)": interactive sends immediately; auto/agent queues for A.
        if coord is not None and is_auto:
            _append_agent(f"[user→B→A] {text}\n", session="answerer",
                          tag="answerer")
            agent_answer_var.set("")
            agent_send_btn.config(state=tk.DISABLED)
            building = bool(state.get("build_running"))

            if interactive or not building:
                def _route(c=coord, t=text, ia=interactive):
                    return c.route_user_request(t, interactive=ia)

                def _route_done(r, e):
                    try:
                        if agent_send_btn.winfo_exists():
                            agent_send_btn.config(state=tk.NORMAL)
                    except tk.TclError:
                        pass
                    if e:
                        _append_agent(f"[error] {e}\n", tag="gate_fail")
                    elif r:
                        _append_agent(f"[B→A] {r}\n", session="answerer",
                                      tag="answerer")

                _run_bg(owner, _route, _route_done)
            else:
                def _queue(c=coord, t=text):
                    return c.queue_user_message(t)

                def _queue_done(r, e):
                    try:
                        if agent_send_btn.winfo_exists():
                            agent_send_btn.config(state=tk.NORMAL)
                    except tk.TclError:
                        pass
                    if e:
                        _append_agent(f"[error] {e}\n", tag="gate_fail")
                    elif r:
                        _append_agent(
                            "[B→A queued] message will reach Session A on "
                            "its next free turn.\n",
                            session="answerer", tag="answerer")

                _run_bg(owner, _queue, _queue_done)
            return

        # Explicit direct chat to a specific session (builder/answerer/validator).
        if coord is not None and target in ("builder", "answerer", "validator"):
            sess = {"builder": getattr(coord, "builder", None),
                    "answerer": getattr(coord, "answerer", None),
                    "validator": getattr(coord, "validator", None)}.get(target)
            if sess is None:
                _append_agent(f"[system] session {target} is not available.\n",
                              tag="gate_fail")
                return
            # Direct-to-A is sent verbatim (the user is steering A themselves).
            # B/C get a short status preface so they answer with current context.
            payload = text
            if target != "builder" and hasattr(coord, "status_preface"):
                preface = coord.status_preface()
                if preface:
                    payload = f"{preface}\n\nUSER: {text}"
            label = "A" if target == "builder" else target
            _append_agent(f"[user→{label}] {text}\n", session=target, tag=target)
            agent_answer_var.set("")
            agent_send_btn.config(state=tk.DISABLED)

            def _chat(s=sess, t=payload):
                return s.send(t)

            def _chat_done(r, e):
                try:
                    if agent_send_btn.winfo_exists():
                        agent_send_btn.config(state=tk.NORMAL)
                except tk.TclError:
                    pass
                if e:
                    _append_agent(f"[error] {e}\n", tag="gate_fail")

            _run_bg(owner, _chat, _chat_done)
            return

        state["manual_answer"] = text
        _append_agent(f"[user] {text}\n", session="builder")
        agent_answer_var.set("")

    agent_send_btn.config(command=_send_agent_reply)

    # Enlarged build-status box (own scrollbar; lives in the lower split).
    status_lf = ttk.LabelFrame(lower, text="Build status", padding=4)
    status_lf.pack(fill=tk.BOTH, expand=True, pady=(6, 0))
    results = scrolledtext.ScrolledText(status_lf, wrap=tk.WORD, height=10)
    results.pack(fill=tk.BOTH, expand=True)

    state = {"workspace": "", "process": None, "pending_answer": None,
             "abuf": {"builder": "", "answerer": "", "validator": ""},
             "last_tool_line": "", "coord": None, "build_running": False,
             "closed": False, "stopped": False}

    def _widget_exists(widget: Any) -> bool:
        try:
            return bool(widget.winfo_exists())
        except tk.TclError:
            return False

    def _safe_config(widget: Any, **kwargs) -> None:
        try:
            if _widget_exists(widget):
                widget.config(**kwargs)
        except tk.TclError:
            pass

    def _append_result(text: str) -> None:
        # The build/app-monitor threads post here via ``after``; if the dialog
        # was closed the widget is gone, so guard against a destroyed widget.
        try:
            if state.get("closed") or not _widget_exists(results):
                return
            results.insert(tk.END, text)
            results.see(tk.END)
        except tk.TclError:
            pass

    def _append_build_status(r: dict) -> None:
        intro = r.get("introspection_status") or {}
        if intro:
            conn = intro.get("connection") or "(none)"
            if intro.get("ok"):
                _append_result(
                    f"  selected DB introspection: OK ({conn}, "
                    f"{intro.get('tables', 0)} table(s)); runtime=SQLite\n")
            else:
                _append_result(
                    f"  selected DB introspection: WARNING ({conn}) — "
                    f"{intro.get('error') or 'no schema loaded'}; runtime=SQLite\n")
        quality = r.get("quality") or {}
        meters = quality.get("meters") or {}
        db_meter_names = (
            "relationship_fidelity", "entity_role_fit", "data_semantics",
            "workflow_coverage", "prediction_grounding",
        )
        db_parts = []
        for name in db_meter_names:
            meter = meters.get(name) or {}
            if meter and (meter.get("evidence") or {}).get("applicable", True):
                db_parts.append(f"{name}={float(meter.get('score', 0.0)):.2f}")
        if db_parts:
            _append_result("  DB semantics meters: " + ", ".join(db_parts) + "\n")
        insight = r.get("insight") or {}
        for note in (insight.get("advisory_notes") or [])[:4]:
            _append_result(f"  DB advisory: {note}\n")
        boot = r.get("boot_check") or {}
        if boot:
            label = "healthy" if boot.get("ok") else "degraded"
            detail = boot.get("error") or (
                f"/health HTTP {boot.get('health_status')}"
                if boot.get("health_status") else "")
            _append_result(f"  boot/health: {label}" + (f" — {detail}" if detail else "") + "\n")
        smoke = r.get("http_smoke") or {}
        if smoke:
            if smoke.get("skipped"):
                _append_result(
                    f"  launch smoke: skipped ({smoke.get('skip_reason') or 'unavailable'})\n")
            else:
                _append_result(
                    "  launch smoke: "
                    + ("passed" if smoke.get("ok") else "failed")
                    + "\n")

    def _append_agent(text: str, *, session: str = "builder", tag: str = "") -> None:
        # Route to each session's own box; builder/tool/gate/system/user lines all
        # belong to the build narrative (Session A).
        box = {"answerer": answerer_box,
               "validator": validator_box}.get(session, builder_box)
        try:
            if state.get("closed") or not _widget_exists(box):
                return
            box.config(state=tk.NORMAL)
            if tag:
                box.insert(tk.END, text, tag)
            else:
                box.insert(tk.END, text)
            box.see(tk.END)
            box.config(state=tk.DISABLED)
        except tk.TclError:
            pass

    def _clear_agent() -> None:
        state["abuf"] = {"builder": "", "answerer": "", "validator": ""}
        state["last_tool_line"] = ""
        for box in (builder_box, answerer_box, validator_box):
            try:
                if state.get("closed") or not _widget_exists(box):
                    continue
                box.config(state=tk.NORMAL)
                box.delete(1.0, tk.END)
                box.config(state=tk.DISABLED)
            except tk.TclError:
                pass

    def _buffer_agent(text: str, key: str) -> None:
        # Accumulate streamed assistant text so token/word deltas form one
        # flowing message instead of being printed one fragment per line.
        buf = state["abuf"].get(key, "")
        if buf and not buf[-1].isspace() and text and not text[0].isspace():
            buf += " "
        buf += text
        state["abuf"][key] = buf
        # Safety net: if a long narration arrives with no structured event to
        # punctuate it, flush at a sentence/word boundary so Session A keeps
        # showing live status instead of staying blank for a long time.
        if len(buf) >= 600:
            _flush_agent_buffers()

    def _flush_agent_buffers() -> None:
        # Emit each session's accumulated message as a single prefixed block,
        # preserving the sentence/paragraph structure the agent produced.
        for key in ("builder", "answerer", "validator"):
            buf = (state["abuf"].get(key) or "").strip()
            state["abuf"][key] = ""
            if buf:
                _append_agent(f"[{key}] {buf}\n", session=key, tag=key)

    def _enable_post_build_chat(agentic: bool = False) -> None:
        # Once an agentic build finishes, auto-switch to INTERACTIVE and let the
        # user keep talking to the SAME persistent A/B/C sessions (no new sessions
        # opened). Non-agentic builds have no live sessions, so skip them.
        state["build_running"] = False
        coord = getattr(svc, "last_coordinator", None) if agentic else None
        if coord is None:
            return
        state["coord"] = coord
        try:
            interaction_var.set("interactive")
            if agent_send_btn.winfo_exists():
                agent_send_btn.config(state=tk.NORMAL)
            if agent_answer_entry.winfo_exists():
                agent_answer_entry.config(state=tk.NORMAL)
        except tk.TclError:
            return
        _append_agent(
            "[system] build finished — INTERACTIVE mode enabled. Pick a session "
            "(builder / answerer / validator) and Send to keep the conversation "
            "going with the same agents.\n", tag="gate_ok")

    for _box in (builder_box, answerer_box, validator_box):
        _box.tag_configure("builder", foreground="#1a5276")
        _box.tag_configure("answerer", foreground="#7d3c98")
        _box.tag_configure("validator", foreground="#b9770e")
        _box.tag_configure("gate_ok", foreground="#1e8449")
        _box.tag_configure("gate_fail", foreground="#c0392b")
        _box.tag_configure("tool", foreground="#566573")
        _box.tag_configure("system", foreground="#117864")

    def start_app():
        workspace = state["workspace"]
        if not workspace:
            messagebox.showinfo("App Builder", "Build the app first.")
            return
        proc = state.get("process")
        if proc is not None and proc.poll() is None:
            messagebox.showinfo("App Builder", "The generated app is already running.")
            return
        # Surface the real gate state, but do not block launch. App Builder keeps
        # a minimal fallback entrypoint so the user can always try opening it.
        ok, err = _preflight_app(workspace)
        if not ok:
            _append_result(
                "\nApp preflight/boot warning — attempting launch anyway:\n"
                + (err or "") + "\n"
            )
        try:
            proc = _start_app_process(workspace, port_var.get().strip())
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Start App", f"Could not start generated app: {exc}")
            return
        state["process"] = proc
        start_btn.config(state=tk.DISABLED)
        stop_btn.config(state=tk.NORMAL)
        _append_result(
            f"\nStarted app at http://127.0.0.1:{port_var.get().strip() or '8000'}\n"
            f"Command: python -m uvicorn src.app:app --host 127.0.0.1 "
            f"--port {port_var.get().strip() or '8000'}\n"
        )

        def monitor():
            assert proc.stdout is not None
            for line in proc.stdout:
                _root(owner).after(0, lambda line=line: _append_result("  " + line))
            code = proc.poll()
            def finish():
                try:
                    if state.get("process") is proc and start_btn.winfo_exists():
                        start_btn.config(
                            state=tk.NORMAL if state["workspace"] else tk.DISABLED)
                        stop_btn.config(state=tk.DISABLED)
                        _append_result(f"App process exited with code {code}.\n")
                except tk.TclError:
                    pass
            _root(owner).after(0, finish)

        threading.Thread(target=monitor, daemon=True).start()

    def stop_app():
        proc = state.get("process")
        if proc is None or proc.poll() is not None:
            stop_btn.config(state=tk.DISABLED)
            start_btn.config(state=tk.NORMAL if state["workspace"] else tk.DISABLED)
            return
        _stop_app_process(proc)
        _append_result("Stopping generated app...\n")

    def approve_and_package():
        """Approve the reviewed build and package it into a shippable bundle."""
        from pathlib import Path

        workspace = state.get("workspace")
        if not workspace:
            messagebox.showinfo("App Builder", "Build the app first.")
            return
        if not (Path(workspace) / "src" / "app.py").exists():
            messagebox.showinfo(
                "App Builder",
                "No runnable app (src/app.py) in the workspace yet — build first.")
            return
        port = (port_var.get().strip() or "8000")
        body = {
            "name": Path(workspace).name,
            "port": int(port) if port.isdigit() else 8000,
            "archive": True,
        }
        _safe_config(approve_btn, state=tk.DISABLED)
        _append_result("\nApproved — packaging app for shipping…\n")

        def work():
            try:
                r = svc.package_app(body)
            except Exception as exc:  # noqa: BLE001
                r = {"ok": False, "issues": [str(exc)]}

            def done():
                if state.get("closed"):
                    return
                for issue in r.get("issues", []):
                    _append_result(f"  packaging issue: {issue}\n")
                if r.get("ok"):
                    _append_result(
                        "  packaged: " + ", ".join(r.get("created", [])) + "\n")
                    if r.get("archive"):
                        _append_result(f"  shippable archive: {r['archive']}\n")
                    _append_result(
                        "  to install elsewhere: run ./install.sh (or "
                        "install.bat), then ./run.sh (or run.bat). See "
                        "INSTALL.md.\n")
                else:
                    _append_result("  packaging failed.\n")
                _safe_config(approve_btn, state=tk.NORMAL)

            _root(owner).after(0, done)

        threading.Thread(target=work, daemon=True).start()

    def _collect_body():
        """Validate inputs and assemble the build request, or return None."""
        conn_name = conn_var.get().strip()
        entities = [e.strip() for e in entities_var.get().replace(
            "(from connection)", "").split(",") if e.strip()]
        body = {
            "name": _ensure_build_name(name_var),
            "mode": mode_var.get(),
            "description": _description(),
            "services": [s for s, v in svc_vars.items() if v.get()],
            "features": [f for f, v in feat_vars.items() if v.get()],
            "entities": entities,
            "connections": [conn_name] if conn_name else [],
            "codebase_path": cb_var.get().strip(),
            "variant": variant_var.get(),
            "build_profile": build_profile_var.get(),
            "db_app_variant": db_variant_var.get(),
            "codebase_variant": codebase_variant_var.get(),
            "use_ai": bool(use_ai.get()),
            "validation_depth": validation_depth_var.get() or "low_token",
            **_train_body_extra(),
        }
        if body["mode"] == "from_database" and not conn_name:
            messagebox.showinfo("App Builder", "Select a database connection first.")
            return None
        if body["mode"] == "from_codebase" and not body["codebase_path"]:
            messagebox.showinfo("App Builder", "Choose a codebase path first.")
            return None
        return body

    def _maybe_live_schema(body):
        """Introspect the live, connected session for from_database builds."""
        conn_name = (body.get("connections") or [""])[0]
        if body["mode"] == "from_database" and conn_name:
            live = getattr(owner, "active_connections", {}) or {}
            dbm = live.get(conn_name)
            if dbm is not None:
                schema = _introspect_live_schema(dbm)
                if schema:
                    body["schema"] = schema

    def _agent_available() -> bool:
        """True when an AI backend is configured (so we can build a real app)."""
        agent = getattr(owner, "ai_agent", None)
        if agent is None:
            return False
        try:
            backend = getattr(agent, "_active_backend", None)
            return bool(backend and backend.is_available())
        except Exception:
            return False

    def build():
        body = _collect_body()
        if body is None:
            return
        # "Build" should produce the REAL app the user described. When an AI
        # backend is configured we drive the agent (plan → build → iterate under
        # engine/meter governance) instead of the offline scaffold. The
        # deterministic generator is only a fallback when no AI is available.
        if _agent_available() and body["mode"] in ("from_scratch", "from_database"):
            auto_build()
            return
        conn_name = (body.get("connections") or [""])[0]

        build_btn.config(state=tk.DISABLED)
        auto_btn.config(state=tk.DISABLED)
        agent_btn.config(state=tk.DISABLED)
        open_btn.config(state=tk.DISABLED)
        start_btn.config(state=tk.DISABLED)
        results.delete(1.0, tk.END)
        results.insert(tk.END, f"Building [{body['mode']}] {body['name']}…\n")

        def work():
            _stop_running_app(state)
            _maybe_live_schema(body)
            return svc.build(body)

        def done(r, e):
            if state.get("closed") or not _widget_exists(build_btn):
                return  # dialog was closed during the build
            state["build_running"] = False
            _safe_config(build_btn, state=tk.NORMAL)
            _safe_config(auto_btn, state=tk.NORMAL)
            _safe_config(agent_btn, state=tk.NORMAL)
            results.delete(1.0, tk.END)
            if e:
                results.insert(tk.END, f"Error: {e}\n")
                return
            state["workspace"] = r.get("workspace", "") or ""
            v = r.get("verdict", {})
            results.insert(
                tk.END,
                f"{'✓ BUILT' if r.get('ok') else '✗ REJECTED'}  "
                f"score={v.get('score')}  agent={r.get('agent')}\n",
            )
            if body["mode"] in ("from_scratch", "from_database"):
                results.insert(
                    tk.END,
                    "ℹ This is an offline baseline scaffold (no AI backend "
                    "configured). To build the full, requirement-driven app, set "
                    "an AI backend in the AI tab — then Build drives the agent to "
                    "plan and build the real app.\n",
                )
            if body["mode"] == "from_database":
                ntables = len(body.get("schema", {}) or {})
                if ntables:
                    results.insert(tk.END, f"Introspected {ntables} table(s) from '{conn_name}'.\n")
                else:
                    results.insert(
                        tk.END,
                        f"⚠ No tables introspected from '{conn_name}'. Connect to the "
                        "database in the AI tab first, then rebuild.\n",
                    )
            if state["workspace"]:
                if r.get("ok"):
                    results.insert(tk.END, f"Wrote {len(r.get('files', []))} file(s) to:\n  {state['workspace']}\n")
                else:
                    results.insert(tk.END, f"workspace: {state['workspace']}\n")
                _safe_config(open_btn, state=tk.NORMAL)
                _safe_config(delete_btn, state=tk.NORMAL)
                _safe_config(start_btn, state=tk.NORMAL)
                _safe_config(approve_btn, state=tk.NORMAL)
            _append_build_status(r)
            if r.get("notes"):
                results.insert(tk.END, f"notes: {r.get('notes')}\n")
            for issue in v.get("issues", []):
                results.insert(tk.END, f"  issue: {issue}\n")
            results.insert(tk.END, f"\nFiles ({len(r.get('files', []))}):\n")
            for f in r.get("files", []):
                results.insert(tk.END, f"  • {f}\n")
            if r.get("analysis"):
                a = r["analysis"]
                results.insert(
                    tk.END,
                    f"\nCodebase analysis: files={a.get('files')} loc={a.get('loc')} "
                    f"max_complexity={a.get('max_complexity')}\n",
                )

        state["build_running"] = True
        state["coord"] = None
        _run_bg(owner, work, done)

    def auto_build():
        """Autonomous build; for database builds, ground the loop with AiQA."""
        body = _collect_body()
        if body is None:
            return
        if body["mode"] == "from_database" and _agent_available():
            agent_build(body_override=body, aiqa_mode=True)
            return
        body["use_ai"] = True
        conn_name = (body.get("connections") or [""])[0]

        from common import paths as _app_paths

        state["workspace"] = str(_app_paths.app_builder_dir() / body["name"])
        state["stopped"] = False

        build_btn.config(state=tk.DISABLED)
        auto_btn.config(state=tk.DISABLED)
        agent_btn.config(state=tk.DISABLED)
        open_btn.config(state=tk.NORMAL)
        delete_btn.config(state=tk.NORMAL)
        start_btn.config(state=tk.DISABLED)
        if body.get("use_ai"):
            agent_send_btn.config(state=tk.NORMAL)
        results.delete(1.0, tk.END)
        results.insert(
            tk.END,
            f"Auto-building [{body['mode']}] {body['name']} — "
            + (
                "the App Builder agent will use the selected chat backend directly under engine/meter "
                "governance…\n"
                if body["mode"] == "from_scratch"
                else "AiQA will profile/interpret the selected DB and the "
                "builder will iterate under engine/meter governance…\n"
            ),
        )

        def progress(rnd):
            if isinstance(rnd, dict) and rnd.get("agent_event"):
                ev = rnd["agent_event"]
                event = ev.get("event", {})
                if event.get("type") == "baseline_ready":
                    if event.get("text"):
                        state["workspace"] = event["text"]
                    def _ready():
                        if state.get("closed"):
                            return
                        _safe_config(open_btn, state=tk.NORMAL)
                        _safe_config(delete_btn, state=tk.NORMAL)
                        _safe_config(start_btn, state=tk.NORMAL)
                        _append_result(
                            "\n  baseline ready — you can Start app / Open "
                            "output folder while the build continues.\n")
                    _root(owner).after(0, _ready)
                return
            line = (
                f"  round {rnd['index']} [{rnd['phase']}] score={rnd['score']} "
                f"coverage={rnd.get('coverage', '-')} "
                f"accepted={rnd['accepted']} — {rnd['note']}\n"
            )
            for gap in rnd.get("coverage_gaps", [])[:6]:
                line += f"      missing requirement: {gap}\n"
            _root(owner).after(0, lambda: _append_result(line))

        def work():
            _stop_running_app(state)
            _maybe_live_schema(body)
            body["run_tests"] = True
            body["deploy_schema"] = bool(deploy_var.get())
            body["interaction"] = interaction_var.get()
            body["uninterrupted"] = interaction_var.get() == "uninterrupted"
            agent = getattr(owner, "ai_agent", None)
            backend = getattr(agent, "_active_backend", None) if agent else None
            live = getattr(owner, "active_connections", {}) or {}
            dbm = None
            if body["mode"] == "from_database" and conn_name:
                dbm = live.get(conn_name)
            # Channel 1 — the code agent writes code/tests.
            # from_scratch MUST use the selected chat/backend directly, without
            # AI Query Assistant/database scope. from_database may use scoped
            # generation plus the separate DB-understanding channel below.
            bridge = None
            if agent is not None:
                from ai_assistant.app_builder.ai_bridge import (
                    AiQueryBridge,
                    DirectChatBridge,
                )
                if body["mode"] == "from_scratch":
                    bridge = DirectChatBridge(agent=agent)
                else:
                    bridge = AiQueryBridge(
                        agent=agent, connection_name=conn_name, db_manager=dbm)
            # Channel 2 — the AI Query Assistant (used as-is) understands the data;
            # only for from_database so the build is grounded in real data.
            db_understanding = None
            if body["mode"] == "from_database" and agent is not None:
                from ai_assistant.app_builder.db_understanding import (
                    DbUnderstandingClient,
                )
                db_understanding = DbUnderstandingClient(
                    query_assistant=agent, db_manager=dbm,
                    connection_name=conn_name,
                    user_description=body.get("description", ""),
                    variant=body.get("db_app_variant", "application"),
                )
            # Schema deployment target (from_scratch + chosen connection only).
            deploy_dbm = None
            if body["mode"] == "from_scratch" and conn_name and body["deploy_schema"]:
                deploy_dbm = live.get(conn_name)
            return svc.auto_build(
                body, bridge=bridge, db_understanding=db_understanding,
                decider=_build_decider(), db_manager=deploy_dbm,
                on_progress=progress, backend=backend)

        def done(r, e):
            if state.get("closed") or not _widget_exists(build_btn):
                return  # dialog was closed during the build
            state["build_running"] = False
            _safe_config(build_btn, state=tk.NORMAL)
            _safe_config(auto_btn, state=tk.NORMAL)
            _safe_config(agent_btn, state=tk.NORMAL)
            if e:
                _append_result(f"Error: {e}\n")
                return
            state["workspace"] = r.get("workspace", "") or ""
            _enable_post_build_chat(agentic=bool(r.get("agentic")))
            _append_result(
                f"\n{'✓ READY' if r.get('ok') else '✗ INCOMPLETE'}  "
                f"final score={r.get('score')}  "
                f"requirement coverage={r.get('requirement_coverage')}  "
                f"used_ai={r.get('used_ai')}  "
                f"rounds={len(r.get('rounds', []))}\n"
            )
            _append_result(
                f"  fidelity to request={r.get('fidelity')}  "
                f"data understanding={r.get('data_understanding')}  "
                f"process adherence={r.get('process_adherence')}\n"
            )
            journal = r.get("journal", {}) or {}
            if journal:
                _append_result(
                    "  process: channels="
                    f"{', '.join(journal.get('channels', [])) or 'none'}"
                    f"  sample_data={journal.get('sample_data_created')}"
                    f"  tests_run={journal.get('tests_run')}"
                    f"  tests_passed={journal.get('tests_passed')}\n"
                )
            deploy = r.get("schema_deploy", {}) or {}
            if deploy:
                if deploy.get("deployed"):
                    _append_result(
                        f"  schema deployed: {deploy.get('executed')} table(s) "
                        f"created in '{conn_name}'\n")
                else:
                    why = "; ".join(deploy.get("errors", [])) or "not deployed"
                    _append_result(f"  schema deploy skipped: {why}\n")
            asked = [d for d in (r.get("decisions") or []) if d.get("asked")]
            if asked:
                _append_result(f"  decisions you made: {len(asked)}\n")
                for d in asked[:8]:
                    _append_result(f"    • {d.get('id')}: {d.get('answer')}\n")
            insight = r.get("insight", {}) or {}
            if insight.get("app_summary"):
                _append_result(
                    f"  data understood as: {insight.get('app_summary')}\n")
            for gap in r.get("gaps", [])[:10]:
                _append_result(f"  unmet requirement: {gap}\n")
            for gap in r.get("fidelity_gaps", [])[:10]:
                _append_result(f"  request not reflected: {gap}\n")
            if not r.get("used_ai"):
                _append_result(
                    "No agentic AI backend was available — wrote the validated "
                    "AiQA-grounded/deterministic baseline. Set an AI backend in "
                    "the AI tab to enable three-session refinement.\n"
                )
            if state["workspace"]:
                _append_result(
                    f"Wrote {len(r.get('files', []))} file(s) to:\n  {state['workspace']}\n")
                _safe_config(open_btn, state=tk.NORMAL)
                _safe_config(delete_btn, state=tk.NORMAL)
                _safe_config(start_btn, state=tk.NORMAL)
                _safe_config(approve_btn, state=tk.NORMAL)
            _append_build_status(r)

        state["build_running"] = True
        state["coord"] = None
        _run_bg(owner, work, done)

    def agent_build(
        body_override: dict | None = None, *, aiqa_mode: bool = False,
    ):
        """Agentic build: dual sessions, direct writes, per-commit gating."""
        body = body_override or _collect_body()
        if body is None:
            return
        if not _agent_available():
            messagebox.showinfo(
                "App Builder",
                "No AI backend available. Set an AI backend in the AI tab first.",
            )
            return
        body["use_ai"] = True
        body["agentic"] = True
        conn_name = (body.get("connections") or [""])[0]

        # A fresh cancel token for this build so "Stop build" can abort it.
        import threading as _threading

        from common import paths as _app_paths

        cancel_event = _threading.Event()
        state["cancel_event"] = cancel_event
        # The workspace path is known up front; expose it so the user can Open
        # the output folder (and Start the app once the baseline is ready)
        # WHILE the build is still running.
        state["workspace"] = str(_app_paths.app_builder_dir() / body["name"])

        build_btn.config(state=tk.DISABLED)
        auto_btn.config(state=tk.DISABLED)
        agent_btn.config(state=tk.DISABLED)
        open_btn.config(state=tk.NORMAL)  # output folder reachable during build
        delete_btn.config(state=tk.NORMAL)
        start_btn.config(state=tk.DISABLED)  # enabled on baseline_ready
        stop_build_btn.config(state=tk.NORMAL)
        state["stopped"] = False
        # Take-control is available whenever the build is not already interactive.
        takeover_btn.config(
            state=tk.DISABLED if interaction_var.get() == "interactive"
            else tk.NORMAL)
        agent_send_btn.config(state=tk.NORMAL)
        results.delete(1.0, tk.END)
        _clear_agent()
        state["manual_answer"] = ""
        results.insert(
            tk.END,
            (
                f"Auto-building with AiQA [{body['mode']}] {body['name']} — "
                "three sessions (builder + answerer + validator) grounded by "
                "AI Query Assistant DB understanding under engine/meter "
                "governance…\n"
                if aiqa_mode
                else f"Agent-building [{body['mode']}] {body['name']} — "
                "dual sessions (builder + answerer) with direct workspace "
                "writes under engine/meter governance…\n"
            ),
        )
        _append_agent(
            "[system] Starting AiQA-grounded agentic build — deterministic DB "
            "profiling and AiQA interpretation feed the governance brief, then "
            "A/B/C sessions build and validate.\n"
            if aiqa_mode
            else "[system] Starting agentic build — preparing an "
            "auto-approved plan, then governance brief is pushed to both "
            "sessions.\n"
        )

        def _on_baseline_ready():
            # Workspace has a runnable baseline — let the user Start/Open it now,
            # even though the agent keeps building.
            if state.get("closed"):
                return
            _safe_config(start_btn, state=tk.NORMAL)
            _safe_config(open_btn, state=tk.NORMAL)
            _safe_config(delete_btn, state=tk.NORMAL)
            _append_agent("[system] baseline ready — you can Start app / Open "
                          "output folder now while the build continues.\n")

        def progress(payload):
            if isinstance(payload, dict) and payload.get("agent_event"):
                ev = payload["agent_event"]
                session = ev.get("session", "?")
                event = ev.get("event", {})
                etype = event.get("type", "")
                text = event.get("text", "")
                tag = {"builder": "builder", "answerer": "answerer",
                       "validator": "validator"}.get(session, "")
                # Coalesce streamed assistant text: buffer fragments (which may
                # arrive a word/token at a time) and render them as one flowing
                # message. Any other (structured) event first flushes the buffer
                # so the streamed paragraph is closed before it is shown.
                if etype == "assistant_text":
                    if text:
                        key = (session if session in ("answerer", "validator")
                               else "builder")
                        _root(owner).after(
                            0, lambda t=text, k=key: _buffer_agent(t, k))
                    return
                _root(owner).after(0, _flush_agent_buffers)
                if etype == "session_id":
                    # Internal plumbing (used to resume the session); never shown.
                    return
                if etype == "session_status":
                    # A status line targeted at a specific session's own panel
                    # (what B/C are doing right now, or that they are waiting).
                    sess = session if session in (
                        "builder", "answerer", "validator") else "builder"
                    stag = {"answerer": "answerer",
                            "validator": "validator"}.get(sess, "system")
                    _root(owner).after(
                        0,
                        lambda t=text, s=sess, g=stag: _append_agent(
                            f"[{s}] {t}\n", session=s, tag=g),
                    )
                    return
                if etype == "build_path":
                    detail = event.get("detail", {}) or {}
                    path = detail.get("path", "")
                    app_name = detail.get("app_name") or ""
                    enforced = detail.get("enforced_by") or ""
                    label = "REAL APP" if path == "real_app" else "SCHEMA/ADMIN"
                    suffix = f" ({app_name})" if app_name else ""
                    line = (
                        f"[system] build path: {label}{suffix} — {text}; "
                        f"gate={enforced}\n"
                    )
                    _root(owner).after(
                        0,
                        lambda m=line: (
                            _append_result(m),
                            _append_agent(m, session="builder", tag="system"),
                        ),
                    )
                    return
                if etype == "structure_published":
                    vdir = (event.get("detail") or {}).get("validator_dir", "")
                    line = f"[system] {text}\n"
                    if vdir:
                        line += f"  validator folder (C only): {vdir}/\n"
                    _root(owner).after(
                        0,
                        lambda m=line: (
                            _append_result(m),
                            _append_agent(m, session="answerer", tag="system"),
                            _append_agent(m, session="validator", tag="system"),
                        ),
                    )
                    return
                if etype == "baseline_ready":
                    if text:
                        state["workspace"] = text
                    _root(owner).after(0, _on_baseline_ready)
                    return
                if etype == "plan_approved":
                    _root(owner).after(
                        0,
                        lambda v=text: _append_agent(
                            f"[system] plan {v} — builder proceeding to build.\n",
                            tag="gate_ok"),
                    )
                    return
                if etype == "requirement_model":
                    _root(owner).after(
                        0,
                        lambda v=text: _append_agent(
                            f"[assistant] requirement understood — {v}\n",
                            session="builder", tag="system"),
                    )
                    return
                if etype == "decision":
                    detail = event.get("detail", {})
                    chosen = detail.get("chosen")
                    head = (f"chose {chosen}" if chosen
                            else "endorsed proposal with guidance")
                    rationale = detail.get("rationale", "")
                    _root(owner).after(
                        0,
                        lambda h=head, r=rationale: _append_agent(
                            f"[assistant] decision: {h} — {r}\n",
                            session="builder", tag="system"),
                    )
                    return
                if etype == "review":
                    detail = event.get("detail", {})
                    rules = detail.get("injected_rules") or []
                    msg = f"[assistant] monitoring Session B — {text}\n"
                    for r in rules[:6]:
                        msg += f"    rule: {r}\n"
                    _root(owner).after(
                        0,
                        lambda m=msg: _append_agent(
                            m, session="builder", tag="system"),
                    )
                    return
                if etype == "validation":
                    detail = event.get("detail", {})
                    vtag = "gate_ok" if detail.get("clean") else "gate_fail"
                    _root(owner).after(
                        0,
                        lambda t=text, g=vtag: _append_agent(
                            f"[validator] {t}\n", session="validator", tag=g),
                    )
                    return
                if etype == "validation_delivered":
                    _root(owner).after(
                        0,
                        lambda t=text: _append_agent(
                            f"[validator → builder] {t}\n",
                            session="validator", tag="system"),
                    )
                    return
                if etype == "test_plan":
                    _root(owner).after(
                        0,
                        lambda t=text: _append_agent(
                            f"[validator] {t}\n", session="validator", tag="gate_ok"),
                    )
                    return
                if etype == "user_message_delivered":
                    _root(owner).after(
                        0,
                        lambda t=text: _append_agent(
                            f"[answerer → builder] {t}\n",
                            session="answerer", tag="answerer"),
                    )
                    return
                if etype == "status":
                    _root(owner).after(
                        0,
                        lambda t=text: _append_agent(
                            f"[system] {t}\n", tag="system"),
                    )
                    return
                if etype == "notice":
                    detail = event.get("detail", {})
                    if detail.get("suppressed_skip"):
                        return
                    sess = session if session in (
                        "builder", "answerer", "validator") else "builder"
                    _root(owner).after(
                        0,
                        lambda t=text, s=sess: _append_agent(
                            f"[{s}] note: {t}\n", session=s, tag="system"),
                    )
                    return
                if etype == "relay":
                    # Session B box: only traffic bound for A (user/B→A handoffs).
                    detail = event.get("detail", {})
                    rid = detail.get("request_id")
                    status = detail.get("status")
                    suffix = ""
                    if rid:
                        suffix = f" [id={rid}" + (f" status={status}" if status else "") + "]"
                    _root(owner).after(
                        0,
                        lambda t=text, s=suffix: _append_agent(
                            f"[B] {t}{s}\n", session="answerer", tag="answerer"),
                    )
                    return
                if etype == "build_agreement":
                    detail = event.get("detail", {})
                    tag = "gate_ok" if detail.get("complete") else "gate_fail"
                    sess = "system" if detail.get("complete") else "answerer"
                    _root(owner).after(
                        0,
                        lambda t=text, g=tag, s=sess: _append_agent(
                            f"[{s}] {t}\n", session=s, tag=g),
                    )
                    if detail.get("complete"):
                        _root(owner).after(0, _on_baseline_ready)
                    return
                if etype == "commit_verdict":
                    detail = event.get("detail", {})
                    tag = "gate_ok" if detail.get("accepted") else "gate_fail"
                    line = f"[gate] {text}\n"
                elif etype == "question":
                    tag = "answerer"
                    line = f"[{session}] (question/permission) {text}\n"
                elif etype == "error":
                    tag = "gate_fail"
                    line = f"[{session}] error: {text}\n"
                elif etype in ("file_write", "shell_run", "tool_call"):
                    tag = "tool"
                    if not text:
                        # Generic tool activity (no label): the buffer flush
                        # above already ran; don't print a noisy empty line.
                        line = ""
                    elif etype == "file_write":
                        line = f"[{session}] wrote {text}\n"
                    elif etype == "shell_run":
                        line = f"[{session}] ran {text}\n"
                    else:
                        line = f"[{session}] tool: {text}\n"
                    # Collapse identical consecutive tool lines (avoid spam when
                    # the agent repeats the same action many times).
                    if line and line == state.get("last_tool_line"):
                        line = ""
                    elif line:
                        state["last_tool_line"] = line
                elif etype == "done":
                    line = ""  # turn boundary; buffered text already flushed
                else:
                    line = f"[{session}] {etype}: {text}\n" if text or etype else ""
                if line:
                    if tag != "tool":
                        state["last_tool_line"] = ""  # reset dedupe on other output
                    _root(owner).after(
                        0,
                        lambda text=line, event_tag=tag, sess=session:
                        _append_agent(text, session=sess, tag=event_tag),
                    )
                return
            rnd = payload
            line = (
                f"  round {rnd['index']} [{rnd['phase']}] score={rnd['score']} "
                f"coverage={rnd.get('coverage', '-')} "
                f"accepted={rnd['accepted']} — {rnd['note']}\n"
            )
            for gap in rnd.get("coverage_gaps", [])[:6]:
                line += f"      missing requirement: {gap}\n"
            _root(owner).after(0, lambda: _append_result(line))

        def work():
            _stop_running_app(state)
            _maybe_live_schema(body)
            body["run_tests"] = True
            body["deploy_schema"] = bool(deploy_var.get())
            body["interaction"] = interaction_var.get()
            body["uninterrupted"] = interaction_var.get() == "uninterrupted"
            agent = getattr(owner, "ai_agent", None)
            live = getattr(owner, "active_connections", {}) or {}
            dbm = None
            if body["mode"] == "from_database" and conn_name:
                dbm = live.get(conn_name)
            backend = getattr(agent, "_active_backend", None) if agent else None
            db_understanding = None
            if body["mode"] == "from_database" and agent is not None:
                from ai_assistant.app_builder.db_understanding import (
                    DbUnderstandingClient,
                )
                db_understanding = DbUnderstandingClient(
                    query_assistant=agent, db_manager=dbm,
                    connection_name=conn_name,
                    user_description=body.get("description", ""),
                    variant=body.get("db_app_variant", "application"),
                )
            deploy_dbm = None
            if body["mode"] == "from_scratch" and conn_name and body["deploy_schema"]:
                deploy_dbm = live.get(conn_name)
            from ai_assistant.app_builder.interaction import decider_from_options

            def agent_ask(decision):
                if decision.id == "agent_question":
                    _root(owner).after(
                        0,
                        lambda: _append_agent(
                            f"[question] {decision.question}\n"
                            f"  proposed: {decision.detail}\n",
                        ),
                    )
                return _ask_decision(owner, decision)

            # Always install the ask callback (even for auto/uninterrupted) so the
            # user can take control mid-build; the level still gates whether it is
            # actually used (auto asks only critical, uninterrupted asks nothing)
            # until "Take control" flips it to interactive.
            decider = decider_from_options(
                interaction=interaction_var.get(),
                uninterrupted=interaction_var.get() == "uninterrupted",
                ask=agent_ask,
            )
            state["decider"] = decider
            state["agent_ask"] = agent_ask
            return svc.auto_build(
                body, db_understanding=db_understanding,
                decider=decider, db_manager=deploy_dbm,
                on_progress=progress, backend=backend,
                cancel_event=cancel_event)

        def done(r, e):
            if state.get("closed") or not _widget_exists(build_btn):
                return
            state["build_running"] = False
            _safe_config(build_btn, state=tk.NORMAL)
            _safe_config(auto_btn, state=tk.NORMAL)
            _safe_config(agent_btn, state=tk.NORMAL)
            _safe_config(agent_send_btn, state=tk.DISABLED)
            _safe_config(stop_build_btn, state=tk.DISABLED)
            _safe_config(takeover_btn, state=tk.DISABLED)
            state["cancel_event"] = None
            state["decider"] = None
            if e:
                _append_result(f"Error: {e}\n")
                _append_agent(f"[error] {e}\n")
                # Keep Open/Start enabled if a baseline workspace exists.
                if state.get("workspace"):
                    _safe_config(open_btn, state=tk.NORMAL)
                    _safe_config(delete_btn, state=tk.NORMAL)
                    _safe_config(approve_btn, state=tk.NORMAL)
                return
            state["workspace"] = r.get("workspace", "") or state.get("workspace", "")
            if r.get("aborted"):
                _append_result(
                    "\n■ BUILD STOPPED by user — partial workspace kept; "
                    "sessions remain active for chat / take control.\n")
                _append_agent(
                    "[system] build stopped gracefully — partial workspace kept. "
                    "The A/B/C sessions are still alive: keep chatting or Take "
                    "control to continue.\n",
                    tag="gate_fail")
            else:
                reason = r.get("stop_reason") or ""
                agr = r.get("agreement") or {}
                build_path = r.get("build_path") or {}
                path_line = ""
                if build_path:
                    path = build_path.get("path", "")
                    label = "REAL APP" if path == "real_app" else (
                        "SCHEMA/ADMIN" if path == "schema_admin" else path or "standard"
                    )
                    path_line = (
                        f"  Build path: {label}; "
                        f"gate={build_path.get('enforced_by', '-')}; "
                        f"{build_path.get('message', '')}\n"
                    )
                agr_line = ""
                if agr:
                    if agr.get("complete"):
                        agr_line = (
                            "  Sessions A+B+C agree — build complete; "
                            "Start / test / verify the app.\n"
                        )
                    elif agr.get("issues"):
                        agr_line = (
                            "  Sessions disagree — issues:\n"
                            + "".join(f"    - {i}\n" for i in agr["issues"][:8])
                        )
                gate_ok = any(c.get("accepted") for c in (r.get("commits") or []))
                pf = r.get("preflight") or {}
                pf_line = ""
                if pf:
                    if pf.get("ok"):
                        pf_line = "  Code gate: compile + import dry-run PASSED.\n"
                    else:
                        errs = (pf.get("syntax_errors") or [])[:3]
                        if pf.get("import_error"):
                            errs.append(pf["import_error"].splitlines()[-1][:160]
                                        if pf["import_error"].strip() else
                                        "app import failed")
                        errs += (pf.get("module_errors") or [])[:3]
                        pf_line = (
                            "  Code gate: compile + import dry-run FAILED "
                            "(app would crash on launch):\n"
                            + "".join(f"    - {e}\n" for e in errs)
                        )
                smoke = r.get("http_smoke") or {}
                smoke_line = ""
                if smoke:
                    if smoke.get("skipped"):
                        smoke_line = (
                            f"  Launch smoke: SKIPPED"
                            f" ({smoke.get('skip_reason') or 'unavailable'}).\n"
                        )
                    elif smoke.get("ok"):
                        smoke_line = "  Launch smoke: uvicorn + HTTP GET PASSED.\n"
                    else:
                        smoke_line = (
                            "  Launch smoke: uvicorn + HTTP GET FAILED:\n"
                            + "".join(
                                f"    - {e}\n"
                                for e in (smoke.get("errors") or [])[:5])
                        )
                status = "✓ READY" if r.get("ok") else "✗ INCOMPLETE"
                if not r.get("ok") and gate_ok and pf.get("ok", True):
                    status += " (commit gate passed — fix failing tests / session issues)"
                _append_result(
                    f"\n{status}  "
                    f"final score={r.get('score')}  agentic={r.get('agentic')}  "
                    f"commits={len(r.get('commits', []))}  "
                    f"rounds={len(r.get('rounds', []))}\n"
                    + path_line
                    + pf_line
                    + smoke_line
                    + agr_line
                    + (f"stopped because: {reason}\n" if reason else "")
                )
            for c in (r.get("commits") or [])[:12]:
                status = "✓" if c.get("accepted") else "✗"
                _append_agent(
                    f"[gate] round {c.get('round')} {status} "
                    f"score={c.get('score')} coverage={c.get('coverage')}\n",
                    tag="gate_ok" if c.get("accepted") else "gate_fail",
                )
            # The workspace always holds at least the runnable baseline, so allow
            # Open/Start regardless of completion.
            if state.get("workspace"):
                _safe_config(open_btn, state=tk.NORMAL)
                _safe_config(delete_btn, state=tk.NORMAL)
                _safe_config(start_btn, state=tk.NORMAL)
                _safe_config(approve_btn, state=tk.NORMAL)
            if r.get("ok"):
                _append_result(
                    f"Wrote {len(r.get('files', []))} file(s) to:\n"
                    f"  {state['workspace']}\n")
            # Auto-enable interactive chat with the same A/B/C sessions. A
            # graceful stop keeps the sessions alive (resumable), so allow chat
            # even on abort whenever a live coordinator exists.
            _enable_post_build_chat(agentic=bool(r.get("agentic")))

        state["build_running"] = True
        state["coord"] = None
        _run_bg(owner, work, done)

    def stop_build():
        """Gracefully abort the running build while keeping sessions alive.

        Sets the cancel token so the orchestrator stops at the next safe point
        (no new commits/rounds), but the A/B/C sessions stay resumable so the
        user can keep chatting or take control once the loop unwinds.
        """
        ev = state.get("cancel_event")
        if ev is not None:
            ev.set()
            state["stopped"] = True
            _safe_config(stop_build_btn, state=tk.DISABLED)
            _append_result(
                "Gracefully stopping build — finishing the current step, then "
                "keeping the sessions active for chat / take control…\n")
            _append_agent(
                "[system] stop requested — aborting at the next safe point. "
                "Sessions stay alive: use Take control or the message box to "
                "keep working with the same agents.\n",
                tag="gate_fail")

    def delete_build():
        """Erase everything for the current build, leaving only status logs.

        Stops the app and any running build, deletes the build workspace from
        disk, and resets the UI so no trace of the artifacts remains. A short
        line is written to the Build status log for the record.
        """
        import shutil
        from pathlib import Path

        workspace = state.get("workspace")
        if not workspace or not Path(workspace).exists():
            messagebox.showinfo("App Builder", "There is no build to delete.")
            return
        if not messagebox.askyesno(
                "Delete build",
                "Erase this build completely?\n\n"
                f"{workspace}\n\n"
                "This permanently removes the generated app and all build "
                "artifacts. Only the status log is kept. This cannot be undone."):
            return

        # Stop the running app and gracefully cancel any in-flight build first.
        _stop_app_process(state.get("process"))
        state["process"] = None
        ev = state.get("cancel_event")
        if ev is not None:
            ev.set()

        target = workspace
        # Delete through the shared service so UI/CLI/API behave identically and
        # the path-safety guard is enforced; fall back to a local rmtree only if
        # the service can't resolve the build by name.
        removed = False
        err = ""
        try:
            res = svc.delete_app({"name": Path(target).name})
            removed = bool(res.get("ok"))
            if not removed:
                err = "; ".join(res.get("issues", [])) or "delete failed"
        except Exception:  # noqa: BLE001
            try:
                shutil.rmtree(target, ignore_errors=False)
                removed = True
            except FileNotFoundError:
                removed = True
            except Exception as exc:  # noqa: BLE001
                err = str(exc)

        state["workspace"] = ""
        state["coord"] = None
        for _btn in (open_btn, start_btn, stop_btn, approve_btn,
                     stop_build_btn, takeover_btn, agent_send_btn):
            _safe_config(_btn, state=tk.DISABLED)
        _safe_config(build_btn, state=tk.NORMAL)
        _safe_config(auto_btn, state=tk.NORMAL)
        _safe_config(agent_btn, state=tk.NORMAL)
        if removed:
            _append_result(f"\n🗑 Build deleted — erased {target}\n")
        else:
            _append_result(f"\nDelete failed for {target}: {err}\n")

    def take_control():
        """Switch a running auto/uninterrupted build to interactive mid-flight."""
        decider = state.get("decider")
        if decider is None:
            return
        decider.take_control(ask=state.get("agent_ask"))
        interaction_var.set("interactive")
        takeover_btn.config(state=tk.DISABLED)
        agent_send_btn.config(state=tk.NORMAL)
        _append_agent(
            "[system] switched to INTERACTIVE — you now approve the agent's "
            "decisions and questions from here on.\n", tag="gate_ok")

    # btn_row is the pinned bottom bar created earlier so it's always visible.
    def train_from_build():
        """Train selected/new LLM model(s) from the last build's OWN data."""
        conn_name = conn_var.get().strip()
        body = {
            "name": _ensure_build_name(name_var),
            "mode": mode_var.get(),
            "connections": [conn_name] if conn_name else [],
            "train_mode": "full",
            **_train_body_extra(),
        }
        ws = state.get("workspace") or ""
        if ws:
            body["workspace"] = ws
        if not (body.get("train_llm") or body.get("train_new_name")):
            messagebox.showinfo(
                "Train from build",
                "Select an existing model or enter a new model name first.")
            return
        train_build_btn.config(state=tk.DISABLED)
        results.delete(1.0, tk.END)
        results.insert(tk.END, "Training LLM from this build's data…\n")

        def work():
            return svc.build_train_llm(body)

        def done(r, e):
            if state.get("closed") or not _widget_exists(train_build_btn):
                return
            _safe_config(train_build_btn, state=tk.NORMAL)
            if e:
                results.insert(tk.END, f"Error: {e}\n")
                return
            if not r.get("ok"):
                results.insert(
                    tk.END,
                    f"✗ {r.get('error') or r.get('reason') or 'Training failed.'}\n")
                return
            cs = r.get("corpus_stats") or {}
            results.insert(
                tk.END,
                f"✓ Trained {len(r.get('models') or [])} model(s) on "
                f"{r.get('pairs')} build-data pair(s) "
                f"(validation={cs.get('validation')}, "
                f"rejected={cs.get('rejected', 0)})\n")
            for m in r.get("models") or []:
                results.insert(
                    tk.END,
                    f"  {m.get('name')}: ok={m.get('ok')} engine={m.get('engine')}\n")

        _run_bg(owner, work, done)

    build_btn = ttk.Button(btn_row, text="Build", command=build)
    build_btn.pack(side=tk.LEFT)
    train_build_btn = ttk.Button(
        btn_row, text="Train from build", command=train_from_build)
    train_build_btn.pack(side=tk.LEFT, padx=6)
    auto_btn = ttk.Button(btn_row, text="Auto-build (AiQA)", command=auto_build)
    auto_btn.pack(side=tk.LEFT, padx=6)
    agent_btn = ttk.Button(btn_row, text="Agent build", command=agent_build)
    agent_btn.pack(side=tk.LEFT, padx=(0, 6))
    stop_build_btn = ttk.Button(
        btn_row, text="Stop build", state=tk.DISABLED, command=stop_build)
    stop_build_btn.pack(side=tk.LEFT, padx=(0, 6))
    takeover_btn = ttk.Button(
        btn_row, text="Take control (interactive)", state=tk.DISABLED,
        command=take_control)
    takeover_btn.pack(side=tk.LEFT, padx=(0, 6))
    open_btn = ttk.Button(
        btn_row, text="Open output folder", state=tk.DISABLED,
        command=lambda: _open_in_file_browser(state["workspace"]),
    )
    open_btn.pack(side=tk.LEFT, padx=6)
    start_btn = ttk.Button(btn_row, text="Start app", state=tk.DISABLED, command=start_app)
    start_btn.pack(side=tk.LEFT, padx=(0, 6))
    stop_btn = ttk.Button(btn_row, text="Stop app", state=tk.DISABLED, command=stop_app)
    stop_btn.pack(side=tk.LEFT, padx=(0, 6))
    approve_btn = ttk.Button(
        btn_row, text="Approve & package", state=tk.DISABLED,
        command=approve_and_package)
    approve_btn.pack(side=tk.LEFT, padx=(0, 6))
    delete_btn = ttk.Button(
        btn_row, text="Delete build", state=tk.DISABLED, command=delete_build)
    delete_btn.pack(side=tk.LEFT)

    def _on_destroy(event) -> None:
        if event.widget is main:
            state["closed"] = True
            _stop_app_process(state.get("process"))

    main.bind(
        "<Destroy>",
        _on_destroy,
    )


# ── window openers ──────────────────────────────────────────────────────────-
def _connection_names(owner: Any) -> list[str]:
    """Connection names for the picker, preferring live (connected) sessions.

    ``from_database`` introspection needs an *active* connection, so connected
    sessions are listed first.
    """
    active = list((getattr(owner, "active_connections", {}) or {}).keys())
    if active:
        return active
    combo = getattr(owner, "ai_conn_combo", None)
    try:
        if combo is not None:
            return list(combo["values"] or [])
    except Exception:
        pass
    return []


def _new_dialog(owner: Any, title: str, geometry: str) -> tk.Toplevel:
    dialog = tk.Toplevel(_root(owner))
    dialog.title(title)
    dialog.geometry(geometry)
    dialog.transient(_root(owner))
    # Keep the window flexible: freely resizable in both directions with a
    # minimum size that keeps the form, panels and button bar usable.
    dialog.resizable(True, True)
    dialog.minsize(640, 480)
    return dialog


def open_app_builder_dialog(owner: Any) -> tk.Toplevel:
    dialog = _new_dialog(owner, "App Builder — AiAppEngine", "780x620")
    main = _make_scrollable(dialog)
    main.configure(padding=10)
    _populate_app_builder(owner, main)
    return dialog


def open_build_an_app_dialog(owner: Any) -> tk.Toplevel:
    """'Build an App' hub (App Builder, governed by the AiAppEngine)."""
    dialog = _new_dialog(owner, "Build an App", "820x680")

    header = ttk.Frame(dialog, padding=(10, 8, 10, 0))
    header.pack(fill=tk.X)
    ttk.Label(header, text="Build an App", font=("Arial", 15, "bold")).pack(anchor=tk.W)
    ttk.Label(
        header,
        text="Build apps from scratch, a database, or a codebase — governed by the "
        "AiAppEngine (code/meters), not prompts alone.",
        foreground="gray", wraplength=780, justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(2, 6))

    body = _make_scrollable(dialog)
    body.configure(padding=10)
    _populate_app_builder(owner, body)
    return dialog
