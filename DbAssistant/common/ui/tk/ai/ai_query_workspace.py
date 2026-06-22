"""
Multi-tab shell for the AI Query Assistant.

Each notebook tab is one AISession with its own AIQueryUI instance.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk, messagebox
from dataclasses import dataclass
from typing import Any

from common.ui.tk.ai.ai_query_ui import (
    AIQueryCallbacks,
    AIQuerySessionContext,
    AIQueryStyling,
    AIQueryUI,
)
from ai_query.cross_tab_orchestrator import CrossTabOrchestrator
from ai_query.session_manager import (
    AISession,
    SessionStatus,
    load_sessions_from_disk,
    merge_session_into_disk,
    read_sessions_file,
    save_sessions_merged,
)

_ACCENT_BG = "#ADD8E6"
_ACCENT_BG_HOVER = "#9FD0E6"
_ACCENT_FG = "#0B3D5C"


@dataclass(frozen=True)
class AIQueryWorkspaceContext:
    """Dependencies for constructing an AI query workspace."""

    parent_frame: Any
    root: Any
    ai_agent: Any
    active_connections: dict
    update_status_callback: Any
    send_to_editor_callback: Any
    theme: dict
    fonts: dict


def _accent_button(parent, text: str, command, *, width: int = 13) -> tk.Label:
    """A colored, button-like control.

    Uses ``tk.Label`` instead of ``tk.Button`` because macOS (Aqua) ignores the
    background of native buttons, so the requested color would not render. A
    label reliably shows the fill on every platform while looking and behaving
    like the adjacent toolbar buttons.
    """
    btn = tk.Label(
        parent, text=text, width=width,
        bg=_ACCENT_BG, fg=_ACCENT_FG,
        relief=tk.RAISED, bd=1, padx=8, pady=3,
        cursor="hand2", font="TkDefaultFont",
    )
    btn.bind("<Button-1>", lambda _e: command())
    btn.bind("<Enter>", lambda _e: btn.configure(bg=_ACCENT_BG_HOVER))
    btn.bind("<Leave>", lambda _e: btn.configure(bg=_ACCENT_BG))
    return btn


class AIQueryWorkspace:
    """Notebook of AIQueryUI tabs backed by AISessionManager on the shared agent."""

    def __init__(
        self,
        context: AIQueryWorkspaceContext | Any = None,
        *legacy_args,
        **legacy,
    ):
        if not isinstance(context, AIQueryWorkspaceContext):
            names = (
                "root", "ai_agent", "active_connections", "update_status_callback",
                "send_to_editor_callback", "theme", "fonts",
            )
            values = dict(legacy)
            if context is not None:
                values.setdefault("parent_frame", context)
            for name, value in zip(names, legacy_args):
                values.setdefault(name, value)
            context = AIQueryWorkspaceContext(**values)
        self.parent = context.parent_frame
        self.root = context.root
        self.ai_agent = context.ai_agent
        self.active_connections = context.active_connections
        self.update_status = context.update_status_callback
        self.send_to_editor = context.send_to_editor_callback
        self.theme = context.theme
        self.fonts = context.fonts
        self.session_manager = self.ai_agent.sessions
        self.orchestrator = CrossTabOrchestrator(
            self.ai_agent,
            self.session_manager,
            lambda name: self.active_connections.get(name),
        )
        self._tab_uis: dict[str, AIQueryUI] = {}
        self._notebook: ttk.Notebook | None = None
        self._toolbar: ttk.Frame | None = None

    def create_ui(self):
        outer = ttk.Frame(self.parent)
        outer.pack(fill=tk.BOTH, expand=True)

        self._toolbar = ttk.Frame(outer)
        self._toolbar.pack(fill=tk.X, padx=4, pady=4)
        ttk.Button(self._toolbar, text="+ New Tab", command=self.add_tab).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(self._toolbar, text="Close Tab", command=self.close_current_tab).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(self._toolbar, text="Save Sessions", command=self._save_sessions).pack(
            side=tk.LEFT, padx=2
        )
        ttk.Button(self._toolbar, text="Load Sessions", command=self._load_sessions).pack(
            side=tk.LEFT, padx=2
        )

        # AI build/RAG entry points at the right end of the bar (light blue).
        # Packed right-to-left so they read left-to-right as:
        # Build an App | RAG Manager | Build or Train LLM.
        from common.editions import advanced_modules_installed

        if advanced_modules_installed():
            _accent_button(self._toolbar, "Build or Train LLM", self._open_build_own_llm).pack(
                side=tk.RIGHT, padx=2
            )
            _accent_button(self._toolbar, "RAG Manager", self._open_rag_manager).pack(
                side=tk.RIGHT, padx=2
            )
            _accent_button(self._toolbar, "Build an App", self._open_build_an_app).pack(
                side=tk.RIGHT, padx=2
            )

        self._notebook = ttk.Notebook(outer)
        self._notebook.pack(fill=tk.BOTH, expand=True)
        self._notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        if not self.session_manager.list_sessions():
            self.add_tab()
        else:
            for meta in self.session_manager.list_sessions():
                sess = self.session_manager.get(meta["session_id"])
                if sess:
                    self._add_tab_for_session(sess)

    def add_tab(self, connection_name: str = "", backend: str = ""):
        sess = self.session_manager.create(
            connection_name=connection_name,
            backend=backend,
        )
        self._add_tab_for_session(sess)

    def _add_tab_for_session(self, sess: AISession):
        frame = ttk.Frame(self._notebook)
        label = self._tab_label(sess)
        self._notebook.add(frame, text=label)

        ui = AIQueryUI(
            frame,
            self.root,
            self.ai_agent,
            self.active_connections,
            AIQueryCallbacks(
                update_status=self.update_status,
                send_to_editor=self.send_to_editor,
                on_session_meta_changed=lambda: self._refresh_tab_label(sess.session_id),
            ),
            AIQueryStyling(theme=self.theme, fonts=self.fonts),
            AIQuerySessionContext(
                session_id=sess.session_id,
                session_manager=self.session_manager,
                orchestrator=self.orchestrator,
            ),
        )
        ui.create_ui()
        if sess.connection_name:
            ui.set_connection(sess.connection_name)
        if sess.backend and hasattr(ui, "ai_backend_var"):
            ui.set_backend(sess.backend)
        self._tab_uis[sess.session_id] = ui
        self._notebook.select(frame)

    def _tab_label(self, sess: AISession) -> str:
        conn = sess.connection_name or "no conn"
        backend = sess.backend or self.ai_agent.get_active_backend_name() or "auto"
        busy = " *" if sess.status != SessionStatus.IDLE else ""
        iso = " [iso]" if sess.isolated else ""
        return f"Tab {sess.tab_number} · {conn} ({backend}){busy}{iso}"

    def _refresh_tab_label(self, session_id: str):
        sess = self.session_manager.get(session_id)
        ui = self._tab_uis.get(session_id)
        if not sess or not self._notebook or not ui:
            return
        idx = self._notebook.index(ui.parent)
        self._notebook.tab(idx, text=self._tab_label(sess))

    def _on_tab_changed(self, _event=None):
        pass

    def _current_session_id(self) -> str | None:
        if not self._notebook:
            return None
        idx = self._notebook.index("current")
        if idx < 0:
            return None
        for sid, ui in self._tab_uis.items():
            if self._notebook.index(ui.parent) == idx:
                return sid
        return None

    def _current_tab_ui(self):
        sid = self._current_session_id()
        return self._tab_uis.get(sid or "") if sid else None

    def close_current_tab(self):
        if not self._notebook:
            return
        sid = self._current_session_id()
        if not sid:
            return
        if len(self._tab_uis) <= 1:
            messagebox.showinfo("Close Tab", "At least one tab must remain open.")
            return

        sess = self.session_manager.get(sid)
        if sess and getattr(sess, "status", SessionStatus.IDLE) != SessionStatus.IDLE:
            messagebox.showwarning(
                "Close Tab",
                "Wait for the current AI request to finish before closing this tab.",
            )
            return

        choice = messagebox.askyesnocancel(
            "Close Tab",
            "Save this session for later?\n\n"
            "Yes — save to sessions.json (use Load Sessions to reopen)\n"
            "No — close without saving\n"
            "Cancel — keep tab open",
        )
        if choice is None:
            return
        if choice and sess:
            try:
                merge_session_into_disk(sess, saved_from_close=True)
            except Exception as exc:
                messagebox.showerror("Save Session", str(exc))
                return

        ui = self._tab_uis.pop(sid)
        if hasattr(ui, "shutdown"):
            try:
                ui.shutdown()
            except Exception:
                pass
        self.session_manager.delete(sid)
        self._notebook.forget(ui.parent)

    def shutdown(self):
        """Cancel + join background workers across all tabs (app exit/teardown)."""
        for ui in list(self._tab_uis.values()):
            if hasattr(ui, "shutdown"):
                try:
                    ui.shutdown()
                except Exception:
                    pass

    def refresh_connections(self):
        for ui in self._tab_uis.values():
            ui.refresh_connections()

    def clear_ai_schema_cache(self):
        for ui in self._tab_uis.values():
            ui.clear_ai_schema_cache()

    def show_cache_info(self):
        first = next(iter(self._tab_uis.values()), None)
        if first:
            first.show_cache_info()

    def show_schema_sent_to_ai(self):
        first = next(iter(self._tab_uis.values()), None)
        if first:
            first.show_schema_sent_to_ai()

    def get_dashboard_snapshot(self) -> dict[str, Any]:
        """Runtime state for the operational dashboard."""
        from ai_query.session_manager import SessionStatus

        sessions_out = []
        running_count = 0
        ui_busy = False
        for meta in self.session_manager.list_sessions():
            sess = self.session_manager.get(meta["session_id"])
            if not sess:
                continue
            status = sess.status.value if hasattr(sess.status, "value") else str(sess.status)
            if status != SessionStatus.IDLE.value:
                running_count += 1
            last_user = ""
            for msg in reversed(sess.conversation_history):
                if isinstance(msg, dict) and msg.get("role") == "user":
                    last_user = str(msg.get("content") or "")[:160]
                    break
            sessions_out.append(
                {
                    "tab": sess.tab_number,
                    "connection": sess.connection_name or "—",
                    "backend": sess.backend or "auto",
                    "status": status,
                    "last_user_message": last_user,
                    "message_count": len(sess.conversation_history),
                    "has_sql": bool(sess.current_sql),
                }
            )
        for ui in self._tab_uis.values():
            if getattr(ui, "query_running", False) or getattr(ui, "auto_loop_running", False):
                ui_busy = True
                break

        active_backend = ""
        try:
            active_backend = self.ai_agent.get_active_backend_name() or ""
        except Exception:
            pass

        return {
            "installed": True,
            "tab_count": len(sessions_out),
            "running_sessions": running_count,
            "ui_busy": ui_busy,
            "active_backend": active_backend,
            "sessions": sessions_out,
            "working_on": _summarize_ai_work(sessions_out, ui_busy),
        }

    def _open_build_an_app(self):
        """Open the App Builder (AiAppEngine-governed) only."""
        try:
            from common.ui.tk.ai.build_apps_dialogs import open_app_builder_dialog

            open_app_builder_dialog(self)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Build an App", f"Could not open App Builder: {exc}")

    def _open_build_own_llm(self):
        """Open the local LLM trainer (train your own NL→SQL model)."""
        try:
            from common.ui.tk.ai.llm_panel import open_llm_panel

            open_llm_panel(self)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Build or Train LLM", f"Could not open LLM trainer: {exc}")

    def _open_rag_manager(self):
        """Open RAG Manager for the currently selected AI tab."""
        try:
            from common.ui.tk.ai.rag_panel import open_rag_panel

            owner = self._current_tab_ui() or self
            open_rag_panel(owner)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("RAG Manager", f"Could not open RAG Manager: {exc}")

    def _save_sessions(self):
        try:
            path = save_sessions_merged(self.session_manager)
            messagebox.showinfo("Save Sessions", f"Saved to {path}")
        except Exception as exc:
            messagebox.showerror("Save Sessions", str(exc))

    def _load_sessions(self):
        try:
            stored = read_sessions_file()
            if not stored:
                messagebox.showinfo(
                    "Load Sessions",
                    "No saved sessions found.\n\n"
                    "Close a tab and choose Yes to save, or use Save Sessions.",
                )
                return
            resume_count = sum(1 for r in stored if r.get("backend_session_id"))
            load_sessions_from_disk(self.session_manager)
            for sid in list(self._tab_uis):
                ui = self._tab_uis.pop(sid)
                if hasattr(ui, "shutdown"):
                    try:
                        ui.shutdown()
                    except Exception:
                        pass
                self._notebook.forget(ui.parent)
            for meta in self.session_manager.list_sessions():
                sess = self.session_manager.get(meta["session_id"])
                if sess:
                    self._add_tab_for_session(sess)
            msg = f"Restored {len(stored)} session(s)."
            if resume_count:
                msg += f" {resume_count} can resume via backend session ID."
            messagebox.showinfo("Load Sessions", msg)
        except Exception as exc:
            messagebox.showerror("Load Sessions", str(exc))


def _summarize_ai_work(sessions: list[dict], ui_busy: bool) -> str:
    running = [s for s in sessions if s.get("status") not in ("idle", "")]
    if ui_busy or running:
        parts = []
        for s in running[:3]:
            conn = s.get("connection") or "no connection"
            hint = s.get("last_user_message") or "Processing…"
            parts.append(f"Tab {s.get('tab')}: {conn} — {hint[:80]}")
        if not parts:
            return "AI query in progress…"
        return " | ".join(parts)
    if sessions:
        return f"{len(sessions)} tab(s) idle — ready for natural-language queries"
    return "No AI tabs open"
