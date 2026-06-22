# ---------------------------------------------------------------------
# description: AI query UI manager for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

# Allow ``python ai_query/ai_query_ui.py`` from project root.
if __name__ == "__main__":
    import sys
    from pathlib import Path

    _root = Path(__file__).resolve().parents[1]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import logging
import sys
import os
import threading
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

# Audit logger for security-relevant operations (SQL execution, file imports,
# backend selection). Routed through the standard logging system so the host
# app can persist/forward it; no-ops quietly if logging isn't configured.
_audit_log = logging.getLogger("dbtool.ai_query.audit")

from ai_query.auto_execute_orchestrator import AutoExecuteOrchestrator
from ai_query.sql_modes import normalize_sql_mode, sql_mode_label, execution_rules_apply
from ai_query.sql_execution_service import (
    check_execution_allowed,
    default_execution_rules_from_config,
    execute_sql_after_gate,
)
from common.ui.tk import (
    create_horizontal_scrollable,
    make_collapsible_section,
    make_scrollable,
)
from common.config_loader import get_window_size
from ai_query import module_config as mc


@dataclass(frozen=True)
class AIQueryCallbacks:
    update_status: object
    send_to_editor: object
    on_session_meta_changed: object | None = None


@dataclass(frozen=True)
class AIQueryStyling:
    theme: object
    fonts: dict


@dataclass(frozen=True)
class AIQuerySessionContext:
    session_id: str | None = None
    session_manager: object | None = None
    orchestrator: object | None = None


class AIQueryUI:
    """AI Query Assistant UI Module - Natural language to SQL query generation"""

    def __init__(
        self,
        parent_frame,
        root,
        ai_agent,
        active_connections,
        callbacks: AIQueryCallbacks,
        styling: AIQueryStyling,
        session: AIQuerySessionContext | None = None,
    ):
        """
        Initialize AI Query UI

        Args:
            parent_frame: tk.Frame to contain the AI query UI
            root: Main window reference for dialogs and after()
            ai_agent: AIQueryAgent instance
            active_connections: Dict of active database connections
            update_status_callback: Callback function(msg, type) for status updates
            send_to_editor_callback: Callback to send SQL to editor
            theme: ColorTheme class for styling
            fonts: Dict with 'ui' and 'mono' font definitions
        """
        self.parent = parent_frame
        self.root = root
        self.ai_agent = ai_agent
        self.active_connections = active_connections
        # Status callback may be 1-arg (msg) or 2-arg (msg, type); call via the
        # tolerant ``update_status`` method below so an optional severity kind is
        # never a hard dependency on the host's callback signature.
        self._update_status_cb = callbacks.update_status
        self.send_to_editor = callbacks.send_to_editor
        self.theme = styling.theme
        self.fonts = styling.fonts

        # Set fonts as separate attributes for compatibility
        self.ui_font = styling.fonts["ui"]
        self.ui_font_mono = styling.fonts["mono"]

        session = session or AIQuerySessionContext()
        self.session_id = session.session_id
        self.session_manager = session.session_manager or getattr(ai_agent, "sessions", None)
        self.orchestrator = session.orchestrator
        self.on_session_meta_changed = callbacks.on_session_meta_changed

        # Configuration
        self.sql_review_rules = ""
        self.sql_execution_rules = default_execution_rules_from_config()

        # Query execution state tracking
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False

        # SQL generation (NL->SQL) state tracking. Generation runs on a worker
        # thread; we keep a handle + cancel flag so the user can cancel a slow
        # backend call and so workers can be joined on shutdown (no leaks).
        self.generation_running = False
        self.generation_cancelled = False
        self._generation_thread = None
        # All background worker threads we spawn, for clean shutdown/join.
        self._worker_threads: list = []
        # Pending root.after() callback ids (scheduled via _safe_after) so they
        # can be cancelled on tab close before they fire on destroyed widgets.
        self._after_ids: set = set()
        # Set once shutdown() begins so late callbacks become no-ops.
        self._shutting_down = False
        # Guards concurrent mutation of batch-question state (load vs. advance).
        self._batch_lock = threading.Lock()

        # Auto-execute pipeline (per-tab UI preferences)
        self.auto_execute_ai_loop = mc.get_bool(
            "ui.ai_query", "auto_execute_ai_loop", default=False
        )
        self.auto_execute_sql = mc.get_bool(
            "ui.ai_query", "auto_execute_summary_sql", default=False
        )
        self.sql_mode = normalize_sql_mode(
            mc.get("ui.ai_query", "default_sql_mode", "summary")
        )
        self.auto_loop_running = False
        self.auto_loop_cancelled = False
        self._auto_iteration = 0
        self._auto_problem = ""
        self._pipeline_callback = None
        self._auto_orchestrator = AutoExecuteOrchestrator(ai_agent)

        # Fallback-corrector continuous training: when on, query pairs that the
        # local LLM got wrong (failed execution or flagged) are re-trained from
        # the fallback-corrected, verified query — connected DB only.
        self.autofix_train = mc.get_bool("ai.llm", "auto_fix_train", default=False)
        try:
            from ai_query.service import _read_ai_state
            _persisted = _read_ai_state()
            if "auto_fix_train" in _persisted:
                self.autofix_train = bool(_persisted["auto_fix_train"])
        except Exception:
            pass
        # Guards so a corrected query that itself fails does not auto-correct
        # forever, and so we know not to auto-train on a corrected query again.
        self._autofix_in_progress = False
        self._last_sql_corrected = False
        # A queued auto-fix training pair ({"model", "connection"}). We only
        # train AFTER the user executes the corrected query and it succeeds —
        # never on an unverified query. Cleared on a fresh question/follow-up.
        self._pending_autofix_train = None

        # Batch "questions from a file" runner: iterate questions one-by-one,
        # generating SQL for each. In auto mode (uninterrupted / auto-execute)
        # each query runs automatically; otherwise we wait for the user to click
        # Execute before advancing to the next question.
        self._batch_questions: list[str] = []
        self._batch_index = 0
        self._batch_active = False
        self._batch_auto = False
        self._batch_step_pending = False

        # UI widgets (initialized in create_ui)
        self.ai_conn_combo: ttk.Combobox = None  # type: ignore[assignment]
        self.ai_question_text: scrolledtext.ScrolledText = None  # type: ignore[assignment]
        self.ai_sql_text: scrolledtext.ScrolledText = None  # type: ignore[assignment]
        self.ai_results_text: tk.Text = None  # type: ignore[assignment]
        self.ai_results_notebook: ttk.Notebook = None  # type: ignore[assignment]
        self.ai_explanation_text: scrolledtext.ScrolledText = None  # type: ignore[assignment]
        self.ai_optimization_text: scrolledtext.ScrolledText = None  # type: ignore[assignment]
        self.ai_rag_text: scrolledtext.ScrolledText = None  # type: ignore[assignment]
        self.ai_chat_history: scrolledtext.ScrolledText = None  # type: ignore[assignment]
        self.ai_followup_text: scrolledtext.ScrolledText = None  # type: ignore[assignment]
        self.ai_review_text: scrolledtext.ScrolledText = None  # type: ignore[assignment]
        self.review_sql_btn: ttk.Menubutton = None  # type: ignore[assignment]
        self.ai_status_label: ttk.Label = None  # type: ignore[assignment]
        self.ai_provider_label: ttk.Label = None  # type: ignore[assignment]
        self.ai_model_label: ttk.Label = None  # type: ignore[assignment]
        self.ai_backend_reason_label: ttk.Label = None  # type: ignore[assignment]
        self.execute_query_btn: ttk.Button = None  # type: ignore[assignment]
        self.stop_query_btn: ttk.Button = None  # type: ignore[assignment]

    # Public API methods
    def refresh_connections(self):
        """Public method called when connections change"""
        self.refresh_ai_connections()
    def _session(self):
        if not self.session_id or not self.session_manager:
            return None
        return self.session_manager.get(self.session_id)

    def set_connection(self, name: str):
        if self.ai_conn_combo is not None:
            self.ai_conn_combo.set(name)
        sess = self._session()
        if sess:
            sess.connection_name = name
            self._notify_session_meta()

    def set_backend(self, name: str):
        if not hasattr(self, "_backend_label_to_name"):
            return
        for label, bname in self._backend_label_to_name.items():
            if bname == name and hasattr(self, "ai_backend_var"):
                self.ai_backend_var.set(label)
                break
        sess = self._session()
        if sess:
            sess.backend = name
            self._notify_session_meta()

    def _notify_session_meta(self):
        if self.on_session_meta_changed:
            try:
                self.on_session_meta_changed()
            except Exception:
                pass

    def _set_mask_pii(self, enabled: bool):
        """Toggle PII/secret masking in prompts sent to external AI backends."""
        # Don't flip the masking mode while a prompt is being built on a worker
        # thread, otherwise one prompt could be partially masked. Defer instead.
        if getattr(self, "generation_running", False) or getattr(self, "query_running", False):
            self.update_status(
                "PII masking change deferred until the current request finishes")
            self._safe_after(lambda: self._set_mask_pii(enabled), delay=300)
            return
        if self.ai_agent:
            self.ai_agent.set_mask_pii(enabled)
        self._refresh_pii_menu_labels()
        state = "enabled" if enabled else "disabled"
        self.update_status(f"PII masking {state} for AI prompts")

    def _refresh_pii_menu_labels(self):
        menu = getattr(self, "ai_options_menu", None)
        if menu is None:
            return
        enabled = bool(getattr(self.ai_agent, "mask_pii_enabled", True))
        try:
            menu.entryconfig(
                self._pii_menu_mask_idx,
                label=("✓ " if enabled else "") + "Mask PII data",
            )
            menu.entryconfig(
                self._pii_menu_unmask_idx,
                label=("✓ " if not enabled else "") + "Unmask PII data",
            )
        except tk.TclError:
            pass

    def _set_uninterrupted_followups(self, enabled: bool):
        self.auto_execute_ai_loop = enabled
        var = getattr(self, "uninterrupted_followups_var", None)
        if var is not None:
            var.set(enabled)
        self.update_status(
            f"Uninterrupted follow-ups {'enabled' if enabled else 'disabled'}"
        )

    def _on_uninterrupted_followups_toggled(self):
        self._set_uninterrupted_followups(self.uninterrupted_followups_var.get())

    def _set_auto_execute_sql(self, enabled: bool):
        self.auto_execute_sql = enabled
        self._refresh_auto_menu_labels()
        self.update_status(
            f"Auto-execute SQL {'enabled' if enabled else 'disabled'}"
        )

    def _set_sql_mode(self, mode: str):
        self.sql_mode = normalize_sql_mode(mode)
        sess = self._session()
        if sess:
            sess.sql_mode = self.sql_mode
            sess.sql_modes_v2 = True
        self._refresh_auto_menu_labels()
        self.update_status(f"SQL mode: {sql_mode_label(self.sql_mode)}")

    def _refresh_auto_menu_labels(self):
        menu = getattr(self, "ai_options_menu", None)
        if menu is None:
            return
        try:
            menu.entryconfig(
                self._auto_sql_menu_idx,
                label=("✓ " if self.auto_execute_sql else "")
                + "Auto-execute SQL queries",
            )
            menu.entryconfig(
                self._strict_summary_mode_menu_idx,
                label=("✓ " if self.sql_mode == "strict_summary" else "")
                + "Strict summary mode",
            )
            menu.entryconfig(
                self._summary_mode_menu_idx,
                label=("✓ " if self.sql_mode == "summary" else "") + "Summary mode",
            )
            menu.entryconfig(
                self._open_mode_menu_idx,
                label=("✓ " if self.sql_mode == "open" else "") + "Open mode",
            )
        except (tk.TclError, AttributeError):
            pass

    def _auto_execute_sql_enabled(self) -> bool:
        return bool(self.auto_execute_sql)

    def _auto_execute_ai_loop_enabled(self) -> bool:
        return bool(self.auto_execute_ai_loop)

    def _sync_panels_to_session(self, result: dict | None = None):
        sess = self._session()
        if not sess:
            return
        sess.sql_mode = self.sql_mode
        if result:
            sess.last_explanation_text = result.get("explanation") or ""
            if result.get("summary_sql") or result.get("sql"):
                sess.current_sql = result.get("summary_sql") or result.get("sql")
        if self.ai_explanation_text is not None:
            sess.last_explanation_text = self.ai_explanation_text.get(1.0, tk.END).strip()
        sess.last_query_output_text = self._get_query_output_text()

    def _get_query_output_text(self) -> str:
        if self.ai_results_text is None:
            return ""
        try:
            return self.ai_results_text.get(1.0, tk.END).strip()
        except tk.TclError:
            return ""

    def _update_busy_ui(self):
        busy = self.query_running or self.auto_loop_running or self.generation_running
        if not hasattr(self, "execute_query_btn") or self.execute_query_btn is None:
            return
        if busy:
            self.execute_query_btn.pack_forget()
            self.stop_query_btn.pack(
                side=tk.LEFT, padx=4, before=self.explain_query_btn
            )
        else:
            self.stop_query_btn.pack_forget()
            self.execute_query_btn.pack(
                side=tk.LEFT, padx=4, before=self.explain_query_btn
            )

    def _sync_session_from_ui(self):
        sess = self._session()
        if not sess:
            return
        if self.ai_conn_combo is not None:
            sess.connection_name = self.ai_conn_combo.get() or sess.connection_name
        if hasattr(self, "ai_backend_var"):
            label = self.ai_backend_var.get()
            sess.backend = self._backend_label_to_name.get(label, sess.backend)
        sess.sql_mode = self.sql_mode
        sess.sql_execution_rules = self.sql_execution_rules
        sess.sql_modes_v2 = True
        self._notify_session_meta()



    def refresh_backend_options(self):
        """Rebuild the backend dropdown from the current set of backends.

        Called after a local model is trained so newly created models appear in
        the selector without restarting the app. Safe to call from any thread
        (it re-dispatches onto the Tk main loop) and preserves the current
        selection when it still exists.
        """
        root = getattr(self, "root", None)

        def _do():
            combo = getattr(self, "ai_backend_combo", None)
            if combo is None or not hasattr(self, "ai_backend_var"):
                return
            try:
                options = self.ai_agent.list_backend_options() or []
            except Exception:
                return
            self._backend_label_to_name = {o["label"]: o["value"] for o in options}
            labels = [o["label"] for o in options]
            try:
                combo.config(values=labels)
            except Exception:
                return
            # Keep the current selection if it still exists; otherwise fall back
            # to the agent's active backend label.
            current = self.ai_backend_var.get()
            if current not in self._backend_label_to_name:
                active_value = self.ai_agent.get_active_backend_value()
                active_label = next(
                    (l for l, v in self._backend_label_to_name.items()
                     if v == active_value),
                    labels[0] if labels else "",
                )
                self.ai_backend_var.set(active_label)

            # Keep the fallback dropdown's options in sync (newly trained local
            # models should be selectable as a fallback corrector too).
            fb_combo = getattr(self, "ai_fallback_combo", None)
            if fb_combo is not None and hasattr(self, "ai_fallback_var"):
                none_label = getattr(self, "_fallback_none_label", "(none)")
                self._fallback_label_to_value = {none_label: ""}
                self._fallback_label_to_value.update(
                    {o["label"]: o["value"] for o in options})
                try:
                    fb_combo.config(values=[none_label] + labels)
                except Exception:
                    pass
                fb_current = self.ai_fallback_var.get()
                if fb_current not in self._fallback_label_to_value:
                    self.ai_fallback_var.set(none_label)

        if root is not None and hasattr(root, "after"):
            try:
                root.after(0, _do)
                return
            except Exception:
                pass
        _do()

    def _on_backend_changed(self, event=None):
        """
        Called when the user picks an AI backend in the dropdown.
        We probe the chosen backend on a background thread (it can take
        a few seconds for codex / cursor) and update the status inline.
        """
        if not hasattr(self, "ai_backend_var"):
            return
        label = self.ai_backend_var.get()
        name  = self._backend_label_to_name.get(label, "")
        if not name:
            return

        # Optimistic UI: show "Checking..." immediately
        if self.ai_status_label is not None:
            self.ai_status_label.config(text="Checking...", foreground="orange")
        if hasattr(self, "ai_backend_reason_label"):
            self.ai_backend_reason_label.config(text="", foreground="gray")

        # Remember the most recent selection so an earlier (slower) probe that
        # finishes late doesn't overwrite the status of a newer selection.
        self._pending_probe_backend = name

        def _probe():
            result = self.ai_agent.check_backend(name, force=True)
            self._safe_after(lambda: self._apply_backend_check(name, result))

        self._start_worker(_probe, name="ai-backend-probe")

    def _on_fallback_changed(self, event=None):
        """User picked a fallback backend (or '(none)') in the dropdown.

        The fallback is set on the agent (in-memory for this session) and
        persisted so it survives restarts. We do not block on a probe here;
        availability is verified lazily when the fallback is actually used.
        """
        if not hasattr(self, "ai_fallback_var"):
            return
        label = self.ai_fallback_var.get()
        value = (getattr(self, "_fallback_label_to_value", {}) or {}).get(label, "")
        try:
            self.ai_agent.set_fallback_backend(value, verify=False)
        except Exception as exc:
            self.update_status(f"Could not set fallback backend: {exc}")
            return
        try:
            from ai_query.service import _update_ai_state
            _update_ai_state({"fallback_backend": value})
        except Exception:
            pass
        self.update_status(
            f"Fallback backend: {value or '(none)'}"
        )

    def fallback_backend_value(self) -> str:
        """Encoded fallback selection for this session ("" when disabled)."""
        try:
            return self.ai_agent.get_fallback_backend_value() or ""
        except Exception:
            return ""

    def _apply_backend_check(self, name: str, result: dict):
        """Update the status / provider / model labels after a backend probe."""
        # Ignore a stale probe whose backend is no longer the selected one.
        pending = getattr(self, "_pending_probe_backend", None)
        if pending is not None and name != pending:
            return
        ok       = result.get("available", False)
        self._audit("backend_selected", backend=name, available=bool(ok))
        reason   = result.get("reason", "")
        info     = result.get("info", {}) or {}

        if ok:
            self.ai_agent.set_backend(name, verify=False)   # already verified
            self.ai_agent.cli_available = True
            ai_info = self.ai_agent.get_api_info()
        else:
            ai_info = {
                "status":   "Not Available",
                "provider": info.get("provider", name),
                "model":    info.get("model", ""),
                "instructions": reason or "Backend unavailable.",
            }

        if self.ai_status_label is not None:
            self.ai_status_label.config(
                text=ai_info["status"],
                foreground="green" if ok else "red",
            )
        if self.ai_provider_label is not None:
            self.ai_provider_label.config(text=ai_info.get("provider", ""))
        if self.ai_model_label is not None:
            self.ai_model_label.config(text=ai_info.get("model", ""))
        if hasattr(self, "ai_backend_reason_label"):
            txt = "" if ok else f"⚠  {reason}" if reason else ""
            self.ai_backend_reason_label.config(
                text=txt,
                foreground="red" if not ok else "gray",
            )
        self._sync_session_from_ui()

    def invalidate_cache(self, conn_name):
        """Public method to invalidate schema cache"""
        self.ai_agent.invalidate_cache(conn_name)

    def _build_monospace_text_grid(self, parent, wrap_mode: Literal["none", "char", "word"] = tk.NONE):  # type: ignore[assignment]
        """Text + vertical + horizontal scrollbars for wide, readable tabular output."""
        outer = ttk.Frame(parent)
        outer.pack(fill=tk.BOTH, expand=True)
        text = tk.Text(
            outer,
            wrap=wrap_mode,
            font=self.ui_font_mono,
            undo=False,
            padx=8,
            pady=8,
            relief=tk.FLAT,
        )
        vsb = ttk.Scrollbar(outer, orient=tk.VERTICAL, command=text.yview)
        hsb = ttk.Scrollbar(outer, orient=tk.HORIZONTAL, command=text.xview)
        text.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        text.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        outer.rowconfigure(0, weight=1)
        outer.columnconfigure(0, weight=1)
        return text

    def _open_ai_settings(self):
        from common.ui.tk.ai.ai_settings_ui import open_ai_settings

        def _reload():
            mc.reload()
            self.auto_execute_ai_loop = mc.get_bool(
                "ui.ai_query", "auto_execute_ai_loop", default=False
            )
            self.auto_execute_sql = mc.get_bool(
                "ui.ai_query", "auto_execute_summary_sql", default=False
            )
            self.sql_mode = normalize_sql_mode(
                mc.get("ui.ai_query", "default_sql_mode", "summary")
            )

        open_ai_settings(self.root, on_change=_reload)

    def create_ui(self):
        """Create UI for AI Query Assistant: split workspace / results, adjustable sashes."""
        main_frame = ttk.Frame(self.parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=6, pady=4)

        title_font_bold = (self.ui_font[0], self.ui_font[1] + 2, "bold")
        small_italic = (self.ui_font[0], max(9, int(self.ui_font[1]) - 1), "italic")

        ai_info = self.ai_agent.get_api_info()
        status_outer = make_collapsible_section(
            main_frame, "AI agent status", title_font_bold, expanded=True
        )
        status_inner = ttk.Frame(status_outer)
        status_inner.pack(fill=tk.X)

        ttk.Button(
            status_inner, text="\u2699 AI Settings",
            command=self._open_ai_settings,
        ).grid(row=0, column=99, sticky=tk.E, padx=4, pady=2)

        # ── row 0: Status + backend selector ─────────────────────────────
        status_color = "green" if ai_info["status"] == "Connected" else "red"
        ttk.Label(status_inner, text="Status:", font=title_font_bold).grid(
            row=0, column=0, sticky=tk.W, padx=4, pady=2
        )
        self.ai_status_label = ttk.Label(
            status_inner, text=ai_info["status"],
            foreground=status_color, font=self.ui_font,
        )
        self.ai_status_label.grid(row=0, column=1, sticky=tk.W, padx=4, pady=2)

        # Backend selector dropdown — lists ALL configured backends, with the
        # local LLM expanded into one entry per trained model ("<model> (local
        # <engine>)"). No probing happens here; selection triggers a check.
        options = self.ai_agent.list_backend_options()
        if options:
            ttk.Label(status_inner, text="Backend:", font=title_font_bold).grid(
                row=0, column=2, sticky=tk.W, padx=(16, 4), pady=2
            )
            backend_labels = [o["label"] for o in options]
            # Map the visible label back to the encoded selection value
            # (``local-llm::<model>`` for trained local models).
            self._backend_label_to_name = {
                o["label"]: o["value"] for o in options
            }

            active_value = self.ai_agent.get_active_backend_value()
            active_label = next(
                (l for l, v in self._backend_label_to_name.items() if v == active_value),
                "",
            )
            self.ai_backend_var = tk.StringVar(value=active_label)
            self.ai_backend_combo = ttk.Combobox(
                status_inner, textvariable=self.ai_backend_var,
                values=backend_labels, state="readonly", width=30,
            )
            self.ai_backend_combo.grid(row=0, column=3, sticky=tk.W, padx=4, pady=2)
            self.ai_backend_combo.bind("<<ComboboxSelected>>", self._on_backend_changed)

            # Fallback backend selector (row 1): failover when the primary is
            # unreachable AND the corrector that repairs wrong/failed SQL.
            self._fallback_none_label = "(none)"
            ttk.Label(status_inner, text="Fallback:", font=title_font_bold).grid(
                row=1, column=0, sticky=tk.W, padx=4, pady=2
            )
            self._fallback_label_to_value = {self._fallback_none_label: ""}
            self._fallback_label_to_value.update(
                {o["label"]: o["value"] for o in options}
            )
            fb_value = ""
            try:
                fb_value = self.ai_agent.get_fallback_backend_value()
            except Exception:
                fb_value = ""
            fb_label = next(
                (l for l, v in self._fallback_label_to_value.items() if v == fb_value),
                self._fallback_none_label,
            )
            self.ai_fallback_var = tk.StringVar(value=fb_label)
            self.ai_fallback_combo = ttk.Combobox(
                status_inner, textvariable=self.ai_fallback_var,
                values=[self._fallback_none_label] + backend_labels,
                state="readonly", width=30,
            )
            self.ai_fallback_combo.grid(row=1, column=1, columnspan=2,
                                        sticky=tk.W, padx=4, pady=2)
            self.ai_fallback_combo.bind(
                "<<ComboboxSelected>>", self._on_fallback_changed)
            ttk.Label(
                status_inner,
                text="(failover + corrects wrong/failed SQL)",
                foreground="gray", font=small_italic,
            ).grid(row=1, column=3, columnspan=2, sticky=tk.W, padx=4, pady=2)

        # Provider / model info  (always present — populated by selection)
        ttk.Label(status_inner, text="Provider:", font=title_font_bold).grid(
            row=0, column=4, sticky=tk.W, padx=(16, 4), pady=2
        )
        self.ai_provider_label = ttk.Label(
            status_inner, text=ai_info.get("provider", ""), font=self.ui_font
        )
        self.ai_provider_label.grid(row=0, column=5, sticky=tk.W, padx=4, pady=2)

        ttk.Label(status_inner, text="Model:", font=title_font_bold).grid(
            row=0, column=6, sticky=tk.W, padx=(16, 4), pady=2
        )
        self.ai_model_label = ttk.Label(
            status_inner, text=ai_info.get("model", ""), font=self.ui_font
        )
        self.ai_model_label.grid(row=0, column=7, sticky=tk.W, padx=4, pady=2)

        # ── row 2: dynamic reason / status note (filled after probing) ────
        self.ai_backend_reason_label = ttk.Label(
            status_inner, text="", foreground="gray",
            font=small_italic, wraplength=720,
        )
        self.ai_backend_reason_label.grid(
            row=2, column=0, columnspan=8, sticky=tk.W, padx=4, pady=2
        )

        # Workspace (left) | Results notebook (right)
        hpaned = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        hpaned.pack(fill=tk.BOTH, expand=True, pady=(6, 0))

        work_column = ttk.Frame(hpaned)
        hpaned.add(work_column, weight=1)

        results_column = ttk.Frame(hpaned)
        hpaned.add(results_column, weight=2)

        # Left column: question / actions (top) | SQL editor (bottom)
        vpaned = ttk.PanedWindow(work_column, orient=tk.VERTICAL)
        vpaned.pack(fill=tk.BOTH, expand=True)

        # Upper section - direct frame without canvas for proper resizing
        upper_left = ttk.Frame(vpaned, padding=(4, 0, 4, 0))
        vpaned.add(upper_left, weight=1)

        lower_left = ttk.Frame(vpaned, padding=(0, 0, 4, 0))
        vpaned.add(lower_left, weight=2)

        conn_row = ttk.Frame(upper_left)
        conn_row.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(conn_row, text="Connection:").pack(side=tk.LEFT, padx=(0, 6))
        self.ai_conn_combo = ttk.Combobox(conn_row, width=36, state="readonly")
        self.ai_conn_combo.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 6))

        question_frame = ttk.LabelFrame(
            upper_left, text="Question (natural language)", padding=6
        )
        question_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 6))

        ttk.Label(
            question_frame,
            text="Examples: show all users · count orders by status · products with price > 100",
            foreground="gray",
            font=self.ui_font,
        ).pack(anchor=tk.W, pady=(0, 4))

        # Question text box - initial height but can expand with frame
        self.ai_question_text = scrolledtext.ScrolledText(
            question_frame, wrap=tk.WORD, font=self.ui_font, height=4
        )
        self.ai_question_text.pack(fill=tk.BOTH, expand=True)

        # Action buttons with horizontal scrolling (optimized) - single row layout
        action_wrapper = ttk.Frame(upper_left)
        action_wrapper.pack(fill=tk.X, pady=(0, 6))
        action_frame = create_horizontal_scrollable(action_wrapper)

        ttk.Button(
            action_frame, text="Generate SQL", command=self.generate_sql_from_question
        ).pack(side=tk.LEFT, padx=(0, 4))
        self.execute_query_btn = ttk.Button(
            action_frame, text="Execute query", command=self.execute_ai_query
        )
        self.execute_query_btn.pack(side=tk.LEFT, padx=4)
        # Batch-run questions from a file: each question is loaded into the box
        # and its SQL generated in turn (auto-executed in uninterrupted mode,
        # otherwise it waits for Execute before moving to the next).
        ttk.Button(
            action_frame, text="Questions from file",
            command=self.load_questions_from_file_dialog,
        ).pack(side=tk.LEFT, padx=4)

        # ── RAG controls (beside Execute): keep the lightweight toggle + index.
        # The full RAG Manager lives in the workspace toolbar next to App Builder.
        self.use_rag_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            action_frame, text="Use RAG", variable=self.use_rag_var,
            command=self._on_use_rag_toggle,
        ).pack(side=tk.LEFT, padx=(8, 2))
        ttk.Button(
            action_frame, text="Index RAG", command=self.rag_index_current
        ).pack(side=tk.LEFT, padx=2)
        self.train_llm_btn = ttk.Button(
            action_frame, text="Train LLM", command=self.open_train_llm_dialog
        )
        self.train_llm_btn.pack(side=tk.LEFT, padx=2)

        self.stop_query_btn = ttk.Button(
            action_frame, text="⏹ Stop Query", command=self.stop_ai_query
        )
        # Stop button is initially hidden

        self.explain_query_btn = ttk.Button(
            action_frame, text="Explain query", command=self.explain_ai_query
        )
        self.explain_query_btn.pack(side=tk.LEFT, padx=4)
        ttk.Button(action_frame, text="Optimize", command=self.optimize_ai_query).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(action_frame, text="Clear all", command=self.clear_ai_query).pack(
            side=tk.LEFT, padx=4
        )

        sql_frame = ttk.LabelFrame(lower_left, text="Generated SQL", padding=6)
        sql_frame.pack(fill=tk.BOTH, expand=True)

        # SQL toolbar with horizontal scrolling (optimized)
        sql_toolbar_wrapper = ttk.Frame(sql_frame)
        sql_toolbar_wrapper.pack(fill=tk.X, pady=(0, 4))
        sql_toolbar = create_horizontal_scrollable(sql_toolbar_wrapper)

        ttk.Button(sql_toolbar, text="Copy SQL", command=self.copy_ai_sql).pack(
            side=tk.LEFT, padx=(0, 4)
        )
        ttk.Button(sql_toolbar, text="Edit SQL", command=self.edit_ai_sql).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(
            sql_toolbar, text="Send to SQL Editor", command=self.send_to_sql_editor
        ).pack(side=tk.LEFT, padx=4)

        # Review SQL button with dropdown menu
        self.review_sql_btn = ttk.Menubutton(sql_toolbar, text="Review SQL ▾")
        self.review_sql_btn.pack(side=tk.LEFT, padx=4)

        review_menu = tk.Menu(self.review_sql_btn, tearoff=0)
        self.review_sql_btn["menu"] = review_menu
        review_menu.add_command(
            label="📝 Write Review Rules", command=self.write_review_rules
        )
        review_menu.add_command(
            label="📁 Import SQL for Review", command=self.import_sql_for_review
        )
        review_menu.add_separator()
        review_menu.add_command(label="🔍 Run Review", command=self.run_sql_review)

        ttk.Button(
            sql_toolbar,
            text="SQL execution rules",
            command=self.write_sql_execution_rules,
        ).pack(side=tk.LEFT, padx=4)

        # Options menu button for connection management and cache control (moved here for better visibility)
        options_menu_btn = ttk.Menubutton(sql_toolbar, text="⟳ Options")
        options_menu_btn.pack(side=tk.LEFT, padx=4)

        options_menu = tk.Menu(options_menu_btn, tearoff=0)
        options_menu_btn.configure(menu=options_menu)
        self.ai_options_menu = options_menu

        options_menu.add_command(
            label="Refresh Connections", command=self.refresh_ai_connections
        )
        options_menu.add_command(
            label="Clear Schema Cache", command=self.clear_ai_schema_cache
        )
        options_menu.add_separator()
        options_menu.add_command(label="Cache Info", command=self.show_cache_info)
        options_menu.add_command(
            label="Show Schema Sent to AI", command=self.show_schema_sent_to_ai
        )
        options_menu.add_separator()
        options_menu.add_command(
            label="✓ Mask PII data", command=lambda: self._set_mask_pii(True)
        )
        options_menu.add_command(
            label="Unmask PII data", command=lambda: self._set_mask_pii(False)
        )
        self._pii_menu_mask_idx = options_menu.index("end") - 1
        self._pii_menu_unmask_idx = options_menu.index("end")
        self._refresh_pii_menu_labels()
        options_menu.add_separator()
        options_menu.add_command(
            label="Auto-execute SQL queries",
            command=lambda: self._set_auto_execute_sql(not self.auto_execute_sql),
        )
        self._auto_sql_menu_idx = options_menu.index("end")
        options_menu.add_separator()
        options_menu.add_command(
            label="Strict summary mode",
            command=lambda: self._set_sql_mode("strict_summary"),
        )
        options_menu.add_command(
            label="Summary mode", command=lambda: self._set_sql_mode("summary")
        )
        options_menu.add_command(
            label="Open mode", command=lambda: self._set_sql_mode("open")
        )
        self._strict_summary_mode_menu_idx = options_menu.index("end") - 2
        self._summary_mode_menu_idx = options_menu.index("end") - 1
        self._open_mode_menu_idx = options_menu.index("end")
        sess = self._session()
        if sess:
            sess.sql_mode = self.sql_mode
            sess.sql_execution_rules = self.sql_execution_rules
            sess.sql_modes_v2 = True
        self._refresh_auto_menu_labels()

        self.ai_sql_text = scrolledtext.ScrolledText(
            sql_frame, wrap=tk.WORD, font=self.ui_font_mono, height=5
        )
        self.ai_sql_text.pack(fill=tk.BOTH, expand=True)
        self.ai_sql_text.config(state=tk.DISABLED)

        results_wrap = ttk.LabelFrame(
            results_column, text="Results & AI insights", padding=6
        )
        results_wrap.pack(fill=tk.BOTH, expand=True)

        self.ai_results_notebook = ttk.Notebook(results_wrap)
        self.ai_results_notebook.pack(fill=tk.BOTH, expand=True)

        results_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(results_tab, text="Query results")
        self.ai_results_text = self._build_monospace_text_grid(
            results_tab, wrap_mode=tk.NONE
        )

        explanation_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(explanation_tab, text="Explanation")
        self.ai_explanation_text = scrolledtext.ScrolledText(
            explanation_tab, wrap=tk.WORD, font=self.ui_font, height=12
        )
        self.ai_explanation_text.pack(fill=tk.BOTH, expand=True)

        optimization_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(optimization_tab, text="Optimization")
        self.ai_optimization_text = scrolledtext.ScrolledText(
            optimization_tab, wrap=tk.WORD, font=self.ui_font, height=12
        )
        self.ai_optimization_text.pack(fill=tk.BOTH, expand=True)

        # RAG context ranking: which retrieved snippets fed this answer + scores.
        rag_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(rag_tab, text="RAG context")
        ttk.Label(
            rag_tab,
            text="Retrieved context ranked by relevance (only when 'Use RAG' is on):",
            foreground="gray", font=self.ui_font,
        ).pack(anchor=tk.W, padx=4, pady=(4, 2))
        self.ai_rag_text = scrolledtext.ScrolledText(
            rag_tab, wrap=tk.WORD, font=self.ui_font, height=12
        )
        self.ai_rag_text.pack(fill=tk.BOTH, expand=True)

        # Chat tab for follow-up conversations
        chat_tab = ttk.Frame(self.ai_results_notebook)
        self.ai_results_notebook.add(chat_tab, text="Chat")

        chat_paned = ttk.PanedWindow(chat_tab, orient=tk.VERTICAL)
        chat_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Conversation history in the top pane (read-only)
        chat_history_frame = ttk.Frame(chat_paned)
        chat_paned.add(chat_history_frame, weight=1)

        ttk.Label(
            chat_history_frame,
            text="Conversation History:",
            font=(self.ui_font[0], self.ui_font[1], "bold"),
        ).pack(anchor=tk.W, pady=(0, 5))

        self.ai_chat_history = scrolledtext.ScrolledText(
            chat_history_frame,
            wrap=tk.WORD,
            font=self.ui_font,
            height=12,
            state=tk.DISABLED,
        )
        self.ai_chat_history.pack(fill=tk.BOTH, expand=True)

        # Follow-up input in the bottom pane — always visible and editable
        followup_frame = ttk.LabelFrame(chat_paned, text="", padding=6)
        chat_paned.add(followup_frame, weight=0)

        followup_header = ttk.Frame(followup_frame)
        followup_header.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(
            followup_header,
            text="Send Follow-up Message",
            font=(self.ui_font[0], self.ui_font[1], "bold"),
        ).pack(side=tk.LEFT)
        self.uninterrupted_followups_var = tk.BooleanVar(
            value=self.auto_execute_ai_loop
        )
        # Clarified label: this enables the auto-execute AI loop, where the
        # assistant keeps generating/refining follow-ups on its own (up to the
        # max-iterations cap) until the problem is satisfied — no manual sends.
        ttk.Checkbutton(
            followup_header,
            text="Auto-run AI follow-ups (until satisfied)",
            variable=self.uninterrupted_followups_var,
            command=self._on_uninterrupted_followups_toggled,
        ).pack(side=tk.RIGHT)

        ttk.Label(
            followup_frame,
            text="Examples: 'Add a WHERE clause for active users' · 'Sort by date descending' · 'The query failed with error X'",
            foreground="gray",
            font=(self.ui_font[0], max(9, int(self.ui_font[1]) - 1)),
        ).pack(anchor=tk.W, pady=(0, 4))

        self.ai_followup_text = scrolledtext.ScrolledText(
            followup_frame, wrap=tk.WORD, font=self.ui_font, height=4
        )
        self.ai_followup_text.pack(fill=tk.BOTH, expand=True, pady=(0, 5))
        self.ai_followup_text.config(state=tk.NORMAL)

        followup_btn_frame = ttk.Frame(followup_frame)
        followup_btn_frame.pack(fill=tk.X)
        ttk.Button(
            followup_btn_frame, text="Send Follow-up", command=self.send_ai_followup
        ).pack(side=tk.LEFT, padx=(0, 4))
        ttk.Button(
            followup_btn_frame, text="Clear Chat", command=self.clear_ai_chat
        ).pack(side=tk.LEFT, padx=4)
        # Flag the current query so the fallback backend repairs it (and, when
        # auto-fix training is enabled, re-trains the local LLM on the fix).
        ttk.Button(
            followup_btn_frame, text="Flag incorrect query",
            command=self.flag_incorrect_query,
        ).pack(side=tk.LEFT, padx=4)
        ttk.Button(
            followup_btn_frame, text="Flag incorrect interpretation",
            command=self.flag_incorrect_interpretation,
        ).pack(side=tk.LEFT, padx=4)

        self.ai_chat_history.tag_config(
            "user",
            foreground="#1976D2",
            font=(self.ui_font[0], self.ui_font[1], "bold"),
        )
        self.ai_chat_history.tag_config(
            "assistant",
            foreground="#2E7D32",
            font=(self.ui_font[0], self.ui_font[1], "bold"),
        )
        self.ai_chat_history.tag_config(
            "system",
            foreground="#F57C00",
            font=(self.ui_font[0], self.ui_font[1], "italic"),
        )

        def _position_chat_sash(attempt=0):
            try:
                chat_paned.update_idletasks()
                h = chat_paned.winfo_height()
                if h <= 80 and attempt < 18:
                    self._safe_after(lambda: _position_chat_sash(attempt + 1), delay=45)
                    return
                if h > 120:
                    chat_paned.sashpos(0, max(80, h - 160))
            except tk.TclError:
                pass

        self.root.after_idle(_position_chat_sash)

        self.refresh_ai_connections()

        def _position_ai_sashes(attempt=0):
            try:
                hpaned.update_idletasks()
                vpaned.update_idletasks()
                w = hpaned.winfo_width()
                h = vpaned.winfo_height()
                if (w <= 160 or h <= 80) and attempt < 18:
                    self._safe_after(lambda: _position_ai_sashes(attempt + 1), delay=45)
                    return
                if w > 160:
                    hpaned.sashpos(0, max(300, min(int(w * 0.36), int(w * 0.52))))
                if h > 80:
                    vpaned.sashpos(0, max(200, int(h * 0.50)))
            except tk.TclError:
                pass

        self.root.after_idle(_position_ai_sashes)

    def refresh_ai_connections(self):
        """Refresh connection dropdown in AI tab"""
        # Reconnect invalidation: if a connection's underlying db_manager object
        # changed (a reconnect creates a fresh manager) or a connection was
        # dropped, drop its stale schema/context cache so the AI re-reads the
        # live schema instead of trusting pre-reconnect metadata.
        if self.ai_agent is not None:
            prev = getattr(self, "_cached_conn_ids", {})
            current: dict[str, int] = {}
            for cname, mgr in self.active_connections.items():
                current[cname] = id(mgr)
                if cname in prev and prev[cname] != current[cname]:
                    try:
                        self.ai_agent.invalidate_cache(cname)
                    except Exception:
                        pass
            for cname in prev:
                if cname not in current:
                    try:
                        self.ai_agent.invalidate_cache(cname)
                    except Exception:
                        pass
            self._cached_conn_ids = current

        # Check if AI tab UI has been created
        if self.ai_conn_combo is None:
            return

        connection_names = list(self.active_connections.keys())
        self.ai_conn_combo["values"] = connection_names

        selected = self.ai_conn_combo.get().strip()
        if selected and selected not in connection_names:
            self.ai_conn_combo.set("")
        if not connection_names:
            self.ai_conn_combo.set("")

    def clear_ai_schema_cache(self):
        """Clear schema cache for AI Query Assistant"""
        if not self.ai_agent:
            return

        # Check if UI is initialized
        if self.ai_conn_combo is None:
            return

        conn_name = self.ai_conn_combo.get()
        if conn_name:
            self.ai_agent.invalidate_cache(conn_name)
            messagebox.showinfo(
                "Cache Cleared", f"Schema cache cleared for {conn_name}"
            )
            self.update_status(f"Schema cache cleared for {conn_name}")
        else:
            self.ai_agent.invalidate_cache()  # Clear all
            messagebox.showinfo("Cache Cleared", "All schema caches cleared")
            self.update_status("All schema caches cleared")

    def show_cache_info(self):
        """Display cache information dialog"""
        if not hasattr(self, "ai_agent") or not self.ai_agent:
            messagebox.showinfo("Cache Info", "No cache data available")
            return

        cache_info = self.ai_agent.get_cache_info()

        if not cache_info:
            messagebox.showinfo(
                "Cache Info",
                "Schema cache is empty\n\nCache will be populated when you generate SQL queries.",
            )
            return

        # Build info message
        msg = "Cached Database Schemas:\n\n"
        for info in cache_info:
            timestamp = info["timestamp"].strftime("%H:%M:%S")
            msg += f"• {info['connection']} ({info['db_type']})\n"
            msg += f"  Tables: {info['table_count']} | Cached at: {timestamp}\n\n"

        msg += "Note: Cache is cleared when connections are disconnected.\n"
        msg += "Use 'Clear Schema Cache' to force refresh."

        messagebox.showinfo("Schema Cache Information", msg)

    def show_schema_sent_to_ai(self):
        """Display the schema that was sent to AI for the last query"""
        if not hasattr(self, "ai_agent") or not self.ai_agent:
            messagebox.showinfo("Schema Info", "AI agent not available")
            return

        schema_text = self.ai_agent.get_last_schema_sent()

        # Create a dialog window with scrollable text
        dialog = tk.Toplevel(self.root)
        dialog.title("Schema Sent to AI")
        width, height = get_window_size("ai_query")
        dialog.geometry(f"{width}x{height}")

        # Add text widget with scrollbar
        text_frame = ttk.Frame(dialog)
        text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        scrollbar = ttk.Scrollbar(text_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        text_widget = tk.Text(
            text_frame, wrap=tk.WORD, yscrollcommand=scrollbar.set, font=("Courier", 10)
        )
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=text_widget.yview)

        text_widget.insert(1.0, schema_text)
        text_widget.config(state=tk.DISABLED)

        # Add close button
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.RIGHT)

        # Copy to clipboard button
        def copy_to_clipboard():
            self.root.clipboard_clear()
            self.root.clipboard_append(schema_text)
            messagebox.showinfo("Copied", "Schema information copied to clipboard")

        ttk.Button(btn_frame, text="Copy to Clipboard", command=copy_to_clipboard).pack(
            side=tk.RIGHT, padx=(0, 5)
        )

    def extract_sql_from_markdown(self, text):
        """
        Extract SQL code from markdown-formatted text.
        If text has no markdown (no ``` fences), returns as-is.
        If text has markdown, removes fences and wraps non-code text in /* */.
        Also cleans up if ALL lines are wrapped in /* */.
        """
        if not text or not text.strip():
            return ""

        # Check if ALL lines are wrapped in /* ... */ (AI sometimes generates this)
        lines = text.strip().split("\n")
        all_commented = all(
            line.strip().startswith("/*") and line.strip().endswith("*/")
            for line in lines
            if line.strip()
        )

        if all_commented:
            # Extract SQL from within comments
            result = []
            for line in lines:
                line = line.strip()
                if line.startswith("/*") and line.endswith("*/"):
                    # Remove /* and */ and keep the SQL
                    sql_part = line[2:-2].strip()
                    if sql_part:
                        result.append(sql_part)
            return "\n".join(result)

        # Check if text contains any markdown code fences
        has_markdown = "```" in text

        if not has_markdown:
            # No markdown detected - treat entire text as SQL, return as-is
            return text.strip()

        # Process markdown: extract code from fences, comment out other text
        result = []
        in_code_block = False
        lines = text.split("\n")

        for line in lines:
            line_stripped = line.strip()

            # Check if this is a code fence (``` or ```sql or ```SQL)
            if line_stripped.startswith("```"):
                if in_code_block:
                    # End of code block
                    in_code_block = False
                else:
                    # Start of code block
                    in_code_block = True
                continue  # Don't include the fence markers

            if in_code_block:
                # Inside code block - ALL lines are SQL, keep exactly as-is
                result.append(line_stripped)
            else:
                # Outside code block - this is explanatory text, wrap in comment
                if line_stripped:
                    result.append(f"/* {line_stripped} */")
                else:
                    # Keep empty lines
                    if result:
                        result.append("")

        # Join all lines
        final_sql = "\n".join(result)

        # Clean up any "/* sql */" or "/* */" artifacts
        final_sql = re.sub(r"/\*\s*sql\s*\*/", "", final_sql, flags=re.IGNORECASE)
        final_sql = re.sub(r"/\*\s*\*/", "", final_sql)

        # Clean up multiple empty lines
        final_sql = re.sub(r"\n\n\n+", "\n\n", final_sql)

        return final_sql.strip()

    def _on_use_rag_toggle(self):
        """Wire the 'Use RAG' checkbox into the agent's grounding flow."""
        enabled = bool(self.use_rag_var.get())
        try:
            if hasattr(self.ai_agent, "set_use_rag"):
                self.ai_agent.set_use_rag(enabled)
        except Exception:
            pass
        self.update_status(
            "RAG grounding ON — Generate SQL will use your indexed schema/examples."
            if enabled else "RAG grounding OFF."
        )

    def rag_index_current(self):
        """Build/refresh the RAG index for the selected connection."""
        from common.ui.tk.ai.rag_panel import rag_index_current

        rag_index_current(self)

    def open_rag_manager(self):
        """Open the RAG management dialog (status/search/examples/glossary)."""
        from common.ui.tk.ai.rag_panel import open_rag_panel

        open_rag_panel(self)

    def open_llm_trainer(self):
        """Open the local LLM train/status/generate dialog."""
        from common.ui.tk.ai.llm_panel import open_llm_panel

        open_llm_panel(self)

    def _llm_training_service(self):
        from ai_assistant.llm.training_service import LlmTrainingService

        try:
            from common.ui.tk.ai.build_apps_dialogs import _ActiveConnectionCore

            return LlmTrainingService(_ActiveConnectionCore(self))
        except Exception:
            return LlmTrainingService(None)

    def open_train_llm_dialog(self):
        """Open the AI Query session training dialog.

        Lets you pick the target model + engine once; the choice persists for
        this AI Query session (including chat and follow-up training). RAG use
        follows the toolbar 'Use RAG' toggle. Richer DB-mining / manual training
        lives in the separate Build or Train LLM panel.
        """
        parent = getattr(self, "root", None) or getattr(self, "parent", None)
        try:
            from ai_assistant.llm.service import LlmService

            svc = LlmService()
            models = [
                m.get("name", "") for m in (svc.list_models().get("models") or [])
                if m.get("name")
            ]
            eng_info = svc.engines().get("engines") or []
        except Exception:
            models = []
            eng_info = []

        dlg = tk.Toplevel(parent)
        dlg.title("Train LLM — AI Query Assistant")
        dlg.transient(parent)
        frm = make_scrollable(dlg)
        frm.configure(padding=12)

        ttk.Label(
            frm,
            text="Train your local NL→SQL model from this AI Query session.\n"
                 "The model and engine below are reused for the whole session — "
                 "current Q→SQL and chat/follow-up training.",
            justify=tk.LEFT, foreground="gray", wraplength=420,
        ).grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 8))

        cur_model = (getattr(self, "_aiqa_train_model", "")
                     or (models[0] if models else "default"))
        ttk.Label(frm, text="Target model:").grid(row=1, column=0, sticky=tk.W, pady=2)
        model_var = tk.StringVar(value=cur_model)
        ttk.Combobox(
            frm, textvariable=model_var, values=models, width=30,
        ).grid(row=1, column=1, sticky=tk.W, pady=2)  # editable: pick existing or type new

        eng_labels = ["(config default)"] + [
            (e["name"] if e.get("available") else f"{e['name']} (unavailable)")
            for e in eng_info
        ]

        def _label_to_engine(label: str):
            if not label or label.startswith("(config"):
                return None
            return label.split(" ")[0]

        cur_engine = getattr(self, "_aiqa_train_engine", None)
        cur_engine_label = "(config default)"
        for e in eng_info:
            if e["name"] == cur_engine:
                cur_engine_label = (
                    e["name"] if e.get("available") else f"{e['name']} (unavailable)")
        ttk.Label(frm, text="Engine:").grid(row=2, column=0, sticky=tk.W, pady=2)
        engine_var = tk.StringVar(value=cur_engine_label)
        ttk.Combobox(
            frm, textvariable=engine_var, values=eng_labels, width=30,
            state="readonly",
        ).grid(row=2, column=1, sticky=tk.W, pady=2)

        rag_on = bool(getattr(self, "use_rag_var", None) and self.use_rag_var.get())
        ttk.Label(
            frm,
            text=f"Use RAG: {'on' if rag_on else 'off'} "
                 "(follows the 'Use RAG' toggle in the toolbar)",
            foreground="gray",
        ).grid(row=3, column=0, columnspan=2, sticky=tk.W, pady=(6, 8))

        # Continuous auto-fix training: re-train this model whenever the local
        # LLM produces a query that fails/ is flagged on the CONNECTED database
        # and the fallback backend supplies a corrected, verified query.
        autofix_var = tk.BooleanVar(value=bool(getattr(self, "autofix_train", False)))

        def _on_autofix_toggle() -> None:
            self._set_autofix_train(autofix_var.get())

        ttk.Checkbutton(
            frm,
            text="Auto-train on fallback-corrected queries (connected DB only)",
            variable=autofix_var,
            command=_on_autofix_toggle,
        ).grid(row=6, column=0, columnspan=2, sticky=tk.W, pady=(0, 2))
        ttk.Label(
            frm,
            text="When a query this local LLM generated fails to execute (or is "
                 "flagged), the fallback backend's corrected query is verified and "
                 "used to incrementally re-train this model.",
            foreground="gray", wraplength=420, justify=tk.LEFT,
        ).grid(row=7, column=0, columnspan=2, sticky=tk.W, pady=(0, 8))

        status_var = tk.StringVar(value="")
        ttk.Label(
            frm, textvariable=status_var, foreground="#1a7f37",
            wraplength=420, justify=tk.LEFT,
        ).grid(row=5, column=0, columnspan=2, sticky=tk.W, pady=(8, 0))

        def _set_status(msg: str) -> None:
            status_var.set(msg)
            try:
                self.update_status(msg)
            except Exception:
                pass

        def _save_config() -> None:
            self._aiqa_train_model = model_var.get().strip() or "default"
            self._aiqa_train_engine = _label_to_engine(engine_var.get())

        def do_current() -> None:
            _save_config()
            self._aiqa_train_current(_set_status)

        def do_chat() -> None:
            _save_config()
            self._aiqa_train_chat(_set_status)

        btns = ttk.Frame(frm)
        btns.grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=(4, 0))
        ttk.Button(btns, text="Train on current Q→SQL", command=do_current).pack(side=tk.LEFT)
        ttk.Button(
            btns, text="Train from chat (incl. follow-ups)", command=do_chat,
        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Close", command=dlg.destroy).pack(side=tk.LEFT, padx=6)
        return dlg

    def _aiqa_session_model(self) -> str:
        return (getattr(self, "_aiqa_train_model", "") or "default")

    def _aiqa_session_engine(self):
        return getattr(self, "_aiqa_train_engine", None)

    def _aiqa_train_current(self, status):
        """Train the session model from the current NL question and generated SQL."""
        conn = self.ai_conn_combo.get().strip()
        question = self.ai_question_text.get("1.0", tk.END).strip()
        sql = self.ai_sql_text.get("1.0", tk.END).strip()
        if not question or not sql:
            status("Generate SQL first, then train on it.")
            return
        model = self._aiqa_session_model()
        engine = self._aiqa_session_engine()
        use_rag = bool(self.use_rag_var.get()) if hasattr(self, "use_rag_var") else False
        status(f"Training '{model}' (engine={engine or 'config default'}) from current Q→SQL…")

        def work():
            return self._llm_training_service().train_pairs(
                [{"question": question, "sql": sql, "description": "AI Query current turn"}],
                names=[model],
                engine=engine,
                connection=conn,
                use_rag=use_rag,
            )

        def done(r, e):
            if e or not (r or {}).get("ok"):
                status(e or (r or {}).get("error") or "LLM training failed")
                return
            status(f"Trained '{model}' from current Q→SQL ({r.get('pairs')} pair(s)).")

        self._run_llm_training_bg(work, done)

    def _aiqa_train_chat(self, status):
        """Train the session model from captured chat turns, including follow-ups."""
        conn = self.ai_conn_combo.get().strip()
        if not conn:
            status("Select a connection first.")
            return
        model = self._aiqa_session_model()
        engine = self._aiqa_session_engine()
        use_rag = bool(self.use_rag_var.get()) if hasattr(self, "use_rag_var") else False
        status(f"Training '{model}' (engine={engine or 'config default'}) from chat/follow-ups…")

        def work():
            extra = []
            question = self.ai_question_text.get("1.0", tk.END).strip()
            sql = self.ai_sql_text.get("1.0", tk.END).strip()
            explanation = ""
            if self.ai_explanation_text is not None:
                explanation = self.ai_explanation_text.get("1.0", tk.END).strip()
            if question and sql:
                # Carry the explanation as the pair description so chat/follow-up
                # turns train from the explanation too (it travels into RAG).
                desc = explanation or "AI Query current Generated SQL"
                if desc.lower().startswith("(no explanation"):
                    desc = "AI Query current Generated SQL"
                extra.append({
                    "question": question,
                    "sql": sql,
                    "description": desc,
                })
            return self._llm_training_service().train_llm({
                "mode": "from_database",
                "connections": [conn],
                "train_llm": [model],
                "train_engine": engine or "",
                "mine_db": False,
                "use_rag": use_rag,
                "include_sample": False,
                "extra_pairs": extra,
            })

        def done(r, e):
            if e or not (r or {}).get("ok"):
                status(e or (r or {}).get("error") or "LLM training failed")
                return
            status(f"Trained '{model}' from chat/follow-ups ({r.get('pairs')} pair(s)).")

        self._run_llm_training_bg(work, done)

    def _run_llm_training_bg(self, work, done):
        def runner():
            try:
                result, err = work(), None
            except Exception as exc:  # noqa: BLE001
                result, err = None, str(exc)
            self._safe_after(lambda: done(result, err))

        self._start_worker(runner, name="ai-llm-train")

    # ------------------------------------------------------------------
    # Fallback correction + continuous auto-fix training
    # ------------------------------------------------------------------

    def _set_autofix_train(self, enabled: bool) -> None:
        """Toggle continuous auto-fix training (persisted for this session)."""
        self.autofix_train = bool(enabled)
        try:
            from ai_query.service import _update_ai_state
            _update_ai_state({"auto_fix_train": self.autofix_train})
        except Exception:
            pass
        self.update_status(
            "Auto-fix training "
            + ("enabled (connected DB only)." if self.autofix_train else "disabled.")
        )

    def _autofix_train_enabled(self) -> bool:
        return bool(getattr(self, "autofix_train", False))

    def _fallback_is_local_llm(self) -> bool:
        return self.fallback_backend_value().split("::")[0] == "local-llm"

    def _train_target_model(self, train_target: str) -> str:
        """Resolve which local model an auto-fix pair should train."""
        if train_target == "primary":
            try:
                return (self.ai_agent.get_active_local_model()
                        or self._aiqa_session_model())
            except Exception:
                return self._aiqa_session_model()
        if train_target == "fallback":
            value = self.fallback_backend_value()
            name, _, model = value.partition("::")
            if name.strip() != "local-llm":
                return ""
            if model.strip():
                return model.strip()
            try:
                return self.ai_agent.get_active_local_model() or "default"
            except Exception:
                return "default"
        return ""

    def _current_qsc(self):
        """Return (question, sql, conn, db_manager) for the current turn or None."""
        conn = self.ai_conn_combo.get().strip()
        question = self.ai_question_text.get(1.0, tk.END).strip()
        sql = self.ai_sql_text.get(1.0, tk.END).strip()
        if not question or not sql:
            messagebox.showwarning(
                "Nothing to flag",
                "Generate a SQL query first, then flag it.")
            return None
        if not conn or conn not in self.active_connections:
            messagebox.showwarning(
                "No Connection", "Select a valid database connection first!")
            return None
        return question, sql, conn, self.active_connections[conn]

    @staticmethod
    def _looks_read_only(sql: str) -> bool:
        body = "\n".join(
            ln for ln in (sql or "").splitlines()
            if not ln.strip().startswith("--")
        ).strip().lstrip("(").lower()
        return body.startswith(
            ("select", "with", "show", "explain", "pragma", "describe", "desc "))

    def _set_question_text(self, text: str) -> None:
        """Replace the Questions (Natural language) box with *text*.

        Used after a chat follow-up so the NL box tracks the brief that produced
        the query now in the Generated SQL preview (also keeps the question↔SQL
        pair consistent for 'Train on current Q→SQL' and auto-fix training)."""
        text = (text or "").strip()
        if not text or self.ai_question_text is None:
            return
        try:
            state = str(self.ai_question_text.cget("state"))
            if state != "normal":
                self.ai_question_text.config(state=tk.NORMAL)
            self.ai_question_text.delete(1.0, tk.END)
            self.ai_question_text.insert(1.0, text)
            if state != "normal":
                self.ai_question_text.config(state=state)
        except Exception:
            pass

    def _post_results_note(self, text: str) -> None:
        if self.ai_results_text is None:
            return
        try:
            self.ai_results_text.delete(1.0, tk.END)
            self.ai_results_text.insert(1.0, text + "\n")
            self._select_results_tab("results")
        except Exception:
            pass

    def _write_corrected_sql_to_editor(self, sql: str, backend_used: str,
                                       mode: str) -> None:
        kind = "interpretation fix" if mode == "interpretation" else "correction"
        comment = (
            f"-- Corrected by fallback backend "
            f"'{backend_used or 'fallback'}' ({kind}). Review and execute.\n"
        )
        try:
            self.ai_sql_text.config(state=tk.NORMAL)
            self.ai_sql_text.delete(1.0, tk.END)
            self.ai_sql_text.insert(1.0, comment + sql)
            self.ai_sql_text.config(state=tk.DISABLED)
        except Exception:
            pass

    def _run_query_correction(self, question, sql, conn, db_manager, *,
                              mode, error_text, corrector_value,
                              train_target, note=None):
        """Repair *sql* via *corrector_value* on a worker thread, then surface
        the result in the editor (and optionally verify + auto-train)."""
        if self._autofix_in_progress:
            self.update_status("A correction is already in progress…")
            return
        self._autofix_in_progress = True
        label = corrector_value.split("::")[0] if corrector_value else "fallback"
        self._post_results_note(
            note or (
                f"Correct query is being generated by the fallback backend "
                f"'{label}', please wait — it will appear in the Generated SQL "
                f"editor for you to execute."))
        self.update_status(f"Generating corrected query via '{label}'…")

        def work():
            return self.ai_agent.correct_sql(
                question, sql,
                db_type=getattr(db_manager, "db_type", "") or "",
                error_text=error_text, mode=mode,
                connection_name=conn, db_manager=db_manager,
                backend_value=corrector_value,
            )

        def done(r, e):
            self._autofix_in_progress = False
            if e or not (r or {}).get("sql"):
                msg = e or (r or {}).get("error") or "no corrected query produced."
                self._post_results_note(f"Fallback correction failed: {msg}")
                self.update_status("Fallback correction failed")
                return
            corrected = r["sql"]
            used = r.get("backend_used") or label
            self._write_corrected_sql_to_editor(corrected, used, mode)
            self._last_sql_corrected = True
            self._add_chat_message(
                "system",
                f"✓ Fallback backend '{used}' produced a corrected query "
                f"(sent to the Generated SQL editor).")
            self.update_status("Corrected query ready — review and execute.")
            # Queue (do NOT train yet): we only train once the user executes the
            # corrected query and it succeeds on the connected DB.
            if self._autofix_train_enabled() and train_target:
                model = self._train_target_model(train_target)
                if model:
                    self._queue_autofix_train(model, conn)
            if self._auto_execute_sql_enabled():
                self.execute_ai_query()

        self._run_llm_training_bg(work, done)

    def _queue_autofix_train(self, model: str, connection: str) -> None:
        """Queue an auto-fix training pair to run after a successful execute."""
        if not model or not connection:
            return
        self._pending_autofix_train = {"model": model, "connection": connection}
        self._add_chat_message(
            "system",
            f"Auto-fix training queued for '{model}' — it will train on this "
            f"query only after you execute it successfully on '{connection}'.")

    def _run_pending_autofix_train(self, executed_sql: str,
                                   connection: str) -> None:
        """Train the queued local model after the corrected query executed OK.

        Called only from the successful-execution path. Trains on the actual
        query that just ran (cleaned of the correction comment), the current
        natural-language question, and the current explanation as description —
        and only when the executed connection matches the queued one."""
        pending = getattr(self, "_pending_autofix_train", None)
        if not pending:
            return
        if connection and pending.get("connection") and \
                connection != pending["connection"]:
            return
        # Consume the queue regardless of the outcome below.
        self._pending_autofix_train = None
        model = pending.get("model")
        if not model:
            return
        clean_sql = "\n".join(
            ln for ln in (executed_sql or "").splitlines()
            if not ln.strip().startswith("--")
        ).strip()
        question = self.ai_question_text.get(1.0, tk.END).strip()
        if not clean_sql or not question:
            return
        # Pair the corrected SQL with its explanation (and any insights) so the
        # model learns the rationale too — mirrors 'Train on current Q→SQL'.
        description = ""
        if self.ai_explanation_text is not None:
            description = self.ai_explanation_text.get(1.0, tk.END).strip()
        if not description or description.lower().startswith("(no explanation"):
            description = f"Auto-fix correction ({connection})"
        use_rag = (bool(self.use_rag_var.get())
                   if hasattr(self, "use_rag_var") else False)

        def work():
            return self._llm_training_service().train_pairs(
                [{"question": question, "sql": clean_sql,
                  "description": description}],
                names=[model], engine=None, connection=connection,
                use_rag=use_rag,
            )

        def done(r, e):
            if e or not (r or {}).get("ok"):
                self.update_status(
                    f"Auto-fix training skipped: "
                    f"{e or (r or {}).get('error') or 'unknown error'}")
                return
            self._add_chat_message(
                "system",
                f"✓ Auto-trained '{model}' on the executed query "
                f"({r.get('pairs')} pair).")
            self.update_status(
                f"Auto-trained '{model}' on the successfully-executed query.")
            try:
                self.refresh_backend_options()
            except Exception:
                pass

        self.update_status(f"Auto-training '{model}' on the executed query…")
        self._run_llm_training_bg(work, done)

    def flag_incorrect_query(self):
        """Mark the current query as incorrectly generated (syntax/logic).

        Local-LLM primary → fallback repairs it (and optionally re-trains the
        local model). Other primary → suggest a chat follow-up; if auto-fix
        training is on and the fallback is a local LLM, train it on the pair."""
        data = self._current_qsc()
        if not data:
            return
        question, sql, conn, db_manager = data
        primary = self.ai_agent.get_active_backend_name()
        if primary == "local-llm":
            fb = self.fallback_backend_value()
            if not fb:
                messagebox.showwarning(
                    "No fallback backend",
                    "Select a fallback backend (top of the panel) so it can "
                    "repair queries from the local LLM.")
                return
            self._add_chat_message(
                "system",
                "Flagged as an incorrect query — asking the fallback backend "
                "to repair it.")
            self._run_query_correction(
                question, sql, conn, db_manager,
                mode="syntax",
                error_text=("User flagged this query as incorrect (syntax or "
                            "logic — e.g. wrong joins, subqueries, date handling)."),
                corrector_value=fb, train_target="primary")
        else:
            self.ai_results_notebook.select(self._chat_tab_index())
            self._add_chat_message(
                "system",
                "Flagged as incorrect. Send a follow-up below describing what's "
                "wrong and the primary backend will fix it.")
            # Queue training of the fallback local LLM on the corrected pair —
            # it only trains after the fixed query executes successfully.
            if self._autofix_train_enabled() and self._fallback_is_local_llm():
                model = self._train_target_model("fallback")
                if model:
                    self._queue_autofix_train(model, conn)

    def flag_incorrect_interpretation(self):
        """Mark the current query as a wrong interpretation of the question.

        Local-LLM primary → fallback re-answers the question. Other primary →
        re-send the question to the primary to fix. Auto-fix training (when on)
        trains the relevant local model on the corrected pair."""
        data = self._current_qsc()
        if not data:
            return
        question, sql, conn, db_manager = data
        primary = self.ai_agent.get_active_backend_name()
        if primary == "local-llm":
            fb = self.fallback_backend_value()
            if not fb:
                messagebox.showwarning(
                    "No fallback backend",
                    "Select a fallback backend (top of the panel) so it can "
                    "re-answer the question more accurately.")
                return
            self._add_chat_message(
                "system",
                "Flagged as a wrong interpretation — asking the fallback "
                "backend to re-answer the question.")
            self._run_query_correction(
                question, sql, conn, db_manager,
                mode="interpretation", error_text="",
                corrector_value=fb, train_target="primary",
                note=("A more accurate query is being generated by the fallback "
                      "backend, please wait — it will appear in the Generated "
                      "SQL editor for you to execute."))
        else:
            primary_value = self.ai_agent.get_active_backend_value()
            self._add_chat_message(
                "system",
                "Flagged as misunderstood — re-sending to the primary backend "
                "to correct the interpretation.")
            self._run_query_correction(
                question, sql, conn, db_manager,
                mode="interpretation", error_text="",
                corrector_value=primary_value, train_target="fallback",
                note=("A more accurate query is being generated, please wait — "
                      "it will appear in the Generated SQL editor for you to "
                      "execute."))

    def _results_tab_index(self, text: str) -> int:
        """Return the index of the results-notebook tab with *text* (0 if absent).

        Looking tabs up by label keeps selection correct even when tabs are
        added/reordered (e.g. the Review tab is created lazily), instead of
        relying on brittle hardcoded indices.
        """
        try:
            for idx in range(self.ai_results_notebook.index("end")):
                if self.ai_results_notebook.tab(idx, "text") == text:
                    return idx
        except Exception:
            pass
        return 0

    # Map short logical names to the tab labels used at creation time.
    _RESULTS_TAB_LABELS = {
        "results": "Query results",
        "explanation": "Explanation",
        "optimization": "Optimization",
        "rag": "RAG context",
        "chat": "Chat",
        "review": "Review",
    }

    def _select_results_tab(self, name: str) -> None:
        label = self._RESULTS_TAB_LABELS.get(name, name)
        try:
            self.ai_results_notebook.select(self._results_tab_index(label))
        except Exception:
            pass

    def _chat_tab_index(self) -> int:
        try:
            for idx in range(self.ai_results_notebook.index("end")):
                if self.ai_results_notebook.tab(idx, "text") == "Chat":
                    return idx
        except Exception:
            pass
        return 0

    def _maybe_autocorrect_on_failure(self, error: str) -> None:
        """When a local-LLM query fails to execute and a fallback is set, auto
        repair it via the fallback (connected-DB scoped). Guarded against loops
        so a corrected query that still fails is not re-corrected endlessly."""
        if self._autofix_in_progress or self._last_sql_corrected:
            return
        try:
            primary = self.ai_agent.get_active_backend_name()
        except Exception:
            return
        if primary != "local-llm":
            return
        fb = self.fallback_backend_value()
        if not fb:
            return
        conn = self.ai_conn_combo.get().strip()
        question = self.ai_question_text.get(1.0, tk.END).strip()
        sql = self.ai_sql_text.get(1.0, tk.END).strip()
        if not question or not sql or not conn or conn not in self.active_connections:
            return
        # Keep the primary LLM's execution error visible above the "generating
        # correction…" note (the note otherwise replaces the results box).
        label = fb.split("::")[0]
        err_text = (error or "Query failed to execute.").strip()
        note = (
            f"The query generated by the primary backend failed to execute:\n\n"
            f"{err_text}\n\n"
            f"Correct query is being generated by the fallback backend "
            f"'{label}', please wait — it will appear in the Generated SQL "
            f"editor for you to execute."
        )
        self._run_query_correction(
            question, sql, conn, self.active_connections[conn],
            mode="syntax", error_text=err_text,
            corrector_value=fb, train_target="primary", note=note)

    def generate_sql_from_question(self):
        """Generate SQL from natural language question"""
        if not self.ai_agent.is_available():
            messagebox.showerror(
                "AI Not Available",
                "AI agent is not configured.\n\nSet one of these environment variables:\n"
                "- OPENAI_API_KEY\n- ANTHROPIC_API_KEY\n- GOOGLE_API_KEY",
            )
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name:
            messagebox.showwarning(
                "No Connection", "Please select a database connection first!"
            )
            return

        question = self.ai_question_text.get(1.0, tk.END).strip()
        if not question:
            messagebox.showwarning("No Question", "Please enter a question!")
            return

        if conn_name not in self.active_connections:
            messagebox.showerror("Error", "Selected connection not found!")
            return

        # A freshly generated query is not (yet) a fallback-corrected one.
        self._last_sql_corrected = False
        self._pending_autofix_train = None

        db_manager = self.active_connections[conn_name]

        if self.generation_running:
            messagebox.showinfo(
                "Generation in progress",
                "A SQL generation is already running. Use 'Stop' to cancel it first.",
            )
            return

        # Show processing message
        self.ai_sql_text.config(state=tk.NORMAL)
        self.ai_sql_text.delete(1.0, tk.END)
        self.ai_sql_text.insert(1.0, "Generating SQL query...\n")
        self.ai_sql_text.config(state=tk.DISABLED)
        self.update_status("Generating SQL...")

        # Mark busy so the Stop button appears and a second generation can't start.
        self.generation_running = True
        self.generation_cancelled = False
        self._update_busy_ui()

        # Run in thread
        thread = threading.Thread(
            target=self._generate_sql_thread, args=(question, db_manager, conn_name),
            daemon=True,
        )
        self._generation_thread = thread
        self._worker_threads.append(thread)
        thread.start()

    # ------------------------------------------------------------------
    # Batch: run a list of questions from a file, one at a time
    # ------------------------------------------------------------------

    def load_questions_from_file_dialog(self):
        """Pick a questions file and run each question through Generate SQL."""
        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            messagebox.showwarning(
                "No Connection", "Please select a database connection first!")
            return
        if self._batch_active:
            if not messagebox.askyesno(
                "Batch in progress",
                "A batch run is already in progress. Stop it and load a new file?"):
                return
            self._batch_active = False
        path = filedialog.askopenfilename(
            title="Select a questions file",
            filetypes=[
                ("Questions", "*.txt *.csv *.json *.jsonl *.md"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return
        try:
            from ai_assistant.llm.question_import import load_questions_from_file
            questions = load_questions_from_file(path)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Could not read file", str(exc))
            return
        if not questions:
            messagebox.showwarning(
                "No questions", "No questions were found in that file.")
            return
        self._start_questions_batch(questions)

    def _start_questions_batch(self, questions: list[str]):
        # Replace the list+index together so a reload can't leave the index
        # pointing into a stale/over-long list (skipped or repeated questions).
        import contextlib
        lock = getattr(self, "_batch_lock", None) or contextlib.nullcontext()
        with lock:
            self._batch_questions = list(questions)
            self._batch_index = 0
            self._batch_active = True
            self._batch_step_pending = False
        self._add_chat_message(
            "system",
            f"▶ Batch started: {len(self._batch_questions)} question(s). "
            "Uninterrupted/auto-execute runs each automatically; otherwise "
            "click Execute to advance to the next.")
        self._batch_run_current()

    def _batch_run_current(self):
        if not self._batch_active:
            return
        if self._batch_index >= len(self._batch_questions):
            self._batch_finish()
            return
        question = self._batch_questions[self._batch_index]
        # Decide per step so a mid-batch toggle is honoured.
        self._batch_auto = (
            self._auto_execute_ai_loop_enabled()
            or self._auto_execute_sql_enabled())
        self._set_question_text(question)
        self._batch_step_pending = True
        self.update_status(
            f"Batch: question {self._batch_index + 1}/{len(self._batch_questions)}"
            + ("" if self._batch_auto else " — click Execute to continue"))
        self.generate_sql_from_question()

    def _batch_on_step_done(self, *, auto_only=False, manual_only=False,
                            force=False):
        """Advance the batch when the current question's work has finished."""
        if not self._batch_active or not self._batch_step_pending:
            return
        if not force:
            if auto_only and not self._batch_auto:
                return
            if manual_only and self._batch_auto:
                return
        self._batch_step_pending = False
        self._batch_index += 1
        # Small delay so the UI/state settles before the next generation.
        try:
            self.root.after(400, self._batch_run_current)
        except Exception:
            self._batch_run_current()

    def _batch_finish(self):
        import contextlib
        lock = getattr(self, "_batch_lock", None) or contextlib.nullcontext()
        with lock:
            total = len(self._batch_questions)
            self._batch_active = False
            self._batch_step_pending = False
            self._batch_questions = []
            self._batch_index = 0
        if total:
            self._add_chat_message(
                "system", f"✓ Batch complete: {total} question(s) processed.")
        self.update_status("Batch complete.")

    def _generate_sql_thread(self, question, db_manager, connection_name):
        """Thread for SQL generation"""
        try:
            if self.generation_cancelled:
                return
            # start_new_conversation returns a dict with 'sql', 'explanation', 'error'
            self._sync_session_from_ui()
            if self.orchestrator and self.session_id:
                out = self.orchestrator.parse_and_execute(
                    self.session_id,
                    question,
                    db_manager,
                    connection_name,
                    mode="ask",
                )
                for msg in out.get("cross_tab_messages") or []:
                    self.root.after(0, self._add_chat_message, "system", msg)
                if out.get("skip_local_ai"):
                    result = out.get("result") or {}
                else:
                    result = out.get("result") or {}
            else:
                result = self.ai_agent.start_new_conversation(
                    question, db_manager, connection_name, session_id=self.session_id
                )

            # Cooperative cancel: a backend call can't be interrupted mid-flight,
            # so if the user cancelled while it ran, discard the result instead of
            # applying it to the UI.
            if self.generation_cancelled:
                self.root.after(0, self._handle_generation_cancelled)
                return

            if result["error"]:
                self.root.after(0, messagebox.showerror, "Error", result["error"])
                self.root.after(
                    0,
                    self._add_chat_message,
                    "system",
                    f"❌ Error: {result['error']}",
                )
                self.root.after(0, self._clear_ai_sql)
                self.root.after(0, self.update_status, "SQL generation failed")
                # Don't stall a batch on a generation error — skip to the next.
                self.root.after(0, lambda: self._batch_on_step_done(force=True))
                return

            self.root.after(0, self._start_post_ai_pipeline, result, question)

            # Add initial message to chat history
            self.root.after(0, self._add_chat_message, "user", question)
            self.root.after(
                0,
                self._add_chat_message,
                "assistant",
                f"Summary SQL:\n```sql\n{result.get('summary_sql') or result.get('sql')}\n```\n\n{result.get('explanation')}",
            )
            self.root.after(
                0,
                self._add_chat_message,
                "system",
                "💡 You can now send follow-up messages to refine this query in the Chat tab.",
            )
            self.root.after(0, self.update_status, "SQL query generated successfully")

        except Exception as e:
            import traceback

            if self.generation_cancelled:
                self.root.after(0, self._handle_generation_cancelled)
                return
            error_msg = f"Error generating SQL:\n{str(e)}\n\n{traceback.format_exc()}"
            print(f"\n=== ERROR in SQL generation ===", file=sys.stderr)
            print(error_msg, file=sys.stderr)
            print("=" * 30, file=sys.stderr)
            self.root.after(0, messagebox.showerror, "SQL Generation Error", error_msg)
            self.root.after(0, self._clear_ai_sql)
            self.root.after(0, self.update_status, "SQL generation failed")
            self.root.after(0, lambda: self._batch_on_step_done(force=True))
        finally:
            self.root.after(0, self._finish_generation)

    def _finish_generation(self):
        """Reset generation state + busy UI after a generation worker finishes."""
        self.generation_running = False
        self._generation_thread = None
        self._prune_worker_threads()
        self._update_busy_ui()

    def _handle_generation_cancelled(self):
        """UI updates when a SQL generation was cancelled by the user."""
        try:
            self._clear_ai_sql()
        except Exception:
            pass
        self.update_status("SQL generation cancelled")

    def _prune_worker_threads(self):
        """Drop finished worker threads from the tracking list."""
        try:
            self._worker_threads = [
                t for t in self._worker_threads if t.is_alive()
            ]
        except Exception:
            self._worker_threads = []

    def _start_worker(self, target, *, args=(), name=None):
        """Create, track and start a daemon worker thread.

        All background workers MUST be created through this so ``shutdown()`` can
        join them on tab close — otherwise their ``root.after(...)`` callbacks
        could fire against destroyed widgets and raise TclError.
        """
        self._prune_worker_threads()
        thread = threading.Thread(target=target, args=args, name=name, daemon=True)
        try:
            self._worker_threads.append(thread)
        except Exception:
            # Tracking must not be silently lost — recreate the list and retry so
            # the worker is always joinable on shutdown.
            self._worker_threads = [thread]
        thread.start()
        return thread

    def _audit(self, action: str, **fields) -> None:
        """Record a security-relevant action to the audit log (best-effort)."""
        try:
            detail = " ".join(f"{k}={v!r}" for k, v in fields.items())
            _audit_log.info("%s %s", action, detail)
        except Exception:
            pass

    def update_status(self, msg, kind=None):
        """Forward a status update, tolerating 1-arg or 2-arg host callbacks."""
        cb = getattr(self, "_update_status_cb", None)
        if cb is None:
            return
        try:
            cb(msg) if kind is None else cb(msg, kind)
        except TypeError:
            try:
                cb(msg)
            except Exception:
                pass
        except Exception:
            pass

    def _widget_alive(self, widget) -> bool:
        """True if *widget* still exists (tab not destroyed)."""
        if widget is None:
            return False
        try:
            return bool(widget.winfo_exists())
        except Exception:
            return False

    def _safe_after(self, callback, *, delay=0):
        """Schedule *callback* on the Tk loop, recording the id for cleanup.

        Guards against the parent tab being destroyed before the callback runs:
        the id is tracked so ``shutdown()`` can cancel any still-pending ones.
        """
        root = getattr(self, "root", None)
        if root is None:
            return None

        def _guarded():
            try:
                self._after_ids.discard(after_id)
            except Exception:
                pass
            if getattr(self, "_shutting_down", False):
                return
            try:
                callback()
            except tk.TclError:
                pass  # widget went away between schedule and fire

        try:
            after_id = root.after(delay, _guarded)
        except Exception:
            return None
        try:
            self._after_ids.add(after_id)
        except Exception:
            self._after_ids = {after_id}
        return after_id

    def _start_post_ai_pipeline(
        self,
        result: dict,
        problem_statement: str = "",
        *,
        idle_status: str = "",
    ):
        """Display AI result and optionally run auto SQL + AI refinement loop."""
        self.auto_loop_cancelled = False
        if problem_statement:
            self._auto_problem = problem_statement
        sess = self._session()
        if sess and problem_statement:
            sess.original_problem_statement = problem_statement

        self._display_ai_response(result)
        self._sync_panels_to_session(result)

        if result.get("error"):
            return
        if result.get("satisfied"):
            self._add_chat_message("system", "✓ Problem marked satisfied.")
            return

        if self._auto_execute_ai_loop_enabled() or self._auto_execute_sql_enabled():
            self.auto_loop_running = True
            self._auto_iteration = 0
            self._update_busy_ui()
            self._continue_pipeline_after_ai(result)
        else:
            self.update_status(
                idle_status or "SQL query generated successfully"
            )

    def _continue_pipeline_after_ai(self, result: dict):
        if self.auto_loop_cancelled:
            self._finish_auto_pipeline()
            return

        summary_sql = result.get("summary_sql") or result.get("sql")
        blocked = result.get("summary_mode_blocked")
        is_clarification = result.get("is_clarification", False)

        if (
            self._auto_execute_sql_enabled()
            and summary_sql
            and not blocked
            and not is_clarification
        ):
            self._pipeline_callback = self._pipeline_after_execute
            self.execute_ai_query(from_pipeline=True)
        elif self._auto_execute_ai_loop_enabled():
            self._run_auto_loop_step()
        else:
            self._finish_auto_pipeline()

    def _pipeline_after_execute(self):
        self._sync_panels_to_session()
        if self.auto_loop_cancelled:
            self._finish_auto_pipeline()
            return
        if self._auto_execute_ai_loop_enabled():
            self._run_auto_loop_step()
        else:
            self._finish_auto_pipeline()

    def _run_auto_loop_step(self):
        if self.auto_loop_cancelled:
            self._finish_auto_pipeline()
            return

        self._auto_iteration += 1
        sess = self._session()
        if sess:
            sess.auto_loop_iteration = self._auto_iteration

        max_iter = self._auto_orchestrator.max_iterations
        if self._auto_iteration > max_iter:
            self._add_chat_message(
                "system",
                f"Uninterrupted follow-ups stopped: max iterations ({max_iter}).",
            )
            self._finish_auto_pipeline()
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            self._finish_auto_pipeline()
            return

        db_manager = self.active_connections[conn_name]
        panel_ctx = self._auto_orchestrator.build_panel_context(
            problem_statement=self._auto_problem
            or (sess.original_problem_statement if sess else ""),
            summary_sql=self.ai_sql_text.get(1.0, tk.END).strip()
            if self.ai_sql_text
            else "",
            explanation=self.ai_explanation_text.get(1.0, tk.END).strip()
            if self.ai_explanation_text
            else "",
            query_output=self._get_query_output_text(),
            iteration=self._auto_iteration,
            sql_mode=self.sql_mode,
        )

        self.update_status(
            f"Uninterrupted follow-ups: iteration {self._auto_iteration}/{max_iter}..."
        )

        def _refine_thread():
            try:
                out = self._auto_orchestrator.run_refine_step(
                    panel_ctx,
                    db_manager,
                    conn_name,
                    session_id=self.session_id,
                )
                self._safe_after(lambda: self._on_auto_loop_result(out))
            except Exception as exc:
                msg = str(exc)
                self._safe_after(
                    lambda: self._on_auto_loop_result(
                        {"error": msg, "satisfied": False}
                    ),
                )

        self._start_worker(_refine_thread, name="ai-auto-refine")

    def _on_auto_loop_result(self, result: dict):
        if self.auto_loop_cancelled:
            self._finish_auto_pipeline()
            return
        if result.get("error"):
            self._add_chat_message(
                "system", f"❌ Uninterrupted follow-ups: {result['error']}"
            )
            self._finish_auto_pipeline()
            return

        self._display_ai_response(result)
        self._sync_panels_to_session(result)

        if result.get("satisfied"):
            self._add_chat_message(
                "system", "✓ Uninterrupted follow-ups: problem satisfied."
            )
            self._finish_auto_pipeline()
            return

        self._continue_pipeline_after_ai(result)

    def _finish_auto_pipeline(self):
        self.auto_loop_running = False
        self.auto_loop_cancelled = False
        self._pipeline_callback = None
        self._update_busy_ui()
        if not self.query_running:
            self.update_status("Ready")
        # Auto-mode batch step (uninterrupted / auto-execute) is now complete.
        self._batch_on_step_done(auto_only=True)

    def _display_ai_response(self, result: dict):
        """
        Route agent result to panels:
          summary_sql → Generated SQL
          explanation (+ detail_sql, insights) → Explanation tab
          Query results → execution output only (cleared until Execute / auto-execute)
        """
        is_clarification = result.get("is_clarification", False)
        summary_sql = result.get("summary_sql") or result.get("sql")
        explanation = result.get("explanation") or ""
        err = (result.get("error") or "").strip()
        if err:
            explanation = f"Error:\n{err}"

        if not is_clarification and summary_sql:
            clean_sql = self.extract_sql_from_markdown(summary_sql)
            self.ai_sql_text.config(state=tk.NORMAL)
            self.ai_sql_text.delete(1.0, tk.END)
            self.ai_sql_text.insert(1.0, clean_sql)
            self.ai_sql_text.config(state=tk.DISABLED)

        if self.ai_explanation_text is not None:
            self.ai_explanation_text.config(state=tk.NORMAL)
            self.ai_explanation_text.delete(1.0, tk.END)
            self.ai_explanation_text.insert(1.0, explanation or "(No explanation provided)")
            self.ai_explanation_text.config(state=tk.DISABLED)
            self._select_results_tab("explanation")

        if self.ai_results_text is not None and not self._auto_execute_sql_enabled():
            self.ai_results_text.delete(1.0, tk.END)
            self.ai_results_text.insert(
                1.0,
                "Query results appear here after you run Execute query.\n",
            )

        self._display_rag_hits(result.get("rag_hits") or [])

    def _display_rag_hits(self, hits: list):
        """Render ranked RAG hits (with scores) into the RAG context tab."""
        if self.ai_rag_text is None:
            return
        self.ai_rag_text.config(state=tk.NORMAL)
        self.ai_rag_text.delete(1.0, tk.END)
        if not hits:
            self.ai_rag_text.insert(
                1.0,
                "No RAG context was used for this query.\n\n"
                "Enable 'Use RAG' and index this connection to retrieve "
                "relevant schema/example snippets.\n",
            )
            self.ai_rag_text.config(state=tk.DISABLED)
            return
        lines = [f"Top {len(hits)} retrieved snippet(s), ranked by relevance:\n"]
        for i, h in enumerate(hits, 1):
            score = h.get("score")
            kind = h.get("kind", "")
            ref = h.get("ref", "")
            text = (h.get("text") or "").strip()
            header = f"{i}. [score {score}]"
            if kind:
                header += f"  ({kind})"
            if ref:
                header += f"  {ref}"
            lines.append(header)
            if text:
                snippet = text if len(text) <= 500 else text[:500] + " …"
                lines.append(f"    {snippet}")
            lines.append("")
        self.ai_rag_text.insert(1.0, "\n".join(lines))
        self.ai_rag_text.config(state=tk.DISABLED)

    def _clear_ai_sql(self):
        """Clear AI SQL text"""
        self.ai_sql_text.config(state=tk.NORMAL)
        self.ai_sql_text.delete(1.0, tk.END)
        self.ai_sql_text.config(state=tk.DISABLED)

    def execute_ai_query(self, *, from_pipeline: bool = False):
        """Execute the generated SQL query"""
        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            if not from_pipeline:
                messagebox.showwarning(
                    "No Connection", "Please select a database connection!"
                )
            if from_pipeline:
                self._finish_auto_pipeline()
            return

        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            if not from_pipeline:
                messagebox.showwarning("No Query", "Generate or enter a SQL query first!")
            if from_pipeline:
                self._finish_auto_pipeline()
            return

        # SQL is already cleaned by extract_sql_from_markdown() when displayed in the UI.

        db_manager = self.active_connections[conn_name]

        gate = self._prepare_execution_gate(
            sql_query, db_manager, from_pipeline=from_pipeline
        )
        if gate is None:
            if from_pipeline:
                self._finish_auto_pipeline()
            return

        explain_sql = (gate or {}).get("explain_sql")

        # Update execution state
        self.query_running = True
        self.current_db_manager = db_manager
        self.cancellation_requested = False

        self._update_busy_ui()

        self.ai_results_text.delete(1.0, tk.END)
        prefix = (gate or {}).get("explain_note") or ""
        if explain_sql:
            self.ai_results_text.insert(
                1.0, f"{prefix}\nRunning EXPLAIN before query...\n"
            )
        else:
            self.ai_results_text.insert(1.0, "Executing query...\n")
        self.update_status("Executing AI-generated query...")
        self._audit(
            "sql_execute",
            connection=getattr(db_manager, "db_type", ""),
            explain=bool(explain_sql),
            sql_len=len(sql_query or ""),
        )

        # Run in thread (tracked + assigned so shutdown can join it)
        thread = self._start_worker(
            self._execute_ai_query_thread,
            args=(sql_query, db_manager, gate or {}),
            name="ai-execute",
        )
        self.current_execution_thread = thread

    def _execute_ai_query_thread(self, sql_query, db_manager, gate):
        """Thread for executing AI query via shared execution service."""
        pipeline_cb = None
        try:
            if self.cancellation_requested:
                self.root.after(0, self._handle_query_cancelled)
                return

            out = execute_sql_after_gate(
                sql_query,
                db_manager,
                gate,
                cancel_check=lambda: self.cancellation_requested,
            )

            if out.get("cancelled"):
                self.root.after(0, self._handle_query_cancelled)
                return

            if out.get("error"):
                self.root.after(0, self._display_ai_error, out["error"])
                self.root.after(0, self._sync_panels_to_session)
                return

            explain_prefix = out.get("explain_output") or ""
            self.root.after(
                0, self._display_ai_results, out["result"], explain_prefix
            )
            self.root.after(0, self._sync_panels_to_session)
            self.root.after(0, self.update_status, "Query executed successfully")
            # The query ran cleanly: now (and only now) train any queued
            # auto-fix pair on the actually-executed query.
            conn_exec = self.ai_conn_combo.get().strip()
            self.root.after(
                0, lambda s=sql_query, c=conn_exec:
                self._run_pending_autofix_train(s, c)
            )
            pipeline_cb = self._pipeline_callback

        except Exception as e:
            import traceback

            error_msg = f"Error executing query:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, self._display_ai_error, error_msg)
            self.root.after(0, self._sync_panels_to_session)
        finally:
            if self.cancellation_requested or self.auto_loop_cancelled:
                pipeline_cb = None
            self.root.after(
                0, lambda cb=pipeline_cb: self._after_execute_complete(cb)
            )

    def _after_execute_complete(self, pipeline_callback):
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False
        self._pipeline_callback = None
        if pipeline_callback:
            pipeline_callback()
        self._update_busy_ui()
        # Manual-mode batch advances only after the user runs Execute.
        self._batch_on_step_done(manual_only=True)

    def stop_ai_query(self):
        """Stop SQL generation, SQL execution, and/or the auto-execute AI loop"""
        self.auto_loop_cancelled = True

        # Abort an in-progress questions-from-file batch.
        if getattr(self, "_batch_active", False):
            self._batch_active = False
            self._batch_step_pending = False
            self.update_status("Batch run stopped.")

        # Cooperatively cancel an in-flight NL->SQL generation.
        if self.generation_running:
            self.generation_cancelled = True
            self.update_status("Cancelling SQL generation...")

        if self.auto_loop_running:
            self.update_status("Stopping uninterrupted follow-ups...")
            if not self.query_running:
                self._finish_auto_pipeline()

        if not self.query_running:
            return

        # Set cancellation flag
        self.cancellation_requested = True

        # Try to cancel at database level
        if self.current_db_manager:
            try:
                self.current_db_manager.cancel_query()
                self.update_status("Query cancellation requested...")
            except Exception as e:
                print(f"Error cancelling query: {e}", file=sys.stderr)
                # Even if cancellation fails, the flag is set and thread will check it

    def shutdown(self, *, join_timeout: float = 2.0):
        """Cancel in-flight work and join worker threads before this tab goes away.

        Called when a tab is closed or sessions are reloaded so background
        generation / execution threads don't outlive their UI (which would
        leak threads and can fire callbacks against destroyed widgets).
        """
        # Signal every cooperative cancel flag.
        self._shutting_down = True
        self.auto_loop_cancelled = True
        self.cancellation_requested = True
        self.generation_cancelled = True
        # Cancel any pending Tk after() callbacks so they can't fire against
        # destroyed widgets after this tab goes away.
        for after_id in list(getattr(self, "_after_ids", set()) or set()):
            try:
                self.root.after_cancel(after_id)
            except Exception:
                pass
        try:
            self._after_ids.clear()
        except Exception:
            self._after_ids = set()
        # Best-effort DB-level cancel for a running execution.
        if getattr(self, "current_db_manager", None):
            try:
                self.current_db_manager.cancel_query()
            except Exception:
                pass
        # Join known worker threads (generation + execution), bounded so the UI
        # never hangs on a stuck backend call.
        threads = list(getattr(self, "_worker_threads", []) or [])
        for t in (self.current_execution_thread, self._generation_thread):
            if t is not None and t not in threads:
                threads.append(t)
        import threading as _threading

        for t in threads:
            try:
                if t is _threading.current_thread():
                    continue
                if t.is_alive():
                    t.join(timeout=join_timeout)
            except Exception:
                pass
        self._worker_threads = []
        self.generation_running = False
        self.query_running = False

    def _handle_query_cancelled(self):
        """Handle UI updates when query is cancelled"""
        self.ai_results_text.delete(1.0, tk.END)
        self.ai_results_text.insert(1.0, "⏹ Query execution cancelled by user\n")
        self._sync_panels_to_session()
        self.update_status("Query cancelled")
        if self.auto_loop_running:
            self._finish_auto_pipeline()

    def _restore_query_ui_state(self):
        """Restore UI state after query execution completes (manual execute)."""
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False
        if not self.auto_loop_running:
            self._update_busy_ui()

    def _display_ai_results(self, result, explain_prefix: str = ""):
        """Display query execution results"""
        self.ai_results_text.delete(1.0, tk.END)
        if explain_prefix:
            self.ai_results_text.insert(tk.END, explain_prefix + "\n")
            self.ai_results_text.insert(tk.END, "=" * 80 + "\n\n")

        # Check if this is multiple results
        if "multiple_results" in result and result["multiple_results"]:
            # Display results from multiple statements
            self.ai_results_text.insert(
                tk.END,
                f"Executed {result['count']} statement(s) in {result['time']:.3f} seconds\n",
            )
            self.ai_results_text.insert(tk.END, "=" * 80 + "\n\n")

            for res in result["results"]:
                stmt_num = res.get("statement_num", "?")
                stmt = res.get("statement", "")

                self.ai_results_text.insert(tk.END, f"Statement {stmt_num}: {stmt}\n")
                self.ai_results_text.insert(tk.END, "-" * 80 + "\n")

                if "message" in res:
                    # DML/DDL result
                    self.ai_results_text.insert(tk.END, f"{res['message']}\n")
                elif "columns" in res:
                    # SELECT result
                    self.ai_results_text.insert(
                        tk.END, f"Returned {res['rowcount']} row(s)\n\n"
                    )

                    # Display column headers
                    headers = res["columns"]
                    header_line = " | ".join(f"{h:20}" for h in headers)
                    self.ai_results_text.insert(tk.END, header_line + "\n")
                    self.ai_results_text.insert(tk.END, "-" * len(header_line) + "\n")

                    # Display rows (limit to first 100 per statement for performance)
                    rows = res["rows"][:100]
                    for row in rows:
                        row_values = []
                        for val in row:
                            if isinstance(val, (bytearray, bytes)):
                                val = val.decode("utf-8", errors="ignore")
                            elif val is None:
                                val = "NULL"
                            else:
                                val = str(val)
                            row_values.append(f"{val:20}")
                        self.ai_results_text.insert(
                            tk.END, " | ".join(row_values) + "\n"
                        )

                    if len(res["rows"]) > 100:
                        self.ai_results_text.insert(
                            tk.END, f"\n... and {len(res['rows']) - 100} more rows\n"
                        )

                self.ai_results_text.insert(tk.END, "\n" + "=" * 80 + "\n\n")

        elif "message" in result:
            # Single DML/DDL result
            self.ai_results_text.insert(tk.END, f"{result['message']}\n")
            self.ai_results_text.insert(
                tk.END, f"Execution time: {result['time']:.3f} seconds\n"
            )
        else:
            # Single SELECT result
            self.ai_results_text.insert(
                tk.END, f"Query returned {result['rowcount']} row(s)\n"
            )
            self.ai_results_text.insert(
                tk.END, f"Execution time: {result['time']:.3f} seconds\n\n"
            )

            # Display column headers
            headers = result["columns"]
            header_line = " | ".join(f"{h:20}" for h in headers)
            self.ai_results_text.insert(tk.END, header_line + "\n")
            self.ai_results_text.insert(tk.END, "-" * len(header_line) + "\n")

            # Display rows (limit to first 1000 for performance)
            rows = result["rows"][:1000]
            for row in rows:
                row_values = []
                for val in row:
                    if isinstance(val, (bytearray, bytes)):
                        val = val.decode("utf-8", errors="ignore")
                    elif val is None:
                        val = "NULL"
                    else:
                        val = str(val)
                    row_values.append(f"{val:20}")
                self.ai_results_text.insert(tk.END, " | ".join(row_values) + "\n")

            if len(result["rows"]) > 1000:
                self.ai_results_text.insert(
                    tk.END, f"\n... and {len(result['rows']) - 1000} more rows\n"
                )

        # Switch to results tab
        self._select_results_tab("results")

    def _display_ai_error(self, error):
        """Display error in AI results"""
        self.ai_results_text.delete(1.0, tk.END)
        self.ai_results_text.insert(1.0, f"Error executing query:\n\n{error}")
        self.update_status("Query execution failed")

        # Add error to chat history with a helpful suggestion
        conversation_info = self.ai_agent.get_conversation_summary()
        if conversation_info["has_active_conversation"]:
            self._add_chat_message(
                "system",
                f"❌ Query failed with error:\n{error[:200]}{'...' if len(error) > 200 else ''}\n\n"
                f"💡 You can send a follow-up message in the Chat tab to fix this error.\n"
                f'Example: "The query failed with error: {error[:80]}..."',
            )
            # Switch to chat tab to make it visible
            self._select_results_tab("chat")

        # If a local-LLM query failed and a fallback corrector is configured,
        # repair it automatically (and re-train when auto-fix training is on).
        self._maybe_autocorrect_on_failure(error)

    def explain_ai_query(self):
        """Get AI explanation of the generated query"""
        if not self.ai_agent.is_available():
            messagebox.showwarning("AI Not Available", "AI agent is not configured")
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            messagebox.showwarning(
                "No Connection", "Please select a database connection!"
            )
            return

        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning("No Query", "Generate a SQL query first!")
            return

        # Note: SQL has already been cleaned when displayed, use it directly

        db_manager = self.active_connections[conn_name]

        self.ai_explanation_text.delete(1.0, tk.END)
        self.ai_explanation_text.insert(1.0, "Getting explanation from AI...\n")
        self._select_results_tab("explanation")

        self._start_worker(
            self._explain_query_thread,
            args=(sql_query, db_manager.db_type),
            name="ai-explain",
        )

    def _explain_query_thread(self, sql_query, db_type):
        """Thread for query explanation"""
        try:
            # explain_query returns a string (not a tuple)
            explanation = self.ai_agent.explain_query(sql_query, db_type)

            if (
                not explanation
                or explanation.startswith("Error")
                or explanation.startswith("Claude CLI not available")
            ):
                self._safe_after(lambda: self._display_explanation_error(explanation))
                return

            self._safe_after(lambda: self._display_explanation(explanation))

        except Exception as e:
            import traceback

            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            print(f"\n=== ERROR in explain query ===", file=sys.stderr)
            print(error_msg, file=sys.stderr)
            print("=" * 30, file=sys.stderr)
            self._safe_after(lambda: self._display_explanation_error(error_msg))

    def _display_explanation(self, explanation):
        """Display query explanation"""
        if not self._widget_alive(self.ai_explanation_text):
            return
        self.ai_explanation_text.delete(1.0, tk.END)
        self.ai_explanation_text.insert(1.0, explanation)

    def _display_explanation_error(self, error):
        """Display explanation error"""
        if not self._widget_alive(self.ai_explanation_text):
            return
        self.ai_explanation_text.delete(1.0, tk.END)
        self.ai_explanation_text.insert(1.0, f"Error getting explanation:\n\n{error}")

    def optimize_ai_query(self):
        """Get AI optimization suggestions"""
        if not self.ai_agent.is_available():
            messagebox.showwarning("AI Not Available", "AI agent is not configured")
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            messagebox.showwarning(
                "No Connection", "Please select a database connection!"
            )
            return

        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning("No Query", "Generate a SQL query first!")
            return

        # Note: SQL has already been cleaned when displayed, use it directly

        db_manager = self.active_connections[conn_name]

        self.ai_optimization_text.delete(1.0, tk.END)
        self.ai_optimization_text.insert(
            1.0, "Getting optimization suggestions from AI...\n"
        )
        self._select_results_tab("optimization")

        self._start_worker(
            self._optimize_query_thread,
            args=(sql_query, db_manager.db_type),
            name="ai-optimize",
        )

    def _optimize_query_thread(self, sql_query, db_type):
        """Thread for query optimization"""
        try:
            # suggest_optimizations returns a string (not a tuple)
            suggestions = self.ai_agent.suggest_optimizations(sql_query, db_type)

            if (
                not suggestions
                or suggestions.startswith("Error")
                or suggestions.startswith("Claude CLI not available")
            ):
                self._safe_after(lambda: self._display_optimization_error(suggestions))
                return

            self._safe_after(lambda: self._display_optimization(suggestions))

        except Exception as e:
            import traceback

            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            print(f"\n=== ERROR in suggest optimizations ===", file=sys.stderr)
            print(error_msg, file=sys.stderr)
            print("=" * 30, file=sys.stderr)
            self._safe_after(lambda: self._display_optimization_error(error_msg))

    def _display_optimization(self, suggestions):
        """Display optimization suggestions"""
        if not self._widget_alive(self.ai_optimization_text):
            return
        self.ai_optimization_text.delete(1.0, tk.END)
        self.ai_optimization_text.insert(1.0, suggestions)

    def _display_optimization_error(self, error):
        """Display optimization error"""
        if not self._widget_alive(self.ai_optimization_text):
            return
        self.ai_optimization_text.delete(1.0, tk.END)
        self.ai_optimization_text.insert(
            1.0, f"Error getting optimization suggestions:\n\n{error}"
        )

    def copy_ai_sql(self):
        """Copy generated SQL to clipboard"""
        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if sql_query:
            self.root.clipboard_clear()
            self.root.clipboard_append(sql_query)
            self.update_status("SQL copied to clipboard")
            messagebox.showinfo("Copied", "SQL query copied to clipboard!")
        else:
            messagebox.showwarning("No Query", "No SQL query to copy!")

    def edit_ai_sql(self):
        """Enable editing of generated SQL"""
        self.ai_sql_text.config(state=tk.NORMAL)
        messagebox.showinfo(
            "Edit Mode",
            "SQL query is now editable. You can modify it before execution.",
        )

    def send_to_sql_editor(self):
        """Send generated SQL to SQL Editor tab"""
        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning("No Query", "No SQL query to send!")
            return

        # Use the callback provided during initialization
        self.send_to_editor(sql_query)
        messagebox.showinfo("Success", "SQL query sent to SQL Editor!")

    def write_sql_execution_rules(self):
        """Edit SQL execution rules (summary/open modes — enforced before Execute)."""
        dialog = tk.Toplevel(self.root)
        dialog.title("SQL Execution Rules")
        width, height = get_window_size("settings")
        dialog.geometry(f"{width}x{height}")
        dialog.transient(self.root)
        dialog.grab_set()

        main_frame = make_scrollable(dialog)
        main_frame.configure(padding=10)

        ttk.Label(
            main_frame, text="SQL Execution Rules", font=("Arial", 14, "bold")
        ).pack(pady=(0, 10))

        ttk.Label(
            main_frame,
            text="Applied before Execute (manual and auto) in Summary and Open modes.\n"
            "Built-in enforcement recognizes lines mentioning LIMIT on user tables "
            "and EXPLAIN before multi-JOIN queries.",
            foreground="gray",
        ).pack(pady=(0, 10))

        rules_frame = ttk.LabelFrame(
            main_frame, text="Rules (one per line)", padding=5
        )
        rules_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        rules_text = scrolledtext.ScrolledText(
            rules_frame, wrap=tk.WORD, font=self.ui_font, height=15
        )
        rules_text.pack(fill=tk.BOTH, expand=True)
        rules_text.insert(1.0, self.sql_execution_rules or "")

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        def save_rules():
            self.sql_execution_rules = rules_text.get(1.0, tk.END).strip()
            sess = self._session()
            if sess:
                sess.sql_execution_rules = self.sql_execution_rules
            messagebox.showinfo("Success", "SQL execution rules saved!")
            dialog.destroy()

        ttk.Button(button_frame, text="Save", command=save_rules).pack(
            side=tk.RIGHT, padx=(5, 0)
        )
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(
            side=tk.RIGHT
        )

    def _user_tables_for_rules(self, db_manager) -> list[str]:
        from ai_query.sql_execution_service import user_table_names

        conn_name = self.ai_conn_combo.get() if self.ai_conn_combo else ""
        return user_table_names(self.ai_agent, db_manager, conn_name)

    def _prepare_execution_gate(
        self, sql: str, db_manager, *, from_pipeline: bool = False
    ) -> dict | None:
        """
        Return None to abort execution, or a dict with optional explain_sql key.
        """
        if not execution_rules_apply(self.sql_mode):
            return {}

        gate = check_execution_allowed(
            sql,
            sql_mode=self.sql_mode,
            rules_text=self.sql_execution_rules,
            db_manager=db_manager,
            agent=self.ai_agent,
            connection_name=self.ai_conn_combo.get() if self.ai_conn_combo else "",
        )
        if not gate["allowed"]:
            msg = gate["blocked_reason"]
            if from_pipeline:
                self._add_chat_message("system", f"❌ {msg}")
            else:
                messagebox.showwarning("SQL execution rules", msg)
            return None

        out: dict = {}
        if gate.get("explain_sql"):
            out["explain_sql"] = gate["explain_sql"]
            out["explain_note"] = gate.get("explain_note") or ""
        return out

    def write_review_rules(self):
        """Open dialog to write/edit SQL review rules"""
        dialog = tk.Toplevel(self.root)
        dialog.title("SQL Review Rules")
        width, height = get_window_size("settings")
        dialog.geometry(f"{width}x{height}")
        dialog.transient(self.root)
        dialog.grab_set()

        main_frame = make_scrollable(dialog)
        main_frame.configure(padding=10)

        # Title
        ttk.Label(main_frame, text="SQL Review Rules", font=("Arial", 14, "bold")).pack(
            pady=(0, 10)
        )

        # Instructions
        instructions = ttk.Label(
            main_frame,
            text="Define rules that the AI will use to review SQL queries.\n"
            "Examples: Check for missing indexes, identify N+1 queries, ensure proper error handling, etc.",
            foreground="gray",
        )
        instructions.pack(pady=(0, 10))

        # Rules text area
        rules_frame = ttk.LabelFrame(
            main_frame, text="Review Rules (one rule per line)", padding=5
        )
        rules_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        rules_text = scrolledtext.ScrolledText(
            rules_frame, wrap=tk.WORD, font=self.ui_font, height=15
        )
        rules_text.pack(fill=tk.BOTH, expand=True)

        # Load existing rules if they exist
        if hasattr(self, "sql_review_rules"):
            rules_text.insert(1.0, self.sql_review_rules)
        else:
            # Default rules
            default_rules = """Check for missing WHERE clauses in UPDATE/DELETE statements
Identify queries without proper indexing hints
Look for N+1 query patterns
Verify proper use of JOINs vs subqueries
Check for SQL injection vulnerabilities
Ensure proper transaction handling
Identify inefficient LIKE patterns (e.g., %value%)
Check for missing LIMIT clauses in large result sets"""
            rules_text.insert(1.0, default_rules)

        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        def save_rules():
            self.sql_review_rules = rules_text.get(1.0, tk.END).strip()
            messagebox.showinfo("Success", "Review rules saved successfully!")
            dialog.destroy()

        def cancel():
            dialog.destroy()

        ttk.Button(
            button_frame, text="Save Rules", command=save_rules, style="Primary.TButton"
        ).pack(side=tk.RIGHT, padx=(5, 0))
        ttk.Button(button_frame, text="Cancel", command=cancel).pack(side=tk.RIGHT)

    def import_sql_for_review(self):
        """Import SQL from file for review"""
        file_path = filedialog.askopenfilename(
            title="Import SQL for Review",
            filetypes=[
                ("SQL Files", "*.sql"),
                ("Text Files", "*.txt"),
                ("All Files", "*.*"),
            ],
        )

        if not file_path:
            return

        try:
            # Guard against loading a huge file into the Text widget (which would
            # freeze/crash the UI). Cap configurable via [ui.ai_query].
            max_bytes = mc.get_int(
                "ui.ai_query", "max_import_sql_bytes", default=10 * 1024 * 1024)
            try:
                size = os.path.getsize(file_path)
            except OSError:
                size = 0
            if max_bytes > 0 and size > max_bytes:
                messagebox.showwarning(
                    "File Too Large",
                    f"The selected file is {size // 1024} KB, which exceeds the "
                    f"{max_bytes // 1024} KB import limit.")
                self.update_status("SQL import skipped: file too large", "error")
                return

            with open(file_path, "r", encoding="utf-8") as f:
                sql_content = f.read(max_bytes + 1) if max_bytes > 0 else f.read()
            if max_bytes > 0 and len(sql_content) > max_bytes:
                messagebox.showwarning(
                    "File Too Large",
                    f"The selected file exceeds the {max_bytes // 1024} KB "
                    "import limit.")
                self.update_status("SQL import skipped: file too large", "error")
                return
            sql_content = sql_content.strip()

            if not sql_content:
                messagebox.showwarning("Empty File", "The selected file is empty!")
                return

            # Display SQL in Generated SQL box
            self.ai_sql_text.config(state=tk.NORMAL)
            self.ai_sql_text.delete(1.0, tk.END)
            self.ai_sql_text.insert(1.0, sql_content)
            self.ai_sql_text.config(state=tk.NORMAL)  # Keep editable for review

            # Clear results
            self.ai_results_text.delete(1.0, tk.END)
            self.ai_results_text.insert(
                1.0, "SQL imported for review. Click 'Review SQL' to analyze.\n\n"
            )

            # Add to explanation tab. Build the text once and insert in order
            # (repeated insert(1.0, ...) would reverse the visible line order).
            self.ai_explanation_text.delete(1.0, tk.END)
            self.ai_explanation_text.insert(
                1.0,
                f"Imported SQL from: {file_path}\n\n"
                "Ready for review. You can:\n"
                "1. Execute the query to test it\n"
                "2. Get AI explanation\n"
                "3. Request optimization suggestions\n"
                "4. Run it through review rules\n\n",
            )

            self._audit("sql_import", file=file_path, bytes=len(sql_content))
            self.update_status(
                f"✓ SQL imported from {os.path.basename(file_path)}", "success"
            )
            messagebox.showinfo(
                "Success",
                f"SQL imported successfully from:\n{os.path.basename(file_path)}",
            )

        except Exception as e:
            messagebox.showerror("Error", f"Failed to import SQL file:\n{str(e)}")
            self.update_status("Failed to import SQL", "error")

    def run_sql_review(self):
        """Run AI-powered review on the SQL using custom rules"""
        if not self.ai_agent.is_available():
            messagebox.showwarning("AI Not Available", "AI agent is not configured")
            return

        sql_query = self.ai_sql_text.get(1.0, tk.END).strip()
        if not sql_query:
            messagebox.showwarning(
                "No Query",
                "No SQL query to review!\n\nPlease generate or import a SQL query first.",
            )
            return

        # Get review rules
        if not hasattr(self, "sql_review_rules") or not self.sql_review_rules:
            response = messagebox.askyesno(
                "No Review Rules",
                "No custom review rules defined. Would you like to set them up now?\n\n"
                "Click 'No' to use default review criteria.",
            )
            if response:
                self.write_review_rules()
                return
            rules = "Use standard SQL best practices and performance optimization guidelines"
        else:
            rules = self.sql_review_rules

        # Get database type for context
        conn_name = self.ai_conn_combo.get()
        db_type = "SQL"
        if conn_name and conn_name in self.active_connections:
            db_manager = self.active_connections[conn_name]
            db_type = db_manager.db_type

        # Create review tab if it doesn't exist
        if not hasattr(self, "ai_review_text"):
            review_tab = ttk.Frame(self.ai_results_notebook)
            self.ai_results_notebook.add(review_tab, text="Review")
            self.ai_review_text = scrolledtext.ScrolledText(
                review_tab, wrap=tk.WORD, font=self.ui_font
            )
            self.ai_review_text.pack(fill=tk.BOTH, expand=True)

        # Show loading message
        self.ai_review_text.delete(1.0, tk.END)
        self.ai_review_text.insert(1.0, "🔍 Running SQL review with AI...\n\n")
        self.ai_review_text.insert(tk.END, f"Database Type: {db_type}\n")
        self.ai_review_text.insert(
            tk.END, f"Review Rules: {len(rules.split(chr(10)))} rules defined\n\n"
        )
        self.ai_review_text.insert(tk.END, "Please wait...\n")

        # Switch to review tab
        self._select_results_tab("review")

        self.update_status("Running SQL review with AI...")

        def review_thread():
            try:
                # Route through the shared AIQueryService so the review uses the
                # SAME prompt/criteria across UI/CLI/API and works with whatever
                # backend the user has selected (not just the Claude CLI).
                from ai_query.service import AIService

                svc = AIService(core=None)
                svc._ai = self.ai_agent  # reuse the UI's active agent/backend
                result = svc.review_sql(
                    sql_query, rules=rules, connection=conn_name, db_type=db_type)
                review_text = (result or {}).get("review")

                if review_text:
                    # Update UI in main thread
                    def update_ui():
                        if not self._widget_alive(self.ai_review_text):
                            return
                        self.ai_review_text.delete(1.0, tk.END)
                        self.ai_review_text.insert(1.0, "🔍 SQL REVIEW RESULTS\n")
                        self.ai_review_text.insert(tk.END, "=" * 80 + "\n\n")
                        self.ai_review_text.insert(tk.END, review_text)
                        self.ai_review_text.insert(tk.END, "\n\n" + "=" * 80 + "\n")
                        self.ai_review_text.insert(
                            tk.END,
                            f"\nReview completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
                        )
                        self.update_status("✓ SQL review completed", "success")

                    self._safe_after(update_ui)
                else:
                    error_msg = (result or {}).get("error") or "Failed to get review from AI"

                    def show_error():
                        if not self._widget_alive(self.ai_review_text):
                            return
                        self.ai_review_text.delete(1.0, tk.END)
                        self.ai_review_text.insert(
                            1.0, f"❌ Review failed:\n\n{error_msg}"
                        )
                        self.update_status("SQL review failed", "error")
                        messagebox.showerror("Review Failed", error_msg)

                    self._safe_after(show_error)

            except Exception as e:
                error_msg = f"Error during review: {str(e)}"

                def show_error():
                    if not self._widget_alive(self.ai_review_text):
                        return
                    self.ai_review_text.delete(1.0, tk.END)
                    self.ai_review_text.insert(1.0, f"❌ Review error:\n\n{error_msg}")
                    self.update_status("SQL review error", "error")
                    messagebox.showerror("Error", error_msg)

                self._safe_after(show_error)

        # Run review in background thread (tracked for clean shutdown)
        self._start_worker(review_thread, name="ai-review")

    def clear_ai_query(self):
        """Clear all AI query fields"""
        self.ai_question_text.delete(1.0, tk.END)
        self.ai_sql_text.config(state=tk.NORMAL)
        self.ai_sql_text.delete(1.0, tk.END)
        self.ai_sql_text.config(state=tk.DISABLED)
        self.ai_results_text.delete(1.0, tk.END)
        self.ai_explanation_text.delete(1.0, tk.END)
        self.ai_optimization_text.delete(1.0, tk.END)
        self.clear_ai_chat()
        self.update_status("AI query fields cleared")

    def _add_chat_message(self, role, message):
        """Add a message to the chat history display"""
        self.ai_chat_history.config(state=tk.NORMAL)

        # Add timestamp
        from datetime import datetime

        timestamp = datetime.now().strftime("%H:%M:%S")

        if role == "user":
            self.ai_chat_history.insert(tk.END, f"[{timestamp}] You: ", "user")
            self.ai_chat_history.insert(tk.END, f"{message}\n\n")
        elif role == "assistant":
            self.ai_chat_history.insert(
                tk.END, f"[{timestamp}] AI Assistant: ", "assistant"
            )
            self.ai_chat_history.insert(tk.END, f"{message}\n\n")
        elif role == "system":
            self.ai_chat_history.insert(tk.END, f"[{timestamp}] ", "system")
            self.ai_chat_history.insert(tk.END, f"{message}\n\n", "system")

        # Auto-scroll to bottom
        self.ai_chat_history.see(tk.END)
        self.ai_chat_history.config(state=tk.DISABLED)

    def send_ai_followup(self):
        """Send a follow-up message to the AI"""
        if not self.ai_agent.is_available():
            messagebox.showerror("AI Not Available", "AI agent is not configured.")
            return

        # Check if there's an active conversation
        conversation_info = self.ai_agent.get_conversation_summary(
            session_id=self.session_id
        )
        if not conversation_info["has_active_conversation"]:
            messagebox.showwarning(
                "No Active Conversation",
                "Please generate an initial SQL query first using 'Generate SQL' button.",
            )
            return

        conn_name = self.ai_conn_combo.get()
        if not conn_name or conn_name not in self.active_connections:
            messagebox.showerror("Error", "Please select a valid database connection!")
            return

        followup_message = self.ai_followup_text.get(1.0, tk.END).strip()
        if not followup_message:
            messagebox.showwarning("Empty Message", "Please enter a follow-up message!")
            return

        db_manager = self.active_connections[conn_name]

        # A new follow-up may yield a fresh (non-corrected) query.
        self._last_sql_corrected = False

        # Add user message to chat
        self._add_chat_message("user", followup_message)

        # Clear input
        self.ai_followup_text.delete(1.0, tk.END)

        # Show processing message
        self._add_chat_message("system", "Processing your request...")
        self.update_status("Processing follow-up...")

        self._start_worker(
            self._send_followup_thread,
            args=(followup_message, db_manager, conn_name),
            name="ai-followup",
        )

    def _send_followup_thread(self, followup_message, db_manager, connection_name):
        """Thread for processing follow-up messages"""
        try:
            self._sync_session_from_ui()
            if self.orchestrator and self.session_id:
                out = self.orchestrator.parse_and_execute(
                    self.session_id,
                    followup_message,
                    db_manager,
                    connection_name,
                    mode="followup",
                )
                for msg in out.get("cross_tab_messages") or []:
                    self.root.after(0, self._add_chat_message, "system", msg)
                if out.get("skip_local_ai"):
                    result = out.get("result") or {}
                else:
                    result = out.get("result") or {}
            else:
                result = self.ai_agent.send_follow_up(
                    followup_message,
                    db_manager,
                    connection_name,
                    session_id=self.session_id,
                )

            if result["error"]:
                self.root.after(
                    0, self._add_chat_message, "system", f"❌ Error: {result['error']}"
                )
                self.root.after(0, self.update_status, "Follow-up failed")
                return

            # Build response message for chat history
            if result.get("is_clarification"):
                response = f"{result.get('explanation') or ''}"
            else:
                sql = result.get("summary_sql") or result.get("sql")
                response = f"Summary SQL:\n```sql\n{sql}\n```\n\n{result.get('explanation') or ''}"
                if result.get("insights"):
                    response += f"\n\nInsights:\n{result['insights']}"
                # Mirror the follow-up brief into the Questions (Natural
                # language) box so it tracks the query now in the preview (and
                # keeps the question↔SQL pair consistent for training).
                if sql:
                    self.root.after(
                        0, self._set_question_text, followup_message)
            self.root.after(0, self._add_chat_message, "assistant", response)
            self.root.after(
                0,
                lambda r=result: self._start_post_ai_pipeline(
                    r,
                    "",
                    idle_status="Follow-up processed successfully",
                ),
            )

        except Exception as e:
            import traceback

            error_detail = traceback.format_exc()
            print(f"Error in _send_followup_thread: {error_detail}", file=sys.stderr)
            self.root.after(
                0, self._add_chat_message, "system", f"❌ Unexpected error: {str(e)}"
            )
            self.root.after(0, self.update_status, "Follow-up failed")

    def clear_ai_chat(self):
        """Clear the chat history"""
        self.ai_chat_history.config(state=tk.NORMAL)
        self.ai_chat_history.delete(1.0, tk.END)
        self.ai_chat_history.config(state=tk.DISABLED)
        self.ai_followup_text.delete(1.0, tk.END)
        self.ai_agent.clear_conversation(session_id=self.session_id)
        self.update_status("Chat history cleared")

    def configure_ai_api_key(self):
        """Show dialog to configure AI API key"""
        # Create configuration dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Configure AI API Key")
        width, height = get_window_size("ai_chat")
        dialog.geometry(f"{width}x{height}")
        dialog.resizable(True, True)

        # Make dialog modal
        dialog.transient(self.root)
        dialog.grab_set()

        # Main frame with scrollbar
        main_frame = ttk.Frame(dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Title
        ttk.Label(
            main_frame, text="Configure AI Provider", font=("Arial", 14, "bold")
        ).pack(pady=(0, 10))

        # Instructions
        instructions = ttk.Label(
            main_frame,
            text="Enter your API key for one of the supported AI providers.\n"
            "⚠️ Key is stored in MEMORY ONLY (not saved to disk).\n"
            "You will need to re-enter it if you restart the application.",
            justify=tk.LEFT,
            foreground="orange",
            wraplength=600,
        )
        instructions.pack(anchor=tk.W, pady=(0, 15))

        # Provider selection
        provider_frame = ttk.LabelFrame(
            main_frame, text="Select Provider", padding="10"
        )
        provider_frame.pack(fill=tk.X, pady=(0, 10))

        provider_var = tk.StringVar(value="anthropic")

        ttk.Radiobutton(
            provider_frame,
            text="Anthropic Claude (Recommended for Claude Code users)",
            variable=provider_var,
            value="anthropic",
        ).pack(anchor=tk.W, pady=2)
        ttk.Radiobutton(
            provider_frame,
            text="OpenAI (GPT-4, GPT-3.5)",
            variable=provider_var,
            value="openai",
        ).pack(anchor=tk.W, pady=2)
        ttk.Radiobutton(
            provider_frame, text="Google Gemini", variable=provider_var, value="google"
        ).pack(anchor=tk.W, pady=2)

        # API Key input
        key_frame = ttk.LabelFrame(main_frame, text="API Key", padding="10")
        key_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(key_frame, text="API Key:").grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=5
        )
        api_key_entry = ttk.Entry(key_frame, width=50, show="*")
        api_key_entry.grid(row=0, column=1, padx=5, pady=5)

        show_key_var = tk.BooleanVar(value=False)

        def toggle_key_visibility():
            api_key_entry.config(show="" if show_key_var.get() else "*")

        ttk.Checkbutton(
            key_frame,
            text="Show key",
            variable=show_key_var,
            command=toggle_key_visibility,
        ).grid(row=0, column=2, padx=5, pady=5)

        # Model (optional)
        ttk.Label(key_frame, text="Model (optional):").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=5
        )
        model_entry = ttk.Entry(key_frame, width=50)
        model_entry.grid(row=1, column=1, padx=5, pady=5)

        # Help text
        help_text = ttk.Label(
            key_frame,
            text="Leave model empty to use defaults:\n"
            "• Anthropic: claude-3-5-sonnet-20241022\n"
            "• OpenAI: gpt-4\n"
            "• Google: gemini-pro",
            justify=tk.LEFT,
            foreground="gray",
            font=("Arial", 9),
        )
        help_text.grid(row=2, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)

        # Info section (move before buttons)
        info_frame = ttk.LabelFrame(main_frame, text="Getting API Keys", padding="10")
        info_frame.pack(fill=tk.X, pady=(5, 10))

        info_text = (
            "• Anthropic: https://console.anthropic.com/\n"
            "• OpenAI: https://platform.openai.com/api-keys\n"
            "• Google: https://makersuite.google.com/app/apikey"
        )

        ttk.Label(info_frame, text=info_text, justify=tk.LEFT, foreground="blue").pack(
            anchor=tk.W
        )

        # Status label
        status_label = ttk.Label(main_frame, text="", foreground="red", wraplength=600)
        status_label.pack(pady=5)

        # Buttons (at the bottom, always visible)
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(side=tk.BOTTOM, pady=15)

        def save_config():
            provider = provider_var.get()
            api_key = api_key_entry.get().strip()
            model = model_entry.get().strip() or None

            if not api_key:
                status_label.config(text="⚠️ Please enter an API key", foreground="red")
                return

            # Show processing
            status_label.config(text="Configuring...", foreground="blue")
            dialog.update()

            # Try to configure
            success, message = self.ai_agent.configure_api_key(provider, api_key, model)

            if success:
                # Update UI
                ai_info = self.ai_agent.get_api_info()
                self.ai_status_label.config(text=ai_info["status"], foreground="green")

                if hasattr(self, "ai_provider_label"):
                    self.ai_provider_label.config(text=ai_info["provider"])
                else:
                    # Labels don't exist, need to recreate them
                    pass

                if hasattr(self, "ai_model_label"):
                    self.ai_model_label.config(text=ai_info["model"])

                messagebox.showinfo(
                    "Success",
                    f"✅ {message}\n\nKey is stored in memory only.\nIt will be cleared when you close the application.",
                    parent=dialog,
                )
                dialog.destroy()
                self.update_status("AI agent configured successfully")
            else:
                status_label.config(text=f"❌ {message}", foreground="red")

        ttk.Button(
            button_frame, text="✓ Save & Connect", command=save_config, width=20
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy, width=15).pack(
            side=tk.LEFT, padx=5
        )

        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")


def launch_ui(**_context) -> None:
    """Canonical desktop UI entry for AI Query Assistant (``--ui`` and direct script)."""
    from common.ui.tk.launcher import launch_desktop_ui

    launch_desktop_ui(feature_module="ai")


if __name__ == "__main__":
    launch_ui()
