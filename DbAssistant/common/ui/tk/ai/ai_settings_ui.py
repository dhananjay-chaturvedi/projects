"""AI Query tab — settings button for ai_query/config.ini."""

from __future__ import annotations

from common.ui.tk.module_config_dialog import FieldSpec, open_module_config_dialog
from ai_query import module_config

_FIELDS: tuple[FieldSpec, ...] = (
    ("ai", "default_backend", "Default backend", "enum", ("auto", "claude", "cursor", "codex")),
    ("ai", "mask_pii", "Mask PII in prompts", "bool", ()),
    ("ai", "max_sessions", "Max in-memory sessions", "int", ()),
    ("ai.claude", "timeout", "Claude timeout (s)", "int", ()),
    ("ai.cursor", "model", "Cursor model", "str", ()),
    ("ai.cursor", "timeout", "Cursor timeout (s)", "int", ()),
    ("ai.codex", "model", "Codex model (blank=default)", "str", ()),
    ("ai.codex", "timeout", "Codex timeout (s)", "int", ()),
    ("ui.ai_query", "auto_execute_ai_loop", "Auto-execute AI loop", "bool", ()),
    ("ui.ai_query", "auto_execute_summary_sql", "Auto-run summary SQL", "bool", ()),
    ("ui.ai_query", "auto_loop_max_iterations", "Auto-loop max iterations", "int", ()),
    ("ui.ai_query", "default_sql_mode", "Default SQL mode", "enum", ("strict_summary", "summary", "open")),
    ("ai.limits", "max_stored_sessions", "Max stored sessions on disk (0=default)", "int", ()),
)


def open_ai_settings(root, *, on_change=None):
    def _saved():
        module_config.reload()
        if on_change:
            on_change()

    open_module_config_dialog(
        root,
        title="AI Settings — ai_query/config.ini",
        config_module=module_config,
        fields=_FIELDS,
        on_saved=_saved,
    )
