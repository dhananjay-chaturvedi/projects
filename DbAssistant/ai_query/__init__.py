"""
AI Query Assistant module.

Self-contained, independently shippable: this package holds the AI agent
(``agent.py``), the CLI-backed backends (``backends/``) and the Tkinter UI
(``ai_query_ui.py``).  Copy this package plus the shared ``core`` bundle to run
the AI Query Assistant on its own.

``AIQueryUI`` is exposed lazily so importing the headless agent
(``from ai_query.agent import AIQueryAgent``) does not pull in tkinter.
"""

from __future__ import annotations


def __getattr__(name: str):
    if name == "AIQueryUI":
        from .ai_query_ui import AIQueryUI
        return AIQueryUI
    if name == "AIQueryWorkspace":
        from .ai_query_workspace import AIQueryWorkspace
        return AIQueryWorkspace
    if name == "AIQueryAgent":
        from .agent import AIQueryAgent
        return AIQueryAgent
    raise AttributeError(f"module 'ai_query' has no attribute {name!r}")


__all__ = ["AIQueryUI", "AIQueryWorkspace", "AIQueryAgent"]
