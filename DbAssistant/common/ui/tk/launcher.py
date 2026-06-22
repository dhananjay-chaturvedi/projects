"""
Launch the shared desktop shell (Connections + Objects + SQL Editor + module tabs).

Shipped in ``common/`` so each module can open its full UI without ``app/``.
"""

from __future__ import annotations

import sys
from typing import Optional


def launch_desktop_ui(feature_module: Optional[str] = None) -> None:
    """
    Open the master Tk UI.

    * ``feature_module=None`` — all installed module tabs (full tool).
    * ``feature_module='migrator'|'ai'|'monitor'`` — core tabs + that module only.
    """
    try:
        import tkinter as tk  # noqa: F401
    except ImportError:
        from common.core import cliutil

        cliutil.err("Desktop UI requires tkinter (python3-tk on Linux).")
        cliutil.info("Use --shell-ui for the bash menu, or install python3-tk.")
        sys.exit(1)

    from common.ui.tk.master_shell import main

    main(feature_module=feature_module)


__all__ = ["launch_desktop_ui"]
