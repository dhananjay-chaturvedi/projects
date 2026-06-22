"""Launch Textual TUI (terminal or web via textual serve)."""

from __future__ import annotations

import os
import subprocess
import sys
from typing import Optional


def _ensure_textual() -> None:
    try:
        import textual  # noqa: F401
    except ImportError:
        from common.core import cliutil

        cliutil.err("Textual UI requires: pip install textual")
        cliutil.info("Use CLI/API, --shell-ui, or Tk desktop UI (--ui).")
        sys.exit(1)


def launch_textual_ui(feature_module: Optional[str] = None) -> None:
    """Run the Textual app in the terminal."""
    _ensure_textual()
    from common.ui.textual.app import DbToolApp

    app = DbToolApp(feature_module=feature_module)
    app.run()


def launch_textual_web(
    feature_module: Optional[str] = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> None:
    """Serve the same Textual app in a browser (xterm.js terminal in page)."""
    _ensure_textual()
    try:
        import textual_serve  # noqa: F401
    except ImportError:
        from common.core import cliutil

        cliutil.err("Web mode requires: pip install textual-serve")
        sys.exit(1)

    if feature_module:
        os.environ["DBTOOL_FEATURE_MODULE"] = feature_module
    else:
        os.environ.pop("DBTOOL_FEATURE_MODULE", None)

    cmd = [
        sys.executable,
        "-m",
        "textual",
        "serve",
        "common.ui.textual.app:DbToolApp",
        "--host",
        host,
        "--port",
        str(port),
    ]
    print(f"Textual web UI on http://{host}:{port}/")
    raise SystemExit(subprocess.call(cmd))
