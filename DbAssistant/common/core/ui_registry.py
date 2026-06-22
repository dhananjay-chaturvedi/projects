"""
Lazy UI launch registry — each UI package is optional and independently deletable.

Core/CLI/API never import UI packages at module load time; they call these
functions only when the user explicitly requests a UI.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from typing import Callable, Optional

# (package_check, entry_module, entry_callable)
# package_check is a dotted package name; all three UIs now live under
# ``common/ui/`` and are each independently deletable.
_UI_ENTRIES = {
    "tk": {
        "full": ("common.ui.tk", "common.ui.tk.launcher", "launch_desktop_ui"),
        "migrator": ("common.ui.tk", "common.ui.tk.migrator.schema_converter_ui", "launch_ui"),
        "ai": ("common.ui.tk", "common.ui.tk.ai.ai_query_ui", "launch_ui"),
        "monitor": ("common.ui.tk", "common.ui.tk.monitor.monitoring_ui", "launch_ui"),
    },
    "textual": {
        "full": ("common.ui.textual", "common.ui.textual.launcher", "launch_textual_ui"),
        "migrator": ("common.ui.textual", "common.ui.textual.launcher", "launch_textual_ui"),
        "ai": ("common.ui.textual", "common.ui.textual.launcher", "launch_textual_ui"),
        "monitor": ("common.ui.textual", "common.ui.textual.launcher", "launch_textual_ui"),
    },
    "web": {
        "full": ("common.ui.web", "common.ui.web.server", "launch_web_ui"),
        "migrator": ("common.ui.web", "common.ui.web.server", "launch_web_ui"),
        "ai": ("common.ui.web", "common.ui.web.server", "launch_web_ui"),
        "monitor": ("common.ui.web", "common.ui.web.server", "launch_web_ui"),
    },
}

_UI_LABELS = {
    "tk": "Tkinter desktop",
    "textual": "Textual TUI",
    "web": "Web",
}


def _package_available(package: str) -> bool:
    """Return True if *package* directory exists and is importable.

    ``package`` is a dotted name (e.g. ``common.ui.web``); convert it to a
    directory path under the project root before checking existence so a
    deleted sub-UI folder is correctly reported as unavailable.
    """
    from pathlib import Path

    # Project root: common/core/ui_registry.py -> parents[2]
    root = Path(__file__).resolve().parents[2]
    pkg_dir = root.joinpath(*package.split("."))
    if not pkg_dir.is_dir():
        return False
    try:
        spec = importlib.util.find_spec(package)
    except (ModuleNotFoundError, ValueError, ImportError):
        return False
    return spec is not None


def _import_callable(mod_path: str, fn_name: str) -> Callable:
    mod = importlib.import_module(mod_path)
    fn = getattr(mod, fn_name, None)
    if fn is None or not callable(fn):
        raise ImportError(f"{mod_path}.{fn_name} is not callable")
    return fn


def _missing_ui_message(ui_kind: str, package: str) -> str:
    label = _UI_LABELS.get(ui_kind, ui_kind)
    folder = package.replace(".", "/")
    return (
        f"{label} UI is not installed ({folder}/ folder missing).\n"
        f"CLI and API remain available. Install UI extras or restore {folder}/."
    )


def launch_ui(
    ui_kind: str,
    *,
    feature_module: Optional[str] = None,
    web: bool = False,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> None:
    """
    Launch a registered UI.

    Parameters
    ----------
    ui_kind:
        ``"tk"`` or ``"textual"``.
    feature_module:
        ``None`` for full app; ``"migrator"`` / ``"ai"`` / ``"monitor"`` for
        standalone module UI.
    web:
        When True and ``ui_kind == "textual"``, serve via ``textual serve``.
    host, port:
        Bind address for web mode.
    """
    entries = _UI_ENTRIES.get(ui_kind)
    if entries is None:
        print(f"Unknown UI kind: {ui_kind!r}", file=sys.stderr)
        sys.exit(1)

    key = feature_module or "full"
    if key not in entries:
        print(f"No {ui_kind} UI entry for module {feature_module!r}", file=sys.stderr)
        sys.exit(1)

    package, mod_path, fn_name = entries[key]
    if not _package_available(package):
        print(_missing_ui_message(ui_kind, package), file=sys.stderr)
        sys.exit(1)

    try:
        fn = _import_callable(mod_path, fn_name)
    except ImportError as exc:
        label = _UI_LABELS.get(ui_kind, ui_kind)
        print(f"Failed to load {label} UI: {exc}", file=sys.stderr)
        sys.exit(1)

    if ui_kind == "textual" and web:
        from common.ui.textual.launcher import launch_textual_web

        launch_textual_web(feature_module=feature_module, host=host, port=port)
        return

    if ui_kind == "web":
        fn(feature_module=feature_module, host=host, port=port)
        return

    if feature_module and ui_kind in ("tk", "textual"):
        fn(feature_module=feature_module)
    else:
        fn()


def launch_tk_ui(feature_module: Optional[str] = None) -> None:
    """Convenience wrapper for Tk desktop UI."""
    launch_ui("tk", feature_module=feature_module)


def launch_textual_ui(
    feature_module: Optional[str] = None,
    *,
    web: bool = False,
    host: str = "127.0.0.1",
    port: int = 8080,
) -> None:
    """Convenience wrapper for Textual TUI (optionally served on web)."""
    launch_ui(
        "textual",
        feature_module=feature_module,
        web=web,
        host=host,
        port=port,
    )


def launch_web_ui(
    feature_module: Optional[str] = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8090,
) -> None:
    """Convenience wrapper for the HTML/JS web UI (served on FastAPI)."""
    launch_ui("web", feature_module=feature_module, host=host, port=port)
