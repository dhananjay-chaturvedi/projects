"""
Module framework for DbManagementTool.

The tool is split into a shared **core** (connections, SQL editor, object
browser, drivers, registry — common to everything) plus three optional,
independently shippable **modules**:

    * migrator — Data Migration (schema convert + data transfer + validation)
    * ai       — AI Query Assistant
    * monitor  — Monitoring (local + cloud + daemon + alerts)

Each module lives in its own self-contained package and declares a
:class:`ModuleManifest`.  A module is *installed* when its package is present
on disk and importable; otherwise the master CLI / API / UI treat it as
missing and report a clear "module not installed" message.

To ship a single module, copy the ``common/`` package and that one module
package (e.g. ``schema_converter/``, which powers the ``migrator`` command)
together.  To ship everything, keep the whole master folder.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# Static catalogue of known modules
# ---------------------------------------------------------------------------
# Maps the *CLI command* a module owns -> where to import it from.  This lets
# the master CLI/API/UI report "module X is not installed" for a module that is
# known to exist conceptually but whose package is absent from this checkout.
#
#   command : (import_package, human_title)
KNOWN_MODULES: dict[str, tuple[str, str]] = {
    "migrator": ("schema_converter",          "Data Migration"),
    "ai":      ("ai_query",                 "AI Query Assistant"),
    "monitor": ("monitoring",               "Monitoring"),
    "app_builder": ("ai_assistant.app_builder", "App Builder"),
}

# Top-level CLI commands each module owns.  Declared statically so the master
# CLI can add "module not installed" stubs and route commands *without* having
# to import a module that may be absent.
MODULE_CLI_COMMANDS: dict[str, list[str]] = {
    "migrator": ["migrator"],
    "ai":      ["ai"],
    "monitor": [
        "monitor",
        "monitor-connections",
        "monitor-db",
        "monitor-config",
        "daemon",
        "thresholds",
        "os",
        "alerts",
        "cloud",
        "notify",
    ],
    "app_builder": ["app-builder"],
}


def module_for_command(command: str) -> Optional[str]:
    """Return the module key that owns a given top-level CLI *command*."""
    for module_key, commands in MODULE_CLI_COMMANDS.items():
        if command in commands:
            return module_key
    return None


@dataclass
class ModuleManifest:
    """Everything the master shell needs to wire a module in.

    All callables are optional so a module can opt into only the surfaces it
    supports (e.g. a UI-less module can omit ``ui_launcher``).
    """

    name: str                     # short command/key, e.g. "schema"
    title: str                    # human title, e.g. "Schema Converter"
    description: str = ""

    # --- CLI integration -------------------------------------------------
    # register_cli(subparsers) -> add this module's argparse subcommand(s).
    register_cli: Optional[Callable] = None
    # dispatch_cli(args) -> int ; handle a parsed namespace, return exit code.
    dispatch_cli: Optional[Callable] = None
    # The top-level CLI command(s) this module owns (for gating / help).
    cli_commands: list[str] = field(default_factory=list)

    # --- REST API integration -------------------------------------------
    # build_router() -> fastapi.APIRouter (mounted by the master API).
    build_router: Optional[Callable] = None

    # --- UI integration --------------------------------------------------
    # launch_ui(**ctx) -> open a standalone Tk window for this module.
    launch_ui: Optional[Callable] = None
    # build_tab(parent, context) -> create the module's panel inside a parent
    # frame, for the combined master UI.
    build_tab: Optional[Callable] = None
    # Tab label used in the combined UI (defaults to title).
    tab_label: str = ""

    # --- Packaging / config ---------------------------------------------
    config_files: list[str] = field(default_factory=list)
    # check_requirements() -> list of missing dependency hints (empty = OK).
    check_requirements: Optional[Callable] = None

    def tab_text(self) -> str:
        return self.tab_label or self.title


class ModuleNotInstalled(Exception):
    """Raised when an operation targets a module that is not present."""

    def __init__(self, command: str):
        self.command = command
        pkg, title = KNOWN_MODULES.get(command, (command, command))
        pkg_path = pkg.replace(".", "/") + "/"
        super().__init__(
            f"Module '{command}' ({title}) is not installed in this build.\n"
            f"To enable it, ship the '{pkg_path}' package alongside the core bundle, "
            f"or install the full tool."
        )


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
_CACHE: Optional[dict[str, ModuleManifest]] = None


def _load_manifest(import_pkg: str) -> Optional[ModuleManifest]:
    """Import ``<import_pkg>.manifest`` and return its MANIFEST, or None."""
    try:
        mod = importlib.import_module(f"{import_pkg}.manifest")
    except Exception:
        return None
    manifest = getattr(mod, "MANIFEST", None)
    return manifest if isinstance(manifest, ModuleManifest) else None


def discover(refresh: bool = False) -> dict[str, ModuleManifest]:
    """Return ``{command: ModuleManifest}`` for every *installed* module."""
    global _CACHE
    if _CACHE is not None and not refresh:
        return _CACHE
    found: dict[str, ModuleManifest] = {}
    for command, (import_pkg, _title) in KNOWN_MODULES.items():
        manifest = _load_manifest(import_pkg)
        if manifest is not None:
            found[command] = manifest
    _CACHE = found
    return found


def is_installed(command: str) -> bool:
    return command in discover()


def get(command: str) -> ModuleManifest:
    """Return the manifest for *command* or raise :class:`ModuleNotInstalled`."""
    mods = discover()
    if command not in mods:
        raise ModuleNotInstalled(command)
    return mods[command]


def installed_commands() -> list[str]:
    return sorted(discover().keys())


def missing_commands() -> list[str]:
    return sorted(set(KNOWN_MODULES) - set(discover()))


def status() -> dict[str, dict]:
    """Human/JSON-friendly snapshot of which modules are present."""
    mods = discover()
    out: dict[str, dict] = {}
    for command, (import_pkg, title) in KNOWN_MODULES.items():
        manifest = mods.get(command)
        missing_deps: list[str] = []
        if manifest and manifest.check_requirements:
            try:
                missing_deps = manifest.check_requirements() or []
            except Exception as exc:  # pragma: no cover - defensive
                missing_deps = [f"requirement check failed: {exc}"]
        out[command] = {
            "title": title,
            "package": import_pkg,
            "installed": manifest is not None,
            "ready": manifest is not None and not missing_deps,
            "missing_requirements": missing_deps,
        }
    return out
