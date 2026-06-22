"""
Module shipping manifest — what each independently installable bundle needs.

Copy the listed directories/files into a deployment folder, then run:

    bash setup/install.sh --module migrator
    # or: python setup/install.py --module migrator
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModuleBundle:
    key: str
    title: str
    description: str
    # Paths relative to project root that must exist to run this module
    required_paths: tuple[str, ...]
    # pip requirement files relative to setup/
    requirement_files: tuple[str, ...]
    # Skipped when --no-optional is passed
    optional_requirement_files: tuple[str, ...] = ()
    cli_examples: tuple[str, ...] = ()
    ui_examples: tuple[str, ...] = ()


SETUP = "setup"

# Every module ships with common/ + root config (created from examples on install)
_COMMON = ("common/",)
# Full tool adds app/ and all feature modules
_FULL_PATHS = _COMMON + (
    "app/",
    "schema_converter/",
    "ai_query/",
    "monitoring/",
    "ai_assistant/",
    "dbtool.py",
    "conDbUi.py",
    "run.sh",
    "install.sh",
)

_CORE_REQ = (f"{SETUP}/requirements-core.txt", f"{SETUP}/requirements-drivers.txt")
_API_REQ = (f"{SETUP}/requirements-api.txt",)
_CLOUD_REQ = (f"{SETUP}/requirements-cloud.txt",)


MODULES: dict[str, ModuleBundle] = {
    "core": ModuleBundle(
        key="core",
        title="Core DB layer",
        description="Connections, SQL editor, objects browser — no feature module tab.",
        required_paths=_COMMON,
        requirement_files=_CORE_REQ,
        cli_examples=(
            "python -m common.core.cli_handlers --help  # via module runner",
            "python dbtool.py connections list  # when app/ is present",
        ),
    ),
    "migrator": ModuleBundle(
        key="migrator",
        title="Data Migration",
        description="Cross-database migration: schema conversion, data transfer, "
                    "and post-migration validation.",
        required_paths=_COMMON + ("schema_converter/",),
        requirement_files=_CORE_REQ + _API_REQ + ("schema_converter/requirements.txt",),
        cli_examples=(
            "python -m schema_converter migrator convert --help",
            "python -m schema_converter connections list",
        ),
        ui_examples=(
            "python -m schema_converter --ui",
            "bash schema_converter/run_schema_converter.sh",
        ),
    ),
    "ai": ModuleBundle(
        key="ai",
        title="AI Query Assistant",
        description="Natural-language to SQL with session management.",
        required_paths=_COMMON + ("ai_query/",),
        requirement_files=_CORE_REQ + _API_REQ + ("ai_query/requirements.txt",),
        cli_examples=("python -m ai_query ai --help",),
        ui_examples=(
            "python -m ai_query --ui",
            "bash ai_query/run_ai_query_assistant.sh",
        ),
    ),
    "monitor": ModuleBundle(
        key="monitor",
        title="Monitoring",
        description="SSH/OS/DB/cloud metrics, thresholds, alerts, daemon.",
        required_paths=_COMMON + ("monitoring/",),
        requirement_files=_CORE_REQ
        + _CLOUD_REQ
        + _API_REQ
        + ("monitoring/requirements.txt",),
        optional_requirement_files=(),  # cloud already in requirement_files
        cli_examples=(
            "python -m monitoring monitor --help",
            "python -m monitoring daemon status",
        ),
        ui_examples=(
            "python -m monitoring --ui",
            "bash monitoring/run_monitor.sh",
        ),
    ),
    "app_builder": ModuleBundle(
        key="app_builder",
        title="App Builder",
        description="Generate apps from scratch, an existing codebase, or a "
                    "database — with governed, agentic build jobs.",
        required_paths=_COMMON + ("ai_query/", "ai_assistant/"),
        requirement_files=_CORE_REQ + _API_REQ + ("ai_query/requirements.txt",),
        optional_requirement_files=("ai_query/requirements-llm.txt",),
        cli_examples=(
            "python -m ai_assistant.app_builder app-builder --help",
            "python dbtool.py app-builder app-status --name myapp",
        ),
        ui_examples=(
            "python dbtool.py ui --module ai      # App Builder opens from the AI tab",
        ),
    ),
    "full": ModuleBundle(
        key="full",
        title="Full DbManagementTool",
        description="All modules + master CLI (dbtool.py) and combined API.",
        required_paths=_FULL_PATHS,
        requirement_files=(f"{SETUP}/requirements-full.txt",),
        cli_examples=("python dbtool.py --help", "python dbtool.py modules"),
        ui_examples=("bash run.sh", "python conDbUi.py"),
    ),
}


def get_module(key: str) -> ModuleBundle:
    k = (key or "full").strip().lower()
    aliases = {
        "monitoring": "monitor",
        "schema_converter": "migrator",
        "schema": "migrator",  # legacy bundle key → new name
        "ai_query": "ai",
    }
    k = aliases.get(k, k)
    if k not in MODULES:
        valid = ", ".join(sorted(MODULES))
        raise ValueError(f"Unknown module {key!r}. Choose one of: {valid}")
    return MODULES[k]


def all_module_keys() -> list[str]:
    return list(MODULES.keys())
