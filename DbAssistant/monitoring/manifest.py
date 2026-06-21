"""Manifest for the Monitoring module — discovered by core.modules."""

from __future__ import annotations

from common.core.modules import ModuleManifest


def _check_requirements() -> list[str]:
    missing: list[str] = []
    try:
        import psutil  # noqa: F401
    except Exception:
        missing.append("psutil (host OS metrics) — pip install psutil")
    return missing


def _register_cli(subparsers):
    from .cli import register_cli
    return register_cli(subparsers)


def _dispatch_cli(args):
    from .cli import dispatch_cli
    return dispatch_cli(args)


def _build_router(svc=None):
    from .api import build_router
    return build_router(svc)


def _launch_ui(**ctx):
    from common.ui.tk.monitor.monitoring_ui import launch_ui

    launch_ui(**ctx)


def _build_tab(parent, context=None):
    from common.ui.tk.monitor.standalone import build_tab

    return build_tab(parent, context)


MANIFEST = ModuleManifest(
    name="monitor",
    title="Monitoring",
    description="Local + cloud database/host monitoring, threshold alerts, "
                "notifications and a background daemon.",
    register_cli=_register_cli,
    dispatch_cli=_dispatch_cli,
    cli_commands=["monitor", "monitor-connections", "monitor-db", "daemon",
                  "thresholds", "os", "cloud", "notify", "monitor-config",
                  "alerts"],
    build_router=_build_router,
    launch_ui=_launch_ui,
    build_tab=_build_tab,
    tab_label="Monitor",
    config_files=["monitoring/monitor_thresholds.ini",
                  "monitoring/monitor_config.ini"],
    check_requirements=_check_requirements,
)
