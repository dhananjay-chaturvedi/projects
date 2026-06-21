"""Dashboard screen — at-a-glance status snapshot."""

from __future__ import annotations

from textual.containers import Horizontal
from textual.widgets import Button, DataTable, Static

from common.ui.textual.screens.base import BaseScreen


class DashboardScreen(BaseScreen):
    """Overall status, core counts, module readiness, and panel states."""

    NAV_ID = "dashboard"

    def screen_title(self) -> str:
        return "Dashboard"

    def compose_body(self):
        with Horizontal(classes="actions-row"):
            yield Button("Refresh", id="dash-refresh", variant="primary")
            yield Button("Reset layout", id="dash-reset-layout")
        yield Static("Drag header to rearrange is available in Tk; TUI uses fixed terminal sections.", classes="hint")
        yield Static("", id="dash-status", classes="status")
        yield Static("[b]Core[/]", classes="section")
        yield DataTable(id="dash-core", zebra_stripes=True)
        yield Static("[b]Modules[/]", classes="section")
        yield DataTable(id="dash-modules", zebra_stripes=True)
        yield Static("[b]Panels[/]", classes="section")
        yield DataTable(id="dash-panels", zebra_stripes=True)

    def on_mount(self) -> None:
        self._load()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if (event.button.id or "") == "dash-refresh":
            self._load()
        elif (event.button.id or "") == "dash-reset-layout":
            try:
                from common.headless import app_service as appsvc
                appsvc.reset_dashboard_layout()
            except Exception:
                pass
            self.query_one("#dash-status", Static).update("Layout reset.")
            self._load()

    def _load(self) -> None:
        try:
            from common.core import modules as app_modules
            from common.dashboard.service import (
                DashboardCapabilities,
                DashboardRuntime,
                DashboardService,
            )

            mod_status = {}
            try:
                mod_status = app_modules.status()
            except Exception:
                pass
            active = getattr(self.svc, "_active", {}) or {}

            dash = DashboardService(
                DashboardRuntime(
                    get_active_connections=lambda: (active if isinstance(active, dict) else {}),
                    get_saved_connections=lambda: self.svc.list_connections(),
                ),
                DashboardCapabilities(
                    has_schema=mod_status.get("migrator", {}).get("installed", False),
                    has_ai=mod_status.get("ai", {}).get("installed", False),
                    has_monitor=mod_status.get("monitor", {}).get("installed", False),
                ),
            )
            self._render_snapshot(dash.collect())
        except Exception as exc:
            self.query_one("#dash-status", Static).update(str(exc))
            self._load_fallback()

    def _load_fallback(self) -> None:
        conns = self.svc.list_connections()
        active = list(getattr(self.svc, "_active", {}) or {})
        core = self.query_one("#dash-core", DataTable)
        core.clear(columns=True)
        core.add_columns("Metric", "Value")
        core.add_row("Saved connections", str(len(conns)))
        core.add_row("Active connections", str(len(active)))
        core.add_row("Active", ", ".join(active))
        self.query_one("#dash-status", Static).update("Snapshot (fallback).")

    def _render_snapshot(self, d: dict) -> None:
        self.query_one("#dash-status", Static).update(
            d.get("overall_label") or d.get("overall_status") or "")
        core = d.get("core") or {}
        ct = self.query_one("#dash-core", DataTable)
        ct.clear(columns=True)
        ct.add_columns("Metric", "Value")
        ct.add_row("Saved connections", str(core.get("saved_connections_count", 0)))
        ct.add_row("Active connections", str(core.get("active_connections_count", 0)))
        ct.add_row("Active", ", ".join(core.get("active_connections", []) or []))

        mt = self.query_one("#dash-modules", DataTable)
        mt.clear(columns=True)
        mt.add_columns("Module", "Installed", "Ready")
        for key, info in (d.get("modules") or {}).items():
            mt.add_row(str(info.get("title", key)),
                       "yes" if info.get("installed") else "no",
                       "yes" if info.get("ready") else "no")

        pt = self.query_one("#dash-panels", DataTable)
        pt.clear(columns=True)
        pt.add_columns("Panel", "Status", "Detail")
        panels = d.get("panels") or {}
        if isinstance(panels, dict):
            for k, v in panels.items():
                if isinstance(v, dict):
                    pt.add_row(str(k), str(v.get("status", "")),
                               str(v.get("detail", v.get("label", ""))))
                else:
                    pt.add_row(str(k), str(v), "")
        elif isinstance(panels, list):
            for v in panels:
                if isinstance(v, dict):
                    pt.add_row(str(v.get("title", v.get("name", ""))),
                               str(v.get("status", "")), str(v.get("detail", "")))
