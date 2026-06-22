"""
Shared cloud connection wizard — used by Connections tab and Monitoring tab.

The full provider form lives on ``ServerMonitorUI``; this adapter lets any host
(Connections tab, Monitor tab) drive the same flow with different options.
"""

from __future__ import annotations

import tkinter as tk
from dataclasses import dataclass
from typing import Any, Callable, Optional

from common.cloud import (
    CLOUD_PROVIDER_SCHEMAS,
    MFA_TYPES,
    MONITOR_TARGET_KINDS,
    PURPOSE_CONNECTIONS,
    PURPOSE_MONITOR,
    TARGET_CLOUD_DB,
    CloudConnectionManager,
)
from common.cloud.profiles import TARGET_CLOUD_SERVICE, TARGET_VM


@dataclass(frozen=True)
class CloudWizardContext:
    """Host dependencies and options for the shared cloud connection wizard."""

    root: tk.Tk
    ui_font: tuple
    update_status: Callable[..., Any]
    purpose: str = PURPOSE_CONNECTIONS
    require_db_identifier: bool | None = True
    target_kind: str = TARGET_CLOUD_DB
    allow_target_kinds: Optional[list[str]] = None
    api_test_fn: Optional[Callable[[dict], tuple[str, str]]] = None
    cloud_databases: Optional[dict] = None
    cloud_manager: Optional[CloudConnectionManager] = None
    connection_manager: Optional[Any] = None
    on_saved: Optional[Callable[[dict], None]] = None


def _default_api_test(data: dict) -> tuple[str, str]:
    """Lightweight test when full monitor UI test helpers are unavailable."""
    try:
        from monitoring.cloud_provider_registry import CloudProviderRegistry

        monitor, err = CloudProviderRegistry.build_monitor(data)
        if err:
            return err, "error"
        errors = monitor.check_health() if monitor else ["No monitor"]
        if errors:
            return "; ".join(errors), "error"
        return "✓ Cloud API connection healthy.", "ok"
    except Exception as exc:
        return str(exc), "error"


class CloudConnectionWizardAdapter:
    """
    Minimal host surface for ``ServerMonitorUI`` cloud wizard methods.

    ``ServerMonitorUI.add_cloud_database`` / ``_open_cloud_provider_form`` are
    invoked with ``self`` bound to this adapter so the same form is reused.
    """

    _CLOUD_PROVIDER_SCHEMAS = CLOUD_PROVIDER_SCHEMAS
    _MFA_TYPES = MFA_TYPES

    def __init__(
        self,
        context: CloudWizardContext | tk.Tk,
        ui_font: tuple | None = None,
        **legacy,
    ):
        if not isinstance(context, CloudWizardContext):
            context = CloudWizardContext(
                root=context,
                ui_font=ui_font or (),
                update_status=legacy["update_status"],
                api_test_fn=legacy.get("api_test_fn"),
                cloud_databases=legacy.get("cloud_databases"),
                cloud_manager=legacy.get("cloud_manager"),
                connection_manager=legacy.get("connection_manager"),
                on_saved=legacy.get("on_saved"),
            )
        self.root = context.root
        self.ui_font = context.ui_font
        self.update_status = context.update_status
        self._api_test_fn = context.api_test_fn or _default_api_test
        self.cloud_connection_manager = context.cloud_manager or CloudConnectionManager()
        self.connection_manager = context.connection_manager
        self.cloud_databases = (
            context.cloud_databases
            if context.cloud_databases is not None
            else self.cloud_connection_manager.load_cloud_databases()
        )
        self._on_saved = context.on_saved
        self._cloud_wizard_opts: dict = {}
        self.active_cloud_databases: dict = {}
        self.active_cloud_monitors: dict = {}

    def update_cloud_db_listbox(self) -> None:
        pass

    def _run_cloud_api_test(self, data: dict) -> tuple[str, str]:
        return self._api_test_fn(data)

    def _clear_cloud_liveness_state(self, display_name: str) -> None:
        pass

    def add_cloud_database(self) -> None:
        from common.ui.tk.monitor.server_monitor.server_monitor_ui import ServerMonitorUI

        ServerMonitorUI.add_cloud_database(self)

    def _open_cloud_provider_form(
        self, provider: str, edit_name: str | None = None
    ) -> None:
        from common.ui.tk.monitor.server_monitor.server_monitor_ui import ServerMonitorUI

        ServerMonitorUI._open_cloud_provider_form(self, provider, edit_name)

    def run(
        self,
        *,
        purpose: str = PURPOSE_MONITOR,
        require_db_identifier: Optional[bool] = None,
        target_kind: str = TARGET_CLOUD_DB,
        allow_target_kinds: Optional[list[str]] = None,
    ) -> None:
        self._cloud_wizard_opts = {
            "purpose": purpose,
            "require_db_identifier": require_db_identifier,
            "target_kind": target_kind,
            "allow_target_kinds": allow_target_kinds
            or list(MONITOR_TARGET_KINDS.keys()),
            "on_saved": self._on_saved,
        }
        self.add_cloud_database()


def run_cloud_connection_wizard(
    context: CloudWizardContext | tk.Tk,
    ui_font: tuple | None = None,
    update_status: Callable[..., Any] | None = None,
    **legacy,
) -> None:
    """Entry point for Connections tab (or scripts) — opens the shared cloud wizard."""
    if not isinstance(context, CloudWizardContext):
        context = CloudWizardContext(
            root=context,
            ui_font=ui_font or (),
            update_status=update_status,
            purpose=legacy.get("purpose", PURPOSE_CONNECTIONS),
            require_db_identifier=legacy.get("require_db_identifier", True),
            target_kind=legacy.get("target_kind", TARGET_CLOUD_DB),
            allow_target_kinds=legacy.get("allow_target_kinds"),
            api_test_fn=legacy.get("api_test_fn"),
            connection_manager=legacy.get("connection_manager"),
            on_saved=legacy.get("on_saved"),
        )
    CloudConnectionWizardAdapter(context).run(
        purpose=context.purpose,
        require_db_identifier=context.require_db_identifier,
        target_kind=context.target_kind,
        allow_target_kinds=context.allow_target_kinds,
    )


__all__ = [
    "CloudConnectionWizardAdapter",
    "CloudWizardContext",
    "run_cloud_connection_wizard",
    "PURPOSE_CONNECTIONS",
    "PURPOSE_MONITOR",
    "TARGET_CLOUD_DB",
    "TARGET_VM",
    "TARGET_CLOUD_SERVICE",
]
