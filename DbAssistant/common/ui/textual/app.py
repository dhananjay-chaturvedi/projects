"""Main Textual application."""

from __future__ import annotations

import os
from typing import Any, ClassVar, Optional

from textual.app import App
from textual.binding import Binding

from common.ui.textual.service_factory import build_service
from common.ui.textual.screens import (
    AiQueryScreen,
    ConnectionsScreen,
    DashboardScreen,
    HomeScreen,
    MigrationScreen,
    MonitoringScreen,
    ObjectsScreen,
    SettingsScreen,
    SqlEditorScreen,
)


class DbToolApp(App):
    """DbManagementTool Textual UI — full or single-module mode."""

    TITLE = "DbManagementTool"
    CSS = """
    Screen { padding: 1; }
    #tabbar {
        height: auto;
        width: 1fr;
        margin-bottom: 1;
        border-bottom: solid $primary-darken-2;
        overflow-x: auto;
        overflow-y: hidden;
    }
    #tabbar Button {
        width: auto;
        min-width: 6;
        margin: 0 1 0 0;
        border: none;
        height: 1;
    }
    #body { height: 1fr; overflow-y: auto; }
    .hint { color: $text-muted; margin-bottom: 1; }
    .section { margin-top: 1; }
    .status { color: $text-muted; margin-top: 1; }
    .actions-row { height: auto; margin-top: 1; }
    .actions-row Button { margin-right: 1; }
    .mini { min-width: 10; }
    TextArea { height: 8; }
    #mig-output { height: 10; }
    DataTable { height: 12; }
    ListView { height: 6; border: round $primary-darken-2; }
    """

    # Canonical tab id (common.ui.shared.tabs) -> installed screen name.
    _TAB_SCREEN: ClassVar[dict[str, str]] = {
        "welcome": "home",
        "connections": "connections",
        "dashboard": "dashboard",
        "objects": "objects",
        "sql_editor": "sql",
        "migrator": "migration",
        "ai": "ai",
        "monitor": "monitor",
        "settings": "settings",
    }

    BINDINGS: ClassVar[list[Binding]] = [
        Binding("ctrl+q", "quit", "Quit"),
        Binding("ctrl+h", "home", "Home"),
    ]

    # Module screens that should only be installed when their module is present.
    _MODULE_SCREENS = {"migration": "migrator", "ai": "ai", "monitor": "monitor"}

    def __init__(self, feature_module: Optional[str] = None) -> None:
        env_mod = os.environ.get("DBTOOL_FEATURE_MODULE", "").strip() or None
        self.feature_module = feature_module or env_mod
        self.svc: Any = build_service(self.feature_module)
        self._screen_names: set[str] = set()
        super().__init__()

    def _available_modules(self) -> list[tuple[str, str]]:
        from common.core import modules as app_modules

        mods: list[tuple[str, str]] = []
        if self.feature_module:
            titles = {"migrator": "Data Migration", "ai": "AI Query", "monitor": "Monitoring"}
            mods.append((self.feature_module, titles.get(self.feature_module, self.feature_module)))
            return mods
        installed = app_modules.discover()
        labels = {
            "migrator": "Data Migration",
            "ai": "AI Query Assistant",
            "monitor": "Monitoring",
        }
        for key in ("migrator", "ai", "monitor"):
            if key in installed:
                mods.append((key, labels[key]))
        return mods

    def _install_screens(self) -> None:
        available = {key for key, _ in self._available_modules()}

        self.install_screen(HomeScreen(self._available_modules()), name="home")
        self._screen_names.add("home")

        core_screens = {
            "connections": ConnectionsScreen,
            "dashboard": DashboardScreen,
            "sql": SqlEditorScreen,
            "objects": ObjectsScreen,
            "settings": SettingsScreen,
        }
        for name, cls in core_screens.items():
            self.install_screen(cls(self.svc), name=name)
            self._screen_names.add(name)

        module_screens = {
            "migration": MigrationScreen,
            "ai": AiQueryScreen,
            "monitor": MonitoringScreen,
        }
        for name, cls in module_screens.items():
            if self._MODULE_SCREENS[name] in available:
                self.install_screen(cls(self.svc), name=name)
                self._screen_names.add(name)

    def on_mount(self) -> None:
        self._install_screens()
        self.push_screen("home")

    def action_home(self) -> None:
        while len(self.screen_stack) > 1:
            self.pop_screen()
        self.push_screen("home")

    def push_screen_by_name(self, name: str) -> None:
        if name in self._screen_names:
            self.push_screen(name)
        else:
            self.notify(f"{name} is not available in this build.", severity="warning")

    def nav_items(self) -> list[tuple[str, str]]:
        """Ordered (screen_name, label) tabs in the canonical Tk notebook order.

        Drives the horizontal one-row tab bar shown on every content screen so
        TUI navigation mirrors the Tk notebook.
        """
        try:
            from common.ui.shared import visible_tabs
        except Exception:  # noqa: BLE001
            return []
        items: list[tuple[str, str]] = []
        for tab in visible_tabs(self.feature_module):
            screen = self._TAB_SCREEN.get(tab.id)
            if screen and screen in self._screen_names:
                items.append((screen, tab.label))
        return items

    def on_button_pressed(self, event) -> None:  # type: ignore[no-untyped-def]
        """Handle the shared tab bar (ids ``tabnav-<screen>``) at the app level.

        Content screens define their own ``on_button_pressed``; because none of
        them stop the message, it bubbles here so one handler drives navigation.
        """
        bid = getattr(event.button, "id", "") or ""
        if not bid.startswith("tabnav-"):
            return
        target = bid[len("tabnav-"):]
        if target not in self._screen_names:
            return
        current = getattr(self.screen, "NAV_ID", "")
        if current == target:
            return
        self.switch_screen(target)


# ``textual serve common.ui.textual.app:DbToolApp`` — class reference, not an instance.
