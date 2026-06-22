"""Shared screen base for DbTool Textual UI."""

from __future__ import annotations

from typing import Any, Iterator

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static


class BaseScreen(Screen):
    """Screen with access to the shared service instance."""

    # Canonical screen name for the shared tab bar; subclasses override so the
    # current tab is highlighted and re-selecting it is a no-op.
    NAV_ID: str = ""

    BINDINGS = [
        # Navigation uses ``switch_screen`` (the stack stays one deep), so a raw
        # ``pop_screen`` would reveal Textual's empty default screen. Go to the
        # Welcome/home screen instead — never a blank screen.
        ("escape", "app.home", "Welcome"),
        ("f1", "show_help", "Help"),
    ]

    def __init__(self, svc: Any, **kwargs) -> None:
        super().__init__(**kwargs)
        self.svc = svc

    def compose(self) -> ComposeResult:
        yield Header()
        yield from self._compose_tabbar()
        with Vertical(id="body"):
            yield Static(self.screen_title(), id="screen-title")
            yield from self.compose_body()
        yield Footer()

    def _compose_tabbar(self) -> Iterator:
        """A horizontal one-row tab bar mirroring the Tk notebook tabs."""
        try:
            items = self.app.nav_items()
        except Exception:  # noqa: BLE001
            items = []
        if not items:
            return
        with Horizontal(id="tabbar"):
            for name, label in items:
                yield Button(
                    label,
                    id=f"tabnav-{name}",
                    variant="primary" if name == self.NAV_ID else "default",
                )

    def compose_body(self) -> Iterator:
        yield Static("")

    def screen_title(self) -> str:
        return self.__class__.__name__

    def action_show_help(self) -> None:
        self.notify(
            "Ctrl+Q quits · Escape returns to Welcome · Ctrl+H Home.", timeout=4
        )
