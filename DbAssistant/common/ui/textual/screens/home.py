"""Home / navigation screen."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import Button, Footer, Header, Static


class HomeScreen(Screen):
    """Welcome / navigation hub.

    The rich Welcome content (overview, per-tab guide, CLI/API access,
    shortcuts, platforms, tips) is rendered from the shared spec
    (:mod:`common.ui.shared.specs`) so the Tk, Textual and Web Welcome screens
    stay in sync from a single source.
    """

    BINDINGS = [("q", "quit", "Quit")]

    DEFAULT_CSS = """
    HomeScreen #welcome-body {
        height: 1fr;
        padding: 1 2;
    }
    HomeScreen .welcome-tagline { color: $text-muted; margin-bottom: 1; }
    HomeScreen .welcome-h { text-style: bold; color: $accent; margin-top: 1; }
    HomeScreen .welcome-tab { text-style: bold; color: $primary; margin-top: 1; }
    HomeScreen .welcome-footer { color: $text-muted; margin-top: 1; }
    """

    def __init__(self, modules: list[tuple[str, str]], **kwargs) -> None:
        super().__init__(**kwargs)
        self._modules = modules

    @staticmethod
    def _label(tab_id: str, default: str) -> str:
        """Tab label from the shared UI spec (common.ui.shared), with fallback."""
        try:
            from common.ui import shared

            spec = shared.tab_by_id(tab_id)
            return spec.label if spec else default
        except Exception:
            return default

    def compose(self) -> ComposeResult:
        try:
            from common.ui import shared

            title = shared.app_title()
        except Exception:
            title = "DbManagementTool"
        yield Header(show_clock=True)
        with Vertical(id="home-body"):
            yield Static(f"[bold]{title}[/] — Textual UI", id="title")
            with Horizontal(id="tabbar"):
                for screen_name, label in self._nav_items():
                    yield Button(
                        label,
                        id=f"tabnav-{screen_name}",
                        variant="primary" if screen_name == "home" else "default",
                    )
            yield from self._compose_welcome()
        yield Footer()

    def _compose_welcome(self) -> ComposeResult:
        """Render the shared Welcome content (single source for all UIs)."""
        try:
            from common.ui.shared import specs

            w = specs.welcome_payload()
        except Exception:
            yield Static(
                "Use the tab row above to open each section. Ctrl+Q quits.",
                classes="hint",
            )
            return

        with VerticalScroll(id="welcome-body"):
            yield Static(f"[i]{w['tagline']}[/]", classes="welcome-tagline")
            yield Static(
                "Use the tab row above to open each section. Ctrl+Q quits.",
                classes="hint",
            )

            yield Static("Quick Overview", classes="welcome-h")
            for item in w["overview"]:
                yield Static(f"• {item}")

            yield Static("Tab Descriptions & Usage Guide", classes="welcome-h")
            for guide in w["tabGuide"]:
                yield Static(guide["title"], classes="welcome-tab")
                for line in guide["lines"]:
                    yield Static(f"  {line}")

            yield Static("CLI, REST API & modular builds", classes="welcome-h")
            for line in w["access"]:
                yield Static(f"• {line}")

            yield Static("Keyboard Shortcuts", classes="welcome-h")
            for sc in w["shortcuts"]:
                yield Static(f"  [b]{sc['keys']}[/]  {sc['action']}")

            yield Static("Platforms", classes="welcome-h")
            for pf in w["platforms"]:
                yield Static(f"  [b]{pf['name']}[/]  {pf['versions']}")

            yield Static("Tips", classes="welcome-h")
            for tip in w["tips"]:
                yield Static(f"• {tip}")

            yield Static(w["footer"], classes="welcome-footer")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        # ``tabnav-*`` buttons bubble to DbToolApp.on_button_pressed.
        pass

    def _nav_items(self) -> list[tuple[str, str]]:
        try:
            return self.app.nav_items()  # type: ignore[attr-defined]
        except Exception:
            return [
                ("home", self._label("welcome", "Welcome")),
                ("connections", self._label("connections", "Connections")),
                ("dashboard", self._label("dashboard", "Dashboard")),
                ("objects", self._label("objects", "Database Objects")),
                ("sql", self._label("sql_editor", "SQL Editor")),
                *[(key, label) for key, label in self._modules],
                ("settings", self._label("settings", "Settings")),
            ]

    def action_quit(self) -> None:
        self.app.exit()
