"""Common UI properties shared by the Tk, Textual and Web UIs.

Values are sourced from ``common.branding`` (product name) and
``properties.ini`` (``[ui.*]`` sections), so a single edit there flows to every
UI. Safe defaults are baked in so the UIs still work if a key is missing or the
properties file is absent.
"""

from __future__ import annotations

from dataclasses import dataclass

from common.branding import APP_NAME, APP_SHORT_NAME
from common.config_loader import properties

# Per-module window/title names (mirrors ui_tk.master_shell).
_MODULE_TITLES = {
    "migrator": "Data Migration",
    "ai": "AI Query Assistant",
    "monitor": "Monitoring",
}


def app_title(feature_module: str | None = None) -> str:
    """Window/title-bar text. Full tool uses the product name; a standalone
    module uses ``"<short> — <module title>"`` (identical to the Tk shell)."""
    if feature_module:
        sub = _MODULE_TITLES.get(feature_module, feature_module)
        return f"{APP_SHORT_NAME} — {sub}"
    return APP_NAME


def default_web_host() -> str:
    return properties.get("ui.web", "host", "127.0.0.1")


def default_web_port() -> int:
    return properties.get_int("ui.web", "port", 8090)


def window_size(name: str = "main") -> tuple[int, int]:
    """Width/height for a named window (delegates to config_loader)."""
    try:
        from common.config_loader import get_window_size

        return get_window_size(name)
    except Exception:
        return (1150, 780)


def fonts() -> dict:
    """Platform-appropriate UI + monospace font families/sizes."""
    import sys

    if sys.platform == "darwin":
        keys = ("macos_ui_font_family", "Helvetica Neue", "macos_ui_font_size", 13,
                "macos_mono_font_family", "Menlo", "macos_mono_font_size", 12)
    elif sys.platform == "win32":
        keys = ("windows_ui_font_family", "Segoe UI", "windows_ui_font_size", 10,
                "windows_mono_font_family", "Consolas", "windows_mono_font_size", 10)
    else:
        keys = ("linux_ui_font_family", "DejaVu Sans", "linux_ui_font_size", 10,
                "linux_mono_font_family", "DejaVu Sans Mono", "linux_mono_font_size", 10)
    uf, ufd, ufs, ufsd, mf, mfd, mfs, mfsd = keys
    return {
        "ui_family": properties.get("ui.fonts", uf, ufd),
        "ui_size": properties.get_int("ui.fonts", ufs, ufsd),
        "mono_family": properties.get("ui.fonts", mf, mfd),
        "mono_size": properties.get_int("ui.fonts", mfs, mfsd),
    }


@dataclass(frozen=True)
class UITheme:
    """Resolved colour palette (hex strings) shared across UIs."""

    primary: str
    primary_dark: str
    primary_light: str
    accent: str
    success: str
    warning: str
    error: str
    bg_main: str
    bg_secondary: str
    bg_dark: str
    text_primary: str
    text_secondary: str
    border: str
    connected: str
    disconnected: str
    error_bg: str
    success_bg: str
    info_bg: str

    def as_dict(self) -> dict:
        return {
            "primary": self.primary,
            "primaryDark": self.primary_dark,
            "primaryLight": self.primary_light,
            "accent": self.accent,
            "success": self.success,
            "warning": self.warning,
            "error": self.error,
            "bgMain": self.bg_main,
            "bgSecondary": self.bg_secondary,
            "bgDark": self.bg_dark,
            "textPrimary": self.text_primary,
            "textSecondary": self.text_secondary,
            "border": self.border,
            "connected": self.connected,
            "disconnected": self.disconnected,
            "errorBg": self.error_bg,
            "successBg": self.success_bg,
            "infoBg": self.info_bg,
        }


def theme() -> UITheme:
    """Load the colour palette from ``properties.ini`` (with Tk-matched defaults)."""
    g = properties.get
    return UITheme(
        primary=g("ui.colors.primary", "primary", "#2196F3"),
        primary_dark=g("ui.colors.primary", "primary_dark", "#1976D2"),
        primary_light=g("ui.colors.primary", "primary_light", "#BBDEFB"),
        accent=g("ui.colors.accent", "accent", "#FF9800"),
        success=g("ui.colors.accent", "success", "#4CAF50"),
        warning=g("ui.colors.accent", "warning", "#FFC107"),
        error=g("ui.colors.accent", "error", "#F44336"),
        bg_main=g("ui.colors.neutral", "bg_main", "#F5F5F5"),
        bg_secondary=g("ui.colors.neutral", "bg_secondary", "#FFFFFF"),
        bg_dark=g("ui.colors.neutral", "bg_dark", "#424242"),
        text_primary=g("ui.colors.neutral", "text_primary", "#212121"),
        text_secondary=g("ui.colors.neutral", "text_secondary", "#757575"),
        border=g("ui.colors.neutral", "border", "#E0E0E0"),
        connected=g("ui.colors.status", "connected", "#4CAF50"),
        disconnected=g("ui.colors.status", "disconnected", "#9E9E9E"),
        error_bg=g("ui.colors.status", "error_bg", "#FFEBEE"),
        success_bg=g("ui.colors.status", "success_bg", "#E8F5E9"),
        info_bg=g("ui.colors.status", "info_bg", "#E3F2FD"),
    )
