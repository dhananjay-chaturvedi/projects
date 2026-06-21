"""Reusable INI loader for independently-shippable modules.

Each module owns ``<module>/config.ini.example`` (shipped default) and an
optional live ``config.ini`` created on first save. Resolution:
live -> example -> built-in defaults dict.
"""

from __future__ import annotations

import configparser
import shutil
import threading
from pathlib import Path
from typing import Optional

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


class ModuleIniConfig:
    """Thread-safe module-owned INI configuration."""

    def __init__(
        self,
        module_dir: Path,
        *,
        basename: str = "config.ini",
        defaults: Optional[dict[str, dict[str, str]]] = None,
    ):
        self._dir = Path(module_dir).resolve()
        self._live = self._dir / basename
        self._example = self._dir / f"{basename}.example"
        self._defaults = defaults or {}
        self._lock = threading.RLock()
        self._parser: configparser.ConfigParser | None = None
        self._loaded_from: Path | None = None
        self._loaded_mtime: float | None = None

    def config_path(self) -> Path | None:
        if self._live.exists():
            return self._live
        if self._example.exists():
            return self._example
        return None

    def live_path(self) -> Path:
        return self._live

    def _load(self, force: bool = False) -> configparser.ConfigParser:
        with self._lock:
            path = self.config_path()
            try:
                mtime = path.stat().st_mtime if path else None
            except OSError:
                mtime = None
            if (
                self._parser is not None
                and not force
                and path == self._loaded_from
                and mtime == self._loaded_mtime
            ):
                return self._parser
            parser = configparser.ConfigParser(inline_comment_prefixes=("#", ";"))
            if path is not None:
                try:
                    parser.read(path, encoding="utf-8")
                except (OSError, configparser.Error):
                    pass
            self._parser = parser
            self._loaded_from = path
            self._loaded_mtime = mtime
            return parser

    def reload(self) -> None:
        self._load(force=True)

    def _default(self, section: str, key: str) -> str | None:
        sec = self._defaults.get(section)
        return sec.get(key) if sec else None

    def get(self, section: str, key: str, default: str | None = None) -> str | None:
        parser = self._load()
        raw = parser.get(section, key, fallback=None)
        if raw is None:
            raw = self._default(section, key)
        return default if raw is None else raw

    def get_int(self, section: str, key: str, default: int = 0) -> int:
        raw = self.get(section, key, None)
        if raw is None:
            return default
        try:
            return int(str(raw).strip())
        except (TypeError, ValueError):
            return default

    def get_float(self, section: str, key: str, default: float = 0.0) -> float:
        raw = self.get(section, key, None)
        if raw is None:
            return default
        try:
            return float(str(raw).strip())
        except (TypeError, ValueError):
            return default

    def get_bool(self, section: str, key: str, default: bool = False) -> bool:
        raw = self.get(section, key, None)
        if raw is None:
            return default
        val = str(raw).strip().lower()
        if val in _TRUE:
            return True
        if val in _FALSE:
            return False
        return default

    def _ensure_live(self) -> Path:
        if not self._live.exists() and self._example.exists():
            shutil.copy2(self._example, self._live)
        return self._live

    def set_value(self, section: str, key: str, value: str) -> None:
        from common.config.ini_writer import set_ini_value

        with self._lock:
            set_ini_value(self._ensure_live(), section, key, "" if value is None else str(value))
            self.reload()

    def restore_defaults(self) -> None:
        with self._lock:
            if self._example.exists():
                shutil.copy2(self._example, self._live)
            elif self._live.exists():
                self._live.unlink()
            self.reload()
