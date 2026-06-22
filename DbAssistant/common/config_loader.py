# ---------------------------------------------------------------------
# description: Config manager for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

"""
Configuration Loader for Database Management Tool

This module provides centralized access to configuration values from
config.ini (environment, paths, credentials) and properties.ini (UI settings,
parameters). It handles type conversion, default values, and path expansion.

Usage:
    from common.config_loader import config, properties, console_print

    # Access config values
    port = config.get_int('database.ports', 'oracle', default=1521)
    timeout = config.get_float('database.connection', 'connection_timeout', default=30.0)
    autocommit = config.get_bool('database.connection', 'default_autocommit', default=False)
    path = paths.dbassistant_home()
    oracle_client = config.get_path_or_none('paths', 'oracle_client_path')

    # Access property values
    width = properties.get_int('ui.window', 'main_window_width', default=1150)
    color = properties.get('ui.colors.primary', 'primary', default='#2196F3')

    # Configurable stdout (controlled by properties.ini: [logging] enable_stdout)
    console_print("This message respects enable_stdout setting")
    print("Error message", file=sys.stderr)  # stderr always shown
"""

import configparser
import os
from pathlib import Path
from typing import Optional, List


class ConfigLoader:
    """Load and access configuration from INI files with type safety"""

    def __init__(self, config_file: str, example_file: Optional[str] = None):
        """
        Initialize config loader

        Args:
            config_file: Path to the INI configuration file
            example_file: Optional path to the read-only ``*.ini.example`` that
                holds shipped defaults. Used by :meth:`restore_defaults`.
        """
        self.config_file = config_file
        self.example_file = example_file
        self.parser = configparser.ConfigParser()
        self._loaded = False
        self._load()

    def _load(self):
        """Load configuration from file"""
        if not os.path.exists(self.config_file):
            # Silently use defaults if config file doesn't exist
            self._loaded = False
            return

        try:
            self.parser.read(self.config_file)
            self._loaded = True
        except Exception:
            # Silently use defaults on error
            self._loaded = False

    def reload(self):
        """Reload configuration from file"""
        self.parser.clear()
        self._load()

    def get(self, section: str, key: str, default: str = "") -> str:
        """
        Get string value from config

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found

        Returns:
            Configuration value as string
        """
        if not self._loaded:
            return default

        try:
            return self.parser.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default

    def get_int(self, section: str, key: str, default: int = 0) -> int:
        """
        Get integer value from config

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found

        Returns:
            Configuration value as integer
        """
        _sentinel = "\x00"
        raw = self.get(section, key, _sentinel)
        if raw == _sentinel:
            return default
        try:
            return int(raw)
        except (ValueError, TypeError):
            return default

    def get_float(self, section: str, key: str, default: float = 0.0) -> float:
        """
        Get float value from config

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found

        Returns:
            Configuration value as float
        """
        value = self.get(section, key, str(default))
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def get_bool(self, section: str, key: str, default: bool = False) -> bool:
        """
        Get boolean value from config

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found

        Returns:
            Configuration value as boolean
        """
        value = self.get(section, key, str(default)).lower()
        if value in ("true", "yes", "1", "on"):
            return True
        elif value in ("false", "no", "0", "off"):
            return False
        return default

    def get_list(
        self,
        section: str,
        key: str,
        default: Optional[List[str]] = None,
        delimiter: str = ",",
    ) -> List[str]:
        """
        Get list value from config (comma-separated by default)

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found
            delimiter: Character to split on

        Returns:
            Configuration value as list of strings
        """
        if default is None:
            default = []

        value = self.get(section, key, "")
        if not value:
            return default

        return [item.strip() for item in value.split(delimiter)]

    def get_path(self, section: str, key: str, default: str = "") -> Path:
        """
        Get path value from config with ~ expansion

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found

        Returns:
            Configuration value as Path object with ~ expanded
        """
        value = self.get(section, key, default).strip()
        if not value:
            value = str(default).strip()
        return Path(os.path.expanduser(value))

    def get_path_or_none(self, section: str, key: str, default: str = "") -> Optional[Path]:
        """
        Get optional path from config; return None when value and default are blank.

        Use for settings like ``oracle_client_path`` where an empty config value
        means "not configured" (avoids ``Path('')`` resolving to the current directory).
        """
        value = self.get(section, key, default).strip()
        if not value:
            return None
        return Path(os.path.expanduser(value))

    def get_octal(self, section: str, key: str, default: int = 0o600) -> int:
        """
        Get octal permission value from config

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found (octal integer)

        Returns:
            Configuration value as octal integer
        """
        value = self.get(section, key, oct(default))
        try:
            # Handle both "0o600" and "600" formats
            if value.startswith("0o"):
                return int(value, 8)
            else:
                return int(value, 8)
        except (ValueError, TypeError):
            return default

    def has_section(self, section: str) -> bool:
        """Check if section exists in config"""
        return self.parser.has_section(section)

    def has_option(self, section: str, key: str) -> bool:
        """Check if option exists in config"""
        return self.parser.has_option(section, key)

    def get_all(self, section: str) -> dict:
        """
        Get all key-value pairs from a section

        Args:
            section: Configuration section name

        Returns:
            Dictionary of all keys and values in the section
        """
        if not self._loaded or not self.has_section(section):
            return {}

        return dict(self.parser.items(section))

    # ------------------------------------------------------------------
    # Write surface (used by the Settings UI / CLI / settings_service)
    # ------------------------------------------------------------------
    def set(self, section: str, key: str, value: str, *, save: bool = True) -> bool:
        """Set a single value and (by default) persist it to disk.

        The on-disk write is a *surgical*, comment-preserving edit of the live
        INI file — only the targeted ``key = value`` line changes. The shipped
        ``*.ini.example`` defaults are never touched. The in-memory parser is
        updated too so subsequent ``get_*`` calls in this process see the new
        value immediately.
        """
        value = "" if value is None else str(value)
        # Update in-memory parser first so reads are consistent even if the
        # file write is deferred (save=False).
        if not self.parser.has_section(section):
            try:
                self.parser.add_section(section)
            except (configparser.DuplicateSectionError, ValueError):
                pass
        try:
            self.parser.set(section, key, value)
            self._loaded = True
        except Exception:
            return False
        if not save:
            return True
        return self.save_value(section, key, value)

    def save_value(self, section: str, key: str, value: str) -> bool:
        """Persist a single ``(section, key, value)`` to the live INI file."""
        from common.config.ini_writer import set_ini_value

        try:
            return set_ini_value(self.config_file, section, key, str(value))
        except Exception:
            return False

    def restore_defaults(self) -> bool:
        """Overwrite the live INI file with the shipped ``*.ini.example``.

        Returns ``False`` if no example file is configured/available. After a
        successful copy the in-memory parser is reloaded.
        """
        if not self.example_file or not os.path.exists(self.example_file):
            return False
        try:
            with open(self.example_file, "r", encoding="utf-8") as src:
                content = src.read()
            from common.config.ini_writer import _atomic_write_text

            _atomic_write_text(Path(self.config_file), content)
            self.reload()
            return True
        except Exception:
            return False


# Module-level singleton instances
_BASE_DIR = Path(__file__).resolve().parent.parent
_config_instance = None
_properties_instance = None


def get_config() -> ConfigLoader:
    """Get the global config instance (lazy initialization)"""
    global _config_instance
    if _config_instance is None:
        config_path = _BASE_DIR / "config.ini"
        example_path = _BASE_DIR / "common" / "config" / "config.ini.example"
        _config_instance = ConfigLoader(str(config_path), str(example_path))
    return _config_instance


def get_properties() -> ConfigLoader:
    """Get the global properties instance (lazy initialization)"""
    global _properties_instance
    if _properties_instance is None:
        properties_path = _BASE_DIR / "properties.ini"
        example_path = _BASE_DIR / "common" / "config" / "properties.ini.example"
        _properties_instance = ConfigLoader(str(properties_path), str(example_path))
    return _properties_instance


# Export convenient module-level access
config = get_config()
properties = get_properties()


# Convenience functions for common patterns
def get_db_port(db_type: str) -> int:
    """Get default port for database type"""
    return config.get_int("database.ports", db_type.lower(), default=0)


def get_project_version(default: str = "1.0.0") -> str:
    """Return the application version from config or the VERSION file."""
    configured = config.get("project", "version", default="").strip()
    if configured:
        return configured
    version_file = _BASE_DIR / "VERSION"
    try:
        value = version_file.read_text(encoding="utf-8").strip()
        return value or default
    except OSError:
        return default


def get_api_host(default: str = "127.0.0.1") -> str:
    """Default bind host for the REST API CLI."""
    return config.get("api", "host", default=default).strip() or default


def get_api_port(default: int = 8000) -> int:
    """Default bind port for the REST API CLI."""
    return config.get_int("api", "port", default=default)


def get_webui_host(default: str = "127.0.0.1") -> str:
    """Default bind host for `dbtool webui`."""
    return config.get("webui", "host", default=default).strip() or default


def get_webui_port(default: int = 8090) -> int:
    """Default bind port for `dbtool webui`."""
    return config.get_int("webui", "port", default=default)


def get_tui_web_host(default: str = "127.0.0.1") -> str:
    """Default bind host for `dbtool tui --web`."""
    return config.get("tui", "host", default=default).strip() or default


def get_tui_web_port(default: int = 8080) -> int:
    """Default bind port for `dbtool tui --web`."""
    return config.get_int("tui", "port", default=default)


def get_api_cors_origins(default: str = "") -> str:
    """Comma-separated REST API CORS origins."""
    return config.get("api", "cors_origins", default=default).strip() or default


def get_api_max_body_bytes(default: int = 10 * 1024 * 1024) -> int:
    """Maximum accepted REST API request body size."""
    return config.get_int("api", "max_body_bytes", default=default)


def get_compare_sample_size() -> int:
    """Rows per table for sample schema/data comparison.

    The Data Migration module now owns this setting in its module config, but
    older installs and tests may still define it in the shared properties.ini.
    Treat an explicit legacy properties value as an override, then fall back to
    the module config default.
    """
    if properties.has_option("schema.conversion", "compare_sample_size"):
        return properties.get_int("schema.conversion", "compare_sample_size", default=10)
    try:
        from schema_converter import module_config as mc
        return mc.get_int("schema.conversion", "compare_sample_size", default=10)
    except Exception:
        return properties.get_int("schema.conversion", "compare_sample_size", default=10)


def get_window_size(window_name: str) -> tuple:
    """
    Get window size as (width, height) tuple

    Args:
        window_name: Name of the window (e.g., 'main', 'history', 'ai_query')
                    Tries both _window_ and _dialog_ suffixes

    Returns:
        Tuple of (width, height)
    """
    # Try _window_ suffix first
    width_key = f"{window_name}_window_width"
    height_key = f"{window_name}_window_height"
    width = properties.get_int("ui.window", width_key, default=-1)
    height = properties.get_int("ui.window", height_key, default=-1)

    # If not found, try _dialog_ suffix
    if width == -1 or height == -1:
        width_key = f"{window_name}_dialog_width"
        height_key = f"{window_name}_dialog_height"
        width = properties.get_int("ui.window", width_key, default=800)
        height = properties.get_int("ui.window", height_key, default=600)

    return (width, height)


def get_color(color_category: str, color_name: str, default: str = "#000000") -> str:
    """
    Get UI color value

    Args:
        color_category: Category (e.g., 'primary', 'accent', 'status')
        color_name: Color name within category
        default: Default color if not found

    Returns:
        Hex color code
    """
    section = f"ui.colors.{color_category}"
    return properties.get(section, color_name, default=default)


def get_font_config(platform: str, font_type: str = "ui") -> tuple:
    """
    Get font configuration for platform

    Args:
        platform: Platform name ('macos', 'windows', 'linux')
        font_type: Font type ('ui' or 'mono')

    Returns:
        Tuple of (font_family, font_size)
    """
    family_key = f"{platform}_{font_type}_font_family"
    size_key = f"{platform}_{font_type}_font_size"

    family = properties.get("ui.fonts", family_key, default="Arial")
    size = properties.get_int("ui.fonts", size_key, default=10)

    return (family, size)


def console_print(*args, **kwargs):
    """
    Print to stdout only if enabled in properties.ini.
    stderr is always enabled regardless of this setting.

    Usage:
        from common.config_loader import console_print
        console_print("This message respects the enable_stdout setting")

    To force stderr output (always shown):
        import sys
        print("Error message", file=sys.stderr)
    """
    if properties.get_bool("logging", "enable_stdout", default=True):
        print(*args, **kwargs)


def console_debug(*args, **kwargs):
    """
    Print a debug-level operational message (driver registration, connection
    open/close traces, etc.) to stdout *only* if ``[logging] enable_info`` is
    explicitly true.  Default is **False** so production CLI/API output stays
    clean for scripted consumers.

    Use this instead of :func:`console_print` for anything the user did not
    explicitly ask for.  Errors should still go to stderr (or use the file
    logger via :mod:`logging`).
    """
    if properties.get_bool("logging", "enable_info", default=False):
        print(*args, **kwargs)


if __name__ == "__main__":
    # Test the configuration loader
    print("=" * 60)
    print("Configuration Loader Test")
    print("=" * 60)

    print("\n--- Config Values ---")
    print(f"Oracle port: {get_db_port('oracle')}")
    print(f"MySQL port: {get_db_port('mysql')}")
    print(
        f"Connection timeout: {config.get_float('database.connection', 'connection_timeout')}s"
    )
    print(
        f"Default autocommit: {config.get_bool('database.connection', 'default_autocommit')}"
    )
    from common import paths as _paths
    print(f"DBAssistant home: {_paths.dbassistant_home()}")
    print(
        f"Key file permissions: {oct(config.get_octal('security', 'key_file_permissions'))}"
    )

    print("\n--- Properties Values ---")
    main_width, main_height = get_window_size("main")
    print(f"Main window size: {main_width}x{main_height}")
    print(f"Primary color: {get_color('primary', 'primary')}")
    print(f"Success color: {get_color('accent', 'success')}")

    font_family, font_size = get_font_config("macos", "ui")
    print(f"macOS UI font: {font_family}, {font_size}pt")

    print(
        f"Standard padding (medium): {properties.get_int('ui.spacing', 'padding_md')}px"
    )
    print(
        f"SQL preview limit: {properties.get_int('ui.limits', 'sql_preview_limit')} chars"
    )

    print("\n" + "=" * 60)
