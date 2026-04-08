#-------------------------------------------------------------------------------
#description: Config manager for the tool
#initial version: 08-APR-2026
#Author: Dhananjay Chaturvedi
#Copyright 2026 Dhananjay Chaturvedi
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#-------------------------------------------------------------------------------

"""
Configuration Loader for Database Management Tool

This module provides centralized access to configuration values from
config.ini (environment, paths, credentials) and properties.ini (UI settings,
parameters). It handles type conversion, default values, and path expansion.

Usage:
    from config_loader import config, properties, console_print

    # Access config values
    port = config.get_int('database.ports', 'oracle', default=1521)
    timeout = config.get_float('database.connection', 'connection_timeout', default=30.0)
    autocommit = config.get_bool('database.connection', 'default_autocommit', default=False)
    path = config.get_path('paths', 'config_dir', default='~/.dbmanager')

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
from typing import Union, Optional, List


class ConfigLoader:
    """Load and access configuration from INI files with type safety"""

    def __init__(self, config_file: str):
        """
        Initialize config loader

        Args:
            config_file: Path to the INI configuration file
        """
        self.config_file = config_file
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
        except Exception as e:
            # Silently use defaults on error
            self._loaded = False

    def reload(self):
        """Reload configuration from file"""
        self.parser.clear()
        self._load()

    def get(self, section: str, key: str, default: str = '') -> str:
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
        value = self.get(section, key, str(default))
        try:
            return int(value)
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
        if value in ('true', 'yes', '1', 'on'):
            return True
        elif value in ('false', 'no', '0', 'off'):
            return False
        return default

    def get_list(self, section: str, key: str, default: Optional[List[str]] = None,
                 delimiter: str = ',') -> List[str]:
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

        value = self.get(section, key, '')
        if not value:
            return default

        return [item.strip() for item in value.split(delimiter)]

    def get_path(self, section: str, key: str, default: str = '') -> Path:
        """
        Get path value from config with ~ expansion

        Args:
            section: Configuration section name
            key: Configuration key name
            default: Default value if not found

        Returns:
            Configuration value as Path object with ~ expanded
        """
        value = self.get(section, key, default)
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
            if value.startswith('0o'):
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


# Module-level singleton instances
_BASE_DIR = Path(__file__).parent
_config_instance = None
_properties_instance = None


def get_config() -> ConfigLoader:
    """Get the global config instance (lazy initialization)"""
    global _config_instance
    if _config_instance is None:
        config_path = _BASE_DIR / 'config.ini'
        _config_instance = ConfigLoader(str(config_path))
    return _config_instance


def get_properties() -> ConfigLoader:
    """Get the global properties instance (lazy initialization)"""
    global _properties_instance
    if _properties_instance is None:
        properties_path = _BASE_DIR / 'properties.ini'
        _properties_instance = ConfigLoader(str(properties_path))
    return _properties_instance


# Export convenient module-level access
config = get_config()
properties = get_properties()


# Convenience functions for common patterns
def get_db_port(db_type: str) -> int:
    """Get default port for database type"""
    return config.get_int('database.ports', db_type.lower(), default=0)


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
    width_key = f'{window_name}_window_width'
    height_key = f'{window_name}_window_height'
    width = properties.get_int('ui.window', width_key, default=None)
    height = properties.get_int('ui.window', height_key, default=None)

    # If not found, try _dialog_ suffix
    if width is None or height is None:
        width_key = f'{window_name}_dialog_width'
        height_key = f'{window_name}_dialog_height'
        width = properties.get_int('ui.window', width_key, default=800)
        height = properties.get_int('ui.window', height_key, default=600)

    return (width, height)


def get_color(color_category: str, color_name: str, default: str = '#000000') -> str:
    """
    Get UI color value

    Args:
        color_category: Category (e.g., 'primary', 'accent', 'status')
        color_name: Color name within category
        default: Default color if not found

    Returns:
        Hex color code
    """
    section = f'ui.colors.{color_category}'
    return properties.get(section, color_name, default=default)


def get_font_config(platform: str, font_type: str = 'ui') -> tuple:
    """
    Get font configuration for platform

    Args:
        platform: Platform name ('macos', 'windows', 'linux')
        font_type: Font type ('ui' or 'mono')

    Returns:
        Tuple of (font_family, font_size)
    """
    family_key = f'{platform}_{font_type}_font_family'
    size_key = f'{platform}_{font_type}_font_size'

    family = properties.get('ui.fonts', family_key, default='Arial')
    size = properties.get_int('ui.fonts', size_key, default=10)

    return (family, size)


def console_print(*args, **kwargs):
    """
    Print to stdout only if enabled in properties.ini
    stderr is always enabled regardless of this setting

    Usage:
        from config_loader import console_print
        console_print("This message respects the enable_stdout setting")

    To force stderr output (always shown):
        import sys
        print("Error message", file=sys.stderr)
    """
    if properties.get_bool('logging', 'enable_stdout', default=True):
        print(*args, **kwargs)


if __name__ == '__main__':
    # Test the configuration loader
    print("=" * 60)
    print("Configuration Loader Test")
    print("=" * 60)

    print("\n--- Config Values ---")
    print(f"Oracle port: {get_db_port('oracle')}")
    print(f"MySQL port: {get_db_port('mysql')}")
    print(f"Connection timeout: {config.get_float('database.connection', 'connection_timeout')}s")
    print(f"Default autocommit: {config.get_bool('database.connection', 'default_autocommit')}")
    print(f"Config directory: {config.get_path('paths', 'config_dir')}")
    print(f"Key file permissions: {oct(config.get_octal('security', 'key_file_permissions'))}")

    print("\n--- Properties Values ---")
    main_width, main_height = get_window_size('main')
    print(f"Main window size: {main_width}x{main_height}")
    print(f"Primary color: {get_color('primary', 'primary')}")
    print(f"Success color: {get_color('accent', 'success')}")

    font_family, font_size = get_font_config('macos', 'ui')
    print(f"macOS UI font: {font_family}, {font_size}pt")

    print(f"Standard padding (medium): {properties.get_int('ui.spacing', 'padding_md')}px")
    print(f"SQL preview limit: {properties.get_int('ui.limits', 'sql_preview_limit')} chars")

    print("\n" + "=" * 60)
