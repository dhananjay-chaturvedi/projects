#-------------------------------------------------------------------------------
#description: Theme manager for the tool
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

"""UI Theme and Font Utilities - Config-Driven"""
from config_loader import properties
import sys


# Load font configurations once at module import time
if sys.platform == "darwin":
    _UI_FONT = (
        properties.get('ui.fonts', 'macos_ui_font_family', 'Helvetica Neue'),
        properties.get_int('ui.fonts', 'macos_ui_font_size', 13)
    )
    _MONO_FONT = (
        properties.get('ui.fonts', 'macos_mono_font_family', 'Menlo'),
        properties.get_int('ui.fonts', 'macos_mono_font_size', 12)
    )
elif sys.platform == "win32":
    _UI_FONT = (
        properties.get('ui.fonts', 'windows_ui_font_family', 'Segoe UI'),
        properties.get_int('ui.fonts', 'windows_ui_font_size', 10)
    )
    _MONO_FONT = (
        properties.get('ui.fonts', 'windows_mono_font_family', 'Consolas'),
        properties.get_int('ui.fonts', 'windows_mono_font_size', 10)
    )
else:
    _UI_FONT = (
        properties.get('ui.fonts', 'linux_ui_font_family', 'DejaVu Sans'),
        properties.get_int('ui.fonts', 'linux_ui_font_size', 10)
    )
    _MONO_FONT = (
        properties.get('ui.fonts', 'linux_mono_font_family', 'DejaVu Sans Mono'),
        properties.get_int('ui.fonts', 'linux_mono_font_size', 10)
    )


def default_ui_font():
    """Get default UI font for current platform"""
    return _UI_FONT


def default_ui_mono():
    """Get default monospace font for current platform"""
    return _MONO_FONT


class ColorTheme:
    """Application color theme - loads from properties.ini"""
    # Primary colors
    PRIMARY = properties.get('ui.colors.primary', 'primary', '#2196F3')
    PRIMARY_DARK = properties.get('ui.colors.primary', 'primary_dark', '#1976D2')
    PRIMARY_LIGHT = properties.get('ui.colors.primary', 'primary_light', '#BBDEFB')

    # Accent colors
    ACCENT = properties.get('ui.colors.accent', 'accent', '#FF9800')
    SUCCESS = properties.get('ui.colors.accent', 'success', '#4CAF50')
    WARNING = properties.get('ui.colors.accent', 'warning', '#FFC107')
    ERROR = properties.get('ui.colors.accent', 'error', '#F44336')

    # Neutral colors
    BG_MAIN = properties.get('ui.colors.neutral', 'bg_main', '#F5F5F5')
    BG_SECONDARY = properties.get('ui.colors.neutral', 'bg_secondary', '#FFFFFF')
    BG_DARK = properties.get('ui.colors.neutral', 'bg_dark', '#424242')
    TEXT_PRIMARY = properties.get('ui.colors.neutral', 'text_primary', '#212121')
    TEXT_SECONDARY = properties.get('ui.colors.neutral', 'text_secondary', '#757575')
    BORDER = properties.get('ui.colors.neutral', 'border', '#E0E0E0')

    # Status colors
    CONNECTED = properties.get('ui.colors.status', 'connected', '#4CAF50')
    DISCONNECTED = properties.get('ui.colors.status', 'disconnected', '#9E9E9E')
    ERROR_BG = properties.get('ui.colors.status', 'error_bg', '#FFEBEE')
    SUCCESS_BG = properties.get('ui.colors.status', 'success_bg', '#E8F5E9')
    INFO_BG = properties.get('ui.colors.status', 'info_bg', '#E3F2FD')
