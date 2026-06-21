"""Screen registry."""

from common.ui.textual.screens.ai_query import AiQueryScreen
from common.ui.textual.screens.connections import ConnectionsScreen
from common.ui.textual.screens.dashboard import DashboardScreen
from common.ui.textual.screens.home import HomeScreen
from common.ui.textual.screens.migration import MigrationScreen
from common.ui.textual.screens.monitoring import MonitoringScreen
from common.ui.textual.screens.objects import ObjectsScreen
from common.ui.textual.screens.settings import SettingsScreen
from common.ui.textual.screens.sql_editor import SqlEditorScreen

__all__ = [
    "HomeScreen",
    "ConnectionsScreen",
    "DashboardScreen",
    "SqlEditorScreen",
    "ObjectsScreen",
    "MigrationScreen",
    "AiQueryScreen",
    "MonitoringScreen",
    "SettingsScreen",
]
