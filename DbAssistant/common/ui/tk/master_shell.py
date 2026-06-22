#!/usr/bin/python
# -*- coding: UTF-8 -*-

# ---------------------------------------------------------------------
# description: UI manager for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

import atexit
import csv
import os
import re
import signal
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime
from tkinter import ttk, messagebox, filedialog
from common.branding import APP_NAME, APP_SHORT_NAME
from common.connection_manager import ConnectionManager
from common.database_registry import DatabaseRegistry

# Module framework — used to decide which optional tabs to show.
try:
    from common.core import modules as app_modules
except Exception:
    app_modules = None

# Optional module imports — guarded so the master UI starts even when a module
# is not shipped. Tabs for missing modules are simply not added.
try:
    from common.ui.tk.migrator.schema_converter_ui import SchemaConverterUI
    _HAS_SCHEMA = True
except Exception:
    _HAS_SCHEMA = False
    SchemaConverterUI = None

try:
    from ai_query.agent import AIQueryAgent
    from common.ui.tk.ai.ai_query_workspace import AIQueryWorkspace
    _HAS_AI = True
except Exception:
    _HAS_AI = False
    AIQueryAgent = AIQueryWorkspace = None

try:
    from monitoring.monitor_connection_manager import MonitorConnectionManager
    from common.ui.tk.monitor.metrics_visualizer import MetricsVisualizer
    from common.ui.tk.monitor.server_monitor import ServerMonitorUI
    _HAS_MONITOR = True
except Exception:
    _HAS_MONITOR = False
    MonitorConnectionManager = MetricsVisualizer = ServerMonitorUI = None
from common.ui.tk import (
    ColorTheme,
    default_ui_font,
    default_ui_mono,
    bind_canvas_mousewheel,
    disable_combobox_mousewheel,
    make_collapsible_section,
    make_scrollable,
)
from common.config_loader import config, properties, get_window_size, console_print
from common.db_manager import DatabaseManager  # headless-safe; shared with CLI/API
from common.ui.tk.dashboard_ui import DashboardUI
from common.ui.tk.database_objects_panel import (
    DatabaseObjectsPanel,
    ObjectsConnectionContext,
    ObjectsPanelActions,
    ObjectsPanelFonts,
)
from common.ui.tk.sql_editor_workspace import SQLEditorTab


# Engines where the "Database" field on the connection form is optional.
# These connectors happily accept an empty database — MySQL/MariaDB simply
# connect to the server without selecting a default schema, and PostgreSQL's
# libpq falls back to a database named after the connecting user.
DB_NAME_OPTIONAL_TYPES = frozenset({"MySQL", "MariaDB", "PostgreSQL"})


def _db_field_label(db_type: str) -> str:
    """Human-friendly name of the service/database field for *db_type*."""
    return "Service name" if db_type == "Oracle" else "Database name"


def _tab_label(tab_id: str, default: str) -> str:
    """Tab title from the shared UI spec (common.ui.shared), with a fallback.

    Reading labels from one place keeps the Tk, Textual and Web tab titles in
    sync — change a label in the shared spec and every UI follows.
    """
    try:
        from common.ui import shared

        spec = shared.tab_by_id(tab_id)
        return spec.label if spec else default
    except Exception:
        return default


class DatabaseConfig:
    """Configuration for different database types"""

    @staticmethod
    def get_db_types():
        """Return available database types from registry"""
        return DatabaseRegistry.get_all_types()

    @staticmethod
    def get_default_port(db_type):
        """Get default port for database type"""
        port = DatabaseRegistry.get_default_port(db_type)
        return str(port) if port else ""

    @staticmethod
    def get_connection_fields(db_type):
        """Get required connection fields for each DB type"""
        return DatabaseRegistry.get_connection_params(db_type)

    @staticmethod
    def get_available_operations(db_type):
        """Get available operations for each database type from registry"""
        return DatabaseRegistry.get_available_operations(db_type)

    @staticmethod
    def get_capabilities(db_type):
        return DatabaseRegistry.get_capabilities(db_type)




class UnifiedDBManagerUI:
    def __init__(self, root, feature_module: str | None = None):
        """
        * ``feature_module=None`` — full combined UI (all installed module tabs).
        * ``feature_module='migrator'|'ai'|'monitor'`` — standalone module UI:
          Connections + Database Objects + SQL Editor + that module's tab only.
        """
        self.root = root
        self.feature_module = feature_module
        self._standalone = feature_module is not None

        # Which optional module tabs to show.
        if self._standalone:
            _show_schema = feature_module == "migrator"
            _show_ai = feature_module == "ai"
            _show_monitor = feature_module == "monitor"
        else:
            _show_schema = _HAS_SCHEMA
            _show_ai = _HAS_AI
            _show_monitor = _HAS_MONITOR

        # Window configuration from config files
        if self._standalone:
            _titles = {
                "migrator": "Data Migration",
                "ai": "AI Query Assistant",
                "monitor": "Monitoring",
            }
            app_name = f"{APP_SHORT_NAME} — {_titles.get(feature_module, feature_module)}"
        else:
            # The product name is fixed/universal — not a configurable setting.
            app_name = APP_NAME
        width, height = get_window_size("main")
        min_width = properties.get_int(
            "ui.window", "main_window_min_width", default=860
        )
        min_height = properties.get_int(
            "ui.window", "main_window_min_height", default=520
        )

        self.root.title(app_name)
        self.root.geometry(f"{width}x{height}")
        # Open at the configured size, but allow users to shrink far enough for
        # tab/dialog scrollbars to engage instead of forcing a wide window.
        self.root.minsize(min(int(min_width), 480), min(int(min_height), 320))
        self.root.configure(bg=ColorTheme.BG_MAIN)

        self.ui_font = default_ui_font()
        self.ui_font_mono = default_ui_mono()
        self._setup_readable_ttk()

        # Closed combobox entries should not change value on scroll. The popup
        # listbox and every other native listbox keep their normal wheel.
        disable_combobox_mousewheel(self.root)

        # Support multiple connections
        self.active_connections = {}  # {connection_name: db_manager}
        self.current_connection_name = None
        self.connection_counter = 0

        self.current_db_type = None
        self.operation_buttons = []
        self.sql_editor = None
        self.connection_manager = ConnectionManager()
        self.monitor_connection_manager = (
            MonitorConnectionManager() if _show_monitor and _HAS_MONITOR else None
        )
        self.ai_agent = AIQueryAgent() if _show_ai and _HAS_AI else None

        # Module instances (lazy init)
        self.server_monitor_ui = None
        self.ai_query_ui = None
        self.schema_converter_ui = None

        # Thread safety for database operations
        self.db_query_lock = threading.Lock()

        # Status Bar - create and pack FIRST to claim bottom space
        self.create_status_bar()

        # Main workspace (menu bar is attached to root above this)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))

        self.welcome_tab = None
        self.clear_cache_tab = None

        if not self._standalone:
            # Welcome Tab (First tab - documentation and help)
            self.welcome_tab = ttk.Frame(self.notebook)
            self.notebook.add(self.welcome_tab, text=_tab_label("welcome", "Welcome"))

        # Connections Tab — always present (core)
        self.connections_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.connections_tab, text=_tab_label("connections", "Connections"))

        self.dashboard_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.dashboard_tab, text=_tab_label("dashboard", "Dashboard"))

        # Database Objects Tab — always present (core)
        self.objects_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.objects_tab, text=_tab_label("objects", "Database Objects"))

        # SQL Editor Tab — always present (core)
        self.sql_editor_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.sql_editor_tab, text=_tab_label("sql_editor", "SQL Editor"))

        # Module tabs — one (standalone) or all installed (full tool).
        self.conversion_tab = None
        self.ai_query_tab = None
        self.monitor_tab = None

        if _show_schema:
            self.conversion_tab = ttk.Frame(self.notebook)
            self.notebook.add(self.conversion_tab, text=_tab_label("migrator", "Data Migration"))

        if _show_ai:
            self.ai_query_tab = ttk.Frame(self.notebook)
            self.notebook.add(self.ai_query_tab, text=_tab_label("ai", "AI Query Assistant"))

        if _show_monitor:
            self.monitor_tab = ttk.Frame(self.notebook)
            self.notebook.add(self.monitor_tab, text=_tab_label("monitor", "Monitor"))

        # Settings tab — always present (configures config.ini / properties.ini).
        # Placed after Monitor and before Clear Cache.
        self.settings_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.settings_tab, text=_tab_label("settings", "Settings"))

        if not self._standalone:
            # Clear Cache "Tab" (acts as button)
            self.clear_cache_tab = ttk.Frame(self.notebook)
            self.notebook.add(self.clear_cache_tab, text=_tab_label("clear_cache", "Clear Cache"))

        # Menubar references notebook tabs — build after tabs exist.
        self._create_menubar()

        # Open the window wide enough that every tab label is fully visible.
        # ttk.Notebook clips ("suppresses") tabs that overflow the width.
        self._fit_window_to_tabs(width, height, min_width, min_height)

        # Track which tabs have been initialized
        self.tabs_initialized = {
            "dashboard": False,
            "welcome": False,
            "connections": False,
            "objects": False,
            "sql_editor": False,
            "conversion": False,
            "ai_query": False,
            "monitor": False,
            "settings": False,
        }

        # Track previous tab for Clear Cache button behavior
        self.previous_tab_index = 0
        self._last_tab_widget = None

        # Bind tab change event for lazy loading
        self.notebook.bind("<<NotebookTabChanged>>", self.on_tab_changed)

        self.dashboard_ui = None

        # Defer dashboard tab creation to after window is shown
        self.root.after(100, self._create_dashboard_tab_deferred)
        if not self._standalone:
            self.root.after(120, self._create_welcome_tab_deferred)

        # Graceful shutdown plumbing — close every live connection on exit,
        # whether the user clicks the window's X button, hits Ctrl+C, or the
        # process gets a SIGTERM.
        self._shutdown_done = False
        self._install_shutdown_hooks()

    def _install_shutdown_hooks(self):
        """Route every exit path (X button, SIGINT, SIGTERM, atexit) through
        ``_graceful_shutdown`` so connections close once and exactly once."""
        try:
            self.root.protocol(
                "WM_DELETE_WINDOW",
                lambda: self._graceful_shutdown("window-close"),
            )
        except Exception as exc:
            console_print(f"[Shutdown] Could not bind window-close hook: {exc}")

        if sys.platform == "darwin":
            self._install_macos_quit_hook()

        # Signal handlers can only be installed from the main thread. Tk
        # also blocks Python-level signal delivery while it sits in its C
        # event loop, so we install a 200 ms heartbeat that wakes the
        # interpreter just often enough to deliver pending signals.
        if threading.current_thread() is threading.main_thread():
            for sig_name in ("SIGINT", "SIGTERM"):
                sig = getattr(signal, sig_name, None)
                if sig is None:
                    continue
                try:
                    signal.signal(sig, self._on_shutdown_signal)
                except (ValueError, OSError) as exc:
                    console_print(
                        f"[Shutdown] Could not install {sig_name} handler: {exc}"
                    )

        self._tick_for_signals()
        atexit.register(self._atexit_shutdown)

    def _install_macos_quit_hook(self):
        """Handle macOS app-menu / Dock Quit events through graceful shutdown."""
        try:
            if self.root.tk.call("tk", "windowingsystem") != "aqua":
                return

            def _quit_from_macos():
                self._graceful_shutdown("macos-quit")
                return ""

            command_name = f"dbtool_macos_quit_{id(self)}"
            self.root.createcommand(command_name, _quit_from_macos)
            self.root.tk.eval(f"proc ::tk::mac::Quit {{}} {{{command_name}}}")
        except Exception as exc:
            console_print(f"[Shutdown] Could not bind macOS Quit hook: {exc}")

    def _tick_for_signals(self):
        """Idle heartbeat — gives the Python interpreter a chance to deliver
        SIGINT/SIGTERM while Tk's mainloop is otherwise blocked in C code."""
        try:
            self.root.after(200, self._tick_for_signals)
        except tk.TclError:
            pass  # Root already destroyed.

    def _on_shutdown_signal(self, signum, _frame):
        name = {
            getattr(signal, "SIGINT", None): "SIGINT",
            getattr(signal, "SIGTERM", None): "SIGTERM",
        }.get(signum, str(signum))
        console_print(f"[Shutdown] Caught {name} — closing connections...")
        try:
            self.root.after(0, lambda: self._graceful_shutdown(name))
        except Exception:
            self._graceful_shutdown(name)

    def _atexit_shutdown(self):
        """Last-resort sweep when the interpreter is unwinding without going
        through the normal exit hooks (uncaught exception, ``sys.exit``)."""
        if self._shutdown_done:
            return
        try:
            self._close_all_connections_now(reason="atexit")
        except Exception as exc:
            try:
                console_print(f"[Shutdown] atexit cleanup error: {exc}")
            except Exception:
                pass

    def _graceful_shutdown(self, reason: str):
        """Idempotent shutdown — closes connections then destroys the Tk root.

        Called from the main thread (window-close handler or after a signal
        was scheduled onto the Tk event loop). Safe to call multiple times.
        """
        if self._shutdown_done:
            return
        self._shutdown_done = True
        console_print(f"[Shutdown] Closing all resources ({reason})...")
        try:
            self._close_all_connections_now(reason=reason)
        finally:
            try:
                self.root.destroy()
            except Exception:
                pass

    def _close_all_connections_now(self, *, reason: str):
        """Close every connection still owned by the UI.

        Order:
          1. Connections tab DB managers (also feeds AI/Schema tabs).
          2. Monitor tab DB managers (independent live connections).
          3. Monitor tab SSH master multiplex sockets.
          4. Drop cloud monitor references so boto3/google-auth let go.
        """
        # 0) SQL Editor tabs each own a private session — close them first.
        editor = getattr(self, "sql_editor", None)
        if editor is not None and hasattr(editor, "dispose_all"):
            try:
                editor.dispose_all()
            except Exception as exc:
                console_print(f"[Shutdown] Error closing SQL editor sessions: {exc}")

        # 1) Connections tab — same dict the "Disconnect All" button drains.
        active = getattr(self, "active_connections", None) or {}
        for name, mgr in list(active.items()):
            try:
                mgr.disconnect()
                console_print(f"[Shutdown] Closed DB connection '{name}'")
            except Exception as exc:
                console_print(f"[Shutdown] Error closing '{name}': {exc}")
        try:
            active.clear()
        except Exception:
            pass

        # Monitor tab is only initialised when the Monitor tab has been
        # opened at least once — skip cleanly if it never came up.
        smu = getattr(self, "server_monitor_ui", None)
        if smu is None:
            return

        # 2) Monitor tab — DB monitoring (same pattern as the per-row remove).
        mon_dbs = getattr(smu, "monitored_databases", None) or {}
        for db_name, mgr in list(mon_dbs.items()):
            try:
                mgr.disconnect()
                console_print(f"[Shutdown] Closed monitored DB '{db_name}'")
            except Exception as exc:
                console_print(
                    f"[Shutdown] Error closing monitored DB '{db_name}': {exc}"
                )
        try:
            mon_dbs.clear()
        except Exception:
            pass

        # 3) Monitor tab — SSH master connections (the multiplex sockets the
        # individual ``Remove`` button shuts down with ``ssh -O exit``).
        mon_conns = getattr(smu, "monitor_connections", None) or {}
        for conn_name, conn in list(mon_conns.items()):
            if not isinstance(conn, dict):
                continue
            ctrl = conn.get("control_path")
            if not ctrl:
                continue
            user = conn.get("username", "")
            host = conn.get("host", "")
            if not (user and host):
                continue
            try:
                subprocess.run(
                    [
                        "ssh",
                        "-O",
                        "exit",
                        "-o",
                        f"ControlPath={ctrl}",
                        f"{user}@{host}",
                    ],
                    timeout=3,
                )
                console_print(f"[Shutdown] Closed SSH master '{conn_name}'")
            except (subprocess.SubprocessError, OSError):
                pass  # Best-effort: never block shutdown on SSH cleanup.
        try:
            mon_conns.clear()
        except Exception:
            pass

        # 4) Cloud monitors hold boto3 / google-auth / Azure SDK clients —
        # they use urllib3 pools that release on GC. Drop refs so GC runs.
        cloud_mons = getattr(smu, "active_cloud_monitors", None)
        if isinstance(cloud_mons, dict):
            try:
                cloud_mons.clear()
            except Exception:
                pass

    def _create_dashboard_tab_deferred(self):
        """Create dashboard tab after main window is shown."""
        if not self.tabs_initialized["dashboard"]:
            console_print("[Startup] Creating Dashboard tab (deferred)...")
            self.create_dashboard_tab_ui()
            self.tabs_initialized["dashboard"] = True
            console_print("[Startup] Dashboard tab ready")
        self.notebook.select(self.dashboard_tab)
        if self.dashboard_ui is not None:
            self.dashboard_ui.on_tab_selected()

    def _create_welcome_tab_deferred(self):
        """Create welcome tab after main window is shown"""
        if not self.tabs_initialized["welcome"]:
            console_print("[Startup] Creating Welcome tab (deferred)...")
            self.create_welcome_tab_ui()
            self.tabs_initialized["welcome"] = True
            console_print("[Startup] Welcome tab ready")

    def on_tab_changed(self, event=None):
        """Handle tab change - lazy load tab UIs.

        Dispatch is by widget identity (not hardcoded index) so optional module
        tabs can be present or absent without breaking the loader.
        """
        try:
            selected_id = self.notebook.select()
            widget = self.root.nametowidget(selected_id)
            current_index = self.notebook.index(selected_id)
        except (tk.TclError, AttributeError, KeyError):
            return

        # Clear Cache (acts as a button, not a real tab)
        if self.clear_cache_tab is not None and widget is self.clear_cache_tab:
            self.clear_all_caches()
            self.notebook.select(self.previous_tab_index)
            return

        # Update previous tab index for normal tabs
        self.previous_tab_index = current_index

        if widget is self.dashboard_tab:
            if self.dashboard_ui is not None:
                self.dashboard_ui.on_tab_selected()
        elif self._last_tab_widget is self.dashboard_tab and self.dashboard_ui is not None:
            self.dashboard_ui.on_tab_hidden()
        self._last_tab_widget = widget

        def _tab_load_failed(tab_name: str, exc: Exception) -> None:
            message = f"Could not load {tab_name} tab: {exc}"
            console_print(f"[TabSwitch] {message}")
            try:
                self.update_status(message)
            except Exception:
                pass
            try:
                messagebox.showerror(f"{tab_name} tab failed to load", message)
            except Exception:
                pass

        # Dashboard
        if widget is self.dashboard_tab and not self.tabs_initialized["dashboard"]:
            try:
                self.create_dashboard_tab_ui()
                self.tabs_initialized["dashboard"] = True
                if self.dashboard_ui:
                    self.dashboard_ui.on_tab_selected()
            except Exception as exc:
                _tab_load_failed("Dashboard", exc)

        # Connections
        if widget is self.connections_tab and not self.tabs_initialized["connections"]:
            console_print("[TabSwitch] Creating Connections tab...")
            self.update_status("Loading Connections tab...")
            try:
                self.create_connections_tab_ui()
                self.tabs_initialized["connections"] = True
                self.update_status("Connections tab ready")
            except Exception as exc:
                _tab_load_failed("Connections", exc)

        # Database Objects
        elif widget is self.objects_tab and not self.tabs_initialized["objects"]:
            console_print("[TabSwitch] Creating Database Objects tab...")
            self.update_status("Loading Database Objects tab...")
            try:
                self.create_objects_tab_ui()
                self.tabs_initialized["objects"] = True
            except Exception as exc:
                _tab_load_failed("Database Objects", exc)

        # SQL Editor
        elif widget is self.sql_editor_tab and not self.tabs_initialized["sql_editor"]:
            console_print("[TabSwitch] Creating SQL Editor tab...")
            self.update_status("Loading SQL Editor tab...")
            if self.sql_editor is None:
                for child in self.sql_editor_tab.winfo_children():
                    child.destroy()
                try:
                    self.sql_editor = SQLEditorTab(
                        self.sql_editor_tab,
                        lambda: self.active_connections,
                        self.update_status,
                        font_ui=self.ui_font,
                        font_mono=self.ui_font_mono,
                    )
                except Exception as exc:
                    self.sql_editor = None
                    _tab_load_failed("SQL Editor", exc)
                    return
            try:
                self.tabs_initialized["sql_editor"] = True
            except Exception as exc:
                _tab_load_failed("SQL Editor", exc)

        # Schema Conversion (module)
        elif (self.conversion_tab is not None and widget is self.conversion_tab
              and not self.tabs_initialized["conversion"]):
            console_print("[TabSwitch] Creating Data Migration tab...")
            self.update_status("Loading Data Migration tab...")
            try:
                self.create_conversion_tab_ui()
                self.tabs_initialized["conversion"] = True
            except Exception as exc:
                _tab_load_failed("Data Migration", exc)

        # AI Query Assistant (module)
        elif (self.ai_query_tab is not None and widget is self.ai_query_tab
              and not self.tabs_initialized["ai_query"]):
            console_print("[TabSwitch] Creating AI Query Assistant tab...")
            self.update_status("Loading AI Query Assistant tab...")
            try:
                self.ai_query_ui = AIQueryWorkspace(
                    parent_frame=self.ai_query_tab,
                    root=self.root,
                    ai_agent=self.ai_agent,
                    active_connections=self.active_connections,
                    update_status_callback=self.update_status,
                    send_to_editor_callback=self._send_sql_to_editor,
                    theme=ColorTheme,
                    fonts={"ui": default_ui_font(), "mono": default_ui_mono()},
                )
                self.ai_query_ui.create_ui()
                self.tabs_initialized["ai_query"] = True
            except Exception as exc:
                self.ai_query_ui = None
                _tab_load_failed("AI Query Assistant", exc)

        # Monitor (module)
        elif (self.monitor_tab is not None and widget is self.monitor_tab
              and not self.tabs_initialized["monitor"]):
            console_print("[TabSwitch] Creating Monitor tab...")
            self.update_status("Loading Monitor tab...")
            try:
                self.server_monitor_ui = ServerMonitorUI(
                    parent_frame=self.monitor_tab,
                    root=self.root,
                    connection_manager=self.connection_manager,
                    active_connections=self.active_connections,
                    update_status_callback=self.update_status,
                    theme=ColorTheme,
                )
                self.server_monitor_ui.create_ui()
                self.tabs_initialized["monitor"] = True
            except Exception as exc:
                self.server_monitor_ui = None
                _tab_load_failed("Monitor", exc)

        # Settings (always present)
        elif (getattr(self, "settings_tab", None) is not None
              and widget is self.settings_tab
              and not self.tabs_initialized.get("settings", False)):
            console_print("[TabSwitch] Creating Settings tab...")
            self.update_status("Loading Settings tab...")
            try:
                self.create_settings_tab_ui()
                self.tabs_initialized["settings"] = True
            except Exception as exc:
                _tab_load_failed("Settings", exc)

    def create_settings_tab_ui(self):
        """Build the Settings tab (config.ini / properties.ini editor)."""
        from common.ui.tk.settings_ui import SettingsUI

        self.settings_ui = SettingsUI(
            parent_frame=self.settings_tab,
            root=self.root,
            update_status_callback=self.update_status,
            theme=ColorTheme,
            on_settings_saved=self._on_settings_saved,
        )
        self.settings_ui.create_ui()

    def _on_settings_saved(self, saved_ids):
        """Apply settings that have live UI/runtime effects."""
        if "config.database.connection.default_autocommit" not in set(saved_ids or []):
            return
        if self.sql_editor:
            self.sql_editor.apply_default_autocommit()
        self.update_status("Autocommit default applied to open SQL editor tabs.")

    def _get_monitor_runtime_snapshot(self) -> dict:
        """In-tool Monitor tab state only — no metric polling."""
        out = {
            "ssh_hosts": 0,
            "cloud_databases_saved": 0,
            "os_servers_active": 0,
            "local_databases_active": 0,
            "cloud_databases_active": 0,
            "db_monitoring_targets": 0,
            "actively_monitoring": 0,
            "polling_active": False,
            "selected_connection": "",
            "tab_initialized": self.tabs_initialized.get("monitor", False),
            "recent_alerts": [],
            "unread_os": 0,
            "unread_db": 0,
        }
        if self.monitor_connection_manager:
            try:
                out["ssh_hosts"] = len(
                    self.monitor_connection_manager.get_all_connections()
                )
            except Exception:
                pass
        if self.server_monitor_ui:
            ui = self.server_monitor_ui
            monitor_conns = getattr(ui, "monitor_connections", {}) or {}
            out["os_servers_active"] = sum(
                1 for c in monitor_conns.values() if c.get("monitoring")
            )
            out["local_databases_active"] = len(
                getattr(ui, "monitored_databases", {}) or {}
            )
            out["cloud_databases_active"] = len(
                getattr(ui, "active_cloud_databases", {}) or {}
            )
            out["actively_monitoring"] = (
                out["os_servers_active"]
                + out["local_databases_active"]
                + out["cloud_databases_active"]
            )
            out["db_monitoring_targets"] = out["local_databases_active"]
            out["polling_active"] = bool(
                getattr(ui, "monitor_update_job", None)
            ) and out["actively_monitoring"] > 0
            out["selected_connection"] = getattr(ui, "selected_db_connection", "") or ""
            out["unread_os"] = int(getattr(ui, "_alert_unread_os", 0) or 0)
            out["unread_db"] = int(getattr(ui, "_alert_unread_db", 0) or 0)
            for entry in list(getattr(ui, "_alert_log", []) or [])[-15:]:
                if isinstance(entry, dict) and entry.get("message"):
                    out["recent_alerts"].append(
                        {
                            "severity": entry.get("severity", "INFO"),
                            "message": entry.get("message", ""),
                            "connection": "monitor",
                            "source": "monitor_tab",
                        }
                    )
        if out["tab_initialized"]:
            try:
                from common.cloud.connection_manager import CloudConnectionManager

                out["cloud_databases_saved"] = len(
                    CloudConnectionManager().load_cloud_databases()
                )
            except Exception:
                pass
        return out

    def _get_objects_runtime_snapshot(self) -> dict:
        if hasattr(self, "_objects_panel") and self._objects_panel:
            return self._objects_panel.runtime_snapshot()
        return {
            "initialized": False,
            "overview": "Database Objects tab not opened yet",
        }

    def _get_sql_runtime_snapshot(self) -> dict:
        if self.sql_editor:
            try:
                return self.sql_editor.get_dashboard_snapshot()
            except Exception:
                pass
        return {
            "initialized": False,
            "query_running": False,
            "overview": "SQL Editor tab not opened yet",
        }

    def _get_connections_runtime_snapshot(self) -> dict:
        active = list(self.active_connections.keys())
        try:
            saved = self.connection_manager.get_all_connections()
            saved_count = len(saved)
        except Exception:
            saved_count = 0
        selected = getattr(self, "current_connection_name", None) or ""
        if active or saved_count:
            overview = f"{len(active)} active, {saved_count} saved profile(s)"
            if selected:
                overview += f" — selected: {selected}"
        else:
            overview = "No connections configured"
        return {
            "active_count": len(active),
            "saved_count": saved_count,
            "active_names": active,
            "selected": selected,
            "overview": overview,
        }

    def _get_ai_runtime_snapshot(self) -> dict:
        if self.ai_query_ui:
            try:
                return self.ai_query_ui.get_dashboard_snapshot()
            except Exception:
                pass
        if self.ai_agent and hasattr(self.ai_agent, "sessions"):
            return {
                "installed": True,
                "tab_count": len(self.ai_agent.sessions.list_sessions()),
                "running_sessions": 0,
                "ui_busy": False,
                "working_on": "Open AI Query tab to view session activity",
                "sessions": self.ai_agent.sessions.list_sessions(),
            }
        return {"installed": self.ai_query_tab is not None}

    def _get_schema_runtime_snapshot(self) -> dict:
        if self.schema_converter_ui:
            try:
                return self.schema_converter_ui.get_dashboard_snapshot()
            except Exception:
                pass
        return {"installed": self.conversion_tab is not None}

    def create_dashboard_tab_ui(self):
        """Operational overview — monitoring, AI, schema activity, connections."""
        if self.dashboard_ui is not None:
            return

        from common.dashboard.service import DashboardCapabilities, DashboardRuntime

        self.dashboard_ui = DashboardUI(
            self.dashboard_tab,
            self.root,
            runtime=DashboardRuntime(
                get_active_connections=lambda: self.active_connections,
                get_saved_connections=lambda: self.connection_manager.get_all_connections(),
                get_monitor_runtime=self._get_monitor_runtime_snapshot,
                get_ai_runtime=self._get_ai_runtime_snapshot,
                get_schema_runtime=self._get_schema_runtime_snapshot,
                get_objects_runtime=self._get_objects_runtime_snapshot,
                get_sql_runtime=self._get_sql_runtime_snapshot,
                get_connections_runtime=self._get_connections_runtime_snapshot,
            ),
            capabilities=DashboardCapabilities(
                feature_module=self.feature_module,
                has_schema=self.conversion_tab is not None,
                has_ai=self.ai_query_tab is not None,
                has_monitor=self.monitor_tab is not None,
            ),
            on_navigate=self._navigate_from_dashboard,
            status_callback=self.update_status,
            font_ui=self.ui_font,
        )
        self.dashboard_ui.create_ui()

    def _navigate_from_dashboard(self, target: str) -> None:
        """Jump from dashboard cards to notebook tabs."""
        mapping = {
            "connections": self.connections_tab,
            "objects": self.objects_tab,
            "sql_editor": self.sql_editor_tab,
            "conversion": self.conversion_tab,
            "ai_query": self.ai_query_tab,
            "monitor": self.monitor_tab,
        }
        tab = mapping.get(target)
        if tab is None:
            return
        self._select_notebook_tab(tab)
        self.on_tab_changed()

    def _refresh_dashboard_if_visible(self) -> None:
        if self.dashboard_ui is None:
            return
        try:
            selected = self.root.nametowidget(self.notebook.select())
            if selected is self.dashboard_tab:
                self.dashboard_ui.refresh_async()
        except Exception:
            pass

    def create_welcome_tab_ui(self):
        """Create modern, visually appealing Welcome tab.

        Content (tagline, overview, per-tab guide, CLI/API access, shortcuts,
        platforms, tips, footer) comes from the shared spec so the Tk, Textual
        and Web Welcome screens stay in sync from one place; only the Tk-native
        colours/fonts/layout below are local.
        """
        from common.ui.shared import specs as _wspec

        # Accent palette cycled across shortcut / platform / tip rows (local).
        _accents = ["#14b8a6", "#3b82f6", "#a855f7", "#f43f5e", "#6366f1",
                    "#0ea5e9", "#ef4444", "#f59e0b", "#22c55e"]

        content_frame = make_scrollable(self.welcome_tab, bg=ColorTheme.BG_MAIN)

        # Hero Section - Eye-catching gradient
        hero_frame = tk.Frame(content_frame, bg="#0ea5e9", bd=0)
        hero_frame.pack(fill=tk.X, padx=0, pady=0)

        hero_inner = tk.Frame(hero_frame, bg="#0ea5e9")
        hero_inner.pack(fill=tk.X, padx=50, pady=40)

        # Bold, attention-grabbing title — centered and responsive to resize.
        title_label = tk.Label(
            hero_inner,
            text=APP_NAME,
            font=(self.ui_font[0], 32, "bold"),
            foreground="white",
            bg="#0ea5e9",
            anchor=tk.CENTER,
            justify=tk.CENTER,
        )
        title_label.pack(fill=tk.X)

        subtitle_label = tk.Label(
            hero_inner,
            text=_wspec.WELCOME_TAGLINE,
            font=(self.ui_font[0], 14),
            foreground="#e0f2fe",
            bg="#0ea5e9",
            anchor=tk.CENTER,
            justify=tk.CENTER,
        )
        subtitle_label.pack(fill=tk.X, pady=(8, 0))

        # Quick Overview - Simple list
        overview_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        overview_section.pack(fill=tk.X, padx=40, pady=(30, 20))

        tk.Label(
            overview_section,
            text="Quick Overview",
            font=(self.ui_font[0], 16, "bold"),
            foreground="#1e293b",
            bg=ColorTheme.BG_MAIN,
        ).pack(anchor=tk.W, pady=(0, 10))

        overview_items = list(_wspec.WELCOME_OVERVIEW)

        for item in overview_items:
            tk.Label(
                overview_section,
                text=f" {item}",
                font=(self.ui_font[0], 13),
                foreground="#475569",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.W,
            ).pack(anchor=tk.W, pady=3)

        # Tab Descriptions - Detailed usage guide
        tabs_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        tabs_section.pack(fill=tk.X, padx=40, pady=(25, 20))

        tk.Label(
            tabs_section,
            text=" Tab Descriptions & Usage Guide",
            font=(self.ui_font[0], 16, "bold"),
            foreground="#1e293b",
            bg=ColorTheme.BG_MAIN,
        ).pack(anchor=tk.W, pady=(0, 15))

        tab_details = [(f" {g['title']}", g["lines"]) for g in _wspec.WELCOME_TAB_GUIDE]

        for tab_name, details in tab_details:
            tab_frame = tk.Frame(tabs_section, bg="white", relief=tk.SOLID, bd=1)
            tab_frame.pack(fill=tk.X, pady=(0, 12))

            # Tab title
            tk.Label(
                tab_frame,
                text=tab_name,
                font=(self.ui_font[0], 13, "bold"),
                foreground="#0284c7",
                bg="white",
                anchor=tk.W,
            ).pack(anchor=tk.W, padx=15, pady=(12, 8))

            # Tab details
            for detail in details:
                detail_label = tk.Label(
                    tab_frame,
                    text=detail,
                    font=(self.ui_font[0], 13),
                    foreground="#475569",
                    bg="white",
                    anchor=tk.W,
                    justify=tk.LEFT,
                )
                detail_label.pack(anchor=tk.W, padx=25, pady=2)

            # Add spacing at bottom
            tk.Frame(tab_frame, height=10, bg="white").pack()

        # CLI, API & modular builds
        access_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        access_section.pack(fill=tk.X, padx=40, pady=(10, 20))

        tk.Label(
            access_section,
            text=" CLI, REST API & modular builds",
            font=(self.ui_font[0], 16, "bold"),
            foreground="#1e293b",
            bg=ColorTheme.BG_MAIN,
        ).pack(anchor=tk.W, pady=(0, 10))

        access_items = list(_wspec.WELCOME_ACCESS)
        for line in access_items:
            tk.Label(
                access_section,
                text=f"• {line}",
                font=(self.ui_font[0], 12),
                foreground="#475569",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.W,
                justify=tk.LEFT,
            ).pack(anchor=tk.W, pady=2)

        # Keyboard Shortcuts & Platforms - Minimal flat layout
        reference_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        reference_section.pack(fill=tk.X, padx=40, pady=(20, 10))

        # Two-column grid
        grid_container = tk.Frame(reference_section, bg=ColorTheme.BG_MAIN)
        grid_container.pack(fill=tk.BOTH, expand=True)

        # Left column: Keyboard Shortcuts
        shortcuts_card = tk.Frame(grid_container, bg=ColorTheme.BG_MAIN, relief=tk.FLAT)
        shortcuts_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 15))

        tk.Label(
            shortcuts_card,
            text=" Keyboard Shortcuts",
            font=(self.ui_font[0], 14, "bold"),
            foreground="#0284c7",
            bg=ColorTheme.BG_MAIN,
            anchor=tk.W,
        ).pack(fill=tk.X, pady=(0, 8))

        shortcuts = [
            (sc["keys"], sc["action"], _accents[i % len(_accents)])
            for i, sc in enumerate(_wspec.KEYBOARD_SHORTCUTS)
        ]

        for key, description, color in shortcuts:
            shortcut_row = tk.Frame(shortcuts_card, bg=ColorTheme.BG_MAIN)
            shortcut_row.pack(fill=tk.X, pady=2)

            tk.Label(
                shortcut_row,
                text=key,
                font=(self.ui_font[0], 12, "bold"),
                foreground=color,
                bg=ColorTheme.BG_MAIN,
                width=12,
                anchor=tk.W,
            ).pack(side=tk.LEFT)

            tk.Label(
                shortcut_row,
                text=description,
                font=(self.ui_font[0], 13),
                foreground="#64748b",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.W,
            ).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Right column: Supported Platforms
        platforms_card = tk.Frame(grid_container, bg=ColorTheme.BG_MAIN, relief=tk.FLAT)
        platforms_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(15, 0))

        tk.Label(
            platforms_card,
            text=" Platforms",
            font=(self.ui_font[0], 14, "bold"),
            foreground="#0284c7",
            bg=ColorTheme.BG_MAIN,
            anchor=tk.W,
        ).pack(fill=tk.X, pady=(0, 8))

        platforms = [
            (pf["name"], pf["versions"], _accents[i % len(_accents)])
            for i, pf in enumerate(_wspec.WELCOME_PLATFORMS)
        ]

        for platform, versions, color in platforms:
            platform_row = tk.Frame(platforms_card, bg=ColorTheme.BG_MAIN)
            platform_row.pack(fill=tk.X, pady=2)

            tk.Label(
                platform_row,
                text=f" {platform}",
                font=(self.ui_font[0], 12, "bold"),
                foreground=color,
                bg=ColorTheme.BG_MAIN,
                width=14,
                anchor=tk.W,
            ).pack(side=tk.LEFT)

            tk.Label(
                platform_row,
                text=versions,
                font=(self.ui_font[0], 13),
                foreground="#94a3b8",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.W,
            ).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Pro Tips Section - Minimal and lightweight
        tips_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        tips_section.pack(fill=tk.X, padx=40, pady=(20, 10))

        tk.Label(
            tips_section,
            text=" Tips",
            font=(self.ui_font[0], 14, "bold"),
            foreground="#0284c7",
            bg=ColorTheme.BG_MAIN,
            anchor=tk.W,
        ).pack(fill=tk.X, pady=(0, 8))

        tips = [
            (tip, _accents[i % len(_accents)])
            for i, tip in enumerate(_wspec.WELCOME_TIPS)
        ]

        # Simple list layout
        for tip, color in tips:
            tip_row = tk.Frame(tips_section, bg=ColorTheme.BG_MAIN)
            tip_row.pack(fill=tk.X, pady=2)

            tk.Label(
                tip_row,
                text="",
                font=(self.ui_font[0], 12),
                foreground=color,
                bg=ColorTheme.BG_MAIN,
            ).pack(side=tk.LEFT, padx=(0, 8))

            tk.Label(
                tip_row,
                text=tip,
                font=(self.ui_font[0], 13),
                foreground="#64748b",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.W,
            ).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Support footer - minimal
        footer_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        footer_section.pack(fill=tk.X, padx=40, pady=(20, 40))

        tk.Label(
            footer_section,
            text=f" {_wspec.WELCOME_FOOTER}",
            font=(self.ui_font[0], 11),
            foreground="#94a3b8",
            bg=ColorTheme.BG_MAIN,
            anchor=tk.CENTER,
        ).pack()

    def create_connections_tab_ui(self):
        """Create UI for connections tab - manage database connections"""
        connections_inner = make_scrollable(
            self.connections_tab, bg=ColorTheme.BG_MAIN
        )

        # Section ORDER and the COLLAPSED-by-default state come from the shared
        # spec (common.ui.shared.specs.CONNECTION_SECTIONS), so the Tk, Textual
        # and Web UIs stay in lockstep — a layout change is made once there and
        # every UI follows. Each builder below renders Tk-native widgets.
        section_builders = {
            "active": self.create_active_connections_frame,
            "saved": self.create_saved_connections_frame,
            "direct": self.create_connection_frame,
            "remote": self.create_remote_connection_frame,
            "cloud": self.create_cloud_connection_frame,
        }
        for section in self._connection_sections():
            builder = section_builders.get(section["id"])
            if builder is not None:
                builder(connections_inner)

        # Initialize with first available DB type (without triggering heavy operations)
        available_types = DatabaseConfig.get_db_types()
        if available_types:
            db_type = available_types[0]
            self.db_type_combo.set(db_type)
            self.current_db_type = db_type

            # Set default port
            self.port_entry.delete(0, tk.END)
            self.port_entry.insert(0, DatabaseConfig.get_default_port(db_type))

            # Update service/database label
            if db_type == "Oracle":
                self.service_label.config(text="Service:")
            elif db_type in DB_NAME_OPTIONAL_TYPES:
                self.service_label.config(text="Database (optional):")
            elif db_type in ["SQLServer", "MongoDB", "DocumentDB"]:
                self.service_label.config(text="Database:")
            self._update_security_fields_visibility(db_type)
        else:
            self._update_security_fields_visibility("")

        # Ensure canvas view starts at the top after content is loaded
        self.root.after(
            100, lambda: connections_inner.scroll_canvas.yview_moveto(0))

    def create_objects_tab_ui(self):
        """Create UI for database objects tab - browse and view database objects"""
        self._objects_panel = DatabaseObjectsPanel(
            self.objects_tab,
            self.root,
            ObjectsConnectionContext(
                get_connections=lambda: self.active_connections,
                get_current_connection=lambda: self.current_connection_name,
                set_current_connection=lambda name: setattr(
                    self, "current_connection_name", name
                ),
                get_db_type=lambda: self.current_db_type,
                set_db_type=lambda t: setattr(self, "current_db_type", t),
                db_query_lock=self.db_query_lock,
                get_available_operations=DatabaseConfig.get_available_operations,
            ),
            ObjectsPanelActions(
                update_status=self.update_status,
                import_data_callback=self.import_data_to_table,
            ),
            ObjectsPanelFonts(ui=self.ui_font, mono=self.ui_font_mono),
        )
        self._objects_panel.build()

        # Back-compat for code that reads these attributes on the shell.
        self.objects_connection_combo = self._objects_panel.connection_combo
        self.operation_buttons = self._objects_panel.operation_buttons
        self.objects_paned = self._objects_panel.paned

        self._objects_panel.refresh_connections()
        if self.current_connection_name:
            self.update_status(
                f"Database Objects ready — {self.current_connection_name}"
            )

    def _setup_readable_ttk(self):
        style = ttk.Style()

        # Set theme base
        try:
            style.theme_use("clam")  # Use clam theme for better customization
        except tk.TclError:
            pass  # Theme not available, use default

        try:
            # General font settings
            style.configure("TLabel", font=self.ui_font, background=ColorTheme.BG_MAIN)
            style.configure(
                "TCheckbutton", font=self.ui_font, background=ColorTheme.BG_MAIN
            )
            style.configure(
                "TRadiobutton", font=self.ui_font, background=ColorTheme.BG_MAIN
            )

            # LabelFrame heading font - bigger for better visibility
            labelframe_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")
            style.configure(
                "TLabelframe.Label",
                font=labelframe_font,
                foreground=ColorTheme.PRIMARY_DARK,
                background=ColorTheme.BG_MAIN,
            )

            # Button styling - modern look with border
            style.configure(
                "TButton",
                font=self.ui_font,
                padding=6,
                relief="solid",
                borderwidth=1,
                bordercolor="#cbd5e1",
            )
            style.map(
                "TButton",
                background=[
                    ("active", ColorTheme.PRIMARY_LIGHT),
                    ("!active", ColorTheme.BG_SECONDARY),
                ],
                foreground=[("active", ColorTheme.PRIMARY_DARK)],
                bordercolor=[("active", "#94a3b8"), ("!active", "#cbd5e1")],
            )

            # Primary button style (for important actions)
            style.configure(
                "Primary.TButton",
                font=self.ui_font,
                padding=6,
                background=ColorTheme.PRIMARY,
                foreground="white",
                relief="solid",
                borderwidth=1,
                bordercolor=ColorTheme.PRIMARY_DARK,
            )
            style.map(
                "Primary.TButton",
                background=[
                    ("active", ColorTheme.PRIMARY_DARK),
                    ("!active", ColorTheme.PRIMARY),
                ],
                bordercolor=[
                    ("active", "#1e3a8a"),
                    ("!active", ColorTheme.PRIMARY_DARK),
                ],
            )

            # Success button style
            style.configure(
                "Success.TButton",
                font=self.ui_font,
                padding=6,
                background=ColorTheme.SUCCESS,
                foreground="white",
                relief="solid",
                borderwidth=1,
                bordercolor="#388e3c",
            )
            style.map(
                "Success.TButton",
                background=[("active", "#45a049"), ("!active", ColorTheme.SUCCESS)],
                foreground=[
                    ("active", "white"),
                    ("pressed", "white"),
                    ("!active", "white"),
                ],
                bordercolor=[("active", "#2e7d32"), ("!active", "#388e3c")],
            )

            # Warning button style
            style.configure(
                "Warning.TButton",
                font=self.ui_font,
                padding=6,
                background=ColorTheme.WARNING,
                foreground=ColorTheme.TEXT_PRIMARY,
                relief="solid",
                borderwidth=1,
                bordercolor="#f57c00",
            )
            style.map(
                "Warning.TButton",
                background=[
                    ("active", "#ffb300"),
                    ("pressed", "#ff8f00"),
                    ("!active", ColorTheme.WARNING),
                ],
                foreground=[
                    ("active", ColorTheme.TEXT_PRIMARY),
                    ("pressed", ColorTheme.TEXT_PRIMARY),
                    ("!active", ColorTheme.TEXT_PRIMARY),
                ],
                bordercolor=[("active", "#e65100"), ("!active", "#f57c00")],
            )

            # Error button style
            style.configure(
                "Error.TButton",
                font=self.ui_font,
                padding=6,
                background=ColorTheme.ERROR,
                foreground="white",
                relief="solid",
                borderwidth=1,
                bordercolor="#c62828",
            )
            style.map(
                "Error.TButton",
                background=[
                    ("active", "#d32f2f"),
                    ("pressed", "#b71c1c"),
                    ("!active", ColorTheme.ERROR),
                ],
                foreground=[
                    ("active", "white"),
                    ("pressed", "white"),
                    ("disabled", "#cccccc"),
                    ("!active", "white"),
                ],
                bordercolor=[("active", "#b71c1c"), ("!active", "#c62828")],
            )

            # Frame styling
            style.configure("TFrame", background=ColorTheme.BG_MAIN)
            style.configure(
                "Card.TFrame",
                background=ColorTheme.BG_SECONDARY,
                relief="flat",
                borderwidth=1,
            )

            # Notebook (tab) styling
            style.configure("TNotebook", background=ColorTheme.BG_MAIN, borderwidth=0)
            style.configure(
                "TNotebook.Tab",
                font=self.ui_font,
                padding=[12, 6],
                background=ColorTheme.BG_SECONDARY,
            )
            style.map(
                "TNotebook.Tab",
                background=[
                    ("selected", ColorTheme.PRIMARY),
                    ("!selected", ColorTheme.BG_SECONDARY),
                ],
                foreground=[
                    ("selected", "white"),
                    ("!selected", ColorTheme.TEXT_PRIMARY),
                ],
            )

            # Entry styling
            style.configure(
                "TEntry",
                fieldbackground=ColorTheme.BG_SECONDARY,
                borderwidth=1,
                relief="solid",
            )

            # Combobox styling
            style.configure(
                "TCombobox",
                fieldbackground=ColorTheme.BG_SECONDARY,
                background=ColorTheme.BG_SECONDARY,
                borderwidth=1,
            )

            # Treeview styling
            style.configure(
                "Treeview",
                font=self.ui_font,
                background=ColorTheme.BG_SECONDARY,
                fieldbackground=ColorTheme.BG_SECONDARY,
                borderwidth=0,
                relief="flat",
            )
            style.configure(
                "Treeview.Heading",
                font=(self.ui_font[0], self.ui_font[1], "bold"),
                background=ColorTheme.PRIMARY_LIGHT,
                foreground=ColorTheme.PRIMARY_DARK,
                borderwidth=1,
                relief="flat",
            )
            style.map("Treeview.Heading", background=[("active", ColorTheme.PRIMARY)])

            # Progressbar styling
            style.configure(
                "TProgressbar",
                background=ColorTheme.PRIMARY,
                troughcolor=ColorTheme.BG_MAIN,
                borderwidth=0,
                thickness=20,
            )

            # LabelFrame styling
            style.configure(
                "TLabelframe",
                background=ColorTheme.BG_MAIN,
                borderwidth=1,
                relief="solid",
            )

        except tk.TclError:
            pass

        try:
            style.configure("Treeview", rowheight=max(22, int(self.ui_font[1]) + 10))
        except tk.TclError:
            pass

    def _select_notebook_tab(self, tab_widget):
        """Select a notebook tab by widget (works in full and standalone module UI)."""
        if tab_widget is not None:
            self.notebook.select(tab_widget)

    def _ensure_sql_editor(self):
        """Select SQL tab and create editor if needed."""
        self._select_notebook_tab(self.sql_editor_tab)
        self.root.update_idletasks()
        if not self.tabs_initialized["sql_editor"]:
            self.on_tab_changed()

    def _send_sql_to_editor(self, sql_text):
        """Callback for AI module to send SQL to editor"""
        self._ensure_sql_editor()
        if self.sql_editor is None:
            return
        self.sql_editor.sql_text.delete(1.0, tk.END)
        self.sql_editor.sql_text.insert(1.0, sql_text)
        self._select_notebook_tab(self.sql_editor_tab)

    def _create_menubar(self):
        menubar = tk.Menu(self.root)
        file_m = tk.Menu(menubar, tearoff=0)
        file_m.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_m)

        view_m = tk.Menu(menubar, tearoff=0)
        view_m.add_command(
            label="Dashboard", command=lambda: self._select_notebook_tab(self.dashboard_tab)
        )
        if self.welcome_tab is not None:
            view_m.add_command(
                label="Welcome", command=lambda: self._select_notebook_tab(self.welcome_tab)
            )
        view_m.add_command(
            label="Connections",
            command=lambda: self._select_notebook_tab(self.connections_tab),
        )
        view_m.add_command(
            label="Database Objects",
            command=lambda: self._select_notebook_tab(self.objects_tab),
        )
        view_m.add_command(
            label="SQL Editor",
            command=lambda: self._select_notebook_tab(self.sql_editor_tab),
        )
        if self.conversion_tab is not None:
            view_m.add_command(
                label="Data Migration",
                command=lambda: self._select_notebook_tab(self.conversion_tab),
            )
        if self.ai_query_tab is not None:
            view_m.add_command(
                label="AI Query Assistant",
                command=lambda: self._select_notebook_tab(self.ai_query_tab),
            )
        if self.monitor_tab is not None:
            view_m.add_command(
                label="Monitor", command=lambda: self._select_notebook_tab(self.monitor_tab)
            )
        menubar.add_cascade(label="View", menu=view_m)

        conn_m = tk.Menu(menubar, tearoff=0)
        conn_m.add_command(
            label="New connection",
            command=lambda: self._select_notebook_tab(self.connections_tab),
        )
        conn_m.add_command(
            label="Disconnect all", command=self.disconnect_all_connections
        )
        conn_m.add_separator()
        conn_m.add_command(
            label="Saved connections", command=self.show_saved_connections
        )
        menubar.add_cascade(label="Connection", menu=conn_m)

        sql_m = tk.Menu(menubar, tearoff=0)
        sql_m.add_command(
            label="Execute at cursor (F5)", command=self._menu_sql_execute_cursor
        )
        sql_m.add_command(
            label="Execute selected", command=self._menu_sql_execute_selected
        )
        sql_m.add_command(label="Execute all", command=self._menu_sql_execute_all)
        sql_m.add_separator()
        sql_m.add_command(label="New editor tab", command=self._menu_sql_new_tab)
        sql_m.add_command(label="Close editor tab", command=self._menu_sql_close_tab)
        sql_m.add_separator()
        sql_m.add_command(label="Load query", command=self._menu_sql_load)
        sql_m.add_command(label="Save query", command=self._menu_sql_save)
        sql_m.add_separator()
        sql_m.add_command(label="Commit", command=self._menu_sql_commit)
        sql_m.add_command(label="Rollback", command=self._menu_sql_rollback)
        sql_m.add_separator()
        sql_m.add_command(label="Query history", command=self._menu_sql_history)
        sql_m.add_command(label="Export results", command=self._menu_sql_export)
        sql_m.add_command(label="Clear results", command=self._menu_sql_clear_results)
        menubar.add_cascade(label="SQL", menu=sql_m)

        help_m = tk.Menu(menubar, tearoff=0)
        help_m.add_command(
            label="Keyboard shortcuts", command=self._show_shortcuts_help
        )
        menubar.add_cascade(label="Help", menu=help_m)

        self.root.config(menu=menubar)

    def _show_shortcuts_help(self):
        text = (
            "Navigation\n"
            "  View menu  jump between tabs\n\n"
            "Connections Tab\n"
            "  Create and manage database connections\n"
            "   /   expand or collapse connection sections\n\n"
            "SQL Editor\n"
            "  F5 or Ctrl+Enter  run query at cursor\n"
            "  + after last tab / × on tab  multiple editor sessions\n"
            "  Collapse 'Connection & actions' for more editor space\n\n"
            "Database Objects\n"
            "  Browse tables, views, procedures, etc.\n"
        )
        messagebox.showinfo("Shortcuts", text)

    def _menu_sql_execute_cursor(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.execute_at_cursor()

    def _menu_sql_execute_selected(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.execute_selected()

    def _menu_sql_execute_all(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.execute_all()

    def _menu_sql_new_tab(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.add_tab()

    def _menu_sql_close_tab(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.close_current_tab()

    def _menu_sql_load(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.load_query()

    def _menu_sql_save(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.save_query()

    def _menu_sql_commit(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.commit_transaction()

    def _menu_sql_rollback(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.rollback_transaction()

    def _menu_sql_history(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.show_history()

    def _menu_sql_export(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.export_results()

    def _menu_sql_clear_results(self):
        self._ensure_sql_editor()
        if self.sql_editor:
            self.sql_editor.clear_results()

    @staticmethod
    def _connection_sections() -> list:
        """Shared Connections-tab layout (order + collapse), with a fallback."""
        try:
            from common.ui.shared import specs

            return list(specs.CONNECTION_SECTIONS)
        except Exception:
            return [{"id": i, "collapsed": i != "active"} for i in
                    ("active", "saved", "direct", "remote", "cloud")]

    def _conn_section_expanded(self, section_id: str) -> bool:
        """Whether a Connections-tab section starts expanded (from shared spec)."""
        for section in self._connection_sections():
            if section.get("id") == section_id:
                return not bool(section.get("collapsed", True))
        return section_id == "active"

    def create_active_connections_frame(self, parent):
        title_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")

        content = make_collapsible_section(
            parent, "Active connections", title_font,
            expanded=self._conn_section_expanded("active"),
        )

        list_container = ttk.Frame(content)
        list_container.pack(anchor=tk.W, padx=10, pady=5)

        scrollbar = ttk.Scrollbar(list_container, orient=tk.VERTICAL)
        self.active_conn_listbox = tk.Listbox(
            list_container,
            width=50,
            height=6,
            font=self.ui_font,
            yscrollcommand=scrollbar.set,
            bg=ColorTheme.BG_SECONDARY,
            fg=ColorTheme.TEXT_PRIMARY,
            selectbackground=ColorTheme.PRIMARY,
            selectforeground="white",
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=ColorTheme.BORDER,
        )
        scrollbar.config(command=self.active_conn_listbox.yview)

        self.active_conn_listbox.pack(side=tk.LEFT)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.active_conn_listbox.bind("<<ListboxSelect>>", self.on_connection_selected)

        btn_frame = ttk.Frame(content)
        btn_frame.pack(anchor=tk.W, padx=10, pady=(5, 5))

        ttk.Button(
            btn_frame,
            text="Disconnect Selected",
            command=self.disconnect_selected_connection,
            style="Warning.TButton",
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            btn_frame,
            text="Disconnect All",
            command=self.disconnect_all_connections,
            style="Error.TButton",
        ).pack(side=tk.LEFT, padx=2)

    def create_connection_frame(self, parent):
        title_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")
        content = make_collapsible_section(
            parent, "Add or select database connection", title_font,
            expanded=self._conn_section_expanded("direct"),
        )
        self.conn_frame = ttk.Frame(content)
        self.conn_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Form Fields
        form_frame = ttk.Frame(self.conn_frame)
        form_frame.pack(fill=tk.X)

        # Database Type
        ttk.Label(
            form_frame,
            text="Database Type:",
            font=(self.ui_font[0], self.ui_font[1], "bold"),
        ).grid(row=0, column=0, sticky=tk.W, padx=5, pady=(0, 5))
        self.db_type_combo = ttk.Combobox(form_frame, width=35, state="readonly")
        self.db_type_combo["values"] = DatabaseConfig.get_db_types()
        self.db_type_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=(0, 5))
        self.db_type_combo.bind(
            "<<ComboboxSelected>>", lambda e: self.on_db_type_changed()
        )

        # Host
        ttk.Label(form_frame, text="Host:").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.host_entry = ttk.Entry(form_frame, width=35)
        self.host_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        self.host_entry.insert(0, "localhost")

        # Port
        ttk.Label(form_frame, text="Port:").grid(
            row=2, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.port_entry = ttk.Entry(form_frame, width=35)
        self.port_entry.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)

        # Service/Database (dynamic label)
        self.service_label = ttk.Label(form_frame, text="Service:")
        self.service_label.grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        self.service_entry = ttk.Entry(form_frame, width=35)
        self.service_entry.grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)

        # Username
        ttk.Label(form_frame, text="Username:").grid(
            row=4, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.user_entry = ttk.Entry(form_frame, width=35)
        self.user_entry.grid(row=4, column=1, sticky=tk.W, padx=5, pady=5)

        # Password
        ttk.Label(form_frame, text="Password:").grid(
            row=5, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.password_entry = ttk.Entry(form_frame, width=35, show="*")
        self.password_entry.grid(row=5, column=1, sticky=tk.W, padx=5, pady=5)

        # --- Security / TLS (capability-driven) ---
        self.ssl_mode_label = ttk.Label(form_frame, text="SSL mode:")
        self.ssl_mode_combo = ttk.Combobox(form_frame, width=33, state="readonly")
        self.ssl_mode_combo.set("disable")

        self.ssl_ca_label = ttk.Label(form_frame, text="SSL CA file:")
        self.ssl_ca_entry = ttk.Entry(form_frame, width=35)
        self.ssl_cert_label = ttk.Label(form_frame, text="SSL client cert:")
        self.ssl_cert_entry = ttk.Entry(form_frame, width=35)
        self.ssl_key_label = ttk.Label(form_frame, text="SSL client key:")
        self.ssl_key_entry = ttk.Entry(form_frame, width=35)
        self.wallet_label = ttk.Label(form_frame, text="Oracle wallet dir:")
        self.wallet_entry = ttk.Entry(form_frame, width=35)

        self.mongo_tls_var = tk.BooleanVar(value=False)
        self.mongo_tls_cb = ttk.Checkbutton(
            form_frame, text="Use TLS (MongoDB / DocumentDB)", variable=self.mongo_tls_var
        )
        self.mongo_tls_ca_label = ttk.Label(form_frame, text="TLS CA file:")
        self.mongo_tls_ca_entry = ttk.Entry(form_frame, width=35)

        self.ssl_mode_label.grid(row=6, column=0, sticky=tk.W, padx=5, pady=2)
        self.ssl_mode_combo.grid(row=6, column=1, sticky=tk.W, padx=5, pady=2)
        self.ssl_ca_label.grid(row=7, column=0, sticky=tk.W, padx=5, pady=2)
        self.ssl_ca_entry.grid(row=7, column=1, sticky=tk.W, padx=5, pady=2)
        self.ssl_cert_label.grid(row=8, column=0, sticky=tk.W, padx=5, pady=2)
        self.ssl_cert_entry.grid(row=8, column=1, sticky=tk.W, padx=5, pady=2)
        self.ssl_key_label.grid(row=9, column=0, sticky=tk.W, padx=5, pady=2)
        self.ssl_key_entry.grid(row=9, column=1, sticky=tk.W, padx=5, pady=2)
        self.wallet_label.grid(row=10, column=0, sticky=tk.W, padx=5, pady=2)
        self.wallet_entry.grid(row=10, column=1, sticky=tk.W, padx=5, pady=2)
        self.mongo_tls_cb.grid(row=6, column=0, columnspan=2, sticky=tk.W, padx=5, pady=2)
        self.mongo_tls_ca_label.grid(row=7, column=0, sticky=tk.W, padx=5, pady=2)
        self.mongo_tls_ca_entry.grid(row=7, column=1, sticky=tk.W, padx=5, pady=2)
        self._update_security_fields_visibility(self.db_type_combo.get() or "")

        # Action Buttons - Horizontal row below password
        button_frame = ttk.Frame(self.conn_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        self.connect_btn = ttk.Button(
            button_frame,
            text="Connect",
            command=self.connect_db,
            style="Primary.TButton",
            width=15,
        )
        self.connect_btn.pack(side=tk.LEFT, padx=(5, 5))

        ttk.Button(
            button_frame,
            text="Test Connection",
            command=self.test_db_connection,
            width=15,
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            button_frame,
            text="Load Saved",
            command=self.show_saved_connections,
            width=15,
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            button_frame,
            text="Save Connection",
            command=self.save_connection_dialog,
            width=15,
        ).pack(side=tk.LEFT, padx=5)

    def _update_security_fields_visibility(self, db_type: str) -> None:
        """Show Mongo TLS or SQL SSL fields based on engine capabilities."""
        sql_widgets = (
            self.ssl_mode_label,
            self.ssl_mode_combo,
            self.ssl_ca_label,
            self.ssl_ca_entry,
            self.ssl_cert_label,
            self.ssl_cert_entry,
            self.ssl_key_label,
            self.ssl_key_entry,
            self.wallet_label,
            self.wallet_entry,
        )
        mongo_widgets = (
            self.mongo_tls_cb,
            self.mongo_tls_ca_label,
            self.mongo_tls_ca_entry,
        )
        for widget in sql_widgets + mongo_widgets:
            widget.grid_remove()

        if db_type in ("MongoDB", "DocumentDB"):
            self.mongo_tls_cb.grid()
            self.mongo_tls_ca_label.grid()
            self.mongo_tls_ca_entry.grid()
            if db_type == "DocumentDB":
                self.mongo_tls_var.set(True)
            return

        caps = DatabaseConfig.get_capabilities(db_type)
        if not caps.supports_ssl:
            return

        self.ssl_mode_label.grid(row=6, column=0, sticky=tk.W, padx=5, pady=2)
        self.ssl_mode_combo.grid(row=6, column=1, sticky=tk.W, padx=5, pady=2)
        modes = list(caps.ssl_mode_options or ("disable",))
        self.ssl_mode_combo["values"] = modes
        if self.ssl_mode_combo.get() not in modes:
            self.ssl_mode_combo.set(modes[0])

        fields = set(caps.ssl_fields or ())
        row = 7
        if "ca" in fields:
            self.ssl_ca_label.grid(row=row, column=0, sticky=tk.W, padx=5, pady=2)
            self.ssl_ca_entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
            row += 1
        if "cert" in fields:
            self.ssl_cert_label.grid(row=row, column=0, sticky=tk.W, padx=5, pady=2)
            self.ssl_cert_entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
            row += 1
        if "key" in fields:
            self.ssl_key_label.grid(row=row, column=0, sticky=tk.W, padx=5, pady=2)
            self.ssl_key_entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
            row += 1
        if "wallet" in fields:
            self.wallet_label.grid(row=row, column=0, sticky=tk.W, padx=5, pady=2)
            self.wallet_entry.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)

    def _collect_security_conn_params(self, db_type: str) -> dict:
        """Build TLS/SSL kwargs for connect/test from the connection form."""
        params: dict = {}
        if db_type in ("MongoDB", "DocumentDB"):
            params["tls"] = self.mongo_tls_var.get()
            tls_ca = self.mongo_tls_ca_entry.get().strip()
            if tls_ca:
                params["tls_ca_file"] = tls_ca
            return params

        caps = DatabaseConfig.get_capabilities(db_type)
        if not caps.supports_ssl:
            return params

        mode = self.ssl_mode_combo.get().strip()
        if mode:
            params["ssl_mode"] = mode
        for key, entry in (
            ("ssl_ca", self.ssl_ca_entry),
            ("ssl_cert", self.ssl_cert_entry),
            ("ssl_key", self.ssl_key_entry),
            ("wallet_location", self.wallet_entry),
        ):
            value = entry.get().strip()
            if value:
                params[key] = value
        return params

    def _apply_security_fields_from_profile(self, conn: dict) -> None:
        """Populate TLS/SSL widgets when loading a saved connection."""
        db_type = conn.get("db_type", "")
        self._update_security_fields_visibility(db_type)
        if db_type in ("MongoDB", "DocumentDB"):
            self.mongo_tls_var.set(bool(conn.get("tls")))
            self.mongo_tls_ca_entry.delete(0, tk.END)
            self.mongo_tls_ca_entry.insert(0, conn.get("tls_ca_file") or "")
            return

        mode = conn.get("ssl_mode") or "disable"
        if hasattr(self, "ssl_mode_combo"):
            self.ssl_mode_combo.set(mode)
        for key, entry in (
            ("ssl_ca", self.ssl_ca_entry),
            ("ssl_cert", self.ssl_cert_entry),
            ("ssl_key", self.ssl_key_entry),
            ("wallet_location", self.wallet_entry),
        ):
            entry.delete(0, tk.END)
            entry.insert(0, conn.get(key) or "")

    def create_remote_connection_frame(self, parent):
        """Embedded remote DB registration (SSH tunnel) on Connections tab."""
        from common.ui.tk.remote_db_connection_panel import RemoteDBConnectionPanel

        title_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")
        self.remote_db_panel = RemoteDBConnectionPanel(
            parent,
            self.root,
            self.ui_font,
            title_font,
            self.connection_manager,
            self.update_status,
            on_register_connection=self._register_external_active_connection,
        )
        self.remote_db_panel.build(expanded=self._conn_section_expanded("remote"))

    def _register_external_active_connection(self, conn_name: str, db_manager) -> None:
        """Add a panel-initiated SQL session to active connections (no direct form)."""
        version = db_manager.get_version() or "?"
        try:
            is_admin = db_manager.is_admin()
        except Exception:
            is_admin = False
        self._connection_success(
            conn_name, db_manager.db_type, db_manager, version, is_admin, {}
        )

    def create_cloud_connection_frame(self, parent):
        """Embedded cloud DB registration (API auth + SQL login) on Connections tab."""
        from common.ui.tk.cloud_db_connection_panel import CloudDBConnectionPanel

        title_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")
        self.cloud_db_panel = CloudDBConnectionPanel(
            parent,
            self.root,
            self.ui_font,
            title_font,
            self.connection_manager,
            self.update_status,
            on_register_connection=self._register_cloud_active_connection,
        )
        self.cloud_db_panel.build(expanded=self._conn_section_expanded("cloud"))

    def _register_cloud_active_connection(self, conn_name: str, db_manager) -> None:
        """Add a cloud-initiated SQL session to active connections (no direct-DB form)."""
        version = db_manager.get_version() or "?"
        try:
            is_admin = db_manager.is_admin()
        except Exception:
            is_admin = False
        self._connection_success(
            conn_name, db_manager.db_type, db_manager, version, is_admin, {}
        )

    def create_conversion_tab_ui(self):
        """Create UI for schema conversion tab"""
        self.schema_converter_ui = SchemaConverterUI(
            parent_frame=self.conversion_tab,
            root=self.root,
            get_connections_callback=lambda: self.active_connections,
            update_status_callback=self.update_status,
            theme=ColorTheme,
            fonts={"ui": self.ui_font, "mono": self.ui_font_mono},
        )
        self.schema_converter_ui.create_ui()

    def create_status_bar(self):
        # Status Bar with modern styling - always visible at bottom
        status_frame = tk.Frame(self.root, bg=ColorTheme.PRIMARY, height=30)
        status_frame.pack(fill=tk.X, side=tk.BOTTOM)
        status_frame.pack_propagate(False)  # Maintain fixed height

        self.status_bar = tk.Label(
            status_frame,
            text="Ready",
            anchor=tk.W,
            font=self.ui_font,
            bg=ColorTheme.PRIMARY,
            fg="white",
            padx=10,
            pady=5,
        )
        self.status_bar.pack(fill=tk.BOTH, expand=True)

    def _fit_window_to_tabs(self, width, height, min_width, min_height):
        """Ensure the window opens wide enough to show every notebook tab.

        ``ttk.Notebook`` does not scroll its tab strip — tabs that don't fit
        the window width are clipped ("suppressed"). We measure the notebook's
        requested width (which includes the full tab row) and open the window
        wide enough that every tab label is visible, capped to the screen so
        the window never opens off-screen.

        The minimum window size is intentionally left unchanged: the user is
        free to drag the window smaller, at which point tabs may clip — that's
        expected for a manual resize.
        """
        try:
            self.root.update_idletasks()
            # Reqwidth of the (empty) notebook is dominated by its tab strip.
            needed = self.notebook.winfo_reqwidth() + 24  # borders + safety
            screen_w = self.root.winfo_screenwidth()
            cap = max(int(min_width), screen_w - 40)
            target_w = int(min(max(int(width), needed), cap))
            self.root.geometry(f"{target_w}x{int(height)}")
        except Exception:
            pass

    def create_toolbar(self):
        """Create top toolbar with global action buttons"""
        toolbar = ttk.Frame(self.root, style="Card.TFrame")
        toolbar.pack(fill=tk.X, padx=5, pady=(5, 0))

        # Spacer on left to push button to right
        ttk.Frame(toolbar).pack(side=tk.LEFT, expand=True)

        # Clear Cache button on right
        clear_btn = ttk.Button(
            toolbar, text="Clear Cache", command=self.clear_all_caches, style="TButton"
        )
        clear_btn.pack(side=tk.RIGHT, padx=5, pady=5)

    def clear_all_caches(self):
        """Clear all application caches (AI, credentials) while preserving active connections"""

        # Confirmation dialog
        active_monitor_count = 0
        if hasattr(self, "server_monitor_ui") and self.server_monitor_ui:
            active_monitor_count = sum(
                1
                for conn in self.server_monitor_ui.monitor_connections.values()
                if conn.get("monitoring", False)
            )

        confirm_msg = (
            "This will clear the following caches:\n\n"
            " AI schema and context caches\n"
            " AI conversation history\n"
            " Saved credentials (reload from disk)\n\n"
            f"Preserved (NOT affected):\n"
            f" Active DB connections: {len(self.active_connections)}\n"
            f" Active monitoring sessions: {active_monitor_count}\n\n"
            "Continue?"
        )

        if not messagebox.askyesno("Clear Cache", confirm_msg, icon="question"):
            return

        try:
            cleared_items = []

            # 1. Clear AI schema and context caches
            if hasattr(self, "ai_agent") and self.ai_agent:
                cache_info = self.ai_agent.get_cache_info()
                cache_count = len(cache_info)
                self.ai_agent.invalidate_cache()  # Clear all connection caches
                cleared_items.append(f"AI caches ({cache_count} connections)")

            # 2. Clear AI conversation history
            if hasattr(self, "ai_agent") and self.ai_agent:
                self.ai_agent.clear_conversation()
                cleared_items.append("AI conversation history")

            # 3. Reload saved credentials from disk
            if hasattr(self, "connection_manager") and self.connection_manager:
                old_count = len(self.connection_manager.get_all_connections())
                self.connection_manager.connections = (
                    self.connection_manager.load_connections()
                )
                new_count = len(self.connection_manager.get_all_connections())
                cleared_items.append(f"Database connections ({old_count}  {new_count})")

            # 4. Reload monitor credentials from disk
            if (
                hasattr(self, "monitor_connection_manager")
                and self.monitor_connection_manager
            ):
                old_count = len(self.monitor_connection_manager.get_all_connections())
                self.monitor_connection_manager.connections = (
                    self.monitor_connection_manager.load_connections()
                )
                new_count = len(self.monitor_connection_manager.get_all_connections())
                cleared_items.append(f"Monitor connections ({old_count}  {new_count})")

            # 5. Refresh AI Query UI if it's initialized
            if hasattr(self, "ai_query_ui") and self.ai_query_ui:
                try:
                    self.ai_query_ui.refresh_connections()
                    cleared_items.append("AI Query UI refreshed")
                except Exception as e:
                    console_print(f"Warning: Could not refresh AI Query UI: {e}")

            # Success message
            success_msg = (
                " Cache cleared successfully!\n\n"
                "Cleared:\n"
                + "\n".join(f"   {item}" for item in cleared_items)
                + f"\n\nPreserved:\n"
                f"   Active DB connections: {len(self.active_connections)}\n"
                f"   Active monitoring sessions: {active_monitor_count}"
            )

            messagebox.showinfo("Cache Cleared", success_msg)
            self.update_status("Cache cleared", "success")

        except Exception as e:
            error_msg = f"Error clearing caches:\n{str(e)}"
            messagebox.showerror("Clear Cache Error", error_msg)
            self.update_status(f" Error clearing caches: {str(e)}", "error")
            import traceback

            traceback.print_exc()

    def on_db_type_changed(self):
        """Handle database type change"""
        db_type = self.db_type_combo.get()
        if not db_type:
            return

        self.current_db_type = db_type

        # Update port default
        self.port_entry.delete(0, tk.END)
        self.port_entry.insert(0, DatabaseConfig.get_default_port(db_type))

        # Update service/database label
        if db_type == "Oracle":
            self.service_label.config(text="Service:")
        elif db_type in DB_NAME_OPTIONAL_TYPES:
            self.service_label.config(text="Database (optional):")
        elif db_type in ["SQLServer", "MongoDB", "DocumentDB"]:
            self.service_label.config(text="Database:")

        self._update_security_fields_visibility(db_type)

        # Recreate operation buttons when connection form DB type changes (Connections tab).
        if hasattr(self, "_objects_panel") and self._objects_panel:
            self._objects_panel.recreate_operation_buttons()

        self.update_status(f"Selected {db_type} database")

    def recreate_operation_buttons(self):
        """Recreate Database Objects operation buttons for the current DB type."""
        if hasattr(self, "_objects_panel") and self._objects_panel:
            self._objects_panel.recreate_operation_buttons()
            self.operation_buttons = self._objects_panel.operation_buttons

    def update_status(self, message, status_type="info"):
        """
        Update status bar with color-coded messages
        status_type: 'info' (blue), 'success' (green), 'error' (red), 'warning' (amber)
        """
        self.status_bar.config(text=message)

        # Set background color based on status type
        if status_type == "success":
            self.status_bar.config(bg=ColorTheme.SUCCESS, fg="white")
        elif status_type == "error":
            self.status_bar.config(bg=ColorTheme.ERROR, fg="white")
        elif status_type == "warning":
            self.status_bar.config(bg=ColorTheme.WARNING, fg=ColorTheme.TEXT_PRIMARY)
        else:  # info (default)
            self.status_bar.config(bg=ColorTheme.PRIMARY, fg="white")

        self.root.update_idletasks()

        # Auto-reset to info color after 3 seconds for success/warning messages
        if status_type in ["success", "warning"]:
            self.root.after(
                3000, lambda: self.status_bar.config(bg=ColorTheme.PRIMARY, fg="white")
            )

    def connect_db(self):
        db_type = self.db_type_combo.get()
        if not db_type:
            messagebox.showerror("Error", "Please select a database type!")
            return

        host = self.host_entry.get()
        port = self.port_entry.get()
        service_or_db = self.service_entry.get().strip()
        user = self.user_entry.get()
        password = self.password_entry.get()

        db_name_required = db_type not in DB_NAME_OPTIONAL_TYPES
        required_values = [host, port, user, password]
        if db_name_required:
            required_values.append(service_or_db)
        if not all(required_values):
            messagebox.showerror(
                "Error",
                f"{_db_field_label(db_type)} is required for {db_type}."
                if db_name_required and not service_or_db
                else "All connection fields are required!",
            )
            return

        # Auto-generate unique connection name. Database may be blank for
        # engines that allow it; fall back to host so the label is still
        # informative.
        self.connection_counter += 1
        name_tail = service_or_db or host
        conn_name = f"{db_type}-{name_tail}-{self.connection_counter}"

        # Ensure connection name is unique (shouldn't happen with counter, but safe check)
        while conn_name in self.active_connections:
            self.connection_counter += 1
            conn_name = f"{db_type}-{name_tail}-{self.connection_counter}"

        try:
            port = int(port)
        except ValueError:
            messagebox.showerror("Error", "Port must be a number!")
            return

        self.update_status(f"Connecting to {db_type} database as '{conn_name}'...")
        self.connect_btn.config(state=tk.DISABLED)

        # Prepare connection parameters
        conn_params = {
            "host": host,
            "port": port,
            "username": user,
            "password": password,
        }

        if db_type == "Oracle":
            conn_params["service"] = service_or_db
        elif db_type in ["MySQL", "MariaDB", "PostgreSQL", "SQLServer", "MongoDB", "DocumentDB"]:
            conn_params["database"] = service_or_db
        conn_params.update(self._collect_security_conn_params(db_type))

        # Run connection in thread
        thread = threading.Thread(
            target=self._connect_thread, args=(conn_name, db_type, conn_params)
        )
        thread.daemon = True
        thread.start()

    def _connect_thread(self, conn_name, db_type, conn_params):
        """
        Thread for database connection with timeout handling.

        IMPORTANT: The 30-second timeout applies ONLY to the initial connection attempt
        (establishing the connection to the database server). Once connected, there is
        NO TIMEOUT on SQL statements - schema conversions, data transfers, and all other
        database operations can run indefinitely until completion.
        """
        connection_result = {
            "success": False,
            "conn": None,
            "db_manager": None,
            "error": None,
        }

        def attempt_connection():
            try:
                db_manager = DatabaseManager(db_type)
                conn = db_manager.connect(**conn_params)
                connection_result["success"] = True
                connection_result["conn"] = conn
                connection_result["db_manager"] = db_manager
            except Exception as e:
                connection_result["error"] = str(e)

        try:
            # Mask password in log
            debug_params = {
                k: ("***" if k == "password" else v) for k, v in conn_params.items()
            }
            console_print(
                f"Attempting to connect to {db_type} as '{conn_name}': {debug_params}"
            )

            # Start connection attempt in a separate thread
            conn_thread = threading.Thread(target=attempt_connection, daemon=True)
            conn_thread.start()

            # Wait for connection with timeout
            connection_timeout = config.get_float(
                "database.connection", "connection_timeout", default=30.0
            )
            conn_thread.join(timeout=connection_timeout)

            if conn_thread.is_alive():
                # Connection is still running after timeout
                console_print(
                    f"Connection timeout after {connection_timeout} seconds for {conn_name}"
                )
                self.root.after(
                    0,
                    self._connection_failed,
                    f"Connection timeout after {connection_timeout} seconds.\n\nThe database server '{conn_params['host']}:{conn_params['port']}' is not responding.",
                )
                return

            # Check if connection was successful
            if not connection_result["success"]:
                error_msg = (
                    connection_result["error"]
                    if connection_result["error"]
                    else "Unknown connection error"
                )
                console_print(f"{db_type} connection failed: {error_msg}")
                self.root.after(0, self._connection_failed, error_msg)
                return

            db_manager = connection_result["db_manager"]
            conn = connection_result["conn"]

            if conn:
                console_print(f"{db_type} connection successful: {conn_name}")

                # Verify connection details
                if db_type in ["MySQL", "MariaDB", "PostgreSQL"]:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT DATABASE()"
                        if db_type in ["MySQL", "MariaDB"]
                        else "SELECT current_database()"
                    )
                    current_db = cursor.fetchone()[0]
                    cursor.close()
                    console_print(f"Connected to database: {current_db}")

                is_admin = db_manager.is_admin()
                version = db_manager.get_version()

                self.root.after(
                    0,
                    self._connection_success,
                    conn_name,
                    db_type,
                    db_manager,
                    version,
                    is_admin,
                    conn_params,
                )
            else:
                console_print(f"{db_type} connection returned None")
                self.root.after(
                    0,
                    self._connection_failed,
                    f"Failed to connect to {db_type}. Check logs.",
                )
        except Exception as e:
            console_print(f"Exception during {db_type} connection: {e}")
            import traceback

            traceback.print_exc()
            self.root.after(0, self._connection_failed, str(e))

    def _connection_success(
        self, conn_name, db_type, db_manager, version, is_admin, conn_params
    ):
        # Store connection in active connections
        self.active_connections[conn_name] = db_manager
        self.current_connection_name = conn_name
        self.current_db_type = db_type

        # Update active connections listbox
        self.active_conn_listbox.insert(tk.END, f"{conn_name} ({db_type} v{version})")

        # Re-enable connect button for next connection
        self.connect_btn.config(state=tk.NORMAL)

        # Refresh all connection dropdowns across all tabs
        self.refresh_objects_connections()
        self.refresh_ai_connections()
        self.refresh_conversion_connections()

        # Initialize or update SQL Editor
        if self.sql_editor is None:
            for child in self.sql_editor_tab.winfo_children():
                child.destroy()
            try:
                self.sql_editor = SQLEditorTab(
                    self.sql_editor_tab, lambda: self.active_connections, self.update_status
                )
            except Exception:
                self.sql_editor = None
                raise
            self.tabs_initialized["sql_editor"] = True
        else:
            # Refresh connections in SQL editor
            self.sql_editor.refresh_connections()

        self.update_status(
            f" Connected to {conn_name} successfully! ({len(self.active_connections)} active)",
            "success",
        )
        self.root.after(400, self._refresh_dashboard_if_visible)

    def _connection_failed(self, error_msg):
        self.connect_btn.config(state=tk.NORMAL)
        self.update_status(" Connection failed", "error")
        messagebox.showerror("Connection Error", f"Failed to connect:\n{error_msg}")

    def test_db_connection(self):
        """Test database connection without adding to active connections"""
        db_type = self.db_type_combo.get()
        if not db_type:
            messagebox.showerror("Error", "Please select a database type!")
            return

        host = self.host_entry.get()
        port = self.port_entry.get()
        service_or_db = self.service_entry.get().strip()
        user = self.user_entry.get()
        password = self.password_entry.get()

        db_name_required = db_type not in DB_NAME_OPTIONAL_TYPES
        required_values = [host, port, user, password]
        if db_name_required:
            required_values.append(service_or_db)
        if not all(required_values):
            messagebox.showerror(
                "Error",
                f"{_db_field_label(db_type)} is required for {db_type}."
                if db_name_required and not service_or_db
                else "All connection fields are required!",
            )
            return

        try:
            port = int(port)
        except ValueError:
            messagebox.showerror("Error", "Port must be a number!")
            return

        connection_timeout = config.get_float(
            "database.connection", "connection_timeout", default=30.0
        )
        self.update_status(
            f"Testing {db_type} connection... (timeout: {connection_timeout:.0f}s)"
        )

        # Prepare connection parameters
        conn_params = {
            "host": host,
            "port": port,
            "username": user,
            "password": password,
        }

        if db_type == "Oracle":
            conn_params["service"] = service_or_db
        elif db_type in ["MySQL", "MariaDB", "PostgreSQL", "SQLServer", "MongoDB", "DocumentDB"]:
            conn_params["database"] = service_or_db
        conn_params.update(self._collect_security_conn_params(db_type))

        # Run test in thread
        thread = threading.Thread(
            target=self._test_connection_thread, args=(db_type, conn_params)
        )
        thread.daemon = True
        thread.start()

    def _test_connection_thread(self, db_type, conn_params):
        """Thread for testing database connection"""
        connection_result = {"success": False, "version": None, "error": None}

        def attempt_connection():
            try:
                db_manager = DatabaseManager(db_type)
                conn = db_manager.connect(**conn_params)
                if conn:
                    connection_result["success"] = True
                    connection_result["version"] = db_manager.get_version()
                    # Disconnect immediately after test
                    db_manager.disconnect()
                else:
                    connection_result["error"] = "Connection returned None"
            except Exception as e:
                connection_result["error"] = str(e)

        try:
            # Mask password in log
            debug_params = {
                k: ("***" if k == "password" else v) for k, v in conn_params.items()
            }
            console_print(f"Testing connection to {db_type}: {debug_params}")

            # Start connection attempt in a separate thread with timeout
            conn_thread = threading.Thread(target=attempt_connection, daemon=True)
            conn_thread.start()

            # Wait for connection with timeout
            connection_timeout = config.get_float(
                "database.connection", "connection_timeout", default=30.0
            )
            conn_thread.join(timeout=connection_timeout)

            if conn_thread.is_alive():
                # Connection is still running after timeout
                console_print(
                    f"Connection test timeout after {connection_timeout} seconds"
                )
                self.root.after(
                    0,
                    self._test_connection_result,
                    False,
                    None,
                    f"Connection timeout after {connection_timeout} seconds.\n\nThe database server '{conn_params['host']}:{conn_params['port']}' is not responding.",
                )
                return

            # Check if connection was successful
            if not connection_result["success"]:
                error_msg = (
                    connection_result["error"]
                    if connection_result["error"]
                    else "Unknown connection error"
                )
                console_print(f"{db_type} connection test failed: {error_msg}")
                self.root.after(0, self._test_connection_result, False, None, error_msg)
                return

            version = connection_result["version"]
            console_print(f"{db_type} connection test successful, version: {version}")
            self.root.after(0, self._test_connection_result, True, version, None)

        except Exception as e:
            console_print(f"Exception during {db_type} connection test: {e}")
            import traceback

            traceback.print_exc()
            self.root.after(0, self._test_connection_result, False, None, str(e))

    def _test_connection_result(self, success, version, error_msg):
        """Display test connection result"""
        if success:
            db_type = self.db_type_combo.get()
            host = self.host_entry.get()
            service_or_db = self.service_entry.get().strip()

            version_text = f" (Version: {version})" if version else ""
            db_line = (
                f"\nService/DB: {service_or_db}" if service_or_db else ""
            )
            message = (
                f" Connection test successful!\n\n"
                f"Database: {db_type}\nHost: {host}{db_line}{version_text}"
            )

            messagebox.showinfo("Connection Test Success", message)
            self.update_status(" Connection test successful", "success")
        else:
            messagebox.showerror(
                "Connection Test Failed", f"Failed to connect:\n\n{error_msg}"
            )
            self.update_status(" Connection test failed", "error")

    def on_connection_selected(self, event=None):
        """Handle connection selection from listbox"""
        selection = self.active_conn_listbox.curselection()
        if selection:
            selected_text = self.active_conn_listbox.get(selection[0])
            # Extract connection name (everything before the first ' (' )
            conn_name = selected_text.split(" (")[0]
            self.current_connection_name = conn_name
            self.update_status(f"Selected connection: {conn_name}")

    def _sync_objects_tab_for_connection(self, selected_conn, *, status_prefix="Using"):
        if hasattr(self, "_objects_panel") and self._objects_panel:
            self._objects_panel.sync_for_connection(
                selected_conn, status_prefix=status_prefix
            )
            self.operation_buttons = self._objects_panel.operation_buttons

    def refresh_objects_connections(self):
        if hasattr(self, "_objects_panel") and self._objects_panel:
            self._objects_panel.refresh_connections()
            self.operation_buttons = self._objects_panel.operation_buttons

    def on_objects_connection_changed(self, event):
        if hasattr(self, "_objects_panel") and self._objects_panel:
            self._objects_panel._on_connection_changed(event)

    def import_data_to_table(self):
        """Import data from file and create table in selected database"""
        # Check if connection is active
        if (
            not self.current_connection_name
            or self.current_connection_name not in self.active_connections
        ):
            messagebox.showerror("Error", "No active database connection selected")
            return

        db_manager = self.active_connections[self.current_connection_name]
        conn = db_manager.conn

        if not conn:
            messagebox.showerror("Error", "Database connection not established")
            return

        # Open file dialog to select data file
        filetypes = [
            ("CSV files", "*.csv"),
            ("All files", "*.*"),
        ]
        filename = filedialog.askopenfilename(
            title="Select data file to import", filetypes=filetypes
        )

        if not filename:
            return

        self.update_status("Reading data file...")

        try:
            # Derive table name from filename (remove extension and sanitize)
            base_name = os.path.splitext(os.path.basename(filename))[0]
            # Sanitize table name: remove special chars, replace spaces with underscore
            table_name = re.sub(r"[^a-zA-Z0-9_]", "_", base_name).upper()

            # Ask user to confirm/modify table name
            table_name = self._prompt_table_name(table_name)
            if not table_name:
                return

            # Read file based on extension
            file_ext = os.path.splitext(filename)[1].lower()
            if file_ext == ".csv":
                data, columns = self._read_csv_file(filename)
            else:
                messagebox.showerror(
                    "Error",
                    f"Unsupported file format: {file_ext}. Only CSV is supported.",
                )
                return

            if not data or not columns:
                messagebox.showerror("Error", "No data found in file")
                return

            self.update_status(f"Creating table {table_name} with {len(data)} rows...")

            # Infer column types from data
            column_types = self._infer_column_types(data, columns)

            # Create table
            success = self._create_table_with_data(
                conn, table_name, columns, column_types, data, db_manager.db_type
            )

            if success:
                messagebox.showinfo(
                    "Success",
                    f"Table '{table_name}' created successfully!\n"
                    f"Rows imported: {len(data)}\n"
                    f"Columns: {len(columns)}",
                )
                self.update_status(f"Import complete: {table_name} ({len(data)} rows)")

                # Refresh tables list - get the correct getTables function name for this DB type
                operations = DatabaseRegistry.get_available_operations(
                    db_manager.db_type
                )
                tables_op = next((op for op in operations if op[0] == "Tables"), None)
                if tables_op and hasattr(self, "_objects_panel") and self._objects_panel:
                    self._objects_panel.execute_operation("Tables", tables_op[1])
            else:
                messagebox.showerror("Error", "Failed to import data")
                self.update_status("Import failed")

        except Exception as e:
            messagebox.showerror("Error", f"Import failed:\n{str(e)}")
            self.update_status(f"Import error: {str(e)}")
            import traceback

            traceback.print_exc()

    def _prompt_table_name(self, default_name):
        """Prompt user to confirm or modify table name"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Table Name")
        dialog.geometry("400x150")
        dialog.transient(self.root)
        dialog.grab_set()

        result: list[str | None] = [None]

        ttk.Label(dialog, text="Enter table name:", font=self.ui_font).pack(pady=10)

        name_entry = ttk.Entry(dialog, font=self.ui_font, width=40)
        name_entry.pack(pady=5, padx=20)
        name_entry.insert(0, default_name)
        name_entry.select_range(0, tk.END)
        name_entry.focus()

        def on_ok():
            name = name_entry.get().strip()
            if name:
                # Validate table name
                if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
                    result[0] = name.upper()
                    dialog.destroy()
                else:
                    messagebox.showerror(
                        "Invalid Name",
                        "Table name must start with letter or underscore\n"
                        "and contain only letters, numbers, and underscores",
                    )
            else:
                messagebox.showerror("Error", "Table name cannot be empty")

        def on_cancel():
            dialog.destroy()

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="OK", command=on_ok, style="Success.TButton").pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(btn_frame, text="Cancel", command=on_cancel).pack(
            side=tk.LEFT, padx=5
        )

        name_entry.bind("<Return>", lambda e: on_ok())
        name_entry.bind("<Escape>", lambda e: on_cancel())

        dialog.wait_window()
        return result[0]

    def _read_csv_file(self, filename):
        """Read CSV file and return data and columns"""
        with open(filename, "r", encoding="utf-8-sig") as f:
            # Try to detect delimiter
            sample = f.read(4096)
            f.seek(0)
            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample)
                reader = csv.reader(f, dialect)
            except (csv.Error, Exception):
                # If dialect detection fails, use default
                reader = csv.reader(f)

            # Read all rows
            rows = list(reader)

            if not rows:
                return [], []

            # First row is headers
            columns = []
            seen = set()
            for idx, col in enumerate(rows[0]):
                # Sanitize column name
                col_name = re.sub(r"[^a-zA-Z0-9_]", "_", col.strip()).upper()
                if not col_name or col_name[0].isdigit():
                    col_name = f"COL_{idx+1}"

                # Handle duplicates
                original_name = col_name
                counter = 1
                while col_name in seen:
                    col_name = f"{original_name}_{counter}"
                    counter += 1

                seen.add(col_name)
                columns.append(col_name)

            data = rows[1:]

            return data, columns

    def _infer_column_types(self, data, columns):
        """Infer column types from data"""
        column_types = []

        for col_idx in range(len(columns)):
            # Sample first 100 rows
            sample_values = [
                row[col_idx] if col_idx < len(row) else None
                for row in data[:100]
                if row
            ]

            # Remove None/empty values
            sample_values = [
                v for v in sample_values if v is not None and str(v).strip()
            ]

            if not sample_values:
                column_types.append("VARCHAR(255)")
                continue

            # Check if all values are integers
            all_int = all(self._is_integer(v) for v in sample_values)
            if all_int:
                column_types.append("INTEGER")
                continue

            # Check if all values are numbers
            all_numeric = all(self._is_numeric(v) for v in sample_values)
            if all_numeric:
                column_types.append("NUMERIC(18,4)")
                continue

            # Check if all values are dates
            all_date = all(self._is_date(v) for v in sample_values)
            if all_date:
                column_types.append("DATE")
                continue

            # Find max length for VARCHAR
            max_len = max(len(str(v)) for v in sample_values)
            varchar_len = max(255, min(4000, max_len + 50))  # Add buffer
            column_types.append(f"VARCHAR({varchar_len})")

        return column_types

    def _is_integer(self, value):
        """Check if value is an integer"""
        try:
            int(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_numeric(self, value):
        """Check if value is numeric"""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_date(self, value):
        """Check if value is a date"""
        date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"]
        for fmt in date_formats:
            try:
                datetime.strptime(str(value), fmt)
                return True
            except (ValueError, TypeError):
                continue
        return False

    def _create_table_with_data(
        self, conn, table_name, columns, column_types, data, db_type
    ):
        """Create table and insert data"""
        try:
            # Use buffered cursor for MySQL/MariaDB to avoid unread result errors
            if db_type in ["MySQL", "MariaDB"]:
                cursor = conn.cursor(buffered=True)
            else:
                cursor = conn.cursor()

            # Build CREATE TABLE statement
            col_defs = []
            for col, col_type in zip(columns, column_types):
                col_defs.append(f"{col} {col_type}")

            create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"

            console_print(f"Creating table: {create_sql}")
            cursor.execute(create_sql)

            # Build INSERT statement
            placeholders = []
            if db_type == "Oracle":
                placeholders = [f":{i+1}" for i in range(len(columns))]
            elif db_type in ["MySQL", "MariaDB"]:
                placeholders = ["%s"] * len(columns)
            elif db_type == "PostgreSQL":
                placeholders = ["%s"] * len(columns)
            elif db_type == "SQLite":
                placeholders = ["?"] * len(columns)

            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

            # Insert data in batches
            batch_size = config.get_int(
                "database.performance", "transfer_batch_size", default=1000
            )
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]

                # Pad rows to match column count
                padded_batch = []
                for row in batch:
                    padded_row = list(row) + [None] * (len(columns) - len(row))
                    padded_row = padded_row[: len(columns)]  # Trim if too long
                    padded_batch.append(padded_row)

                if db_type == "Oracle":
                    cursor.executemany(insert_sql, padded_batch)
                else:
                    cursor.executemany(insert_sql, padded_batch)

                console_print(
                    f"Inserted {min(i+batch_size, len(data))}/{len(data)} rows"
                )
                self.update_status(
                    f"Importing... {min(i+batch_size, len(data))}/{len(data)} rows"
                )

            conn.commit()
            cursor.close()

            console_print(
                f"Successfully created table {table_name} with {len(data)} rows"
            )
            return True

        except Exception as e:
            error_msg = f"Error creating table: {e}"
            print(error_msg, file=sys.stderr)
            import traceback

            traceback.print_exc()
            try:
                conn.rollback()
            except Exception as e:
                console_print(f"Rollback failed: {e}")
            return False

    def disconnect_selected_connection(self):
        """Disconnect the selected connection"""
        selection = self.active_conn_listbox.curselection()
        if not selection:
            messagebox.showwarning(
                "Warning", "Please select a connection to disconnect!"
            )
            return

        selected_text = self.active_conn_listbox.get(selection[0])
        conn_name = selected_text.split(" (")[0]

        if messagebox.askyesno("Confirm Disconnect", f"Disconnect from '{conn_name}'?"):
            try:
                db_manager = self.active_connections[conn_name]
                db_manager.disconnect()
                del self.active_connections[conn_name]

                # Invalidate schema cache for this connection
                if hasattr(self, "ai_agent") and self.ai_agent:
                    self.ai_agent.invalidate_cache(conn_name)
                    console_print(f"[Disconnect] Cleared schema cache for {conn_name}")

                # Remove from listbox
                self.active_conn_listbox.delete(selection[0])

                # Update SQL editor
                if self.sql_editor:
                    self.sql_editor.refresh_connections()

                # Update all connection dropdowns across all tabs
                self.refresh_objects_connections()
                self.refresh_ai_connections()
                self.refresh_conversion_connections()

                # Update status
                if len(self.active_connections) == 0:
                    # Disable operation buttons
                    for btn in self.operation_buttons:
                        btn.config(state=tk.DISABLED)

                self.update_status(f" Disconnected from '{conn_name}'", "success")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to disconnect:\n{str(e)}")

    def disconnect_all_connections(self):
        """Disconnect all active connections"""
        if not self.active_connections:
            messagebox.showinfo("Info", "No active connections to disconnect")
            return

        count = len(self.active_connections)
        if messagebox.askyesno(
            "Confirm Disconnect All", f"Disconnect all {count} connection(s)?"
        ):
            try:
                for conn_name, db_manager in list(self.active_connections.items()):
                    db_manager.disconnect()

                self.active_connections.clear()
                self.active_conn_listbox.delete(0, tk.END)
                self.current_connection_name = None

                # Clear all schema caches
                if hasattr(self, "ai_agent") and self.ai_agent:
                    self.ai_agent.invalidate_cache()  # Clear all
                    console_print("[Disconnect All] Cleared all schema caches")

                # Update SQL editor
                if self.sql_editor:
                    self.sql_editor.refresh_connections()

                # Update all connection dropdowns across all tabs
                self.refresh_objects_connections()
                self.refresh_ai_connections()
                self.refresh_conversion_connections()

                # Disable operation buttons
                for btn in self.operation_buttons:
                    btn.config(state=tk.DISABLED)

                self.update_status(
                    f" Disconnected all {count} connection(s)", "success"
                )
            except Exception as e:
                messagebox.showerror("Error", f"Failed to disconnect all:\n{str(e)}")

    def save_connection_dialog(self):
        """Show dialog to save current connection"""
        db_type = self.db_type_combo.get()
        host = self.host_entry.get()
        port = self.port_entry.get()
        service_or_db = self.service_entry.get().strip()
        user = self.user_entry.get()
        password = self.password_entry.get()

        db_name_required = db_type not in DB_NAME_OPTIONAL_TYPES
        required_values = [db_type, host, port, user]
        if db_name_required:
            required_values.append(service_or_db)
        if not all(required_values):
            messagebox.showwarning(
                "Warning",
                f"{_db_field_label(db_type)} is required for {db_type}."
                if db_name_required and not service_or_db
                else "Please fill in all connection fields!",
            )
            return

        # Create dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Save Connection")
        dialog.geometry("400x250")
        dialog.transient(self.root)
        dialog.grab_set()

        # Connection Name
        ttk.Label(dialog, text="Connection Name:").grid(
            row=0, column=0, sticky=tk.W, padx=10, pady=10
        )
        name_entry = ttk.Entry(dialog, width=30)
        name_entry.grid(row=0, column=1, padx=10, pady=10)
        name_entry.focus()

        # Save Password Option
        save_pwd_var = tk.BooleanVar(value=False)
        save_pwd_check = ttk.Checkbutton(
            dialog,
            text="Save Password (Warning: Password will be stored in plain text)",
            variable=save_pwd_var,
        )
        save_pwd_check.grid(
            row=1, column=0, columnspan=2, padx=10, pady=10, sticky=tk.W
        )

        # Info Label
        info_label = ttk.Label(dialog, text="", foreground="blue")
        info_label.grid(row=2, column=0, columnspan=2, padx=10, pady=5)

        def save():
            from common.connection_params import ConnectionParams

            name = name_entry.get().strip()
            if not name:
                messagebox.showwarning("Warning", "Please enter a connection name!")
                return

            security_params = self._collect_security_conn_params(db_type)
            success, message = self.connection_manager.add_connection(
                ConnectionParams.from_mapping({
                    "name": name,
                    "db_type": db_type,
                    "host": host,
                    "port": port,
                    "service_or_db": service_or_db,
                    "username": user,
                    "password": password,
                    "save_password": save_pwd_var.get(),
                    **security_params,
                }),
            )

            if success:
                self.update_status(f" {message}", "success")
                if getattr(self, "_refresh_saved_connections", None):
                    self._refresh_saved_connections()
                dialog.destroy()
            else:
                messagebox.showerror("Error", message)

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=3, column=0, columnspan=2, pady=20)

        ttk.Button(btn_frame, text="Save", command=save, width=15).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy, width=15).pack(
            side=tk.LEFT, padx=5
        )

    def _load_saved_profile_into_form(self, conn_name: str) -> bool:
        """Populate the direct connection form from a saved profile.

        Shared by the Load Saved dialog and the inline Saved connections panel.
        Returns True when the named profile was found and applied.
        """
        conn = self.connection_manager.get_connection(conn_name)
        if not conn:
            return False
        self.db_type_combo.set(conn["db_type"])
        self.on_db_type_changed()
        self.host_entry.delete(0, tk.END)
        self.host_entry.insert(0, conn.get("host", ""))
        self.port_entry.delete(0, tk.END)
        self.port_entry.insert(0, str(conn.get("port", "")))
        self.service_entry.delete(0, tk.END)
        self.service_entry.insert(0, conn.get("service_or_db", ""))
        self.user_entry.delete(0, tk.END)
        self.user_entry.insert(0, conn.get("username", ""))
        self.password_entry.delete(0, tk.END)
        if conn.get("save_password") and conn.get("password"):
            self.password_entry.insert(0, conn["password"])
        self._apply_security_fields_from_profile(conn)
        return True

    def create_saved_connections_frame(self, parent):
        """Inline Saved connections panel (parity with the TUI and Web UIs).

        Sits directly below Active connections, collapsed by default, and lets
        the user Load / Connect / Test / Remove a saved profile without opening
        the separate Load Saved dialog.
        """
        title_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")
        content = make_collapsible_section(
            parent, "Saved connections", title_font,
            expanded=self._conn_section_expanded("saved"),
        )

        ttk.Label(
            content,
            text="Select a row, then Load / Connect / Test / Remove:",
            font=self.ui_font,
        ).pack(anchor=tk.W, padx=10, pady=(5, 0))

        tree_frame = ttk.Frame(content)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        vsb = ttk.Scrollbar(tree_frame, orient="vertical")
        tree = ttk.Treeview(
            tree_frame,
            columns=("DB Type", "Host", "Port", "Database", "Username", "SSH"),
            yscrollcommand=vsb.set,
            height=6,
        )
        vsb.config(command=tree.yview)
        tree.heading("#0", text="Name")
        tree.heading("DB Type", text="DB Type")
        tree.heading("Host", text="Host")
        tree.heading("Port", text="Port")
        tree.heading("Database", text="Database/Service")
        tree.heading("Username", text="Username")
        tree.heading("SSH", text="SSH")
        tree.column("#0", width=150)
        tree.column("DB Type", width=90)
        tree.column("Host", width=130)
        tree.column("Port", width=60)
        tree.column("Database", width=130)
        tree.column("Username", width=110)
        tree.column("SSH", width=50)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(fill=tk.BOTH, expand=True)
        self.saved_conn_tree = tree

        def refresh():
            for item in tree.get_children():
                tree.delete(item)
            for conn in self.connection_manager.get_all_connections():
                tree.insert(
                    "", tk.END, text=conn.get("name", ""),
                    values=(
                        conn.get("db_type", ""),
                        conn.get("host", ""),
                        conn.get("port", ""),
                        conn.get("service_or_db", ""),
                        conn.get("username", ""),
                        "yes" if conn.get("ssh_tunnel") else "",
                    ),
                )

        def _selected_name():
            selection = tree.selection()
            if not selection:
                messagebox.showwarning(
                    "Saved connections", "Please select a connection!"
                )
                return None
            return tree.item(selection[0])["text"]

        def _is_remote(name: str) -> bool:
            conn = self.connection_manager.get_connection(name)
            return bool(conn and conn.get("ssh_tunnel"))

        def load_selected():
            name = _selected_name()
            if name and self._load_saved_profile_into_form(name):
                self.update_status(f" Loaded '{name}' into the form.", "success")

        def connect_selected():
            name = _selected_name()
            if not name:
                return
            if _is_remote(name):
                messagebox.showinfo(
                    "Saved connections",
                    f"'{name}' is a remote (SSH) connection — use the remote "
                    "database section to connect.",
                )
                return
            if self._load_saved_profile_into_form(name):
                self.connect_db()

        def test_selected():
            name = _selected_name()
            if not name:
                return
            if _is_remote(name):
                messagebox.showinfo(
                    "Saved connections",
                    f"'{name}' is a remote (SSH) connection — use the remote "
                    "database section to test.",
                )
                return
            if self._load_saved_profile_into_form(name):
                self.test_db_connection()

        def remove_selected():
            name = _selected_name()
            if not name:
                return
            if messagebox.askyesno("Confirm Delete", f"Delete connection '{name}'?"):
                success, message = self.connection_manager.delete_connection(name)
                if success:
                    refresh()
                    self.update_status(f" {message}", "success")
                else:
                    messagebox.showerror("Error", message)

        btn_frame = ttk.Frame(content)
        btn_frame.pack(anchor=tk.W, padx=10, pady=(0, 5))
        ttk.Button(btn_frame, text="Refresh", command=refresh,
                   style="Primary.TButton", width=12).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Load", command=load_selected,
                   width=12).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Connect", command=connect_selected,
                   style="Success.TButton", width=12).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Test", command=test_selected,
                   width=12).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Remove", command=remove_selected,
                   style="Error.TButton", width=12).pack(side=tk.LEFT, padx=2)

        self._refresh_saved_connections = refresh
        refresh()

    def show_saved_connections(self):
        """Show dialog with saved connections"""
        from common.cloud.connection_manager import CloudConnectionManager
        from common.cloud.profiles import PURPOSE_CONNECTIONS, TARGET_CLOUD_DB
        from common.cloud.sql_bridge import sync_all_cloud_dbs_to_saved_connections

        dialog = tk.Toplevel(self.root)
        dialog.title("Saved Connections")
        dialog.geometry("800x500")
        dialog.transient(self.root)

        # Create treeview for connections
        tree_frame = ttk.Frame(dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient="vertical")
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal")

        tree = ttk.Treeview(
            tree_frame,
            columns=("DB Type", "Host", "Port", "Database", "Username"),
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set,
        )
        vsb.config(command=tree.yview)
        hsb.config(command=tree.xview)

        # Configure columns
        tree.heading("#0", text="Name")
        tree.heading("DB Type", text="DB Type")
        tree.heading("Host", text="Host")
        tree.heading("Port", text="Port")
        tree.heading("Database", text="Database/Service")
        tree.heading("Username", text="Username")

        tree.column("#0", width=150)
        tree.column("DB Type", width=100)
        tree.column("Host", width=150)
        tree.column("Port", width=80)
        tree.column("Database", width=150)
        tree.column("Username", width=120)

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        tree.pack(fill=tk.BOTH, expand=True)

        empty_label = ttk.Label(tree_frame, text="No saved connections", font=("Arial", 12))

        def populate_tree(connections):
            for item in tree.get_children():
                tree.delete(item)
            if not connections:
                empty_label.pack(expand=True)
                return
            empty_label.pack_forget()
            for conn in connections:
                tree.insert(
                    "",
                    tk.END,
                    text=conn["name"],
                    values=(
                        conn["db_type"],
                        conn["host"],
                        conn["port"],
                        conn["service_or_db"],
                        conn["username"],
                    ),
                )

        populate_tree(self.connection_manager.get_all_connections())

        def refresh_tree_if_open():
            if not dialog.winfo_exists():
                return
            populate_tree(self.connection_manager.get_all_connections())

        def background_sync_cloud_profiles():
            try:
                cloud_mgr = CloudConnectionManager()
                profiles = {
                    name: profile
                    for name, profile in cloud_mgr.load_cloud_databases().items()
                    if profile.get("purpose") == PURPOSE_CONNECTIONS
                    and profile.get("target_kind", TARGET_CLOUD_DB) == TARGET_CLOUD_DB
                }
                if profiles and sync_all_cloud_dbs_to_saved_connections(
                    profiles,
                    self.connection_manager,
                    resolve_remote=False,
                ):
                    self.root.after(0, refresh_tree_if_open)
            except Exception:
                pass

        threading.Thread(target=background_sync_cloud_profiles, daemon=True).start()

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        def load_selected():
            selection = tree.selection()
            if not selection:
                messagebox.showwarning("Warning", "Please select a connection!")
                return

            item = tree.item(selection[0])
            conn_name = item["text"]

            if self._load_saved_profile_into_form(conn_name):
                dialog.destroy()
                self.update_status(
                    f" Loaded connection '{conn_name}' - Click Connect to establish connection",
                    "success",
                )

        def delete_selected():
            selection = tree.selection()
            if not selection:
                messagebox.showwarning("Warning", "Please select a connection!")
                return

            item = tree.item(selection[0])
            conn_name = item["text"]

            if messagebox.askyesno(
                "Confirm Delete", f"Delete connection '{conn_name}'?"
            ):
                success, message = self.connection_manager.delete_connection(conn_name)
                if success:
                    tree.delete(selection[0])
                    self.update_status(f" {message}", "success")
                else:
                    messagebox.showerror("Error", message)

        ttk.Button(
            btn_frame, text="Load Connection", command=load_selected, width=18
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            btn_frame, text="Delete Connection", command=delete_selected, width=18
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Close", command=dialog.destroy, width=18).pack(
            side=tk.RIGHT, padx=5
        )

    # ========== Schema Conversion Methods ==========

    def refresh_conversion_connections(self):
        """Refresh connection dropdowns in conversion tab"""
        if self.schema_converter_ui:
            self.schema_converter_ui.refresh_connections()

    # ========== AI Query Assistant Methods ==========

    def refresh_ai_connections(self):
        """Refresh connection dropdown in AI tab"""
        # Delegate to AI Query UI module if it exists
        if self.ai_query_ui:
            self.ai_query_ui.refresh_connections()

    def clear_ai_schema_cache(self):
        """Clear schema cache for AI Query Assistant"""
        # Delegate to AI Query UI module if it exists
        if self.ai_query_ui:
            self.ai_query_ui.clear_ai_schema_cache()

    def show_cache_info(self):
        """Display cache information dialog"""
        # Delegate to AI Query UI module if it exists
        if self.ai_query_ui:
            self.ai_query_ui.show_cache_info()

    def show_schema_sent_to_ai(self):
        """Display the schema that was sent to AI for the last query"""
        # Delegate to AI Query UI module if it exists
        if self.ai_query_ui:
            self.ai_query_ui.show_schema_sent_to_ai()

    def extract_sql_from_markdown(self, text):
        """
        Extract SQL code from markdown-formatted text.
        If text has no markdown (no ``` fences), returns as-is.
        If text has markdown, removes fences and wraps non-code text in /* */.
        Also cleans up if ALL lines are wrapped in /* */.
        """
        import re

        if not text or not text.strip():
            return ""

        # Check if ALL lines are wrapped in /* ... */ (AI sometimes generates this)
        lines = text.strip().split("\n")
        all_commented = all(
            line.strip().startswith("/*") and line.strip().endswith("*/")
            for line in lines
            if line.strip()
        )

        if all_commented:
            # Extract SQL from within comments
            result = []
            for line in lines:
                line = line.strip()
                if line.startswith("/*") and line.endswith("*/"):
                    # Remove /* and */ and keep the SQL
                    sql_part = line[2:-2].strip()
                    if sql_part:
                        result.append(sql_part)
            return "\n".join(result)

        # Check if text contains any markdown code fences
        has_markdown = "```" in text

        if not has_markdown:
            # No markdown detected - treat entire text as SQL, return as-is
            return text.strip()

        # Process markdown: extract code from fences, comment out other text
        result = []
        in_code_block = False
        lines = text.split("\n")

        for line in lines:
            line_stripped = line.strip()

            # Check if this is a code fence (``` or ```sql or ```SQL)
            if line_stripped.startswith("```"):
                if in_code_block:
                    # End of code block
                    in_code_block = False
                else:
                    # Start of code block
                    in_code_block = True
                continue  # Don't include the fence markers

            if in_code_block:
                # Inside code block - ALL lines are SQL, keep exactly as-is
                result.append(line_stripped)
            else:
                # Outside code block - this is explanatory text, wrap in comment
                if line_stripped:
                    result.append(f"/* {line_stripped} */")
                else:
                    # Keep empty lines
                    if result:
                        result.append("")

        # Join all lines
        final_sql = "\n".join(result)

        # Clean up any "/* sql */" or "/* */" artifacts
        final_sql = re.sub(r"/\*\s*sql\s*\*/", "", final_sql, flags=re.IGNORECASE)
        final_sql = re.sub(r"/\*\s*\*/", "", final_sql)

        # Clean up multiple empty lines
        final_sql = re.sub(r"\n\n\n+", "\n\n", final_sql)

        return final_sql.strip()


def main(feature_module: str | None = None):
    root = tk.Tk()

    # Check if any database modules are available
    if not DatabaseConfig.get_db_types():
        messagebox.showerror(
            "No Database Modules",
            "No database modules available!\n\nPlease ensure either conOracle.py or conMysql.py is available.",
        )
        return

    app = UnifiedDBManagerUI(root, feature_module=feature_module)
    try:
        root.mainloop()
    except KeyboardInterrupt:
        # Belt-and-braces: a SIGINT delivered between event ticks lands here.
        app._graceful_shutdown("KeyboardInterrupt")


if __name__ == "__main__":
    main()
