#-------------------------------------------------------------------------------
#description: UI manager for the tool
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

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import sys
import threading
import time
import csv
import subprocess
import os
import tempfile
import re
from datetime import datetime
from connection_manager import ConnectionManager
from monitor_connection_manager import MonitorConnectionManager
from schema_converter import SchemaConverter, DataConverter, ConversionValidator
from ai_query_agent import AIQueryAgent
from database_registry import DatabaseRegistry
from metrics_visualizer import MetricsVisualizer
from server_monitor import ServerMonitorUI
from ai_query import AIQueryUI
from ui import ColorTheme, default_ui_font, default_ui_mono, bind_canvas_mousewheel, create_horizontal_scrollable, make_collapsible_section
from config_loader import config, properties, get_window_size, console_print


# Import database modules
try:
    import conOracle
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False
    console_print("Oracle module not available")

try:
    import conMysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False
    console_print("MySQL module not available")

try:
    import conMariadb
    MARIADB_AVAILABLE = True
except ImportError:
    MARIADB_AVAILABLE = False
    console_print("MariaDB module not available")

try:
    import conPostgres
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    console_print("PostgreSQL module not available")


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


class DatabaseManager:
    """Unified database manager for all database types using registry"""

    def __init__(self, db_type):
        self.db_type = db_type
        self.conn = None
        self.config = DatabaseRegistry.get_config(db_type)
        if not self.config:
            raise ValueError(f"Unsupported database type: {db_type}")

    def connect(self, **kwargs):
        """Connect to database using registry"""
        connect_func = DatabaseRegistry.get_operation(self.db_type, 'connect')
        if not connect_func:
            raise NotImplementedError(f"Connect operation not available for {self.db_type}")

        # Map generic parameter names to database-specific ones
        # Oracle uses 'service' or 'db', others use 'database'
        if self.db_type == "Oracle":
            self.conn = connect_func(
                db=kwargs.get('service'),
                host=kwargs.get('host'),
                user=kwargs.get('username'),
                password=kwargs.get('password'),
                port=int(kwargs.get('port', self.config['default_port']))
            )
        else:
            self.conn = connect_func(
                database=kwargs.get('database'),
                host=kwargs.get('host'),
                user=kwargs.get('username'),
                password=kwargs.get('password'),
                port=int(kwargs.get('port', self.config['default_port']))
            )
        return self.conn

    def disconnect(self):
        """Disconnect from database using registry"""
        if self.conn:
            disconnect_func = DatabaseRegistry.get_operation(self.db_type, 'disconnect')
            if disconnect_func:
                disconnect_func(self.conn)
            self.conn = None

    def get_version(self):
        """Get database version using registry"""
        if not self.conn:
            return None
        return DatabaseRegistry.execute_operation(self.db_type, 'getVersion', self.conn)

    def is_admin(self):
        """Check if user has admin privileges using registry"""
        if not self.conn:
            return False
        return DatabaseRegistry.execute_operation(self.db_type, 'isRoot', self.conn) or False

    def execute_operation(self, operation_func_name):
        """Execute a database operation by function name"""
        if not self.conn:
            return []

        # Get the module for this database type
        module = self.config.get('module')
        if not module:
            return []

        # Get the function from the module
        func = getattr(module, operation_func_name, None)
        if func:
            return func(self.conn)
        return []

    def execute_query(self, sql):
        """Execute a SQL query and return results

        Note: Autocommit is controlled by the connection's autocommit property
        """
        if not self.conn:
            return None, "Not connected to database"

        cursor = None
        try:
            sql = sql.strip()

            # Split SQL by semicolons to handle multiple statements
            statements = self._split_sql_statements(sql)

            # If only one statement, execute directly
            if len(statements) == 1:
                return self._execute_single_statement(statements[0])

            # Multiple statements - execute each one
            results_list = []
            start_time = time.time()

            for i, stmt in enumerate(statements, 1):
                stmt = stmt.strip()
                if not stmt:
                    continue

                result, error = self._execute_single_statement(stmt)
                if error:
                    # Show more of the failing statement for better debugging
                    stmt_preview = stmt[:300] + ('...' if len(stmt) > 300 else '')
                    return None, f"Error in statement {i} of {len(statements)}:\n{error}\n\nFull statement:\n{stmt_preview}"

                # Add statement number to result
                if result:
                    result['statement_num'] = i
                    sql_preview_limit = properties.get_int('ui.limits', 'sql_preview_limit', default=100)
                    result['statement'] = stmt[:sql_preview_limit] + ('...' if len(stmt) > sql_preview_limit else '')
                results_list.append(result)

            execution_time = time.time() - start_time

            # Return ALL results with special flag for multiple results
            return {
                'multiple_results': True,
                'results': results_list,
                'time': execution_time,
                'count': len(statements)
            }, None

        except Exception as e:
            if cursor:
                cursor.close()
            return None, str(e)

    def _split_sql_statements(self, sql):
        """Split SQL by semicolons, handling strings and comments"""
        statements = []
        current = []
        in_string = False
        string_char = None
        in_multiline_comment = False
        in_single_line_comment = False

        i = 0
        while i < len(sql):
            char = sql[i]

            # Check for multi-line comment start /*
            if not in_string and not in_single_line_comment and i < len(sql) - 1:
                if char == '/' and sql[i+1] == '*':
                    in_multiline_comment = True
                    current.append(char)
                    current.append(sql[i+1])
                    i += 2
                    continue

            # Check for multi-line comment end */
            if in_multiline_comment and i < len(sql) - 1:
                if char == '*' and sql[i+1] == '/':
                    in_multiline_comment = False
                    current.append(char)
                    current.append(sql[i+1])
                    i += 2
                    continue

            # Check for single-line comment --
            if not in_string and not in_multiline_comment and i < len(sql) - 1:
                if char == '-' and sql[i+1] == '-':
                    in_single_line_comment = True

            # End single-line comment at newline
            if in_single_line_comment and char == '\n':
                in_single_line_comment = False

            # Handle string literals
            if not in_multiline_comment and not in_single_line_comment:
                if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None

            # Handle semicolon (statement separator) - only if not in string or comment
            if (char == ';' and not in_string and
                not in_multiline_comment and not in_single_line_comment):
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
                i += 1
                continue

            current.append(char)
            i += 1

        # Add last statement if any
        stmt = ''.join(current).strip()
        if stmt:
            statements.append(stmt)

        return statements

    def _execute_single_statement(self, sql):
        """Execute a single SQL statement

        Note: Autocommit is controlled by the connection's autocommit property
        """
        cursor = None
        original_sql = sql  # Keep original for error messages
        try:
            # Clean up the SQL statement
            sql = sql.strip()

            # Check if statement is only comments (no actual SQL)
            import re
            # Remove all comments temporarily to check for actual SQL
            sql_without_comments = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
            sql_without_comments = re.sub(r'--[^\n]*', '', sql_without_comments)
            sql_without_comments = sql_without_comments.strip()

            # If nothing left after removing comments, skip this statement
            if not sql_without_comments:
                return {'message': "Comment-only statement skipped.", 'time': 0}, None

            # Oracle-specific cleaning (less aggressive - only remove trailing semicolons)
            if self.db_type == "Oracle":
                # Oracle doesn't accept trailing semicolons in cursor.execute()
                sql = sql.rstrip(';').strip()

            # Skip empty statements
            if not sql:
                return {'message': "Empty statement skipped.", 'time': 0}, None

            # Create cursor with buffering for MySQL/MariaDB
            if self.db_type in ["MySQL", "MariaDB"]:
                cursor = self.conn.cursor(buffered=True)
            else:
                cursor = self.conn.cursor()

            start_time = time.time()
            cursor.execute(sql)

            # Check if query returns results
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                execution_time = time.time() - start_time
                cursor.close()
                return {'columns': columns, 'rows': rows, 'time': execution_time, 'rowcount': len(rows)}, None
            else:
                # DML/DDL statement
                # Commit is handled by connection's autocommit property
                execution_time = time.time() - start_time
                rowcount = cursor.rowcount
                cursor.close()

                # Check connection's autocommit status for message
                is_autocommit = getattr(self.conn, 'autocommit', False)
                commit_status = " (auto-committed)" if is_autocommit else " (use Commit button)"
                return {'message': f"Query executed successfully. {rowcount} row(s) affected{commit_status}.", 'time': execution_time}, None

        except Exception as e:
            if cursor:
                cursor.close()
            # Show more SQL in error for better debugging
            sql_error_limit = properties.get_int('ui.limits', 'sql_error_limit', default=500)
            sql_preview = sql[:sql_error_limit] + ('...' if len(sql) > sql_error_limit else '')
            error_msg = f"{str(e)}\n\nSQL attempted:\n{sql_preview}"
            return None, error_msg

    def commit(self):
        """Commit transaction"""
        if self.conn:
            self.conn.commit()
            return True
        return False

    def rollback(self):
        """Rollback transaction"""
        if self.conn:
            self.conn.rollback()
            return True
        return False

    def cancel_query(self):
        """
        Cancel the currently executing query

        Different databases have different cancellation mechanisms:
        - Oracle: Use cursor.cancel() or connection.cancel()
        - MySQL/MariaDB: Kill query via KILL QUERY command
        - PostgreSQL: Use pg_cancel_backend()
        - SQLite: No native cancellation (query runs in-process)
        """
        if not self.conn:
            return False

        try:
            if self.db_type == "Oracle":
                # Oracle supports connection.cancel() to cancel running operations
                if hasattr(self.conn, 'cancel'):
                    self.conn.cancel()
                    console_print("Oracle query cancellation requested")
                    return True

            elif self.db_type in ["MySQL", "MariaDB"]:
                # MySQL/MariaDB: Get connection ID and kill the query
                try:
                    cursor = self.conn.cursor(buffered=True)
                    cursor.execute("SELECT CONNECTION_ID()")
                    connection_id = cursor.fetchone()[0]
                    cursor.close()

                    # Create a new connection to kill the query
                    kill_cursor = self.conn.cursor(buffered=True)
                    kill_cursor.execute(f"KILL QUERY {connection_id}")
                    kill_cursor.close()
                    console_print(f"{self.db_type} query cancellation requested (killed query on connection {connection_id})")
                    return True
                except Exception as e:
                    print(f"Error cancelling {self.db_type} query: {e}", file=sys.stderr)
                    return False

            elif self.db_type == "PostgreSQL":
                # PostgreSQL: Use pg_cancel_backend with current backend PID
                try:
                    cursor = self.conn.cursor()
                    cursor.execute("SELECT pg_backend_pid()")
                    backend_pid = cursor.fetchone()[0]
                    cursor.close()

                    # Cancel the backend
                    cancel_cursor = self.conn.cursor()
                    cancel_cursor.execute(f"SELECT pg_cancel_backend({backend_pid})")
                    cancel_cursor.close()
                    console_print(f"PostgreSQL query cancellation requested (cancelled backend PID {backend_pid})")
                    return True
                except Exception as e:
                    print(f"Error cancelling PostgreSQL query: {e}", file=sys.stderr)
                    return False

            elif self.db_type == "SQLite":
                # SQLite doesn't support query cancellation as queries run in-process
                # The only way is to use interrupt() if available
                if hasattr(self.conn, 'interrupt'):
                    self.conn.interrupt()
                    console_print("SQLite query interruption requested")
                    return True
                else:
                    console_print("SQLite doesn't support query cancellation")
                    return False

            else:
                console_print(f"Query cancellation not implemented for {self.db_type}")
                return False

        except Exception as e:
            print(f"Error cancelling query: {e}", file=sys.stderr)
            return False


class SQLEditorTab:
    """SQL Editor workspace"""

    def __init__(self, parent, get_connections_callback, status_callback, font_ui=None, font_mono=None):
        self.parent = parent
        self.get_connections_callback = get_connections_callback  # Function to get active connections
        self.status_callback = status_callback
        self.query_history = []
        self.selected_connection_name = None
        self._font_ui = font_ui or default_ui_font()
        self._font_mono = font_mono or default_ui_mono()
        self._title_font = (self._font_ui[0], self._font_ui[1] + 2, "bold")
        self.autocommit_var = None  # Will be set in create_editor_ui()

        # Query execution state tracking
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False

        self.create_editor_ui()

    def create_editor_ui(self):
        # Main container with vertical split: drag sash between editor and results
        main_frame = ttk.Frame(self.parent)
        main_frame.pack(fill=tk.BOTH, expand=True)

        paned = ttk.PanedWindow(main_frame, orient=tk.VERTICAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=6, pady=4)
        self._editor_results_paned = paned

        # SQL Editor Frame
        editor_frame = ttk.LabelFrame(paned, text="SQL Query Editor", padding="8")
        paned.add(editor_frame, weight=2)

        tool_host = make_collapsible_section(
            editor_frame, "Connection & actions (collapse to enlarge editor)", self._title_font, expanded=True
        )

        # Create scrollable wrapper for toolbars using optimized helper
        toolbar_container = create_horizontal_scrollable(tool_host)

        # Connection Selector Toolbar
        connection_toolbar = ttk.Frame(toolbar_container)
        connection_toolbar.pack(fill=tk.X, pady=(0, 5))

        ttk.Label(connection_toolbar, text="Active connection:").pack(side=tk.LEFT, padx=5)
        self.connection_combo = ttk.Combobox(connection_toolbar, width=40, state="readonly")
        self.connection_combo.pack(side=tk.LEFT, padx=5)
        self.connection_combo.bind('<<ComboboxSelected>>', self.on_connection_changed)

        ttk.Button(connection_toolbar, text="Refresh connections", command=self.refresh_connections).pack(side=tk.LEFT, padx=5)

        # Autocommit toggle
        self.autocommit_var = tk.BooleanVar(value=config.get_bool('database.connection', 'default_autocommit', default=False))  # take from config file Default: OFF if not set
        autocommit_cb = ttk.Checkbutton(connection_toolbar, text="Auto-commit", variable=self.autocommit_var,
                                        command=self.toggle_autocommit)
        autocommit_cb.pack(side=tk.LEFT, padx=10)

        # Toolbar
        toolbar = ttk.Frame(toolbar_container)
        toolbar.pack(fill=tk.X, pady=(0, 5))

        # Button container frame - use grid to maintain fixed positions
        self.execute_buttons_container = ttk.Frame(toolbar)
        self.execute_buttons_container.pack(side=tk.LEFT, padx=0)

        self.execute_cursor_btn = ttk.Button(self.execute_buttons_container, text="Execute at cursor (F5)", command=self.execute_at_cursor)
        self.execute_cursor_btn.grid(row=0, column=0, padx=2)
        self.execute_selected_btn = ttk.Button(self.execute_buttons_container, text="Execute selected", command=self.execute_selected)
        self.execute_selected_btn.grid(row=0, column=1, padx=2)
        self.execute_all_btn = ttk.Button(self.execute_buttons_container, text="Execute all", command=self.execute_all)
        self.execute_all_btn.grid(row=0, column=2, padx=2)

        self.stop_query_btn = ttk.Button(self.execute_buttons_container, text="⏹ Stop Query", command=self.stop_query)
        self.stop_query_btn.grid(row=0, column=0, columnspan=3, padx=2, sticky='ew')
        self.stop_query_btn.grid_remove()  # Initially hidden (keeps position reserved)

        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)

        ttk.Button(toolbar, text="Clear editor", command=self.clear_editor).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="Load query", command=self.load_query).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="Save query", command=self.save_query).pack(side=tk.LEFT, padx=2)

        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)

        ttk.Button(toolbar, text="Commit", command=self.commit_transaction).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="Rollback", command=self.rollback_transaction).pack(side=tk.LEFT, padx=2)

        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)

        ttk.Button(toolbar, text="Query history", command=self.show_history).pack(side=tk.LEFT, padx=2)

        # SQL Text Editor (height is a hint; pane expansion fills available space)
        self.sql_text = scrolledtext.ScrolledText(editor_frame, wrap=tk.WORD, height=8, font=self._font_mono)
        self.sql_text.pack(fill=tk.BOTH, expand=True)
        self.sql_text.bind("<F5>", lambda e: self.execute_at_cursor())
        self.sql_text.bind("<Control-Return>", lambda e: self.execute_at_cursor())

        # Add some example queries as comment
        example = "-- SQL Query Editor\n-- Press F5 or Ctrl+Enter to execute query at cursor\n-- Separate multiple queries with semicolons\n-- Use 'Execute All' to run all queries\n\n"
        self.sql_text.insert(1.0, example)

        # Results Frame
        results_frame = ttk.LabelFrame(paned, text="Query Results", padding="8")
        paned.add(results_frame, weight=1)

        # Results toolbar
        results_toolbar = ttk.Frame(results_frame)
        results_toolbar.pack(fill=tk.X, pady=(0, 5))

        self.result_info_label = ttk.Label(results_toolbar, text="Ready")
        self.result_info_label.pack(side=tk.LEFT, padx=5)

        ttk.Button(results_toolbar, text="Clear All Results", command=self.clear_results).pack(side=tk.RIGHT, padx=2)
        ttk.Button(results_toolbar, text="Export Data", style="Success.TButton",
                  command=self.export_results).pack(side=tk.RIGHT, padx=2)

        # Results display with Notebook for multiple result tabs
        self.results_notebook = ttk.Notebook(results_frame)
        self.results_notebook.pack(fill=tk.BOTH, expand=True)

        self.result_tabs = []  # Store result tab references

        # Initialize connections list
        self.refresh_connections()

        def _position_sql_sash(attempt=0):
            try:
                p = self._editor_results_paned
                p.update_idletasks()
                h = p.winfo_height()
                if h <= 100 and attempt < 8:
                    self.parent.after(50, lambda: _position_sql_sash(attempt + 1))
                    return
                if h > 100:
                    p.sashpos(0, max(160, int(h * 0.5)))
            except tk.TclError:
                pass

        self.parent.after_idle(_position_sql_sash)

    def refresh_connections(self):
        """Refresh the list of active connections"""
        connections = self.get_connections_callback()

        # Update combo box
        connection_names = list(connections.keys())
        self.connection_combo['values'] = connection_names

        # If current selection is not valid, select first if available
        if self.selected_connection_name not in connections and connection_names:
            self.selected_connection_name = connection_names[0]
            self.connection_combo.set(self.selected_connection_name)
        elif self.selected_connection_name:
            self.connection_combo.set(self.selected_connection_name)
        else:
            # No connections available - clear selection
            self.connection_combo.set('')
            self.selected_connection_name = None

        self.status_callback(f"Found {len(connections)} active connection(s)")

    def on_connection_changed(self, event=None):
        """Handle connection selection change"""
        selected = self.connection_combo.get()
        if selected:
            self.selected_connection_name = selected
            self.status_callback(f"Using connection: {selected}")

            # Apply current autocommit setting to the new connection
            if self.autocommit_var:
                self._apply_autocommit_setting()

    def get_current_db_manager(self):
        """Get the currently selected database manager"""
        if not self.selected_connection_name:
            return None

        connections = self.get_connections_callback()
        return connections.get(self.selected_connection_name)

    def update_connection(self, db_manager):
        """Update database manager when reconnecting (legacy method)"""
        # This method is kept for backward compatibility but not used with multiple connections
        self.refresh_connections()

    def get_query_text(self):
        """Get the SQL query text from editor"""
        return self.sql_text.get(1.0, tk.END).strip()

    def get_selected_text(self):
        """Get selected text from editor"""
        try:
            return self.sql_text.get(tk.SEL_FIRST, tk.SEL_LAST).strip()
        except tk.TclError:
            return None

    def parse_queries(self, text):
        """Parse multiple SQL queries separated by semicolons"""
        queries = []
        current_query_lines = []
        in_string = False
        string_char = None

        lines = text.split('\n')
        for line in lines:
            stripped = line.strip()

            # Skip empty lines and comments (don't add to query)
            if not stripped or stripped.startswith('--'):
                continue

            # Check if line contains semicolon (outside strings)
            has_semicolon = False
            line_without_semicolon = line

            # Track strings to avoid splitting on semicolons inside strings
            i = 0
            while i < len(line):
                char = line[i]

                if char in ('"', "'") and not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char and in_string:
                    in_string = False
                    string_char = None
                elif char == ';' and not in_string:
                    # Found semicolon - this ends the query
                    has_semicolon = True
                    line_without_semicolon = line[:i].strip()
                    break

                i += 1

            # Add the line (or part before semicolon) to current query
            if line_without_semicolon:
                current_query_lines.append(line_without_semicolon)

            # If we found a semicolon, save the query and start a new one
            if has_semicolon:
                query = '\n'.join(current_query_lines).strip()
                if query:
                    queries.append(query)
                current_query_lines = []
                in_string = False
                string_char = None

        # Add last query if exists (no trailing semicolon)
        if current_query_lines:
            query = '\n'.join(current_query_lines).strip()
            if query:
                queries.append(query)

        return queries

    def get_query_at_cursor(self):
        """Get the SQL query at cursor position"""
        # Get cursor position
        cursor_pos = self.sql_text.index(tk.INSERT)

        # Get all text
        text = self.get_query_text()
        if not text:
            return None

        # Simple approach: find the query block around the cursor
        # Look backwards and forwards for semicolons
        text_before_cursor = self.sql_text.get("1.0", cursor_pos)
        text_after_cursor = self.sql_text.get(cursor_pos, tk.END)

        # Check if cursor is right after a semicolon (with only whitespace between)
        # If so, we want the query BEFORE the semicolon, not after
        last_semicolon_before = text_before_cursor.rfind(';')

        if last_semicolon_before != -1:
            # Get text after the last semicolon
            text_after_last_semi = text_before_cursor[last_semicolon_before + 1:]

            # If text after semicolon is only whitespace, cursor is right after the semicolon
            # In this case, use the query BEFORE the semicolon
            if text_after_last_semi.strip() == '':
                # Find the semicolon before the last one
                prev_text = text_before_cursor[:last_semicolon_before]
                prev_semicolon = prev_text.rfind(';')

                if prev_semicolon != -1:
                    query_start_text = prev_text[prev_semicolon + 1:]
                else:
                    query_start_text = prev_text

                query_end_text = ''  # Query ends at the semicolon
            else:
                # Cursor is in the middle of a query
                query_start_text = text_after_last_semi

                # Find the end of current query (before next semicolon or end of text)
                next_semicolon_after = text_after_cursor.find(';')
                if next_semicolon_after != -1:
                    query_end_text = text_after_cursor[:next_semicolon_after]
                else:
                    query_end_text = text_after_cursor
        else:
            # No semicolon before cursor - start from beginning
            query_start_text = text_before_cursor

            # Find the end of current query (before next semicolon or end of text)
            next_semicolon_after = text_after_cursor.find(';')
            if next_semicolon_after != -1:
                query_end_text = text_after_cursor[:next_semicolon_after]
            else:
                query_end_text = text_after_cursor

        # Combine to get the full query at cursor
        full_query = (query_start_text + query_end_text).strip()

        # Remove comment lines
        lines = full_query.split('\n')
        clean_lines = [line for line in lines if line.strip() and not line.strip().startswith('--')]
        clean_query = '\n'.join(clean_lines).strip()

        return clean_query if clean_query else None

    def execute_at_cursor(self):
        """Execute query at cursor position"""
        sql = self.get_query_at_cursor()
        if not sql:
            messagebox.showwarning("Warning", "No query found at cursor position!")
            return

        self._execute_sql(sql, "result")

    def execute_all(self):
        """Execute all queries in the editor"""
        text = self.get_query_text()
        if not text:
            messagebox.showwarning("Warning", "No queries to execute!")
            return

        queries = self.parse_queries(text)
        if not queries:
            messagebox.showwarning("Warning", "No valid queries found!")
            return

        if messagebox.askyesno("Execute All", f"Execute {len(queries)} queries?"):
            self._execute_multiple_queries(queries)

    def execute_query(self):
        """Execute the SQL query"""
        sql = self.get_query_text()
        if not sql or sql.startswith("--"):
            messagebox.showwarning("Warning", "Please enter a SQL query!")
            return

        self._execute_sql(sql)

    def execute_selected(self):
        """Execute selected SQL text"""
        sql = self.get_selected_text()
        if not sql:
            messagebox.showwarning("Warning", "Please select a SQL query to execute!")
            return

        self._execute_sql(sql, "Selected Query")

    def _execute_sql(self, sql, query_name="Query"):
        """Execute SQL in background thread"""
        db_manager = self.get_current_db_manager()
        if not db_manager or not db_manager.conn:
            messagebox.showerror("Error", "Not connected to any database! Please select a connection.")
            return

        # Update execution state
        self.query_running = True
        self.current_db_manager = db_manager
        self.cancellation_requested = False

        # Update UI to show stop button and hide execute buttons
        self._show_stop_button()

        self.status_callback("Executing query...")
        self.result_info_label.config(text="Executing query...")

        # Add to history
        self.query_history.append({
            'sql': sql,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

        thread = threading.Thread(target=self._execute_query_thread, args=(sql, query_name))
        thread.daemon = True
        self.current_execution_thread = thread
        thread.start()

    def _execute_multiple_queries(self, queries):
        """Execute multiple queries sequentially"""
        db_manager = self.get_current_db_manager()
        if not db_manager or not db_manager.conn:
            messagebox.showerror("Error", "Not connected to any database! Please select a connection.")
            return

        # Update execution state
        self.query_running = True
        self.current_db_manager = db_manager
        self.cancellation_requested = False

        # Update UI to show stop button and hide execute buttons
        self._show_stop_button()

        self.status_callback(f"Executing {len(queries)} queries...")
        self.result_info_label.config(text=f"Executing {len(queries)} queries...")

        thread = threading.Thread(target=self._execute_multiple_thread, args=(queries,))
        thread.daemon = True
        self.current_execution_thread = thread
        thread.start()

    def _execute_query_thread(self, sql, query_name="Query"):
        """Execute query in background thread"""
        try:
            # Check if cancellation was requested before starting
            if self.cancellation_requested:
                self.parent.after(0, self._handle_query_cancelled)
                return

            db_manager = self.get_current_db_manager()
            if not db_manager:
                self.parent.after(0, self._show_error, "No active connection", query_name)
                return

            result, error = db_manager.execute_query(sql)

            # Check if query was cancelled during execution
            if self.cancellation_requested:
                self.parent.after(0, self._handle_query_cancelled)
                return

            if error:
                self.parent.after(0, self._show_error, error, query_name)
            else:
                self.parent.after(0, self._show_results, result, query_name, sql)
        except Exception as e:
            self.parent.after(0, self._show_error, str(e), query_name)
        finally:
            # Always restore UI state when query completes
            self.parent.after(0, self._restore_query_ui_state)

    def _execute_multiple_thread(self, queries):
        """Execute multiple queries in background thread"""
        try:
            # Check if cancellation was requested before starting
            if self.cancellation_requested:
                self.parent.after(0, self._handle_query_cancelled)
                return

            db_manager = self.get_current_db_manager()
            if not db_manager:
                self.parent.after(0, messagebox.showerror, "Error", "No active connection")
                return

            results = []
            for idx, sql in enumerate(queries, 1):
                # Check for cancellation before each query
                if self.cancellation_requested:
                    self.parent.after(0, self._handle_query_cancelled)
                    return

                query_name = f"Query {idx}"
                try:
                    result, error = db_manager.execute_query(sql)
                    if error:
                        results.append((query_name, None, error, sql))
                    else:
                        results.append((query_name, result, None, sql))
                except Exception as e:
                    results.append((query_name, None, str(e), sql))

            # Check for cancellation after all queries
            if self.cancellation_requested:
                self.parent.after(0, self._handle_query_cancelled)
                return

            self.parent.after(0, self._show_multiple_results, results)
        finally:
            # Always restore UI state when query completes
            self.parent.after(0, self._restore_query_ui_state)

    def stop_query(self):
        """Stop the currently executing query"""
        if not self.query_running:
            return

        # Set cancellation flag
        self.cancellation_requested = True

        # Try to cancel at database level
        if self.current_db_manager:
            try:
                self.current_db_manager.cancel_query()
                self.status_callback("Query cancellation requested...")
                self.result_info_label.config(text="Query cancellation requested...")
            except Exception as e:
                print(f"Error cancelling query: {e}", file=sys.stderr)
                # Even if cancellation fails, the flag is set and thread will check it

    def _show_stop_button(self):
        """Show stop button and hide execute buttons"""
        self.execute_cursor_btn.grid_remove()
        self.execute_selected_btn.grid_remove()
        self.execute_all_btn.grid_remove()
        self.stop_query_btn.grid()  # Show in same position

    def _restore_query_ui_state(self):
        """Restore UI state after query execution completes"""
        self.query_running = False
        self.current_execution_thread = None
        self.current_db_manager = None
        self.cancellation_requested = False

        # Hide stop button, show execute buttons
        self.stop_query_btn.grid_remove()
        self.execute_cursor_btn.grid()
        self.execute_selected_btn.grid()
        self.execute_all_btn.grid()

    def _handle_query_cancelled(self):
        """Handle UI updates when query is cancelled"""
        self.result_info_label.config(text="⏹ Query execution cancelled by user")
        self.status_callback("Query cancelled")
        messagebox.showinfo("Query Cancelled", "Query execution was cancelled by user")

    def _create_result_tab(self, query_name):
        """Create a new result tab"""
        tab_frame = ttk.Frame(self.results_notebook)

        # Create treeview for this tab
        tree_container = ttk.Frame(tab_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)

        vsb = ttk.Scrollbar(tree_container, orient="vertical")
        hsb = ttk.Scrollbar(tree_container, orient="horizontal")

        tree = ttk.Treeview(tree_container, yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.config(command=tree.yview)
        hsb.config(command=tree.xview)

        # Configure style for better column separation
        style = ttk.Style()
        style.configure("Treeview",
                       rowheight=25,
                       borderwidth=1,
                       relief=tk.SOLID)
        style.configure("Treeview.Heading",
                       font=self._font_ui,
                       relief=tk.RAISED,
                       borderwidth=1)
        style.layout("Treeview", [('Treeview.treearea', {'sticky': 'nswe'})])  # Enable borders

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        tree.pack(fill=tk.BOTH, expand=True)

        # Add context menu and sorting for the tree
        self._setup_tree_features(tree, tab_frame)

        self.results_notebook.add(tab_frame, text=query_name)
        self.results_notebook.select(tab_frame)

        return tree, tab_frame

    def _show_results(self, result, query_name="Query", sql=""):
        """Display query results in a new tab"""
        if 'message' in result:
            # DML/DDL statement result
            self.result_info_label.config(text=f"{result['message']} (Time: {result['time']:.3f}s)")
            self.status_callback(result['message'])
            messagebox.showinfo("Success", result['message'])
        else:
            # SELECT query result
            columns = result['columns']
            rows = result['rows']
            rowcount = result['rowcount']
            exec_time = result['time']

            # Create new tab
            tree, tab_frame = self._create_result_tab(f"{query_name} ({rowcount} rows)")

            # Store result for export
            tab_frame.result_data = result

            # Store original rows for filtering (decoded rows will be stored after insertion)
            tab_frame.original_rows = []

            # Configure columns with row number column
            # Add '#' as the first column for row numbers
            all_columns = ['#'] + columns
            tree['columns'] = all_columns
            tree['show'] = 'tree headings'  # Show tree column (for row numbers) and headings

            # Configure row number column (first column)
            tree.heading('#0', text='')  # Hide the default tree column
            tree.column('#0', width=0, stretch=False)  # Make it invisible

            tree.heading('#', text='Row #')
            tree.column('#', width=60, minwidth=60, anchor=tk.CENTER, stretch=False)

            # Configure data columns with borders
            for col in columns:
                tree.heading(col, text=str(col))
                tree.column(col, width=150, minwidth=80, anchor=tk.W)

            # Add tag for alternating row colors and borders
            tree.tag_configure('oddrow', background='#FFFFFF')
            tree.tag_configure('evenrow', background='#F5F5F5')

            # Add rows with row numbers
            for idx, row in enumerate(rows, start=1):
                decoded_row = [str(idx)]  # Start with row number
                for val in row:
                    if isinstance(val, (bytearray, bytes)):
                        if val:
                            # Try multiple encodings for Oracle compatibility
                            for encoding in ['utf-8', 'windows-1252', 'iso-8859-1', 'latin1']:
                                try:
                                    decoded_row.append(val.decode(encoding))
                                    break
                                except (UnicodeDecodeError, AttributeError):
                                    continue
                            else:
                                # If all encodings fail, use replace strategy
                                decoded_row.append(val.decode('utf-8', errors='replace'))
                        else:
                            decoded_row.append('')
                    else:
                        decoded_row.append(str(val) if val is not None else '')

                # Store decoded row for filtering
                tab_frame.original_rows.append(decoded_row)

                # Alternate row colors for better readability
                tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
                tree.insert('', tk.END, values=decoded_row, tags=(tag,))

            info_text = f"Rows: {rowcount} | Time: {exec_time:.3f}s"
            self.result_info_label.config(text=info_text)
            self.status_callback(f"Query executed successfully - {rowcount} rows returned")

    def _show_multiple_results(self, results):
        """Display results from multiple queries"""
        success_count = 0
        error_count = 0

        for query_name, result, error, sql in results:
            if error:
                # Show error in a message tab
                error_count += 1
                tab_frame = ttk.Frame(self.results_notebook)
                error_text = scrolledtext.ScrolledText(tab_frame, wrap=tk.WORD, height=10)
                error_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
                error_text.insert(1.0, f"SQL:\n{sql}\n\nError:\n{error}")
                error_text.config(state=tk.DISABLED)
                self.results_notebook.add(tab_frame, text=f"{query_name} ❌")
            else:
                # Show result
                success_count += 1
                self._show_results(result, query_name, sql)

        summary = f"Executed {len(results)} queries: {success_count} succeeded, {error_count} failed"
        self.result_info_label.config(text=summary)
        self.status_callback(summary)

    def _show_error(self, error, query_name="Query"):
        """Display error message"""
        self.result_info_label.config(text="Query failed")
        self.status_callback("Query execution failed")
        messagebox.showerror("Query Error", f"Error executing {query_name}:\n\n{error}")

    def clear_editor(self):
        """Clear the SQL editor"""
        self.sql_text.delete(1.0, tk.END)

    def clear_results(self):
        """Clear all result tabs"""
        for tab in self.results_notebook.tabs():
            self.results_notebook.forget(tab)
        self.result_tabs = []
        self.result_info_label.config(text="Ready")
        self.status_callback("Results cleared")

    def _setup_tree_features(self, tree, tab_frame):
        """Setup sorting, filtering, and copy features for result tree"""
        # Store reference to tree
        tab_frame.tree = tree

        # Track which cell (column) was clicked for copy operations
        tree.clicked_column = None
        tree.clicked_column_index = None

        # Context menu for tree (right-click on data)
        tree_menu = tk.Menu(tree, tearoff=0)
        tree_menu.add_command(label="📋 Copy Cell", command=lambda: self._copy_tree_cell(tree))
        tree_menu.add_command(label="📋 Copy Row", command=lambda: self._copy_tree_row(tree))
        tree_menu.add_command(label="📋 Copy Column", command=lambda: self._copy_tree_column(tree))
        tree_menu.add_command(label="📋 Copy All Data", command=lambda: self._copy_tree_all(tree))
        tree_menu.add_separator()
        tree_menu.add_command(label="⬆️ Sort Ascending", command=lambda: self._sort_tree_column(tree, True))
        tree_menu.add_command(label="⬇️ Sort Descending", command=lambda: self._sort_tree_column(tree, False))
        tree_menu.add_separator()
        tree_menu.add_command(label="🔍 Filter Column...", command=lambda: self._filter_tree_column(tree, tab_frame))
        tree_menu.add_command(label="✖️ Clear Filter", command=lambda: self._clear_tree_filter(tree, tab_frame))

        def show_tree_menu(event):
            try:
                # Track which cell was right-clicked
                region = tree.identify_region(event.x, event.y)
                if region == "cell":
                    column = tree.identify_column(event.x)
                    col_index = int(column.replace('#', '')) - 1
                    tree.clicked_column = column
                    tree.clicked_column_index = col_index
                tree_menu.post(event.x_root, event.y_root)
            finally:
                tree_menu.grab_release()

        tree.bind("<Button-2>", show_tree_menu)  # macOS
        tree.bind("<Button-3>", show_tree_menu)  # Windows/Linux

        # Tab context menu for closing tabs
        self.results_notebook.bind("<Button-2>", self._show_tab_menu)  # macOS
        self.results_notebook.bind("<Button-3>", self._show_tab_menu)  # Windows/Linux

        # Bind combined handler for both cell clicks and header clicks
        tree.bind("<ButtonRelease-1>", lambda e: self._handle_tree_click(e, tree))

    def _handle_tree_click(self, event, tree):
        """Handle click on tree - either cell or column header"""
        region = tree.identify_region(event.x, event.y)

        if region == "cell":
            # Track which cell was clicked for copy operations
            column = tree.identify_column(event.x)
            col_index = int(column.replace('#', '')) - 1
            tree.clicked_column = column
            tree.clicked_column_index = col_index

        elif region == "heading":
            # Sort by clicked column header
            column = tree.identify_column(event.x)
            col_index = int(column.replace('#', '')) - 1
            if col_index >= 0:
                columns = tree['columns']
                if col_index < len(columns):
                    col_name = columns[col_index]
                    # Toggle sort direction
                    if hasattr(tree, '_sort_column') and tree._sort_column == col_name:
                        tree._sort_ascending = not getattr(tree, '_sort_ascending', True)
                    else:
                        tree._sort_ascending = True
                    tree._sort_column = col_name
                    self._sort_tree_column(tree, tree._sort_ascending, col_name)

    def _copy_tree_cell(self, tree):
        """Copy selected cell value"""
        selection = tree.selection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select a row first")
            return

        item = tree.item(selection[0])
        values = item['values']

        # Use the tracked clicked column if available
        if hasattr(tree, 'clicked_column_index') and tree.clicked_column_index is not None:
            col_index = tree.clicked_column_index
            if col_index >= 0 and col_index < len(values):
                cell_value = str(values[col_index])
                self.parent.clipboard_clear()
                self.parent.clipboard_append(cell_value)
                columns = tree['columns']
                col_name = columns[col_index] if col_index < len(columns) else f"Column {col_index}"
                self.status_callback(f"Copied cell from '{col_name}': {cell_value[:50]}{'...' if len(cell_value) > 50 else ''}")
            else:
                messagebox.showwarning("Invalid Column", "Could not identify cell to copy")
        else:
            # Fallback: copy first data column (skip row number column)
            if len(values) > 1:
                self.parent.clipboard_clear()
                self.parent.clipboard_append(str(values[1]))
                self.status_callback("Cell value copied (first column)")

    def _copy_tree_row(self, tree):
        """Copy selected row as tab-separated values"""
        selection = tree.selection()
        if not selection:
            return
        item = tree.item(selection[0])
        values = item['values']
        row_text = '\t'.join(str(v) for v in values)
        self.parent.clipboard_clear()
        self.parent.clipboard_append(row_text)
        self.status_callback("Row copied")

    def _copy_tree_column(self, tree):
        """Copy entire column"""
        columns = tree['columns']
        if not columns:
            return

        # Use the tracked clicked column if available
        if hasattr(tree, 'clicked_column_index') and tree.clicked_column_index is not None:
            col_index = tree.clicked_column_index
        else:
            # Fallback: copy first data column (skip row number column at index 0)
            col_index = 1 if len(columns) > 1 else 0

        if col_index >= len(columns):
            messagebox.showwarning("Invalid Column", "Could not identify column to copy")
            return

        all_data = []
        # Add header
        all_data.append(columns[col_index])
        # Add all values
        for item_id in tree.get_children():
            item = tree.item(item_id)
            values = item['values']
            if col_index < len(values):
                all_data.append(str(values[col_index]))

        column_text = '\n'.join(all_data)
        self.parent.clipboard_clear()
        self.parent.clipboard_append(column_text)
        self.status_callback(f"Column '{columns[col_index]}' copied ({len(all_data)-1} rows)")

    def _copy_tree_all(self, tree):
        """Copy all data as TSV"""
        columns = tree['columns']
        if not columns:
            return

        all_data = []
        # Add header
        all_data.append('\t'.join(columns))
        # Add all rows
        for item_id in tree.get_children():
            item = tree.item(item_id)
            values = item['values']
            all_data.append('\t'.join(str(v) for v in values))

        data_text = '\n'.join(all_data)
        self.parent.clipboard_clear()
        self.parent.clipboard_append(data_text)
        self.status_callback(f"Copied {len(all_data)-1} rows")

    def _sort_tree_column(self, tree, ascending=True, column=None):
        """Sort tree by column"""
        if column is None:
            columns = tree['columns']
            if not columns:
                return
            column = columns[0]  # Default to first column

        # Get all items
        items = [(tree.set(item, column), item) for item in tree.get_children('')]

        # Sort items
        try:
            # Try numeric sort first
            items.sort(key=lambda x: float(x[0]) if x[0] and x[0] != 'NULL' else float('inf'),
                      reverse=not ascending)
        except (ValueError, TypeError):
            # Fall back to string sort
            items.sort(key=lambda x: str(x[0]).lower(), reverse=not ascending)

        # Rearrange items in sorted order
        for index, (val, item) in enumerate(items):
            tree.move(item, '', index)

        self.status_callback(f"Sorted by '{column}' ({'ascending' if ascending else 'descending'})")

    def _filter_tree_column(self, tree, tab_frame):
        """Filter tree by column value"""
        columns = tree['columns']
        if not columns:
            return

        # Create filter dialog
        dialog = tk.Toplevel(self.parent)
        dialog.title("Filter Column")
        dialog.geometry("400x200")
        dialog.transient(self.parent)

        ttk.Label(dialog, text="Column:", font=self._font_ui).pack(pady=(10, 5))
        col_combo = ttk.Combobox(dialog, values=columns, state="readonly", width=30)
        col_combo.pack(pady=5)
        if columns:
            col_combo.current(0)

        ttk.Label(dialog, text="Filter (contains):", font=self._font_ui).pack(pady=(10, 5))
        filter_entry = ttk.Entry(dialog, width=35)
        filter_entry.pack(pady=5)
        filter_entry.focus()

        def apply_filter():
            column = col_combo.get()
            filter_text = filter_entry.get().strip().lower()

            if not filter_text:
                dialog.destroy()
                return

            # Use original_rows stored during result display
            if not hasattr(tab_frame, 'original_rows') or not tab_frame.original_rows:
                messagebox.showwarning("Warning", "No data available to filter")
                dialog.destroy()
                return

            # Clear tree
            for item in tree.get_children():
                tree.delete(item)

            # Re-add filtered items (remember: first column '#' is row number, so add 1 to col_index)
            filtered_count = 0
            col_index = columns.index(column) + 1  # +1 because first column is row number
            for values in tab_frame.original_rows:
                if col_index < len(values):
                    cell_value = str(values[col_index]).lower()
                    if filter_text in cell_value:
                        tag = 'evenrow' if filtered_count % 2 == 0 else 'oddrow'
                        tree.insert('', 'end', values=values, tags=(tag,))
                        filtered_count += 1

            self.status_callback(f"Filtered '{column}': showing {filtered_count} of {len(tab_frame.original_rows)} rows")
            dialog.destroy()

        button_frame = ttk.Frame(dialog)
        button_frame.pack(pady=15)
        ttk.Button(button_frame, text="Apply", command=apply_filter).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT, padx=5)

        filter_entry.bind('<Return>', lambda e: apply_filter())

        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")

    def _clear_tree_filter(self, tree, tab_frame):
        """Clear filter and restore original data"""
        if not hasattr(tab_frame, 'original_rows') or not tab_frame.original_rows:
            self.status_callback("No active filter")
            return

        # Clear tree
        for item in tree.get_children():
            tree.delete(item)

        # Restore original data with alternating row colors
        for idx, values in enumerate(tab_frame.original_rows, 1):
            tag = 'evenrow' if idx % 2 == 0 else 'oddrow'
            tree.insert('', 'end', values=values, tags=(tag,))

        self.status_callback(f"Filter cleared - showing all {len(tab_frame.original_rows)} rows")

    def _show_tab_menu(self, event):
        """Show context menu on tab right-click"""
        try:
            # Identify which tab was clicked
            clicked_tab = self.results_notebook.tk.call(self.results_notebook._w, "identify", "tab", event.x, event.y)
            if clicked_tab != '':
                self.results_notebook.select(clicked_tab)

                # Create tab menu
                tab_menu = tk.Menu(self.results_notebook, tearoff=0)
                tab_menu.add_command(label="✖️ Close This Tab", command=self._close_current_tab)
                tab_menu.add_command(label="✖️ Close All Tabs", command=self.clear_results)
                tab_menu.add_separator()
                tab_menu.add_command(label="✖️ Close Other Tabs", command=self._close_other_tabs)

                tab_menu.post(event.x_root, event.y_root)
        except tk.TclError:
            pass

    def _close_current_tab(self):
        """Close currently selected tab"""
        current_tab = self.results_notebook.select()
        if current_tab:
            self.results_notebook.forget(current_tab)
            if current_tab in self.result_tabs:
                self.result_tabs.remove(current_tab)
            if not self.results_notebook.tabs():
                self.result_info_label.config(text="Ready")
            self.status_callback("Tab closed")

    def _close_other_tabs(self):
        """Close all tabs except current"""
        current_tab = self.results_notebook.select()
        if not current_tab:
            return
        tabs_to_close = [tab for tab in self.results_notebook.tabs() if tab != current_tab]
        for tab in tabs_to_close:
            self.results_notebook.forget(tab)
            if tab in self.result_tabs:
                self.result_tabs.remove(tab)
        self.status_callback(f"Closed {len(tabs_to_close)} other tab(s)")

    def load_query(self):
        """Load query from file"""
        filename = filedialog.askopenfilename(
            title="Load SQL Query",
            filetypes=[("SQL Files", "*.sql"), ("Text Files", "*.txt"), ("All Files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'r') as f:
                    content = f.read()
                    self.sql_text.delete(1.0, tk.END)
                    self.sql_text.insert(1.0, content)
                self.status_callback(f"Loaded query from {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load file:\n{str(e)}")

    def save_query(self):
        """Save query to file"""
        sql = self.get_query_text()
        if not sql:
            messagebox.showwarning("Warning", "No query to save!")
            return

        filename = filedialog.asksaveasfilename(
            title="Save SQL Query",
            defaultextension=".sql",
            filetypes=[("SQL Files", "*.sql"), ("Text Files", "*.txt"), ("All Files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'w') as f:
                    f.write(sql)
                self.status_callback(f"Saved query to {filename}")
                messagebox.showinfo("Success", "Query saved successfully!")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save file:\n{str(e)}")

    def commit_transaction(self):
        """Commit current transaction"""
        db_manager = self.get_current_db_manager()
        if not db_manager or not db_manager.conn:
            messagebox.showerror("Error", "Not connected to database!")
            return

        if db_manager.commit():
            self.status_callback("Transaction committed")
            messagebox.showinfo("Success", "Transaction committed successfully!")
        else:
            messagebox.showerror("Error", "Failed to commit transaction")

    def rollback_transaction(self):
        """Rollback current transaction"""
        db_manager = self.get_current_db_manager()
        if not db_manager or not db_manager.conn:
            messagebox.showerror("Error", "Not connected to database!")
            return

        if db_manager.rollback():
            self.status_callback("Transaction rolled back")
            messagebox.showinfo("Success", "Transaction rolled back successfully!")
        else:
            messagebox.showerror("Error", "Failed to rollback transaction")

    def _apply_autocommit_setting(self):
        """Apply current autocommit setting to the active connection"""
        is_enabled = self.autocommit_var.get()

        # Get current database connection
        db_manager = self.get_current_db_manager()
        if db_manager and db_manager.conn:
            try:
                # Set autocommit on the connection object
                if db_manager.db_type in ["MySQL", "MariaDB"]:
                    db_manager.conn.autocommit = is_enabled
                elif db_manager.db_type == "PostgreSQL":
                    db_manager.conn.autocommit = is_enabled
                elif db_manager.db_type == "Oracle":
                    db_manager.conn.autocommit = is_enabled
                elif db_manager.db_type == "SQLite":
                    # SQLite uses isolation_level - None means autocommit
                    db_manager.conn.isolation_level = None if is_enabled else "DEFERRED"

                return True
            except Exception as e:
                self.status_callback(f"⚠️ Warning: Could not set autocommit on connection: {str(e)}")
                return False
        return False

    def toggle_autocommit(self):
        """Toggle autocommit mode"""
        is_enabled = self.autocommit_var.get()
        status = "ON" if is_enabled else "OFF"

        # Apply to current connection
        success = self._apply_autocommit_setting()

        if success:
            self.status_callback(f"✓ Auto-commit mode: {status}")

            if is_enabled:
                self.status_callback("⚠️ Auto-commit ON: Changes will be committed automatically after each statement")
            else:
                self.status_callback("ℹ️ Auto-commit OFF: Use Commit/Rollback buttons to finalize changes")
        else:
            self.status_callback(f"Auto-commit mode set to: {status} (will apply when connected)")

            if is_enabled:
                self.status_callback("⚠️ Auto-commit ON: Changes will be committed automatically")
            else:
                self.status_callback("ℹ️ Auto-commit OFF: Use Commit/Rollback buttons")

    def show_history(self):
        """Show query history"""
        if not self.query_history:
            messagebox.showinfo("Query History", "No queries executed yet!")
            return

        history_window = tk.Toplevel(self.parent)
        history_window.title("Query History")
        width, height = get_window_size('history')
        history_window.geometry(f"{width}x{height}")

        # History list
        list_frame = ttk.Frame(history_window)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        history_listbox = tk.Listbox(list_frame, yscrollcommand=scrollbar.set, font=self._font_mono)
        history_listbox.pack(fill=tk.BOTH, expand=True)
        scrollbar.config(command=history_listbox.yview)

        # Populate history
        for idx, item in enumerate(reversed(self.query_history)):
            display_sql = item['sql'].replace('\n', ' ')[:100]
            history_listbox.insert(tk.END, f"[{item['timestamp']}] {display_sql}...")

        # Buttons
        btn_frame = ttk.Frame(history_window)
        btn_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

        def load_selected():
            selection = history_listbox.curselection()
            if selection:
                idx = len(self.query_history) - 1 - selection[0]
                sql = self.query_history[idx]['sql']
                self.sql_text.delete(1.0, tk.END)
                self.sql_text.insert(1.0, sql)
                history_window.destroy()

        ttk.Button(btn_frame, text="Load Selected", command=load_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Close", command=history_window.destroy).pack(side=tk.RIGHT, padx=5)

    def export_results(self):
        """Export results from current tab to CSV or Excel"""
        # Get currently selected tab
        current_tab = self.results_notebook.select()
        if not current_tab:
            messagebox.showwarning("Warning", "No result tab selected!")
            return

        # Get the tab frame
        tab_frame = self.results_notebook.nametowidget(current_tab)

        # Check if tab has result data
        if not hasattr(tab_frame, 'result_data'):
            messagebox.showwarning("Warning", "Current tab has no exportable results!")
            return

        result_data = tab_frame.result_data

        filename = filedialog.asksaveasfilename(
            title="Export Results",
            defaultextension=".csv",
            filetypes=[
                ("CSV Files", "*.csv"),
                ("Excel Files", "*.xlsx"),
                ("All Files", "*.*")
            ]
        )

        if filename:
            try:
                file_ext = os.path.splitext(filename)[1].lower()

                if file_ext == '.xlsx':
                    self._export_to_excel(filename, result_data)
                else:
                    self._export_to_csv(filename, result_data)

                self.status_callback(f"Exported {result_data['rowcount']} rows to {filename}")
                messagebox.showinfo("Success", f"Exported {result_data['rowcount']} rows!")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export:\n{str(e)}")
                import traceback
                traceback.print_exc()

    def _export_to_csv(self, filename, result_data):
        """Export result data to CSV file"""
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write headers
            writer.writerow(result_data['columns'])
            # Write rows
            for row in result_data['rows']:
                decoded_row = []
                for val in row:
                    if isinstance(val, (bytearray, bytes)):
                        if val:
                            # Try multiple encodings for Oracle compatibility
                            for encoding in ['utf-8', 'windows-1252', 'iso-8859-1', 'latin1']:
                                try:
                                    decoded_row.append(val.decode(encoding))
                                    break
                                except (UnicodeDecodeError, AttributeError):
                                    continue
                            else:
                                # If all encodings fail, use replace strategy
                                decoded_row.append(val.decode('utf-8', errors='replace'))
                        else:
                            decoded_row.append('')
                    else:
                        decoded_row.append(str(val) if val is not None else '')
                writer.writerow(decoded_row)

    def _export_to_excel(self, filename, result_data):
        """Export result data to Excel file"""
        try:
            import pandas as pd

            # Prepare data with decoded values
            data_rows = []
            for row in result_data['rows']:
                decoded_row = []
                for val in row:
                    if isinstance(val, (bytearray, bytes)):
                        if val:
                            # Try multiple encodings for Oracle compatibility
                            for encoding in ['utf-8', 'windows-1252', 'iso-8859-1', 'latin1']:
                                try:
                                    decoded_row.append(val.decode(encoding))
                                    break
                                except (UnicodeDecodeError, AttributeError):
                                    continue
                            else:
                                # If all encodings fail, use replace strategy
                                decoded_row.append(val.decode('utf-8', errors='replace'))
                        else:
                            decoded_row.append('')
                    else:
                        decoded_row.append(val)
                data_rows.append(decoded_row)

            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=result_data['columns'])

            # Export to Excel
            df.to_excel(filename, index=False, engine='openpyxl')

        except ImportError:
            messagebox.showerror("Error",
                "pandas and openpyxl libraries required for Excel export.\n"
                "Install with: pip install pandas openpyxl\n\n"
                "Falling back to CSV export...")
            # Fallback to CSV
            csv_filename = os.path.splitext(filename)[0] + '.csv'
            self._export_to_csv(csv_filename, result_data)
            raise Exception("Excel export not available, exported to CSV instead")


class UnifiedDBManagerUI:
    def __init__(self, root):
        self.root = root

        # Window configuration from config files
        app_name = config.get('project', 'app_name', 'Database Manager - Multi-DB Tool')
        width, height = get_window_size('main')
        min_width = properties.get_int('ui.window', 'main_window_min_width', default=860)
        min_height = properties.get_int('ui.window', 'main_window_min_height', default=520)

        self.root.title(app_name)
        self.root.geometry(f"{width}x{height}")
        self.root.minsize(min_width, min_height)
        self.root.configure(bg=ColorTheme.BG_MAIN)

        self.ui_font = default_ui_font()
        self.ui_font_mono = default_ui_mono()
        self._setup_readable_ttk()
        self._create_menubar()

        # Support multiple connections
        self.active_connections = {}  # {connection_name: db_manager}
        self.current_connection_name = None
        self.connection_counter = 0

        self.current_db_type = None
        self.operation_buttons = []
        self.sql_editor = None
        self.connection_manager = ConnectionManager()
        self.monitor_connection_manager = MonitorConnectionManager()
        self.ai_agent = AIQueryAgent()

        # Module instances (lazy init)
        self.server_monitor_ui = None
        self.ai_query_ui = None
        self.server_monitor_ui = None

        # Thread safety for database operations
        self.db_query_lock = threading.Lock()

        # Conversion operation control
        self.conversion_stop_event = threading.Event()
        self.conversion_running = False

        # Status Bar - create and pack FIRST to claim bottom space
        self.create_status_bar()

        # Main workspace (menu bar is attached to root above this)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0, 5))

        # Welcome Tab (First tab - documentation and help)
        self.welcome_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.welcome_tab, text="📖 Welcome")

        # Connections Tab (NEW - first tab for managing database connections)
        self.connections_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.connections_tab, text="Connections")

        # Database Objects Tab
        self.objects_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.objects_tab, text="Database Objects")

        # SQL Editor Tab
        self.sql_editor_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.sql_editor_tab, text="SQL Editor")

        # Schema Conversion Tab
        self.conversion_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.conversion_tab, text="Schema Conversion")

        # AI Query Tab
        self.ai_query_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.ai_query_tab, text="AI Query Assistant")

        # Server Monitor Tab
        self.monitor_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.monitor_tab, text="Server Monitor")

        # Clear Cache "Tab" (acts as button)
        self.clear_cache_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.clear_cache_tab, text="🗑️ Clear Cache")

        # Track which tabs have been initialized
        self.tabs_initialized = {
            'welcome': False,
            'connections': False,
            'objects': False,
            'sql_editor': False,
            'conversion': False,
            'ai_query': False,
            'monitor': False
        }

        # Track previous tab for Clear Cache button behavior
        self.previous_tab_index = 0

        # Bind tab change event for lazy loading
        self.notebook.bind('<<NotebookTabChanged>>', self.on_tab_changed)

        # Defer welcome tab creation to after window is shown (improves startup time)
        self.root.after(100, self._create_welcome_tab_deferred)

    def _create_welcome_tab_deferred(self):
        """Create welcome tab after main window is shown"""
        if not self.tabs_initialized['welcome']:
            console_print("[Startup] Creating Welcome tab (deferred)...")
            self.create_welcome_tab_ui()
            self.tabs_initialized['welcome'] = True
            console_print("[Startup] Welcome tab ready")

    def on_tab_changed(self, event=None):
        """Handle tab change - lazy load tab UIs"""
        try:
            current_tab = self.notebook.index(self.notebook.select())
        except (tk.TclError, AttributeError):
            return

        # Tab 7: Clear Cache (acts as button, not a real tab)
        if current_tab == 7:
            # Run clear cache function
            self.clear_all_caches()
            # Switch back to previous tab
            self.notebook.select(self.previous_tab_index)
            return

        # Update previous tab index for normal tabs
        self.previous_tab_index = current_tab

        # Tab 0: Welcome (already created)

        # Tab 1: Connections
        if current_tab == 1 and not self.tabs_initialized['connections']:
            console_print("[TabSwitch] Creating Connections tab...")
            self.update_status("Loading Connections tab...")
            self.create_connections_tab_ui()
            self.tabs_initialized['connections'] = True
            console_print("[TabSwitch] Connections tab created successfully")
            self.update_status("Connections tab ready")

        # Tab 2: Database Objects
        elif current_tab == 2 and not self.tabs_initialized['objects']:
            console_print("[TabSwitch] Creating Database Objects tab...")
            self.update_status("Loading Database Objects tab...")
            self.create_objects_tab_ui()
            self.tabs_initialized['objects'] = True
            console_print("[TabSwitch] Database Objects tab created successfully")

        # Tab 3: SQL Editor
        elif current_tab == 3 and not self.tabs_initialized['sql_editor']:
            console_print("[TabSwitch] Creating SQL Editor tab...")
            self.update_status("Loading SQL Editor tab...")
            if self.sql_editor is None:
                self.sql_editor = SQLEditorTab(
                    self.sql_editor_tab,
                    lambda: self.active_connections,
                    self.update_status,
                    font_ui=self.ui_font,
                    font_mono=self.ui_font_mono,
                )
            self.tabs_initialized['sql_editor'] = True
            console_print("[TabSwitch] SQL Editor tab created successfully")

        # Tab 4: Schema Conversion
        elif current_tab == 4 and not self.tabs_initialized['conversion']:
            console_print("[TabSwitch] Creating Schema Conversion tab...")
            self.update_status("Loading Schema Conversion tab...")
            self.create_conversion_tab_ui()
            self.tabs_initialized['conversion'] = True
            console_print("[TabSwitch] Schema Conversion tab created successfully")

        # Tab 5: AI Query Assistant
        elif current_tab == 5 and not self.tabs_initialized['ai_query']:
            console_print("[TabSwitch] Creating AI Query Assistant tab...")
            self.update_status("Loading AI Query Assistant tab...")
            self.ai_query_ui = AIQueryUI(
                parent_frame=self.ai_query_tab,
                root=self.root,
                ai_agent=self.ai_agent,
                active_connections=self.active_connections,
                update_status_callback=self.update_status,
                send_to_editor_callback=self._send_sql_to_editor,
                theme=ColorTheme,
                fonts={'ui': default_ui_font(), 'mono': default_ui_mono()}
            )
            self.ai_query_ui.create_ui()
            self.tabs_initialized['ai_query'] = True
            console_print("[TabSwitch] AI Query Assistant tab created successfully")

        # Tab 6: Server Monitor
        elif current_tab == 6 and not self.tabs_initialized['monitor']:
            console_print("[TabSwitch] Creating Server Monitor tab...")
            self.update_status("Loading Server Monitor tab...")
            self.server_monitor_ui = ServerMonitorUI(
                parent_frame=self.monitor_tab,
                root=self.root,
                connection_manager=self.connection_manager,
                active_connections=self.active_connections,
                update_status_callback=self.update_status,
                theme=ColorTheme
            )
            self.server_monitor_ui.create_ui()
            self.tabs_initialized['monitor'] = True
            console_print("[TabSwitch] Server Monitor tab created successfully")

    def create_welcome_tab_ui(self):
        """Create modern, visually appealing Welcome tab"""
        # Create scrollable container
        canvas = tk.Canvas(self.welcome_tab, highlightthickness=0, bd=0, bg=ColorTheme.BG_MAIN)
        scrollbar = ttk.Scrollbar(self.welcome_tab, orient=tk.VERTICAL, command=canvas.yview)
        content_frame = ttk.Frame(canvas, style="TFrame")

        canvas_window = canvas.create_window((0, 0), window=content_frame, anchor=tk.NW)
        canvas.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Debounced scrollregion update - responsive to window resize
        self._welcome_resize_timer = None

        def _update_welcome_scrollregion(event=None):
            if self._welcome_resize_timer:
                self.root.after_cancel(self._welcome_resize_timer)
            self._welcome_resize_timer = self.root.after(150,
                lambda: canvas.configure(scrollregion=canvas.bbox("all")))

        def _on_canvas_resize(event):
            """Expand content_frame to canvas width when canvas resizes"""
            canvas_width = event.width
            canvas.itemconfig(canvas_window, width=canvas_width)

        content_frame.bind("<Configure>", _update_welcome_scrollregion)
        canvas.bind("<Configure>", _on_canvas_resize)
        bind_canvas_mousewheel(canvas)

        # Hero Section - Eye-catching gradient
        hero_frame = tk.Frame(content_frame, bg="#0ea5e9", bd=0)
        hero_frame.pack(fill=tk.X, padx=0, pady=0)

        hero_inner = tk.Frame(hero_frame, bg="#0ea5e9")
        hero_inner.pack(fill=tk.BOTH, padx=50, pady=40)

        # Bold, attention-grabbing title
        title_label = tk.Label(hero_inner,
                               text="🗄️ Database Management Tool",
                               font=(self.ui_font[0], 32, "bold"),
                               foreground="white",
                               bg="#0ea5e9")
        title_label.pack(anchor=tk.W)

        subtitle_label = tk.Label(hero_inner,
                                  text="Multi-Database Control Center | AI-Powered | Enterprise Ready",
                                  font=(self.ui_font[0], 14),
                                  foreground="#e0f2fe",
                                  bg="#0ea5e9")
        subtitle_label.pack(anchor=tk.W, pady=(8, 0))

        # Quick Overview - Simple list
        overview_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        overview_section.pack(fill=tk.X, padx=40, pady=(30, 20))

        tk.Label(overview_section,
                text="🎯 Quick Overview",
                font=(self.ui_font[0], 16, "bold"),
                foreground="#1e293b",
                bg=ColorTheme.BG_MAIN).pack(anchor=tk.W, pady=(0, 10))

        overview_items = [
            "• Supports 5 databases: Oracle, MySQL, PostgreSQL, MariaDB, SQLite",
            "• Manage unlimited concurrent connections",
            "• AI-powered query assistant with Claude",
            "• Real-time performance monitoring",
            "• Schema migration and conversion tools"
        ]

        for item in overview_items:
            tk.Label(overview_section,
                    text=item,
                    font=(self.ui_font[0], 13),
                    foreground="#475569",
                    bg=ColorTheme.BG_MAIN,
                    anchor=tk.W).pack(anchor=tk.W, pady=3)

        # Tab Descriptions - Detailed usage guide
        tabs_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        tabs_section.pack(fill=tk.X, padx=40, pady=(25, 20))

        tk.Label(tabs_section,
                text="📚 Tab Descriptions & Usage Guide",
                font=(self.ui_font[0], 16, "bold"),
                foreground="#1e293b",
                bg=ColorTheme.BG_MAIN).pack(anchor=tk.W, pady=(0, 15))

        tab_details = [
            ("🔌 Connection Management", [
                "Purpose: Manage all database connections from a centralized hub",
                "Usage: Create, test, edit, and delete connection profiles",
                "Features: Encrypted credential storage, connection testing, quick connect buttons",
                "How to use: Click 'Add Connection' → Select database type → Enter credentials → Test → Save"
            ]),
            ("📊 Database Objects", [
                "Purpose: Browse and explore database structure and objects",
                "Usage: View tables, views, indexes, triggers, procedures, functions, and more",
                "Features: Tree view navigation, object filtering, DDL generation, refresh",
                "How to use: Select connection → Expand tree to browse → Right-click for options"
            ]),
            ("💻 SQL Editor", [
                "Purpose: Write and execute SQL queries with full editor capabilities",
                "Usage: Create, edit, and run SQL statements with syntax highlighting",
                "Features: Query execution (F5), result export, query history, multi-tab support",
                "How to use: Write SQL → Press F5 or click Execute → View results → Export if needed"
            ]),
            ("🤖 AI Query Assistant", [
                "Purpose: Generate SQL queries from natural language questions using AI",
                "Usage: Ask questions in plain English and get optimized SQL",
                "Features: Schema awareness, query explanation, optimization suggestions, follow-up questions",
                "How to use: Select connection → Type question → Generate SQL → Execute → Refine with follow-ups"
            ]),
            ("🔄 Schema Conversion", [
                "Purpose: Convert database schemas between different database types",
                "Usage: Migrate schemas from one database platform to another",
                "Features: Table structure conversion, type mapping, constraint preservation",
                "How to use: Select source → Select target → Choose tables → Convert → Review → Apply"
            ]),
            ("📈 Performance Monitor", [
                "Purpose: Monitor real-time database performance and resource usage",
                "Usage: Track CPU, memory, connections, and query performance",
                "Features: Real-time graphs, connection statistics, process monitoring, alerts",
                "How to use: Select connection → Monitor displays automatically → View metrics over time"
            ])
        ]

        for tab_name, details in tab_details:
            tab_frame = tk.Frame(tabs_section, bg="white", relief=tk.SOLID, bd=1)
            tab_frame.pack(fill=tk.X, pady=(0, 12))

            # Tab title
            tk.Label(tab_frame,
                    text=tab_name,
                    font=(self.ui_font[0], 13, "bold"),
                    foreground="#0284c7",
                    bg="white",
                    anchor=tk.W).pack(anchor=tk.W, padx=15, pady=(12, 8))

            # Tab details
            for detail in details:
                detail_label = tk.Label(tab_frame,
                        text=detail,
                        font=(self.ui_font[0], 13),
                        foreground="#475569",
                        bg="white",
                        anchor=tk.W,
                        justify=tk.LEFT)
                detail_label.pack(anchor=tk.W, padx=25, pady=2)

            # Add spacing at bottom
            tk.Frame(tab_frame, height=10, bg="white").pack()

        # Keyboard Shortcuts & Platforms - Minimal flat layout
        reference_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        reference_section.pack(fill=tk.X, padx=40, pady=(20, 10))

        # Two-column grid
        grid_container = tk.Frame(reference_section, bg=ColorTheme.BG_MAIN)
        grid_container.pack(fill=tk.BOTH, expand=True)

        # Left column: Keyboard Shortcuts
        shortcuts_card = tk.Frame(grid_container, bg=ColorTheme.BG_MAIN, relief=tk.FLAT)
        shortcuts_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 15))

        tk.Label(shortcuts_card,
                text="⌨️ Shortcuts",
                font=(self.ui_font[0], 14, "bold"),
                foreground="#0284c7",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.W).pack(fill=tk.X, pady=(0, 8))

        shortcuts = [
            ("F5", "Execute query", "#14b8a6"),
            ("Ctrl+Enter", "Execute (alt)", "#3b82f6"),
            ("Ctrl+Tab", "Cycle tabs", "#a855f7"),
            ("Escape", "Cancel dialog", "#f43f5e")
        ]

        for key, description, color in shortcuts:
            shortcut_row = tk.Frame(shortcuts_card, bg=ColorTheme.BG_MAIN)
            shortcut_row.pack(fill=tk.X, pady=2)

            tk.Label(shortcut_row,
                    text=key,
                    font=(self.ui_font[0], 12, "bold"),
                    foreground=color,
                    bg=ColorTheme.BG_MAIN,
                    width=12,
                    anchor=tk.W).pack(side=tk.LEFT)

            tk.Label(shortcut_row,
                    text=description,
                    font=(self.ui_font[0], 13),
                    foreground="#64748b",
                    bg=ColorTheme.BG_MAIN,
                    anchor=tk.W).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Right column: Supported Platforms
        platforms_card = tk.Frame(grid_container, bg=ColorTheme.BG_MAIN, relief=tk.FLAT)
        platforms_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(15, 0))

        tk.Label(platforms_card,
                text="💿 Platforms",
                font=(self.ui_font[0], 14, "bold"),
                foreground="#0284c7",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.W).pack(fill=tk.X, pady=(0, 8))

        platforms = [
            ("Oracle", "11g - 21c", "#ef4444"),
            ("MySQL", "5.7, 8.0+", "#f59e0b"),
            ("MariaDB", "10.x", "#14b8a6"),
            ("PostgreSQL", "10 - 15+", "#3b82f6"),
            ("SQLite", "3.x", "#a855f7")
        ]

        for platform, versions, color in platforms:
            platform_row = tk.Frame(platforms_card, bg=ColorTheme.BG_MAIN)
            platform_row.pack(fill=tk.X, pady=2)

            tk.Label(platform_row,
                    text=f"• {platform}",
                    font=(self.ui_font[0], 12, "bold"),
                    foreground=color,
                    bg=ColorTheme.BG_MAIN,
                    width=14,
                    anchor=tk.W).pack(side=tk.LEFT)

            tk.Label(platform_row,
                    text=versions,
                    font=(self.ui_font[0], 13),
                    foreground="#94a3b8",
                    bg=ColorTheme.BG_MAIN,
                    anchor=tk.W).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Pro Tips Section - Minimal and lightweight
        tips_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        tips_section.pack(fill=tk.X, padx=40, pady=(20, 10))

        tk.Label(tips_section,
                text="💡 Tips",
                font=(self.ui_font[0], 14, "bold"),
                foreground="#0284c7",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.W).pack(fill=tk.X, pady=(0, 8))

        tips = [
            ("Start with Connection Management", "#14b8a6"),
            ("Status bar shows real-time feedback", "#3b82f6"),
            ("Credentials encrypted & stored locally", "#f59e0b"),
            ("Multiple concurrent connections supported", "#a855f7"),
            ("AI Assistant requires Claude CLI", "#f43f5e"),
            ("Batch processing for imports/exports", "#0ea5e9")
        ]

        # Simple list layout
        for tip, color in tips:
            tip_row = tk.Frame(tips_section, bg=ColorTheme.BG_MAIN)
            tip_row.pack(fill=tk.X, pady=2)

            tk.Label(tip_row,
                    text="•",
                    font=(self.ui_font[0], 12),
                    foreground=color,
                    bg=ColorTheme.BG_MAIN).pack(side=tk.LEFT, padx=(0, 8))

            tk.Label(tip_row,
                    text=tip,
                    font=(self.ui_font[0], 13),
                    foreground="#64748b",
                    bg=ColorTheme.BG_MAIN,
                    anchor=tk.W).pack(side=tk.LEFT, fill=tk.X, expand=True)

        # Support footer - minimal
        footer_section = tk.Frame(content_frame, bg=ColorTheme.BG_MAIN)
        footer_section.pack(fill=tk.X, padx=40, pady=(20, 40))

        tk.Label(footer_section,
                text="📚 Need help? Refer to application documentation",
                font=(self.ui_font[0], 11),
                foreground="#94a3b8",
                bg=ColorTheme.BG_MAIN,
                anchor=tk.CENTER).pack()

    def create_connections_tab_ui(self):
        """Create UI for connections tab - manage database connections"""
        # Create scrollable container
        connections_canvas = tk.Canvas(self.connections_tab, highlightthickness=0, bd=0, bg=ColorTheme.BG_MAIN)
        connections_scroll = ttk.Scrollbar(self.connections_tab, orient=tk.VERTICAL, command=connections_canvas.yview)
        connections_inner = ttk.Frame(connections_canvas)
        inner_win = connections_canvas.create_window((0, 0), window=connections_inner, anchor=tk.NW)

        def _sync_conn_width(event):
            connections_canvas.itemconfigure(inner_win, width=event.width)

        def _update_conn_scrollregion(event=None):
            connections_canvas.configure(scrollregion=connections_canvas.bbox("all"))

        connections_canvas.bind("<Configure>", _sync_conn_width)
        connections_inner.bind("<Configure>", _update_conn_scrollregion)
        connections_canvas.configure(yscrollcommand=connections_scroll.set)
        connections_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        connections_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        bind_canvas_mousewheel(connections_canvas)

        # Add all connection-related frames stacked vertically
        self.create_active_connections_frame(connections_inner)
        self.create_connection_frame(connections_inner)

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
            elif db_type in ["MySQL", "MariaDB", "PostgreSQL"]:
                self.service_label.config(text="Database:")

        # Ensure canvas view starts at the top after content is loaded
        self.root.after(100, lambda: connections_canvas.yview_moveto(0))

    def create_objects_tab_ui(self):
        """Create UI for database objects tab - browse and view database objects"""
        # Connection selector at the top with horizontal scrolling (optimized)
        selector_outer = ttk.Frame(self.objects_tab)
        selector_outer.pack(fill=tk.X, padx=10, pady=5)
        selector_frame = create_horizontal_scrollable(selector_outer)

        ttk.Label(selector_frame, text="Active Connection:", font=self.ui_font).pack(side=tk.LEFT, padx=(0, 5))
        self.objects_connection_combo = ttk.Combobox(selector_frame, state="readonly", width=40, font=self.ui_font)
        self.objects_connection_combo.pack(side=tk.LEFT, padx=5)
        self.objects_connection_combo.bind('<<ComboboxSelected>>', self.on_objects_connection_changed)

        ttk.Button(selector_frame, text="Refresh", command=self.refresh_objects_connections).pack(side=tk.LEFT, padx=5)

        # Import Data button
        ttk.Button(selector_frame, text="Import Data", style="Success.TButton",
                  command=self.import_data_to_table).pack(side=tk.LEFT, padx=5)

        # Horizontal paned window with buttons (left) and results (right)
        self.objects_paned = ttk.PanedWindow(self.objects_tab, orient=tk.HORIZONTAL)
        self.objects_paned.pack(fill=tk.BOTH, expand=True, padx=6, pady=4)

        self.create_buttons_frame()
        self.create_results_frame()

        # Populate connection dropdown first (this will auto-select first connection)
        self.refresh_objects_connections()

        # Now create operation buttons - after connection is selected
        # If a connection was auto-selected, buttons will be for that DB type
        if self.current_connection_name and self.current_db_type:
            self.recreate_operation_buttons()
            # Enable buttons since we have an active connection
            for btn in self.operation_buttons:
                btn.config(state=tk.NORMAL)
            self.update_status(f"Database Objects tab ready - using {self.current_connection_name}")

    def _setup_readable_ttk(self):
        style = ttk.Style()

        # Set theme base
        try:
            style.theme_use('clam')  # Use clam theme for better customization
        except tk.TclError:
            pass  # Theme not available, use default

        try:
            # General font settings
            style.configure("TLabel", font=self.ui_font, background=ColorTheme.BG_MAIN)
            style.configure("TCheckbutton", font=self.ui_font, background=ColorTheme.BG_MAIN)
            style.configure("TRadiobutton", font=self.ui_font, background=ColorTheme.BG_MAIN)

            # LabelFrame heading font - bigger for better visibility
            labelframe_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")
            style.configure("TLabelframe.Label", font=labelframe_font, foreground=ColorTheme.PRIMARY_DARK, background=ColorTheme.BG_MAIN)

            # Button styling - modern look with border
            style.configure("TButton",
                          font=self.ui_font,
                          padding=6,
                          relief="solid",
                          borderwidth=1,
                          bordercolor="#cbd5e1")
            style.map("TButton",
                     background=[('active', ColorTheme.PRIMARY_LIGHT),
                               ('!active', ColorTheme.BG_SECONDARY)],
                     foreground=[('active', ColorTheme.PRIMARY_DARK)],
                     bordercolor=[('active', '#94a3b8'),
                                ('!active', '#cbd5e1')])

            # Primary button style (for important actions)
            style.configure("Primary.TButton",
                          font=self.ui_font,
                          padding=6,
                          background=ColorTheme.PRIMARY,
                          foreground="white",
                          relief="solid",
                          borderwidth=1,
                          bordercolor=ColorTheme.PRIMARY_DARK)
            style.map("Primary.TButton",
                     background=[('active', ColorTheme.PRIMARY_DARK),
                               ('!active', ColorTheme.PRIMARY)],
                     bordercolor=[('active', '#1e3a8a'),
                                ('!active', ColorTheme.PRIMARY_DARK)])

            # Success button style
            style.configure("Success.TButton",
                          font=self.ui_font,
                          padding=6,
                          background=ColorTheme.SUCCESS,
                          foreground="white",
                          relief="solid",
                          borderwidth=1,
                          bordercolor="#388e3c")
            style.map("Success.TButton",
                     background=[('active', '#45a049'),
                               ('!active', ColorTheme.SUCCESS)],
                     foreground=[('active', 'white'),
                               ('pressed', 'white'),
                               ('!active', 'white')],
                     bordercolor=[('active', '#2e7d32'),
                                ('!active', '#388e3c')])

            # Warning button style
            style.configure("Warning.TButton",
                          font=self.ui_font,
                          padding=6,
                          background=ColorTheme.WARNING,
                          foreground=ColorTheme.TEXT_PRIMARY,
                          relief="solid",
                          borderwidth=1,
                          bordercolor="#f57c00")
            style.map("Warning.TButton",
                     background=[('active', '#ffb300'),
                               ('pressed', '#ff8f00'),
                               ('!active', ColorTheme.WARNING)],
                     foreground=[('active', ColorTheme.TEXT_PRIMARY),
                               ('pressed', ColorTheme.TEXT_PRIMARY),
                               ('!active', ColorTheme.TEXT_PRIMARY)],
                     bordercolor=[('active', '#e65100'),
                                ('!active', '#f57c00')])

            # Error button style
            style.configure("Error.TButton",
                          font=self.ui_font,
                          padding=6,
                          background=ColorTheme.ERROR,
                          foreground="white",
                          relief="solid",
                          borderwidth=1,
                          bordercolor="#c62828")
            style.map("Error.TButton",
                     background=[('active', '#d32f2f'),
                               ('pressed', '#b71c1c'),
                               ('!active', ColorTheme.ERROR)],
                     foreground=[('active', 'white'),
                               ('pressed', 'white'),
                               ('disabled', '#cccccc'),
                               ('!active', 'white')],
                     bordercolor=[('active', '#b71c1c'),
                                ('!active', '#c62828')])

            # Frame styling
            style.configure("TFrame", background=ColorTheme.BG_MAIN)
            style.configure("Card.TFrame", background=ColorTheme.BG_SECONDARY, relief="flat", borderwidth=1)

            # Notebook (tab) styling
            style.configure("TNotebook",
                          background=ColorTheme.BG_MAIN,
                          borderwidth=0)
            style.configure("TNotebook.Tab",
                          font=self.ui_font,
                          padding=[12, 6],
                          background=ColorTheme.BG_SECONDARY)
            style.map("TNotebook.Tab",
                     background=[('selected', ColorTheme.PRIMARY),
                               ('!selected', ColorTheme.BG_SECONDARY)],
                     foreground=[('selected', 'white'),
                               ('!selected', ColorTheme.TEXT_PRIMARY)])

            # Entry styling
            style.configure("TEntry",
                          fieldbackground=ColorTheme.BG_SECONDARY,
                          borderwidth=1,
                          relief="solid")

            # Combobox styling
            style.configure("TCombobox",
                          fieldbackground=ColorTheme.BG_SECONDARY,
                          background=ColorTheme.BG_SECONDARY,
                          borderwidth=1)

            # Treeview styling
            style.configure("Treeview",
                          font=self.ui_font,
                          background=ColorTheme.BG_SECONDARY,
                          fieldbackground=ColorTheme.BG_SECONDARY,
                          borderwidth=0,
                          relief="flat")
            style.configure("Treeview.Heading",
                          font=(self.ui_font[0], self.ui_font[1], "bold"),
                          background=ColorTheme.PRIMARY_LIGHT,
                          foreground=ColorTheme.PRIMARY_DARK,
                          borderwidth=1,
                          relief="flat")
            style.map("Treeview.Heading",
                     background=[('active', ColorTheme.PRIMARY)])

            # Progressbar styling
            style.configure("TProgressbar",
                          background=ColorTheme.PRIMARY,
                          troughcolor=ColorTheme.BG_MAIN,
                          borderwidth=0,
                          thickness=20)

            # LabelFrame styling
            style.configure("TLabelframe",
                          background=ColorTheme.BG_MAIN,
                          borderwidth=1,
                          relief="solid")

        except tk.TclError:
            pass

        try:
            style.configure("Treeview", rowheight=max(22, int(self.ui_font[1]) + 10))
        except tk.TclError:
            pass

    def _ensure_sql_editor(self):
        """Select SQL tab and create editor if needed."""
        self.notebook.select(3)  # SQL Editor is now tab 3
        self.root.update_idletasks()
        if not self.tabs_initialized["sql_editor"]:
            self.on_tab_changed()

    def _send_sql_to_editor(self, sql_text):
        """Callback for AI module to send SQL to editor"""
        self._ensure_sql_editor()
        self.sql_editor.query_text.delete(1.0, tk.END)
        self.sql_editor.query_text.insert(1.0, sql_text)
        self.notebook.select(3)  # Switch to SQL Editor tab

    def _create_menubar(self):
        menubar = tk.Menu(self.root)
        file_m = tk.Menu(menubar, tearoff=0)
        file_m.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_m)

        view_m = tk.Menu(menubar, tearoff=0)
        view_m.add_command(label="Welcome", command=lambda: self.notebook.select(0))
        view_m.add_command(label="Connections", command=lambda: self.notebook.select(1))
        view_m.add_command(label="Database Objects", command=lambda: self.notebook.select(2))
        view_m.add_command(label="SQL Editor", command=lambda: self.notebook.select(3))
        view_m.add_command(label="Schema Conversion", command=lambda: self.notebook.select(4))
        view_m.add_command(label="AI Query Assistant", command=lambda: self.notebook.select(5))
        view_m.add_command(label="Server Monitor", command=lambda: self.notebook.select(6))
        menubar.add_cascade(label="View", menu=view_m)

        conn_m = tk.Menu(menubar, tearoff=0)
        conn_m.add_command(label="New connection…", command=lambda: self.notebook.select(1))
        conn_m.add_command(label="Disconnect all", command=self.disconnect_all_connections)
        conn_m.add_separator()
        conn_m.add_command(label="Saved connections…", command=self.show_saved_connections)
        menubar.add_cascade(label="Connection", menu=conn_m)

        sql_m = tk.Menu(menubar, tearoff=0)
        sql_m.add_command(label="Execute at cursor (F5)", command=self._menu_sql_execute_cursor)
        sql_m.add_command(label="Execute selected", command=self._menu_sql_execute_selected)
        sql_m.add_command(label="Execute all…", command=self._menu_sql_execute_all)
        sql_m.add_separator()
        sql_m.add_command(label="Load query…", command=self._menu_sql_load)
        sql_m.add_command(label="Save query…", command=self._menu_sql_save)
        sql_m.add_separator()
        sql_m.add_command(label="Commit", command=self._menu_sql_commit)
        sql_m.add_command(label="Rollback", command=self._menu_sql_rollback)
        sql_m.add_separator()
        sql_m.add_command(label="Query history…", command=self._menu_sql_history)
        sql_m.add_command(label="Export results…", command=self._menu_sql_export)
        sql_m.add_command(label="Clear results", command=self._menu_sql_clear_results)
        menubar.add_cascade(label="SQL", menu=sql_m)

        help_m = tk.Menu(menubar, tearoff=0)
        help_m.add_command(label="Keyboard shortcuts…", command=self._show_shortcuts_help)
        menubar.add_cascade(label="Help", menu=help_m)

        self.root.config(menu=menubar)

    def _show_shortcuts_help(self):
        text = (
            "Navigation\n"
            "  View menu — jump between tabs\n\n"
            "Connections Tab\n"
            "  Create and manage database connections\n"
            "  ▼ / ▶ — expand or collapse connection sections\n\n"
            "SQL Editor\n"
            "  F5 or Ctrl+Enter — run query at cursor\n"
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

    def create_active_connections_frame(self, parent):
        title_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")

        content = make_collapsible_section(parent, "Active connections", title_font, expanded=True)

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
            highlightbackground=ColorTheme.BORDER
        )
        scrollbar.config(command=self.active_conn_listbox.yview)

        self.active_conn_listbox.pack(side=tk.LEFT)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.active_conn_listbox.bind('<<ListboxSelect>>', self.on_connection_selected)

        btn_frame = ttk.Frame(content)
        btn_frame.pack(anchor=tk.W, padx=10, pady=(5, 5))

        ttk.Button(btn_frame, text="Disconnect Selected", command=self.disconnect_selected_connection, style="Warning.TButton").pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="Disconnect All", command=self.disconnect_all_connections, style="Error.TButton").pack(side=tk.LEFT, padx=2)

    def create_connection_frame(self, parent):
        title_font = (self.ui_font[0], self.ui_font[1] + 2, "bold")
        content = make_collapsible_section(parent, "New database connection", title_font, expanded=True)
        self.conn_frame = ttk.Frame(content)
        self.conn_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Form Fields
        form_frame = ttk.Frame(self.conn_frame)
        form_frame.pack(fill=tk.X)

        # Database Type
        ttk.Label(form_frame, text="Database Type:", font=(self.ui_font[0], self.ui_font[1], "bold")).grid(row=0, column=0, sticky=tk.W, padx=5, pady=(0, 5))
        self.db_type_combo = ttk.Combobox(form_frame, width=35, state="readonly")
        self.db_type_combo['values'] = DatabaseConfig.get_db_types()
        self.db_type_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=(0, 5))
        self.db_type_combo.bind('<<ComboboxSelected>>', lambda e: self.on_db_type_changed())

        # Host
        ttk.Label(form_frame, text="Host:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.host_entry = ttk.Entry(form_frame, width=35)
        self.host_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        self.host_entry.insert(0, "localhost")

        # Port
        ttk.Label(form_frame, text="Port:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.port_entry = ttk.Entry(form_frame, width=35)
        self.port_entry.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)

        # Service/Database (dynamic label)
        self.service_label = ttk.Label(form_frame, text="Service:")
        self.service_label.grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        self.service_entry = ttk.Entry(form_frame, width=35)
        self.service_entry.grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)

        # Username
        ttk.Label(form_frame, text="Username:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=5)
        self.user_entry = ttk.Entry(form_frame, width=35)
        self.user_entry.grid(row=4, column=1, sticky=tk.W, padx=5, pady=5)

        # Password
        ttk.Label(form_frame, text="Password:").grid(row=5, column=0, sticky=tk.W, padx=5, pady=5)
        self.password_entry = ttk.Entry(form_frame, width=35, show="*")
        self.password_entry.grid(row=5, column=1, sticky=tk.W, padx=5, pady=5)

        # Action Buttons - Horizontal row below password
        button_frame = ttk.Frame(self.conn_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        self.connect_btn = ttk.Button(button_frame, text="Connect", command=self.connect_db, style="Primary.TButton", width=15)
        self.connect_btn.pack(side=tk.LEFT, padx=(5, 5))

        ttk.Button(button_frame, text="Test Connection", command=self.test_db_connection, width=15).pack(side=tk.LEFT, padx=5)

        ttk.Button(button_frame, text="Load Saved", command=self.show_saved_connections, width=15).pack(side=tk.LEFT, padx=5)

        ttk.Button(button_frame, text="Save Connection", command=self.save_connection_dialog, width=15).pack(side=tk.LEFT, padx=5)


    def create_buttons_frame(self):
        # Buttons Frame - Left side of paned window with scrollbar
        btn_container = ttk.LabelFrame(self.objects_paned, text="Database Objects", padding="10")
        self.objects_paned.add(btn_container, weight=0)

        # Clear button (always visible at top - outside scrollable area)
        self.clear_btn = ttk.Button(btn_container, text="Clear Results", command=self.clear_results, width=20)
        self.clear_btn.pack(padx=5, pady=(0, 5), fill=tk.X)

        # Canvas and scrollbar for operation buttons
        canvas = tk.Canvas(btn_container, width=200, highlightthickness=0, bd=0, bg=ColorTheme.BG_MAIN)
        scrollbar = ttk.Scrollbar(btn_container, orient="vertical", command=canvas.yview)
        self.btn_frame = ttk.Frame(canvas)

        self.btn_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=self.btn_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        bind_canvas_mousewheel(canvas)

    def create_results_frame(self):
        # Results Frame - Right side of paned window
        self.results_frame_container = ttk.LabelFrame(self.objects_paned, text="Results", padding="10")
        self.objects_paned.add(self.results_frame_container, weight=1)

        # Create TWO separate containers that can be switched:
        # 1. Canvas container for collapsible table sections
        self.results_canvas_container = ttk.Frame(self.results_frame_container)

        self.results_canvas = tk.Canvas(
            self.results_canvas_container,
            bg=ColorTheme.BG_SECONDARY,
            highlightthickness=0,
            bd=0
        )
        self.results_scrollbar = ttk.Scrollbar(self.results_canvas_container, orient="vertical", command=self.results_canvas.yview)
        self.results_content_frame = ttk.Frame(self.results_canvas)

        self.results_content_frame.bind(
            "<Configure>",
            lambda e: self.results_canvas.configure(scrollregion=self.results_canvas.bbox("all"))
        )

        self.results_canvas_window = self.results_canvas.create_window((0, 0), window=self.results_content_frame, anchor="nw")
        self.results_canvas.configure(yscrollcommand=self.results_scrollbar.set)

        # Bind canvas width to frame width
        def _on_canvas_configure(event):
            self.results_canvas.itemconfig(self.results_canvas_window, width=event.width)
        self.results_canvas.bind('<Configure>', _on_canvas_configure)

        self.results_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.results_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        bind_canvas_mousewheel(self.results_canvas)

        # 2. Simple text container for non-table results
        self.results_text_container = ttk.Frame(self.results_frame_container)

        # Create ScrolledText widget for simple displays
        self.results_text = scrolledtext.ScrolledText(
            self.results_text_container,
            wrap=tk.WORD,
            font=self.ui_font_mono,
            bg=ColorTheme.BG_SECONDARY,
            fg=ColorTheme.TEXT_PRIMARY,
            relief=tk.FLAT,
            borderwidth=0
        )
        self.results_text.pack(fill=tk.BOTH, expand=True)

        # Start with text container hidden (will be shown as needed)
        # Nothing is packed initially

    def create_conversion_tab_ui(self):
        """Create UI for schema conversion tab"""
        main_canvas = tk.Canvas(self.conversion_tab, highlightthickness=0, bd=0)
        scrollbar = ttk.Scrollbar(self.conversion_tab, orient="vertical", command=main_canvas.yview)
        scrollable_frame = ttk.Frame(main_canvas)
        conv_inner_win = main_canvas.create_window((0, 0), window=scrollable_frame, anchor=tk.NW)

        # Debounced configure bindings - responsive to window resize
        self._conv_resize_timer = None

        def _sync_conv_width(event):
            main_canvas.itemconfigure(conv_inner_win, width=event.width)

        def _update_conv_scrollregion(event=None):
            if self._conv_resize_timer:
                self.root.after_cancel(self._conv_resize_timer)
            self._conv_resize_timer = self.root.after(150,
                lambda: main_canvas.configure(scrollregion=main_canvas.bbox("all")))

        main_canvas.bind("<Configure>", _sync_conv_width)
        scrollable_frame.bind("<Configure>", _update_conv_scrollregion)

        main_canvas.configure(yscrollcommand=scrollbar.set)

        main_canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Bind mousewheel to main canvas
        bind_canvas_mousewheel(main_canvas)

        title_font = (self.ui_font[0], self.ui_font[1] + 3, "bold")
        # Title
        title_label = ttk.Label(scrollable_frame, text="Database Schema & Data Conversion",
                                font=title_font)
        title_label.pack(pady=10)

        # Source Connection Frame
        source_frame = ttk.LabelFrame(scrollable_frame, text="Source Database", padding="10")
        source_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(source_frame, text="Select Source Connection:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.source_conn_combo = ttk.Combobox(source_frame, width=40, state="readonly")
        self.source_conn_combo.grid(row=0, column=1, padx=5, pady=5)
        self.source_conn_combo.bind('<<ComboboxSelected>>', self.on_source_connection_changed)

        ttk.Button(source_frame, text="Refresh", command=self.refresh_conversion_connections).grid(row=0, column=2, padx=5, pady=5)

        ttk.Label(source_frame, text="Select Tables:").grid(row=1, column=0, sticky=tk.NW, padx=5, pady=5)

        # Table selection with checkboxes
        table_outer = ttk.Frame(source_frame)
        table_outer.grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)

        # Scrollable canvas for checkboxes
        table_canvas = tk.Canvas(table_outer, height=150, bg=ColorTheme.BG_SECONDARY, highlightthickness=1, highlightbackground='#ccc')
        table_scroll = ttk.Scrollbar(table_outer, orient=tk.VERTICAL, command=table_canvas.yview)
        self.table_checkboxes_frame = ttk.Frame(table_canvas)

        # Debounced table canvas scrollregion update
        self._table_resize_timer = None

        def _update_table_scrollregion(event=None):
            if self._table_resize_timer:
                self.root.after_cancel(self._table_resize_timer)
            self._table_resize_timer = self.root.after(150,
                lambda: table_canvas.configure(scrollregion=table_canvas.bbox("all")))

        self.table_checkboxes_frame.bind("<Configure>", _update_table_scrollregion)

        table_canvas.create_window((0, 0), window=self.table_checkboxes_frame, anchor="nw")
        table_canvas.configure(yscrollcommand=table_scroll.set)

        table_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        table_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Bind mousewheel to table canvas
        bind_canvas_mousewheel(table_canvas)

        # Store checkbox variables
        self.table_checkbox_vars = {}  # {table_name: BooleanVar}

        # Buttons for table selection
        table_btn_frame = ttk.Frame(source_frame)
        table_btn_frame.grid(row=1, column=2, padx=5, pady=5, sticky=tk.N)

        ttk.Button(table_btn_frame, text="Load Tables", command=self.load_source_tables, width=12).pack(pady=2)
        ttk.Button(table_btn_frame, text="Check All", command=self.check_all_tables, width=12).pack(pady=2)
        ttk.Button(table_btn_frame, text="Uncheck All", command=self.uncheck_all_tables, width=12).pack(pady=2)

        # Target Connection Frame
        target_frame = ttk.LabelFrame(scrollable_frame, text="Target Database", padding="10")
        target_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(target_frame, text="Select Target Connection:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.target_conn_combo = ttk.Combobox(target_frame, width=40, state="readonly")
        self.target_conn_combo.grid(row=0, column=1, columnspan=3, padx=5, pady=5)

        # Table naming options
        ttk.Label(target_frame, text="Table Naming:", font=(self.ui_font[0], self.ui_font[1], "bold")).grid(row=1, column=0, sticky=tk.W, padx=5, pady=(10, 5))

        ttk.Label(target_frame, text="Prefix:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.target_prefix_entry = ttk.Entry(target_frame, width=15)
        self.target_prefix_entry.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(target_frame, text="(optional)", foreground="gray", font=('Arial', 9)).grid(row=2, column=2, sticky=tk.W, padx=5, pady=5)

        ttk.Label(target_frame, text="Suffix:").grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        self.target_suffix_entry = ttk.Entry(target_frame, width=15)
        self.target_suffix_entry.grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(target_frame, text="(optional)", foreground="gray", font=('Arial', 9)).grid(row=3, column=2, sticky=tk.W, padx=5, pady=5)

        ttk.Label(target_frame, text="Example: If table is 'users' with prefix 'new_' and suffix '_bak', target will be 'new_users_bak'",
                 foreground="blue", font=('Arial', 9)).grid(row=4, column=0, columnspan=4, sticky=tk.W, padx=5, pady=(5, 5))

        # Options Frame
        options_frame = ttk.LabelFrame(scrollable_frame, text="Conversion Options", padding="10")
        options_frame.pack(fill=tk.X, padx=10, pady=5)

        self.create_indexes_var = tk.BooleanVar(value=True)
        self.drop_if_exists_var = tk.BooleanVar(value=False)

        ttk.Checkbutton(options_frame, text="Create Indexes (with schema)", variable=self.create_indexes_var).grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Checkbutton(options_frame, text="Drop Table If Exists (before schema conversion)", variable=self.drop_if_exists_var).grid(row=0, column=1, sticky=tk.W, padx=5, pady=2)

        ttk.Label(options_frame, text="Batch Size (for data transfer):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.batch_size_entry = ttk.Entry(options_frame, width=15)
        self.batch_size_entry.insert(0, config.get_int('database.performance', 'transfer_batch_size', default=1000))
        self.batch_size_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)

        # Action Buttons Frame (split into two rows) with horizontal scrolling (optimized)
        action_frame_outer = ttk.Frame(scrollable_frame)
        action_frame_outer.pack(fill=tk.X, padx=10, pady=10)
        action_frame = create_horizontal_scrollable(action_frame_outer)

        # First row - Analysis buttons
        row1_frame = ttk.Frame(action_frame)
        row1_frame.pack(fill=tk.X, pady=(0, 5))

        self.preview_schema_btn = ttk.Button(row1_frame, text="Preview Schema", command=self.preview_schema_conversion, width=16)
        self.preview_schema_btn.pack(side=tk.LEFT, padx=5)

        self.row_counts_btn = ttk.Button(row1_frame, text="Row Counts", command=self.show_row_counts, width=14)
        self.row_counts_btn.pack(side=tk.LEFT, padx=5)

        self.sample_data_btn = ttk.Button(row1_frame, text="Sample Data", command=self.show_sample_data, width=14)
        self.sample_data_btn.pack(side=tk.LEFT, padx=5)

        ttk.Button(row1_frame, text="Clear Preview", command=self.clear_conversion_preview, width=14).pack(side=tk.LEFT, padx=5)

        # Second row - Conversion buttons
        row2_frame = ttk.Frame(action_frame)
        row2_frame.pack(fill=tk.X)

        self.convert_schema_btn = ttk.Button(row2_frame, text="Convert Schema", command=self.convert_schema_only, width=16)
        self.convert_schema_btn.pack(side=tk.LEFT, padx=5)

        self.transfer_data_btn = ttk.Button(row2_frame, text="Transfer Data", command=self.transfer_data_only, width=14)
        self.transfer_data_btn.pack(side=tk.LEFT, padx=5)

        self.stop_conversion_btn = ttk.Button(row2_frame, text="Stop", command=self.stop_conversion_operation, width=14, style="Error.TButton")
        self.stop_conversion_btn.pack(side=tk.LEFT, padx=5)
        self.stop_conversion_btn.config(state=tk.DISABLED)  # Initially disabled

        # Preview/Results Frame
        preview_frame = ttk.LabelFrame(scrollable_frame, text="Conversion Preview & Results", padding="10")
        preview_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Preview Text
        self.conversion_preview_text = scrolledtext.ScrolledText(
            preview_frame, wrap=tk.WORD, height=25, font=self.ui_font_mono
        )
        self.conversion_preview_text.pack(fill=tk.BOTH, expand=True)

        # Progress Frame
        progress_frame = ttk.Frame(scrollable_frame)
        progress_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(progress_frame, text="Progress:").pack(side=tk.LEFT, padx=5)
        self.conversion_progress = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.conversion_progress.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        self.conversion_status_label = ttk.Label(progress_frame, text="Ready", foreground="blue")
        self.conversion_status_label.pack(side=tk.LEFT, padx=5)

        # Initialize connection combos
        self.refresh_conversion_connections()

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
            pady=5
        )
        self.status_bar.pack(fill=tk.BOTH, expand=True)

    def create_toolbar(self):
        """Create top toolbar with global action buttons"""
        toolbar = ttk.Frame(self.root, style='Card.TFrame')
        toolbar.pack(fill=tk.X, padx=5, pady=(5, 0))

        # Spacer on left to push button to right
        ttk.Frame(toolbar).pack(side=tk.LEFT, expand=True)

        # Clear Cache button on right
        clear_btn = ttk.Button(
            toolbar,
            text="🗑️ Clear Cache",
            command=self.clear_all_caches,
            style="TButton"
        )
        clear_btn.pack(side=tk.RIGHT, padx=5, pady=5)

    def clear_all_caches(self):
        """Clear all application caches (AI, credentials) while preserving active connections"""

        # Confirmation dialog
        active_monitor_count = 0
        if hasattr(self, 'server_monitor_ui') and self.server_monitor_ui:
            active_monitor_count = sum(1 for conn in self.server_monitor_ui.monitor_connections.values() if conn.get('monitoring', False))

        confirm_msg = (
            "This will clear the following caches:\n\n"
            "• AI schema and context caches\n"
            "• AI conversation history\n"
            "• Saved credentials (reload from disk)\n\n"
            f"Preserved (NOT affected):\n"
            f"• Active DB connections: {len(self.active_connections)}\n"
            f"• Active monitoring sessions: {active_monitor_count}\n\n"
            "Continue?"
        )

        if not messagebox.askyesno("Clear Cache", confirm_msg, icon='question'):
            return

        try:
            cleared_items = []

            # 1. Clear AI schema and context caches
            if hasattr(self, 'ai_agent') and self.ai_agent:
                cache_info = self.ai_agent.get_cache_info()
                cache_count = len(cache_info)
                self.ai_agent.invalidate_cache()  # Clear all connection caches
                cleared_items.append(f"AI caches ({cache_count} connections)")

            # 2. Clear AI conversation history
            if hasattr(self, 'ai_agent') and self.ai_agent:
                self.ai_agent.clear_conversation()
                cleared_items.append("AI conversation history")

            # 3. Reload saved credentials from disk
            if hasattr(self, 'conn_manager') and self.conn_manager:
                old_count = len(self.conn_manager.get_all_connections())
                self.conn_manager.connections = self.conn_manager.load_connections()
                new_count = len(self.conn_manager.get_all_connections())
                cleared_items.append(f"Database connections ({old_count} → {new_count})")

            # 4. Reload monitor credentials from disk
            if hasattr(self, 'monitor_conn_manager') and self.monitor_conn_manager:
                old_count = len(self.monitor_conn_manager.get_all_connections())
                self.monitor_conn_manager.connections = self.monitor_conn_manager.load_connections()
                new_count = len(self.monitor_conn_manager.get_all_connections())
                cleared_items.append(f"Monitor connections ({old_count} → {new_count})")

            # 5. Refresh AI Query UI if it's initialized
            if hasattr(self, 'ai_query_ui') and self.ai_query_ui:
                try:
                    self.ai_query_ui.refresh_connections()
                    cleared_items.append("AI Query UI refreshed")
                except Exception as e:
                    console_print(f"Warning: Could not refresh AI Query UI: {e}")

            # Success message
            success_msg = (
                "✓ Cache cleared successfully!\n\n"
                "Cleared:\n" +
                "\n".join(f"  • {item}" for item in cleared_items) +
                f"\n\nPreserved:\n"
                f"  • Active DB connections: {len(self.active_connections)}\n"
                f"  • Active monitoring sessions: {active_monitor_count}"
            )

            messagebox.showinfo("Cache Cleared", success_msg)
            self.update_status("Cache cleared", "success")

        except Exception as e:
            error_msg = f"Error clearing caches:\n{str(e)}"
            messagebox.showerror("Clear Cache Error", error_msg)
            self.update_status(f"✗ Error clearing caches: {str(e)}", "error")
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
        elif db_type in ["MySQL", "MariaDB", "PostgreSQL"]:
            self.service_label.config(text="Database:")

        # Recreate operation buttons
        self.recreate_operation_buttons()

        self.update_status(f"Selected {db_type} database")

    def recreate_operation_buttons(self):
        """Recreate operation buttons based on selected database type"""
        # Check if Database Objects tab has been initialized
        if not hasattr(self, 'btn_frame'):
            # Tab not created yet, will be created when user switches to it
            return

        # Remove old buttons
        for btn in self.operation_buttons:
            btn.destroy()
        self.operation_buttons.clear()

        # Create new buttons - stacked vertically
        operations = DatabaseConfig.get_available_operations(self.current_db_type)

        for text, func_name in operations:
            btn = ttk.Button(
                self.btn_frame,
                text=text,
                command=lambda t=text, f=func_name: self.execute_db_operation(t, f),
                state=tk.DISABLED,
                width=20
            )
            btn.pack(padx=5, pady=5, fill=tk.X)
            self.operation_buttons.append(btn)

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
            self.root.after(3000, lambda: self.status_bar.config(bg=ColorTheme.PRIMARY, fg="white"))

    def connect_db(self):
        db_type = self.db_type_combo.get()
        if not db_type:
            messagebox.showerror("Error", "Please select a database type!")
            return

        host = self.host_entry.get()
        port = self.port_entry.get()
        service_or_db = self.service_entry.get()
        user = self.user_entry.get()
        password = self.password_entry.get()

        if not all([host, port, service_or_db, user, password]):
            messagebox.showerror("Error", "All connection fields are required!")
            return

        # Auto-generate unique connection name
        self.connection_counter += 1
        conn_name = f"{db_type}-{service_or_db}-{self.connection_counter}"

        # Ensure connection name is unique (shouldn't happen with counter, but safe check)
        while conn_name in self.active_connections:
            self.connection_counter += 1
            conn_name = f"{db_type}-{service_or_db}-{self.connection_counter}"

        try:
            port = int(port)
        except ValueError:
            messagebox.showerror("Error", "Port must be a number!")
            return

        self.update_status(f"Connecting to {db_type} database as '{conn_name}'...")
        self.connect_btn.config(state=tk.DISABLED)

        # Prepare connection parameters
        conn_params = {
            'host': host,
            'port': port,
            'username': user,
            'password': password
        }

        if db_type == "Oracle":
            conn_params['service'] = service_or_db
        elif db_type in ["MySQL", "MariaDB", "PostgreSQL"]:
            conn_params['database'] = service_or_db

        # Run connection in thread
        thread = threading.Thread(target=self._connect_thread, args=(conn_name, db_type, conn_params))
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
        connection_result = {'success': False, 'conn': None, 'db_manager': None, 'error': None}

        def attempt_connection():
            try:
                db_manager = DatabaseManager(db_type)
                conn = db_manager.connect(**conn_params)
                connection_result['success'] = True
                connection_result['conn'] = conn
                connection_result['db_manager'] = db_manager
            except Exception as e:
                connection_result['error'] = str(e)

        try:
            # Mask password in log
            debug_params = {k: ('***' if k == 'password' else v) for k, v in conn_params.items()}
            console_print(f"Attempting to connect to {db_type} as '{conn_name}': {debug_params}")

            # Start connection attempt in a separate thread
            conn_thread = threading.Thread(target=attempt_connection, daemon=True)
            conn_thread.start()

            # Wait for connection with timeout
            connection_timeout = config.get_float('database.connection', 'connection_timeout', default=30.0)
            conn_thread.join(timeout=connection_timeout)

            if conn_thread.is_alive():
                # Connection is still running after timeout
                console_print(f"Connection timeout after {connection_timeout} seconds for {conn_name}")
                self.root.after(0, self._connection_failed, f"Connection timeout after {connection_timeout} seconds.\n\nThe database server '{conn_params['host']}:{conn_params['port']}' is not responding.")
                return

            # Check if connection was successful
            if not connection_result['success']:
                error_msg = connection_result['error'] if connection_result['error'] else "Unknown connection error"
                console_print(f"{db_type} connection failed: {error_msg}")
                self.root.after(0, self._connection_failed, error_msg)
                return

            db_manager = connection_result['db_manager']
            conn = connection_result['conn']

            if conn:
                console_print(f"{db_type} connection successful: {conn_name}")

                # Verify connection details
                if db_type in ["MySQL", "MariaDB", "PostgreSQL"]:
                    cursor = conn.cursor()
                    cursor.execute("SELECT DATABASE()" if db_type in ["MySQL", "MariaDB"] else "SELECT current_database()")
                    current_db = cursor.fetchone()[0]
                    cursor.close()
                    console_print(f"Connected to database: {current_db}")

                is_admin = db_manager.is_admin()
                version = db_manager.get_version()

                self.root.after(0, self._connection_success, conn_name, db_type, db_manager, version, is_admin, conn_params)
            else:
                console_print(f"{db_type} connection returned None")
                self.root.after(0, self._connection_failed, f"Failed to connect to {db_type}. Check logs.")
        except Exception as e:
            console_print(f"Exception during {db_type} connection: {e}")
            import traceback
            traceback.print_exc()
            self.root.after(0, self._connection_failed, str(e))

    def _connection_success(self, conn_name, db_type, db_manager, version, is_admin, conn_params):
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
            # Create new SQL Editor on first connection
            self.sql_editor = SQLEditorTab(
                self.sql_editor_tab,
                lambda: self.active_connections,
                self.update_status
            )
            self.tabs_initialized['sql_editor'] = True
        else:
            # Refresh connections in SQL editor
            self.sql_editor.refresh_connections()

        self.update_status(f"✓ Connected to {conn_name} successfully! ({len(self.active_connections)} active)", "success")

    def _connection_failed(self, error_msg):
        self.connect_btn.config(state=tk.NORMAL)
        self.update_status("✗ Connection failed", "error")
        messagebox.showerror("Connection Error", f"Failed to connect:\n{error_msg}")

    def test_db_connection(self):
        """Test database connection without adding to active connections"""
        db_type = self.db_type_combo.get()
        if not db_type:
            messagebox.showerror("Error", "Please select a database type!")
            return

        host = self.host_entry.get()
        port = self.port_entry.get()
        service_or_db = self.service_entry.get()
        user = self.user_entry.get()
        password = self.password_entry.get()

        if not all([host, port, service_or_db, user, password]):
            messagebox.showerror("Error", "All connection fields are required!")
            return

        try:
            port = int(port)
        except ValueError:
            messagebox.showerror("Error", "Port must be a number!")
            return

        connection_timeout = config.get_float('database.connection', 'connection_timeout', default=30.0)
        self.update_status(f"Testing {db_type} connection... (timeout: {connection_timeout:.0f}s)")

        # Prepare connection parameters
        conn_params = {
            'host': host,
            'port': port,
            'username': user,
            'password': password
        }

        if db_type == "Oracle":
            conn_params['service'] = service_or_db
        elif db_type in ["MySQL", "MariaDB", "PostgreSQL"]:
            conn_params['database'] = service_or_db

        # Run test in thread
        thread = threading.Thread(target=self._test_connection_thread, args=(db_type, conn_params))
        thread.daemon = True
        thread.start()

    def _test_connection_thread(self, db_type, conn_params):
        """Thread for testing database connection"""
        connection_result = {'success': False, 'version': None, 'error': None}

        def attempt_connection():
            try:
                db_manager = DatabaseManager(db_type)
                conn = db_manager.connect(**conn_params)
                if conn:
                    connection_result['success'] = True
                    connection_result['version'] = db_manager.get_version()
                    # Disconnect immediately after test
                    db_manager.disconnect()
                else:
                    connection_result['error'] = "Connection returned None"
            except Exception as e:
                connection_result['error'] = str(e)

        try:
            # Mask password in log
            debug_params = {k: ('***' if k == 'password' else v) for k, v in conn_params.items()}
            console_print(f"Testing connection to {db_type}: {debug_params}")

            # Start connection attempt in a separate thread with timeout
            conn_thread = threading.Thread(target=attempt_connection, daemon=True)
            conn_thread.start()

            # Wait for connection with timeout
            connection_timeout = config.get_float('database.connection', 'connection_timeout', default=30.0)
            conn_thread.join(timeout=connection_timeout)

            if conn_thread.is_alive():
                # Connection is still running after timeout
                console_print(f"Connection test timeout after {connection_timeout} seconds")
                self.root.after(0, self._test_connection_result, False, None,
                    f"Connection timeout after {connection_timeout} seconds.\n\nThe database server '{conn_params['host']}:{conn_params['port']}' is not responding.")
                return

            # Check if connection was successful
            if not connection_result['success']:
                error_msg = connection_result['error'] if connection_result['error'] else "Unknown connection error"
                console_print(f"{db_type} connection test failed: {error_msg}")
                self.root.after(0, self._test_connection_result, False, None, error_msg)
                return

            version = connection_result['version']
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
            service_or_db = self.service_entry.get()

            version_text = f" (Version: {version})" if version else ""
            message = f"✓ Connection test successful!\n\nDatabase: {db_type}\nHost: {host}\nService/DB: {service_or_db}{version_text}"

            messagebox.showinfo("Connection Test Success", message)
            self.update_status("✓ Connection test successful", "success")
        else:
            messagebox.showerror("Connection Test Failed", f"Failed to connect:\n\n{error_msg}")
            self.update_status("✗ Connection test failed", "error")

    def on_connection_selected(self, event=None):
        """Handle connection selection from listbox"""
        selection = self.active_conn_listbox.curselection()
        if selection:
            selected_text = self.active_conn_listbox.get(selection[0])
            # Extract connection name (everything before the first ' (' )
            conn_name = selected_text.split(' (')[0]
            self.current_connection_name = conn_name
            self.update_status(f"Selected connection: {conn_name}")

    def refresh_objects_connections(self):
        """Refresh the connection dropdown in Database Objects tab"""
        # Check if objects tab has been initialized
        if not hasattr(self, 'objects_connection_combo'):
            return

        connection_names = list(self.active_connections.keys())
        self.objects_connection_combo['values'] = connection_names

        # Select current connection if set, or first connection if available
        if self.current_connection_name and self.current_connection_name in connection_names:
            self.objects_connection_combo.set(self.current_connection_name)
        elif connection_names:
            # Auto-select first connection
            first_conn = connection_names[0]
            self.objects_connection_combo.set(first_conn)
            self.current_connection_name = first_conn

            # Set the database type
            db_manager = self.active_connections[first_conn]
            self.current_db_type = db_manager.db_type

            # If buttons exist, recreate them for this database type and enable
            if hasattr(self, 'operation_buttons'):
                self.recreate_operation_buttons()
                for btn in self.operation_buttons:
                    btn.config(state=tk.NORMAL)

            self.update_status(f"Auto-selected connection: {first_conn} ({self.current_db_type})")
        else:
            self.objects_connection_combo.set('')
            self.current_connection_name = None
            self.current_db_type = None
            # Disable operation buttons if they exist
            if hasattr(self, 'operation_buttons'):
                for btn in self.operation_buttons:
                    btn.config(state=tk.DISABLED)

    def on_objects_connection_changed(self, event):
        """Handle connection selection change in Database Objects tab"""
        selected = self.objects_connection_combo.get()
        if not selected or selected not in self.active_connections:
            return

        self.current_connection_name = selected
        db_manager = self.active_connections[selected]
        self.current_db_type = db_manager.db_type

        # Recreate operation buttons for the correct database type
        self.recreate_operation_buttons()

        # Enable operation buttons
        for btn in self.operation_buttons:
            btn.config(state=tk.NORMAL)

        self.update_status(f"Using {selected} in Objects View ({self.current_db_type})")

    def import_data_to_table(self):
        """Import data from file and create table in selected database"""
        # Check if connection is active
        if not self.current_connection_name or self.current_connection_name not in self.active_connections:
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
            ("Excel files", "*.xlsx *.xls"),
            ("All files", "*.*")
        ]
        filename = filedialog.askopenfilename(
            title="Select data file to import",
            filetypes=filetypes
        )

        if not filename:
            return

        self.update_status("Reading data file...")

        try:
            # Derive table name from filename (remove extension and sanitize)
            base_name = os.path.splitext(os.path.basename(filename))[0]
            # Sanitize table name: remove special chars, replace spaces with underscore
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name).upper()

            # Ask user to confirm/modify table name
            table_name = self._prompt_table_name(table_name)
            if not table_name:
                return

            # Read file based on extension
            file_ext = os.path.splitext(filename)[1].lower()
            if file_ext == '.csv':
                data, columns = self._read_csv_file(filename)
            elif file_ext in ['.xlsx', '.xls']:
                data, columns = self._read_excel_file(filename)
            else:
                messagebox.showerror("Error", f"Unsupported file format: {file_ext}")
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
                messagebox.showinfo("Success",
                    f"Table '{table_name}' created successfully!\n"
                    f"Rows imported: {len(data)}\n"
                    f"Columns: {len(columns)}")
                self.update_status(f"Import complete: {table_name} ({len(data)} rows)")

                # Refresh tables list - get the correct getTables function name for this DB type
                operations = DatabaseRegistry.get_available_operations(db_manager.db_type)
                tables_op = next((op for op in operations if op[0] == 'Tables'), None)
                if tables_op:
                    self.execute_db_operation('Tables', tables_op[1])
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

        result = [None]

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
                if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
                    result[0] = name.upper()
                    dialog.destroy()
                else:
                    messagebox.showerror("Invalid Name",
                        "Table name must start with letter or underscore\n"
                        "and contain only letters, numbers, and underscores")
            else:
                messagebox.showerror("Error", "Table name cannot be empty")

        def on_cancel():
            dialog.destroy()

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="OK", command=on_ok, style="Success.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=on_cancel).pack(side=tk.LEFT, padx=5)

        name_entry.bind('<Return>', lambda e: on_ok())
        name_entry.bind('<Escape>', lambda e: on_cancel())

        dialog.wait_window()
        return result[0]

    def _read_csv_file(self, filename):
        """Read CSV file and return data and columns"""
        with open(filename, 'r', encoding='utf-8-sig') as f:
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
                col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col.strip()).upper()
                if not col_name or col_name[0].isdigit():
                    col_name = f'COL_{idx+1}'

                # Handle duplicates
                original_name = col_name
                counter = 1
                while col_name in seen:
                    col_name = f'{original_name}_{counter}'
                    counter += 1

                seen.add(col_name)
                columns.append(col_name)

            data = rows[1:]

            return data, columns

    def _read_excel_file(self, filename):
        """Read Excel file and return data and columns"""
        try:
            import pandas as pd

            df = pd.read_excel(filename)

            # Get column names with proper sanitization
            columns = []
            seen = set()
            for idx, col in enumerate(df.columns):
                # Sanitize column name
                col_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(col).strip()).upper()
                if not col_name or col_name[0].isdigit():
                    col_name = f'COL_{idx+1}'

                # Handle duplicates
                original_name = col_name
                counter = 1
                while col_name in seen:
                    col_name = f'{original_name}_{counter}'
                    counter += 1

                seen.add(col_name)
                columns.append(col_name)

            # Convert to list of lists
            data = df.values.tolist()

            # Convert NaN to None
            for row in data:
                for i in range(len(row)):
                    if pd.isna(row[i]):
                        row[i] = None
                    else:
                        row[i] = str(row[i])

            return data, columns

        except ImportError:
            messagebox.showerror("Error",
                "pandas and openpyxl libraries required for Excel import.\n"
                "Install with: pip install pandas openpyxl")
            return [], []

    def _infer_column_types(self, data, columns):
        """Infer column types from data"""
        column_types = []

        for col_idx in range(len(columns)):
            # Sample first 100 rows
            sample_values = [row[col_idx] if col_idx < len(row) else None
                           for row in data[:100] if row]

            # Remove None/empty values
            sample_values = [v for v in sample_values if v is not None and str(v).strip()]

            if not sample_values:
                column_types.append('VARCHAR(255)')
                continue

            # Check if all values are integers
            all_int = all(self._is_integer(v) for v in sample_values)
            if all_int:
                column_types.append('INTEGER')
                continue

            # Check if all values are numbers
            all_numeric = all(self._is_numeric(v) for v in sample_values)
            if all_numeric:
                column_types.append('NUMERIC(18,4)')
                continue

            # Check if all values are dates
            all_date = all(self._is_date(v) for v in sample_values)
            if all_date:
                column_types.append('DATE')
                continue

            # Find max length for VARCHAR
            max_len = max(len(str(v)) for v in sample_values)
            varchar_len = max(255, min(4000, max_len + 50))  # Add buffer
            column_types.append(f'VARCHAR({varchar_len})')

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
        date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']
        for fmt in date_formats:
            try:
                datetime.strptime(str(value), fmt)
                return True
            except (ValueError, TypeError):
                continue
        return False

    def _create_table_with_data(self, conn, table_name, columns, column_types, data, db_type):
        """Create table and insert data"""
        try:
            # Use buffered cursor for MySQL/MariaDB to avoid unread result errors
            if db_type in ['MySQL', 'MariaDB']:
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
            if db_type == 'Oracle':
                placeholders = [f":{i+1}" for i in range(len(columns))]
            elif db_type in ['MySQL', 'MariaDB']:
                placeholders = ['%s'] * len(columns)
            elif db_type == 'PostgreSQL':
                placeholders = ['%s'] * len(columns)
            elif db_type == 'SQLite':
                placeholders = ['?'] * len(columns)

            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

            # Insert data in batches
            batch_size = config.get_int('database.performance', 'transfer_batch_size', default=1000)
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]

                # Pad rows to match column count
                padded_batch = []
                for row in batch:
                    padded_row = list(row) + [None] * (len(columns) - len(row))
                    padded_row = padded_row[:len(columns)]  # Trim if too long
                    padded_batch.append(padded_row)

                if db_type == 'Oracle':
                    cursor.executemany(insert_sql, padded_batch)
                else:
                    cursor.executemany(insert_sql, padded_batch)

                console_print(f"Inserted {min(i+batch_size, len(data))}/{len(data)} rows")
                self.update_status(f"Importing... {min(i+batch_size, len(data))}/{len(data)} rows")

            conn.commit()
            cursor.close()

            console_print(f"Successfully created table {table_name} with {len(data)} rows")
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
            messagebox.showwarning("Warning", "Please select a connection to disconnect!")
            return

        selected_text = self.active_conn_listbox.get(selection[0])
        conn_name = selected_text.split(' (')[0]

        if messagebox.askyesno("Confirm Disconnect", f"Disconnect from '{conn_name}'?"):
            try:
                db_manager = self.active_connections[conn_name]
                db_manager.disconnect()
                del self.active_connections[conn_name]

                # Invalidate schema cache for this connection
                if hasattr(self, 'ai_agent') and self.ai_agent:
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

                self.update_status(f"✓ Disconnected from '{conn_name}'", "success")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to disconnect:\n{str(e)}")

    def disconnect_all_connections(self):
        """Disconnect all active connections"""
        if not self.active_connections:
            messagebox.showinfo("Info", "No active connections to disconnect")
            return

        count = len(self.active_connections)
        if messagebox.askyesno("Confirm Disconnect All", f"Disconnect all {count} connection(s)?"):
            try:
                for conn_name, db_manager in list(self.active_connections.items()):
                    db_manager.disconnect()

                self.active_connections.clear()
                self.active_conn_listbox.delete(0, tk.END)
                self.current_connection_name = None

                # Clear all schema caches
                if hasattr(self, 'ai_agent') and self.ai_agent:
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

                self.update_status(f"✓ Disconnected all {count} connection(s)", "success")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to disconnect all:\n{str(e)}")

    def disconnect_db(self):
        """Legacy disconnect method - now redirects to disconnect selected"""
        self.disconnect_selected_connection()

    def save_connection_dialog(self):
        """Show dialog to save current connection"""
        db_type = self.db_type_combo.get()
        host = self.host_entry.get()
        port = self.port_entry.get()
        service_or_db = self.service_entry.get()
        user = self.user_entry.get()
        password = self.password_entry.get()

        if not all([db_type, host, port, service_or_db, user]):
            messagebox.showwarning("Warning", "Please fill in all connection fields!")
            return

        # Create dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("Save Connection")
        dialog.geometry("400x250")
        dialog.transient(self.root)
        dialog.grab_set()

        # Connection Name
        ttk.Label(dialog, text="Connection Name:").grid(row=0, column=0, sticky=tk.W, padx=10, pady=10)
        name_entry = ttk.Entry(dialog, width=30)
        name_entry.grid(row=0, column=1, padx=10, pady=10)
        name_entry.focus()

        # Save Password Option
        save_pwd_var = tk.BooleanVar(value=False)
        save_pwd_check = ttk.Checkbutton(
            dialog,
            text="Save Password (Warning: Password will be stored in plain text)",
            variable=save_pwd_var
        )
        save_pwd_check.grid(row=1, column=0, columnspan=2, padx=10, pady=10, sticky=tk.W)

        # Info Label
        info_label = ttk.Label(dialog, text="", foreground="blue")
        info_label.grid(row=2, column=0, columnspan=2, padx=10, pady=5)

        def save():
            name = name_entry.get().strip()
            if not name:
                messagebox.showwarning("Warning", "Please enter a connection name!")
                return

            success, message = self.connection_manager.add_connection(
                name, db_type, host, port, service_or_db, user, password, save_pwd_var.get()
            )

            if success:
                self.update_status(f"✓ {message}", "success")
                dialog.destroy()
            else:
                messagebox.showerror("Error", message)

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.grid(row=3, column=0, columnspan=2, pady=20)

        ttk.Button(btn_frame, text="Save", command=save, width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy, width=15).pack(side=tk.LEFT, padx=5)

    def show_saved_connections(self):
        """Show dialog with saved connections"""
        connections = self.connection_manager.get_all_connections()

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
            xscrollcommand=hsb.set
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

        # Populate connections
        for conn in connections:
            tree.insert(
                "",
                tk.END,
                text=conn['name'],
                values=(conn['db_type'], conn['host'], conn['port'], conn['service_or_db'], conn['username'])
            )

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        def load_selected():
            selection = tree.selection()
            if not selection:
                messagebox.showwarning("Warning", "Please select a connection!")
                return

            item = tree.item(selection[0])
            conn_name = item['text']
            conn = self.connection_manager.get_connection(conn_name)

            if conn:
                # Set database type
                self.db_type_combo.set(conn['db_type'])
                self.on_db_type_changed()

                # Set connection fields
                self.host_entry.delete(0, tk.END)
                self.host_entry.insert(0, conn['host'])

                self.port_entry.delete(0, tk.END)
                self.port_entry.insert(0, conn['port'])

                self.service_entry.delete(0, tk.END)
                self.service_entry.insert(0, conn['service_or_db'])

                self.user_entry.delete(0, tk.END)
                self.user_entry.insert(0, conn['username'])

                self.password_entry.delete(0, tk.END)
                if conn.get('save_password') and conn.get('password'):
                    self.password_entry.insert(0, conn['password'])

                dialog.destroy()
                self.update_status(f"✓ Loaded connection '{conn_name}' - Click Connect to establish connection", "success")

        def delete_selected():
            selection = tree.selection()
            if not selection:
                messagebox.showwarning("Warning", "Please select a connection!")
                return

            item = tree.item(selection[0])
            conn_name = item['text']

            if messagebox.askyesno("Confirm Delete", f"Delete connection '{conn_name}'?"):
                success, message = self.connection_manager.delete_connection(conn_name)
                if success:
                    tree.delete(selection[0])
                    self.update_status(f"✓ {message}", "success")
                else:
                    messagebox.showerror("Error", message)

        ttk.Button(btn_frame, text="Load Connection", command=load_selected, width=18).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Delete Connection", command=delete_selected, width=18).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Close", command=dialog.destroy, width=18).pack(side=tk.RIGHT, padx=5)

        if not connections:
            ttk.Label(tree_frame, text="No saved connections", font=("Arial", 12)).pack(expand=True)

    def clear_results(self):
        """Clear all results"""
        # Hide both containers
        self.results_canvas_container.pack_forget()
        self.results_text_container.pack_forget()

        # Clear canvas content frame
        for widget in self.results_content_frame.winfo_children():
            widget.destroy()

        # Clear text widget
        if self.results_text:
            self.results_text.delete(1.0, tk.END)

    def display_results(self, title, items):
        """Display results - use collapsible sections for tables, simple text for others"""
        self.clear_results()

        # Check if we're displaying tables
        if title.lower() == "tables" and items:
            # Show canvas container for collapsible tables
            self.results_canvas_container.pack(fill=tk.BOTH, expand=True)
            self.display_tables_with_schema(title, items)
        else:
            # Show text container for simple list display
            self.results_text_container.pack(fill=tk.BOTH, expand=True)
            self.results_text.insert(tk.END, f"=== {title} ({self.current_db_type}) ===\n\n")

            if not items:
                self.results_text.insert(tk.END, "No items found.\n")
            else:
                self.results_text.insert(tk.END, f"Total: {len(items)} item(s)\n\n")
                for idx, item in enumerate(items, 1):
                    self.results_text.insert(tk.END, f"{idx}. {item}\n")

        self.update_status(f"Found {len(items)} {title.lower()}")

    def display_tables_with_schema(self, title, tables):
        """Display tables with collapsible schema details"""
        # Title header
        header_frame = ttk.Frame(self.results_content_frame)
        header_frame.pack(fill=tk.X, padx=10, pady=(10, 5))

        title_label = ttk.Label(
            header_frame,
            text=f"=== {title} ({self.current_db_type}) ===",
            font=(self.ui_font[0], self.ui_font[1] + 1, "bold")
        )
        title_label.pack(side=tk.LEFT)

        count_label = ttk.Label(
            header_frame,
            text=f"Total: {len(tables)} table(s)",
            foreground="gray"
        )
        count_label.pack(side=tk.RIGHT, padx=10)

        # Check if schema operation is supported
        db_manager = self.active_connections.get(self.current_connection_name)
        if db_manager:
            supports_schema = DatabaseRegistry.supports_operation(db_manager.db_type, 'getTableSchema')
            if not supports_schema:
                warning_frame = ttk.Frame(self.results_content_frame)
                warning_frame.pack(fill=tk.X, padx=10, pady=5)
                ttk.Label(
                    warning_frame,
                    text=f"⚠️ Schema details not available for {db_manager.db_type}",
                    foreground="orange",
                    font=(self.ui_font[0], self.ui_font[1], "bold")
                ).pack()

        # Create collapsible section for each table
        for table_name in tables:
            self._create_table_section(table_name)

    def _create_table_section(self, table_name):
        """Create a collapsible section for a single table"""
        # Get database manager
        if not self.current_connection_name or self.current_connection_name not in self.active_connections:
            return None

        db_manager = self.active_connections[self.current_connection_name]

        # Create collapsible section
        shell = ttk.Frame(self.results_content_frame, relief=tk.RIDGE, borderwidth=1)
        shell.pack(fill=tk.X, pady=(2, 2), padx=10)

        header = ttk.Frame(shell, style="Card.TFrame")
        header.pack(fill=tk.X, padx=5, pady=5)

        state = {"collapsed": True, "schema_loaded": False}
        btn = ttk.Button(header, text="▶", width=3)

        # Table name label (truncate if too long)
        display_name = table_name if len(table_name) <= 50 else table_name[:47] + "..."
        ttk.Label(header, text=f"📋 {display_name}", font=(self.ui_font[0], self.ui_font[1], "bold")).pack(side=tk.LEFT, padx=(5, 0))

        # Loading status label
        status_label = ttk.Label(header, text="(click to load schema)", foreground="gray")
        status_label.pack(side=tk.LEFT, padx=10)

        # Content frame (hidden initially)
        content = ttk.Frame(shell)

        def toggle_collapse():
            if state["collapsed"]:
                # Expand
                btn.config(text="▼")
                state["collapsed"] = False

                # Load schema if not already loaded
                if not state["schema_loaded"]:
                    status_label.config(text="Loading schema...")
                    self.root.update_idletasks()

                    # Check if operation is supported
                    if not DatabaseRegistry.supports_operation(db_manager.db_type, 'getTableSchema'):
                        status_label.config(text="(schema not supported)")
                        ttk.Label(content, text=f"  Schema retrieval not supported for {db_manager.db_type}",
                                foreground="orange").pack(anchor=tk.W, pady=5)
                        content.pack(fill=tk.X, padx=10, pady=(0, 10))
                        return

                    # Fetch schema in background
                    def fetch_schema():
                        error_msg = None
                        schema = None

                        try:
                            console_print(f"[Schema] Fetching schema for table: {table_name} (DB: {db_manager.db_type})")

                            schema = DatabaseRegistry.execute_operation(
                                db_manager.db_type, 'getTableSchema', db_manager.conn, table_name
                            )

                            console_print(f"[Schema] Got schema for {table_name}: {len(schema) if schema else 0} columns")

                            def update_ui():
                                if schema is not None and len(schema) > 0:
                                    self._populate_table_schema(content, schema)
                                    status_label.config(text=f"({len(schema)} columns)")
                                    state["schema_loaded"] = True
                                elif schema is not None and len(schema) == 0:
                                    # Empty schema - might be a view or table with no columns
                                    error_label = ttk.Label(content, text="  ℹ️ Table exists but has no columns (might be a view or external table)",
                                                          foreground="blue")
                                    error_label.pack(anchor=tk.W, pady=5)
                                    status_label.config(text="(0 columns)")
                                else:
                                    # None returned - access denied or not found
                                    error_label = ttk.Label(content, text="  ⚠️ Could not retrieve schema - check permissions or table name",
                                                          foreground="orange")
                                    error_label.pack(anchor=tk.W, pady=5)

                                    # Add retry button
                                    def retry():
                                        state["schema_loaded"] = False
                                        for widget in content.winfo_children():
                                            widget.destroy()
                                        content.pack_forget()
                                        btn.config(text="▶")
                                        state["collapsed"] = True
                                        status_label.config(text="(click to retry)")

                                    ttk.Button(content, text="Retry", command=retry).pack(anchor=tk.W, padx=10, pady=2)
                                    status_label.config(text="(failed)")

                                content.pack(fill=tk.X, padx=10, pady=(0, 10))

                            self.root.after(0, update_ui)

                        except Exception as e:
                            import traceback
                            error_detail = traceback.format_exc()
                            console_print(f"[Schema] Error fetching schema for {table_name}:")
                            print(error_detail, file=sys.stderr)
                            error_msg = str(e)

                            def show_error():
                                error_text = f"  ❌ Error loading schema:\n  {error_msg}"
                                error_label = ttk.Label(content, text=error_text, foreground="red", justify=tk.LEFT)
                                error_label.pack(anchor=tk.W, pady=5)

                                # Add details button
                                def show_details():
                                    messagebox.showerror(
                                        "Schema Load Error",
                                        f"Table: {table_name}\n\n"
                                        f"Error: {error_msg}\n\n"
                                        f"Check console for full traceback."
                                    )

                                ttk.Button(content, text="Show Details", command=show_details).pack(anchor=tk.W, padx=10, pady=2)
                                status_label.config(text="(error)")
                                content.pack(fill=tk.X, padx=10, pady=(0, 10))

                            self.root.after(0, show_error)

                    thread = threading.Thread(target=fetch_schema, daemon=True)
                    thread.start()
                else:
                    # Already loaded, just show it
                    content.pack(fill=tk.X, padx=10, pady=(0, 10))
            else:
                # Collapse
                btn.config(text="▶")
                content.pack_forget()
                state["collapsed"] = True

        btn.config(command=toggle_collapse)
        btn.pack(side=tk.LEFT, padx=(0, 5))

    def _populate_table_schema(self, parent, columns):
        """Populate the schema details in the content frame with horizontal scrolling"""
        # Create a grid to display columns
        if not columns:
            ttk.Label(parent, text="  No columns found", foreground="gray").pack(anchor=tk.W)
            return

        # Create horizontal scrollable container
        scroll_container = ttk.Frame(parent)
        scroll_container.pack(fill=tk.BOTH, expand=True, pady=(5, 0))

        canvas = tk.Canvas(scroll_container, highlightthickness=0, bd=0, height=0, bg=ColorTheme.BG_MAIN)
        h_scrollbar = ttk.Scrollbar(scroll_container, orient=tk.HORIZONTAL, command=canvas.xview)
        schema_frame = ttk.Frame(canvas)

        canvas_window = canvas.create_window((0, 0), window=schema_frame, anchor=tk.NW)
        canvas.configure(xscrollcommand=h_scrollbar.set)

        def on_schema_frame_configure(event):
            # Update scroll region and canvas height
            canvas.configure(scrollregion=canvas.bbox("all"))
            req_height = schema_frame.winfo_reqheight()
            if req_height > 0:
                canvas.configure(height=min(req_height, 400))  # Max height 400px

        schema_frame.bind("<Configure>", on_schema_frame_configure)

        canvas.pack(fill=tk.BOTH, expand=True)
        h_scrollbar.pack(fill=tk.X)

        # Header row
        header_frame = ttk.Frame(schema_frame)
        header_frame.pack(fill=tk.X, pady=(0, 2))

        ttk.Label(header_frame, text="Column Name", font=(self.ui_font[0], self.ui_font[1], "bold"), width=30).grid(row=0, column=0, sticky=tk.W, padx=5)
        ttk.Label(header_frame, text="Data Type", font=(self.ui_font[0], self.ui_font[1], "bold"), width=30).grid(row=0, column=1, sticky=tk.W, padx=5)
        ttk.Label(header_frame, text="Nullable", font=(self.ui_font[0], self.ui_font[1], "bold"), width=12).grid(row=0, column=2, sticky=tk.W, padx=5)
        ttk.Label(header_frame, text="Default", font=(self.ui_font[0], self.ui_font[1], "bold"), width=25).grid(row=0, column=3, sticky=tk.W, padx=5)

        # Separator
        ttk.Separator(schema_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=2)

        # Data rows
        for idx, col in enumerate(columns):
            row_frame = ttk.Frame(schema_frame)
            row_frame.pack(fill=tk.X, pady=1)

            # Alternate background colors
            bg_color = ColorTheme.BG_SECONDARY if idx % 2 == 0 else ColorTheme.BG_MAIN

            # Column name
            name_label = ttk.Label(row_frame, text=f"  {col['name']}", width=30, background=bg_color)
            name_label.grid(row=0, column=0, sticky=tk.W, padx=5)

            # Data type
            type_label = ttk.Label(row_frame, text=col['type'], width=30, foreground=ColorTheme.PRIMARY, background=bg_color)
            type_label.grid(row=0, column=1, sticky=tk.W, padx=5)

            # Nullable
            nullable_text = "NULL" if col['nullable'] else "NOT NULL"
            nullable_color = "gray" if col['nullable'] else ColorTheme.ERROR
            nullable_label = ttk.Label(row_frame, text=nullable_text, width=12, foreground=nullable_color, background=bg_color)
            nullable_label.grid(row=0, column=2, sticky=tk.W, padx=5)

            # Default value
            default_text = str(col.get('default', '')) if col.get('default') else '-'
            default_label = ttk.Label(row_frame, text=default_text, width=25, foreground="gray", background=bg_color)
            default_label.grid(row=0, column=3, sticky=tk.W, padx=5)

    def execute_db_operation(self, title, func_name):
        if not self.current_connection_name or self.current_connection_name not in self.active_connections:
            messagebox.showwarning("Warning", "Please select an active connection first!")
            return

        self.update_status(f"Fetching {title.lower()}...")
        thread = threading.Thread(target=self._fetch_and_display, args=(title, func_name))
        thread.daemon = True
        thread.start()

    def _fetch_and_display(self, title, func_name):
        try:
            if self.current_connection_name in self.active_connections:
                db_manager = self.active_connections[self.current_connection_name]
                console_print(f"Fetching {title} using {func_name} for {self.current_connection_name} ({db_manager.db_type})")

                # Use lock to protect shared connection access
                with self.db_query_lock:
                    items = db_manager.execute_operation(func_name)

                console_print(f"Got {len(items) if items else 0} items")
                self.root.after(0, self.display_results, title, items)
            else:
                self.root.after(0, messagebox.showerror, "Error", "No active connection selected")
        except Exception as e:
            import traceback
            error_details = f"Failed to fetch {title.lower()}:\n{str(e)}\n\nDetails:\n{traceback.format_exc()}"
            print(error_details, file=sys.stderr)
            self.root.after(0, messagebox.showerror, "Error", error_details)
            self.root.after(0, self.update_status, f"Error fetching {title.lower()}")

    # ========== Schema Conversion Methods ==========

    def refresh_conversion_connections(self):
        """Refresh connection dropdowns in conversion tab"""
        # Check if conversion tab has been initialized
        if not hasattr(self, 'source_conn_combo') or not hasattr(self, 'target_conn_combo'):
            return

        connection_names = list(self.active_connections.keys())
        self.source_conn_combo['values'] = connection_names
        self.target_conn_combo['values'] = connection_names

        if connection_names:
            if not self.source_conn_combo.get():
                self.source_conn_combo.current(0)
            if not self.target_conn_combo.get():
                if len(connection_names) > 1:
                    self.target_conn_combo.current(1)
                else:
                    self.target_conn_combo.current(0)
        else:
            # No connections available - clear both dropdowns
            self.source_conn_combo.set('')
            self.target_conn_combo.set('')

    def on_source_connection_changed(self, event=None):
        """Handle source connection change"""
        # Clear existing checkboxes when connection changes
        for widget in self.table_checkboxes_frame.winfo_children():
            widget.destroy()
        self.table_checkbox_vars.clear()
        self.clear_conversion_preview()

    def load_source_tables(self):
        """Load tables from source connection"""
        source_conn_name = self.source_conn_combo.get()
        if not source_conn_name:
            messagebox.showwarning("Warning", "Please select a source connection first!")
            return

        if source_conn_name not in self.active_connections:
            messagebox.showerror("Error", "Source connection not found!")
            return

        db_manager = self.active_connections[source_conn_name]

        try:
            # Get tables using registry (works for any registered database type)
            tables = DatabaseRegistry.execute_operation(
                db_manager.db_type, 'getTables', db_manager.conn
            ) or []

            console_print(f"Schema Conversion: Loaded {len(tables)} tables from {source_conn_name} ({db_manager.db_type})")

            # Clear existing checkboxes
            for widget in self.table_checkboxes_frame.winfo_children():
                widget.destroy()
            self.table_checkbox_vars.clear()

            # Create checkboxes for each table
            for table in tables:
                var = tk.BooleanVar(value=False)
                self.table_checkbox_vars[table] = var
                cb = ttk.Checkbutton(
                    self.table_checkboxes_frame,
                    text=table,
                    variable=var
                )
                cb.pack(anchor=tk.W, padx=5, pady=2)

            if not tables:
                messagebox.showwarning("No Tables", f"No tables found in {source_conn_name}.\n\nMake sure:\n1. Database is selected\n2. User has SELECT privileges\n3. Database contains tables")

        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            console_print(f"Error loading tables: {error_detail}")
            messagebox.showerror("Error", f"Failed to load tables:\n{str(e)}\n\nCheck console for details.")

    def check_all_tables(self):
        """Check all table checkboxes"""
        if not self.table_checkbox_vars:
            messagebox.showinfo("Info", "Please load tables first!")
            return

        for var in self.table_checkbox_vars.values():
            var.set(True)
        messagebox.showinfo("Selected", f"Checked all {len(self.table_checkbox_vars)} tables")

    def uncheck_all_tables(self):
        """Uncheck all table checkboxes"""
        if not self.table_checkbox_vars:
            messagebox.showinfo("Info", "Please load tables first!")
            return

        for var in self.table_checkbox_vars.values():
            var.set(False)
        messagebox.showinfo("Deselected", f"Unchecked all {len(self.table_checkbox_vars)} tables")

    def get_selected_tables(self):
        """Get list of selected tables from checkboxes"""
        selected = []
        for table_name, var in self.table_checkbox_vars.items():
            if var.get():
                selected.append(table_name)
        return selected

    def get_target_table_name(self, source_table):
        """Generate target table name with prefix/suffix"""
        prefix = self.target_prefix_entry.get().strip()
        suffix = self.target_suffix_entry.get().strip()
        return f"{prefix}{source_table}{suffix}"

    def preview_schema_conversion(self):
        """Preview the schema conversion for all selected tables"""
        # Get selected tables
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            messagebox.showwarning("Warning", "Please select at least one table!")
            return

        # Validate connections
        source_conn_name = self.source_conn_combo.get()
        target_conn_name = self.target_conn_combo.get()

        if not all([source_conn_name, target_conn_name]):
            messagebox.showwarning("Warning", "Please select source and target connections!")
            return

        if source_conn_name not in self.active_connections or target_conn_name not in self.active_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]

            # Check if converting between different DB types
            if source_manager.db_type == target_manager.db_type:
                messagebox.showinfo("Info", f"Both connections are {source_manager.db_type}. No type conversion needed.")

            # Start operation
            self._start_conversion_operation()

            self.conversion_status_label.config(text=f"Analyzing schema for {len(selected_tables)} table(s)...", foreground="blue")
            self.conversion_progress.start()

            # Clear preview area first
            self.conversion_preview_text.delete(1.0, tk.END)

            # Run in thread
            thread = threading.Thread(target=self._preview_multiple_schemas_thread,
                                     args=(source_manager, target_manager, selected_tables))
            thread.daemon = True
            thread.start()

        except Exception as e:
            messagebox.showerror("Error", f"Preview failed:\n{str(e)}")
            self.conversion_status_label.config(text="Preview failed", foreground="red")
            self._end_conversion_operation()

    def _preview_multiple_schemas_thread(self, source_manager, target_manager, selected_tables):
        """Thread for previewing multiple table schemas"""
        total_tables = len(selected_tables)

        try:
            converter = SchemaConverter(source_manager, target_manager)

            # Header
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"SCHEMA CONVERSION PREVIEW\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Previewing {total_tables} table(s)\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n\n")

            for idx, source_table in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"\n⚠️  Preview stopped by user at table {idx} of {total_tables}\n")
                    break

                try:
                    target_table = self.get_target_table_name(source_table)

                    # Update status
                    self.root.after(0, self.conversion_status_label.config,
                                  {'text': f'Analyzing {idx} of {total_tables}: {source_table}',
                                   'foreground': 'blue'})

                    # Get source schema
                    source_schema = converter.get_table_schema(source_table)
                    if not source_schema:
                        self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                      f"[{idx}/{total_tables}] ❌ Could not retrieve schema for {source_table}\n\n")
                        continue

                    # Convert schema
                    converted_schema = converter.convert_schema(source_schema)
                    converted_schema['table_name'] = target_table

                    # Generate DDL
                    create_table_ddl = converter.generate_create_table_ddl(converted_schema)
                    indexes_ddl = converter.generate_indexes_ddl(converted_schema)

                    # Validate
                    validator = ConversionValidator()
                    issues = validator.validate_schema_conversion(source_schema, converted_schema)

                    # Display this table's preview
                    self.root.after(0, self._append_schema_preview, idx, total_tables,
                                  source_schema, converted_schema, create_table_ddl, indexes_ddl, issues)

                except Exception as e:
                    import traceback
                    error_detail = str(e)
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"[{idx}/{total_tables}] ❌ Preview failed for {source_table}\n")
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"Error: {error_detail}\n\n")

            # Final summary
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Preview complete for {total_tables} table(s)!\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Click 'Convert Schema' to create tables, then 'Transfer Data' to copy data.\n")

        except Exception as e:
            import traceback
            error_msg = f"Schema preview failed:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
        finally:
            self.root.after(0, self.conversion_progress.stop)
            status_text = 'Preview stopped' if self.conversion_stop_event.is_set() else f'Preview complete for {total_tables} table(s)'
            status_color = 'orange' if self.conversion_stop_event.is_set() else 'green'
            self.root.after(0, self.conversion_status_label.config,
                          {'text': status_text, 'foreground': status_color})
            self.root.after(0, self._end_conversion_operation)

    def _append_schema_preview(self, idx, total_tables, source_schema, converted_schema, ddl, indexes_ddl, issues):
        """Append a single table's schema preview to the text widget"""
        # Table header
        self.conversion_preview_text.insert(tk.END, f"\n[{idx}/{total_tables}] {source_schema['table_name']} → {converted_schema['table_name']}\n")
        self.conversion_preview_text.insert(tk.END, "-" * 80 + "\n")

        # Source Info
        self.conversion_preview_text.insert(tk.END, f"Columns: {len(source_schema['columns'])}\n")
        self.conversion_preview_text.insert(tk.END, f"Primary Key: {', '.join(source_schema['primary_key']) if source_schema['primary_key'] else 'None'}\n")
        self.conversion_preview_text.insert(tk.END, f"Indexes: {len(source_schema['indexes'])}\n\n")

        # Validation Issues
        if issues:
            self.conversion_preview_text.insert(tk.END, "⚠️  VALIDATION WARNINGS:\n")
            for issue in issues:
                self.conversion_preview_text.insert(tk.END, f"  - {issue}\n")
            self.conversion_preview_text.insert(tk.END, "\n")

        # Column Mapping (abbreviated)
        self.conversion_preview_text.insert(tk.END, "COLUMN TYPE MAPPINGS:\n")
        for src_col, tgt_col in zip(source_schema['columns'], converted_schema['columns']):
            nullable = "NULL" if tgt_col['nullable'] else "NOT NULL"
            self.conversion_preview_text.insert(tk.END,
                f"  {src_col['name']}: {src_col['type']} → {tgt_col['type']} ({nullable})\n")

        # DDL (collapsed for multi-table view)
        self.conversion_preview_text.insert(tk.END, f"\nCREATE TABLE DDL:\n")
        self.conversion_preview_text.insert(tk.END, ddl + "\n")

        if indexes_ddl:
            self.conversion_preview_text.insert(tk.END, f"\nINDEXES ({len(indexes_ddl)}):\n")
            for idx_ddl in indexes_ddl[:3]:  # Show first 3 indexes
                self.conversion_preview_text.insert(tk.END, "  " + idx_ddl + "\n")
            if len(indexes_ddl) > 3:
                self.conversion_preview_text.insert(tk.END, f"  ... and {len(indexes_ddl) - 3} more\n")

        self.conversion_preview_text.insert(tk.END, "\n")

    def _display_schema_preview(self, source_schema, converted_schema, ddl, indexes_ddl, issues, total_selected=1):
        """Display schema preview in text widget (legacy single-table method)"""
        self.conversion_preview_text.delete(1.0, tk.END)

        # Header
        self.conversion_preview_text.insert(tk.END, "=" * 80 + "\n")
        self.conversion_preview_text.insert(tk.END, "SCHEMA CONVERSION PREVIEW\n")
        if total_selected > 1:
            self.conversion_preview_text.insert(tk.END, f"(Showing preview for 1 of {total_selected} selected tables)\n")
        self.conversion_preview_text.insert(tk.END, "=" * 80 + "\n\n")

        # Source Info
        self.conversion_preview_text.insert(tk.END, f"Source Table: {source_schema['table_name']}\n")
        self.conversion_preview_text.insert(tk.END, f"Target Table: {converted_schema['table_name']}\n")
        self.conversion_preview_text.insert(tk.END, f"Columns: {len(source_schema['columns'])}\n")
        self.conversion_preview_text.insert(tk.END, f"Primary Key: {', '.join(source_schema['primary_key']) if source_schema['primary_key'] else 'None'}\n")
        self.conversion_preview_text.insert(tk.END, f"Indexes: {len(source_schema['indexes'])}\n\n")

        # Validation Issues
        if issues:
            self.conversion_preview_text.insert(tk.END, "⚠️  VALIDATION WARNINGS:\n")
            for issue in issues:
                self.conversion_preview_text.insert(tk.END, f"  - {issue}\n")
            self.conversion_preview_text.insert(tk.END, "\n")

        # Column Mapping
        self.conversion_preview_text.insert(tk.END, "COLUMN TYPE MAPPINGS:\n")
        self.conversion_preview_text.insert(tk.END, "-" * 80 + "\n")
        for src_col, tgt_col in zip(source_schema['columns'], converted_schema['columns']):
            nullable = "NULL" if tgt_col['nullable'] else "NOT NULL"
            self.conversion_preview_text.insert(tk.END,
                f"{src_col['name']:30} {src_col['type']:20} -> {tgt_col['type']:20} {nullable}\n")
        self.conversion_preview_text.insert(tk.END, "\n")

        # Generated DDL
        self.conversion_preview_text.insert(tk.END, "GENERATED CREATE TABLE DDL:\n")
        self.conversion_preview_text.insert(tk.END, "-" * 80 + "\n")
        self.conversion_preview_text.insert(tk.END, ddl + "\n\n")

        # Indexes DDL
        if indexes_ddl:
            self.conversion_preview_text.insert(tk.END, "GENERATED INDEX DDL:\n")
            self.conversion_preview_text.insert(tk.END, "-" * 80 + "\n")
            for idx_ddl in indexes_ddl:
                self.conversion_preview_text.insert(tk.END, idx_ddl + "\n")
            self.conversion_preview_text.insert(tk.END, "\n")

        self.conversion_preview_text.insert(tk.END, "=" * 80 + "\n")
        self.conversion_preview_text.insert(tk.END, "Preview generated successfully!\n")
        self.conversion_preview_text.insert(tk.END, "Click 'Convert Schema' to create table, then 'Transfer Data' to copy data.\n")

    def convert_schema_only(self):
        """Convert schema for selected tables without data"""
        # Get selected tables
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            messagebox.showwarning("Warning", "Please check at least one table!")
            return

        # Validate inputs
        source_conn_name = self.source_conn_combo.get()
        target_conn_name = self.target_conn_combo.get()

        if not all([source_conn_name, target_conn_name]):
            messagebox.showwarning("Warning", "Please select source and target connections!")
            return

        # Show table names with prefix/suffix applied
        prefix = self.target_prefix_entry.get().strip()
        suffix = self.target_suffix_entry.get().strip()
        naming_info = ""
        if prefix or suffix:
            naming_info = f"\nTarget naming: {prefix}<table>{suffix}"

        table_count = len(selected_tables)
        if not messagebox.askyesno("Confirm", f"Convert schema for {table_count} table(s)?\n\nTables: {', '.join(selected_tables[:5])}{'...' if table_count > 5 else ''}{naming_info}\n\nNote: This will create table structures only (no data)."):
            return

        # Start operation
        self._start_conversion_operation()

        self.conversion_status_label.config(text=f"Converting {table_count} table(s)...", foreground="blue")
        self.conversion_progress.start()

        thread = threading.Thread(target=self._convert_multiple_schemas_thread,
                                 args=(source_conn_name, target_conn_name, selected_tables))
        thread.daemon = True
        thread.start()

    def transfer_data_only(self):
        """Transfer data for selected tables from source to target"""
        # Get selected tables
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            messagebox.showwarning("Warning", "Please check at least one table!")
            return

        # Validate inputs
        source_conn_name = self.source_conn_combo.get()
        target_conn_name = self.target_conn_combo.get()

        if not all([source_conn_name, target_conn_name]):
            messagebox.showwarning("Warning", "Please select source and target connections!")
            return

        # Check if target connection exists
        target_manager = self.active_connections.get(target_conn_name)
        if not target_manager:
            messagebox.showerror("Error", "Target connection not found!")
            return

        # Show table names with prefix/suffix applied
        prefix = self.target_prefix_entry.get().strip()
        suffix = self.target_suffix_entry.get().strip()
        naming_info = ""
        if prefix or suffix:
            naming_info = f"\nTarget naming: {prefix}<table>{suffix}"

        table_count = len(selected_tables)
        if not messagebox.askyesno("Confirm", f"Transfer data for {table_count} table(s)?\n\nTables: {', '.join(selected_tables[:5])}{'...' if table_count > 5 else ''}{naming_info}\n\nNote: Target tables must already exist.\nThis may take a while for large tables."):
            return

        # Start operation
        self._start_conversion_operation()

        self.conversion_status_label.config(text=f"Transferring data for {table_count} table(s)...", foreground="blue")
        self.conversion_progress.start()

        thread = threading.Thread(target=self._transfer_multiple_data_thread,
                                 args=(source_conn_name, target_conn_name, selected_tables))
        thread.daemon = True
        thread.start()

    def _convert_schema_thread(self, source_conn_name, target_conn_name, source_table, target_table):
        """Thread for schema conversion only"""
        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]

            converter = SchemaConverter(source_manager, target_manager)

            # Get and convert schema
            source_schema = converter.get_table_schema(source_table)
            if not source_schema:
                self.root.after(0, messagebox.showerror, "Error", f"Could not retrieve schema for {source_table}")
                return

            converted_schema = converter.convert_schema(source_schema)
            converted_schema['table_name'] = target_table  # Use target table name

            # Drop table if option selected
            if self.drop_if_exists_var.get():
                try:
                    target_cursor = target_manager.conn.cursor()
                    target_cursor.execute(f"DROP TABLE IF EXISTS {target_table}")
                    target_manager.conn.commit()
                    target_cursor.close()
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"\n✓ Dropped existing table: {target_table}\n")
                except Exception as e:
                    pass  # Table might not exist

            # Create table
            create_ddl = converter.generate_create_table_ddl(converted_schema)
            target_cursor = target_manager.conn.cursor()
            target_cursor.execute(create_ddl)
            target_manager.conn.commit()
            target_cursor.close()

            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n✓ Created table: {target_table}\n")

            # Create indexes if option selected
            if self.create_indexes_var.get():
                indexes_ddl = converter.generate_indexes_ddl(converted_schema)
                for idx_ddl in indexes_ddl:
                    try:
                        target_cursor = target_manager.conn.cursor()
                        target_cursor.execute(idx_ddl)
                        target_manager.conn.commit()
                        target_cursor.close()
                        self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                      f"✓ Created index\n")
                    except Exception as e:
                        self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                      f"⚠️  Index creation failed: {e}\n")

            # Success
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n{'=' * 80}\n✓ SCHEMA CONVERSION COMPLETE!\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Table structure created. Click 'Transfer Data' to copy data.\n")
            self.root.after(0, messagebox.showinfo, "Success",
                          f"Schema converted successfully!\nTarget table: {target_table}\n\nYou can now transfer data using the 'Transfer Data' button.")

        except Exception as e:
            import traceback
            error_msg = f"Schema conversion failed:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n❌ ERROR: {str(e)}\n")
        finally:
            self.root.after(0, self.conversion_progress.stop)
            self.root.after(0, self.conversion_status_label.config,
                          {'text': 'Schema conversion complete', 'foreground': 'green'})

    def _transfer_data_thread(self, source_conn_name, target_conn_name, source_table, target_table):
        """Thread for data transfer only"""
        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]

            data_converter = DataConverter(source_manager, target_manager)

            batch_size = int(self.batch_size_entry.get())

            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Starting data transfer (batch size: {batch_size})...\n")

            # Progress callback
            def progress_callback(rows_transferred, total_rows):
                if total_rows:
                    percentage = (rows_transferred / total_rows) * 100
                    # Show ~ to indicate estimated total
                    status_text = f"Transferring {source_table}: {rows_transferred:,} / ~{total_rows:,} rows ({percentage:.1f}%)"
                else:
                    status_text = f"Transferring {source_table}: {rows_transferred:,} rows transferred"
                self.root.after(0, self.conversion_status_label.config,
                              {'text': status_text, 'foreground': 'blue'})

            # Transfer data with progress callback and stop event
            rows_transferred = data_converter.transfer_table_data(source_table, target_table, batch_size, progress_callback, self.conversion_stop_event)

            # Validate
            source_count = data_converter.get_row_count(source_table, is_source=True)
            target_count = data_converter.get_row_count(target_table, is_source=False)

            validation_msg = ConversionValidator.validate_data_transfer(source_count, target_count)

            if validation_msg:
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"⚠️  {validation_msg}\n")
                self.root.after(0, messagebox.showwarning, "Warning",
                              f"Data transferred but row counts don't match!\n{validation_msg}")
            else:
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"✓ Transferred {rows_transferred} rows successfully\n")
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"✓ Validation passed: Source={source_count}, Target={target_count}\n")

            # Success
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n{'=' * 80}\n✓ DATA TRANSFER COMPLETE!\n")
            self.root.after(0, messagebox.showinfo, "Success",
                          f"Data transferred successfully!\n\nRows transferred: {rows_transferred}\nSource count: {source_count}\nTarget count: {target_count}")

        except Exception as e:
            import traceback
            error_msg = f"Data transfer failed:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n❌ ERROR: {str(e)}\n")
        finally:
            self.root.after(0, self.conversion_progress.stop)
            self.root.after(0, self.conversion_status_label.config,
                          {'text': 'Data transfer complete', 'foreground': 'green'})

    def _convert_multiple_schemas_thread(self, source_conn_name, target_conn_name, selected_tables):
        """
        Thread for converting multiple table schemas.

        NO TIMEOUT: This operation runs until all tables are converted or stopped by user.
        Schema creation (CREATE TABLE, CREATE INDEX) can take as long as needed.
        """
        total_tables = len(selected_tables)
        successful = []
        failed = []

        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]

            converter = SchemaConverter(source_manager, target_manager)

            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"BATCH SCHEMA CONVERSION\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Converting {total_tables} table(s)...\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n\n")

            for idx, source_table in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"\n⚠️  Conversion stopped by user at table {idx} of {total_tables}\n")
                    break

                try:
                    # Generate target table name with prefix/suffix
                    target_table = self.get_target_table_name(source_table)

                    # Update status
                    self.root.after(0, self.conversion_status_label.config,
                                  {'text': f'Converting table {idx} of {total_tables}: {source_table}',
                                   'foreground': 'blue'})
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"[{idx}/{total_tables}] {source_table} → {target_table}\n")

                    # Get and convert schema
                    source_schema = converter.get_table_schema(source_table)
                    if not source_schema:
                        raise Exception(f"Could not retrieve schema for {source_table}")

                    converted_schema = converter.convert_schema(source_schema)
                    converted_schema['table_name'] = target_table  # Use target name with prefix/suffix

                    # Drop table if option selected
                    if self.drop_if_exists_var.get():
                        try:
                            target_cursor = target_manager.conn.cursor()
                            target_cursor.execute(f"DROP TABLE IF EXISTS {target_table}")
                            target_manager.conn.commit()
                            target_cursor.close()
                            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                          f"  ✓ Dropped existing table: {target_table}\n")
                        except Exception as e:
                            pass  # Table might not exist

                    # Create table
                    create_ddl = converter.generate_create_table_ddl(converted_schema)
                    target_cursor = target_manager.conn.cursor()
                    target_cursor.execute(create_ddl)
                    target_manager.conn.commit()
                    target_cursor.close()

                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  ✓ Created table: {target_table}\n")

                    # Create indexes if option selected
                    if self.create_indexes_var.get():
                        indexes_ddl = converter.generate_indexes_ddl(converted_schema)
                        for idx_ddl in indexes_ddl:
                            try:
                                target_cursor = target_manager.conn.cursor()
                                target_cursor.execute(idx_ddl)
                                target_manager.conn.commit()
                                target_cursor.close()
                                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                              f"  ✓ Created index\n")
                            except Exception as e:
                                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                              f"  ⚠️  Index creation failed: {e}\n")

                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  ✓ SUCCESS: {source_table} → {target_table}\n\n")
                    successful.append(f"{source_table} → {target_table}")

                except Exception as e:
                    import traceback
                    error_detail = str(e)
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  ❌ FAILED: {source_table}\n")
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"     Error: {error_detail}\n\n")
                    failed.append((source_table, error_detail))

            # Final summary
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"BATCH CONVERSION COMPLETE\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Total tables: {total_tables}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"✓ Successful: {len(successful)}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"❌ Failed: {len(failed)}\n")

            if successful:
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"\nSuccessful tables:\n")
                for table in successful:
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  • {table}\n")

            if failed:
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"\nFailed tables:\n")
                for table, error in failed:
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  • {table}: {error}\n")

            # Show summary dialog
            if len(failed) == 0:
                self.root.after(0, messagebox.showinfo, "Batch Conversion Complete",
                              f"All {len(successful)} table(s) converted successfully!\n\nYou can now transfer data using the 'Transfer Data' button.")
            else:
                self.root.after(0, messagebox.showwarning, "Batch Conversion Complete",
                              f"Conversion complete with some errors:\n\n✓ Successful: {len(successful)}\n❌ Failed: {len(failed)}\n\nCheck the preview area for details.")

        except Exception as e:
            import traceback
            error_msg = f"Batch schema conversion failed:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n❌ BATCH ERROR: {str(e)}\n")
        finally:
            self.root.after(0, self.conversion_progress.stop)
            if self.conversion_stop_event.is_set():
                status_text = f'Conversion stopped: {len(successful)} completed before stop'
                status_color = 'orange'
            else:
                status_text = f'Batch conversion complete: {len(successful)} successful, {len(failed)} failed'
                status_color = 'green' if len(failed) == 0 else 'orange'
            self.root.after(0, self.conversion_status_label.config,
                          {'text': status_text, 'foreground': status_color})
            self.root.after(0, self._end_conversion_operation)

    def _transfer_multiple_data_thread(self, source_conn_name, target_conn_name, selected_tables):
        """
        Thread for transferring data for multiple tables.

        NO TIMEOUT: This operation runs until all data is transferred or stopped by user.
        Data transfer (INSERT statements) can take as long as needed, even for millions of rows.
        Progress is reported after each batch commit.
        """
        total_tables = len(selected_tables)
        successful = []
        failed = []
        total_rows = 0

        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]

            data_converter = DataConverter(source_manager, target_manager)
            batch_size = int(self.batch_size_entry.get())

            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"BATCH DATA TRANSFER\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Transferring data for {total_tables} table(s) (batch size: {batch_size})...\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n\n")

            for idx, source_table in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"\n⚠️  Data transfer stopped by user at table {idx} of {total_tables}\n")
                    break

                try:
                    # Generate target table name with prefix/suffix
                    target_table = self.get_target_table_name(source_table)

                    # Initial status
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"[{idx}/{total_tables}] {source_table} → {target_table}\n")

                    # Progress callback for this table
                    def progress_callback(rows_transferred_current, total_rows_current):
                        if total_rows_current:
                            percentage = (rows_transferred_current / total_rows_current) * 100
                            # Show ~ to indicate estimated total
                            status_text = f"[{idx}/{total_tables}] {source_table}: {rows_transferred_current:,} / ~{total_rows_current:,} rows ({percentage:.1f}%)"
                        else:
                            status_text = f"[{idx}/{total_tables}] {source_table}: {rows_transferred_current:,} rows"
                        self.root.after(0, self.conversion_status_label.config,
                                      {'text': status_text, 'foreground': 'blue'})

                    # Transfer data with progress callback and stop event
                    rows_transferred = data_converter.transfer_table_data(source_table, target_table, batch_size, progress_callback, self.conversion_stop_event)

                    # Validate
                    source_count = data_converter.get_row_count(source_table, is_source=True)
                    target_count = data_converter.get_row_count(target_table, is_source=False)

                    validation_msg = ConversionValidator.validate_data_transfer(source_count, target_count)

                    if validation_msg:
                        self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                      f"  ⚠️  {validation_msg}\n")
                        self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                      f"  Rows transferred: {rows_transferred} (Source: {source_count}, Target: {target_count})\n\n")
                        failed.append((f"{source_table} → {target_table}", validation_msg))
                    else:
                        self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                      f"  ✓ Transferred {rows_transferred} rows successfully\n")
                        self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                      f"  ✓ Validation passed (Source: {source_count}, Target: {target_count})\n\n")
                        successful.append((f"{source_table} → {target_table}", rows_transferred))
                        total_rows += rows_transferred

                except Exception as e:
                    import traceback
                    error_detail = str(e)
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  ❌ FAILED: {source_table}\n")
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"     Error: {error_detail}\n\n")
                    failed.append((source_table, error_detail))

            # Final summary
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"BATCH DATA TRANSFER COMPLETE\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Total tables: {total_tables}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"✓ Successful: {len(successful)}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"❌ Failed: {len(failed)}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Total rows transferred: {total_rows:,}\n")

            if successful:
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"\nSuccessful tables:\n")
                for table, rows in successful:
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  • {table}: {rows:,} rows\n")

            if failed:
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"\nFailed/Warning tables:\n")
                for table, error in failed:
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  • {table}: {error}\n")

            # Show summary dialog
            if len(failed) == 0:
                self.root.after(0, messagebox.showinfo, "Batch Transfer Complete",
                              f"All {len(successful)} table(s) transferred successfully!\n\nTotal rows: {total_rows:,}")
            else:
                self.root.after(0, messagebox.showwarning, "Batch Transfer Complete",
                              f"Transfer complete with some errors:\n\n✓ Successful: {len(successful)}\n❌ Failed/Warning: {len(failed)}\n\nTotal rows transferred: {total_rows:,}\n\nCheck the preview area for details.")

        except Exception as e:
            import traceback
            error_msg = f"Batch data transfer failed:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"\n❌ BATCH ERROR: {str(e)}\n")
        finally:
            self.root.after(0, self.conversion_progress.stop)
            if self.conversion_stop_event.is_set():
                status_text = f'Transfer stopped: {len(successful)} completed, {total_rows:,} rows transferred'
                status_color = 'orange'
            else:
                status_text = f'Batch transfer complete: {len(successful)} successful, {len(failed)} failed'
                status_color = 'green' if len(failed) == 0 else 'orange'
            self.root.after(0, self.conversion_status_label.config,
                          {'text': status_text, 'foreground': status_color})
            self.root.after(0, self._end_conversion_operation)

    def clear_conversion_preview(self):
        """Clear conversion preview text"""
        self.conversion_preview_text.delete(1.0, tk.END)
        self.conversion_status_label.config(text="Ready", foreground="blue")

    def show_row_counts(self):
        """Show row counts for all selected tables"""
        # Get selected tables
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            messagebox.showwarning("Warning", "Please select at least one table!")
            return

        # Validate source connection
        source_conn_name = self.source_conn_combo.get()
        if not source_conn_name:
            messagebox.showwarning("Warning", "Please select source connection!")
            return

        if source_conn_name not in self.active_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        try:
            source_manager = self.active_connections[source_conn_name]

            # Start operation
            self._start_conversion_operation()

            self.conversion_status_label.config(text=f"Getting row counts for {len(selected_tables)} table(s)...", foreground="blue")
            self.conversion_progress.start()

            # Clear preview area first
            self.conversion_preview_text.delete(1.0, tk.END)

            # Run in thread
            thread = threading.Thread(target=self._get_row_counts_thread,
                                     args=(source_manager, selected_tables))
            thread.daemon = True
            thread.start()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to get row counts:\n{str(e)}")
            self.conversion_status_label.config(text="Failed to get row counts", foreground="red")
            self._end_conversion_operation()

    def _get_row_counts_thread(self, source_manager, selected_tables):
        """Thread for getting row counts"""
        total_tables = len(selected_tables)
        results = []

        try:
            # Header
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"ROW COUNTS\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Checking {total_tables} table(s)\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n\n")

            cursor = source_manager.conn.cursor()

            for idx, table_name in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"\n⚠️  Stopped by user at table {idx} of {total_tables}\n")
                    break

                try:
                    # Update status
                    self.root.after(0, self.conversion_status_label.config,
                                  {'text': f'Counting rows {idx} of {total_tables}: {table_name}',
                                   'foreground': 'blue'})

                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    result = cursor.fetchone()
                    row_count = result[0] if result else 0

                    results.append((table_name, row_count))

                    # Display result
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"[{idx}/{total_tables}] {table_name}: {row_count:,} rows\n")

                except Exception as e:
                    error_msg = str(e)
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"[{idx}/{total_tables}] {table_name}: ❌ ERROR - {error_msg}\n")

            cursor.close()

            # Summary
            if results:
                total_rows = sum(count for _, count in results)
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"\n{'=' * 80}\n")
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"SUMMARY\n")
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"{'=' * 80}\n")
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"Total tables: {len(results)}\n")
                self.root.after(0, self.conversion_preview_text.insert, tk.END,
                              f"Total rows: {total_rows:,}\n")

        except Exception as e:
            import traceback
            error_msg = f"Failed to get row counts:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
        finally:
            self.root.after(0, self.conversion_progress.stop)
            status_text = 'Row counts stopped' if self.conversion_stop_event.is_set() else f'Row counts complete for {len(results)} table(s)'
            status_color = 'orange' if self.conversion_stop_event.is_set() else 'green'
            self.root.after(0, self.conversion_status_label.config,
                          {'text': status_text, 'foreground': status_color})
            self.root.after(0, self._end_conversion_operation)

    def show_sample_data(self):
        """Show sample data (one row) from each selected table"""
        # Get selected tables
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            messagebox.showwarning("Warning", "Please select at least one table!")
            return

        # Validate source connection
        source_conn_name = self.source_conn_combo.get()
        if not source_conn_name:
            messagebox.showwarning("Warning", "Please select source connection!")
            return

        if source_conn_name not in self.active_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        try:
            source_manager = self.active_connections[source_conn_name]

            # Start operation
            self._start_conversion_operation()

            self.conversion_status_label.config(text=f"Getting sample data for {len(selected_tables)} table(s)...", foreground="blue")
            self.conversion_progress.start()

            # Clear preview area first
            self.conversion_preview_text.delete(1.0, tk.END)

            # Run in thread
            thread = threading.Thread(target=self._get_sample_data_thread,
                                     args=(source_manager, selected_tables))
            thread.daemon = True
            thread.start()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to get sample data:\n{str(e)}")
            self.conversion_status_label.config(text="Failed to get sample data", foreground="red")
            self._end_conversion_operation()

    def _get_sample_data_thread(self, source_manager, selected_tables):
        """Thread for getting sample data"""
        total_tables = len(selected_tables)
        results_count = 0

        try:
            # Header
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"SAMPLE DATA (First Row from Each Table)\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Checking {total_tables} table(s)\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n\n")

            cursor = source_manager.conn.cursor()

            for idx, table_name in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"\n⚠️  Stopped by user at table {idx} of {total_tables}\n")
                    break

                try:
                    # Update status
                    self.root.after(0, self.conversion_status_label.config,
                                  {'text': f'Getting sample data {idx} of {total_tables}: {table_name}',
                                   'foreground': 'blue'})

                    # Get one row with column names
                    if source_manager.db_type == "Oracle":
                        cursor.execute(f"SELECT * FROM {table_name} WHERE ROWNUM <= 1")
                    elif source_manager.db_type == "PostgreSQL":
                        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                    else:  # MySQL/MariaDB
                        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")

                    row = cursor.fetchone()

                    # Display table header
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"[{idx}/{total_tables}] {table_name}\n")
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"{'-' * 80}\n")

                    if row:
                        # Get column names
                        column_names = [desc[0] for desc in cursor.description]

                        # Display column names and values
                        for col_name, col_value in zip(column_names, row):
                            # Handle different data types for display
                            if col_value is None:
                                display_value = "NULL"
                            elif isinstance(col_value, (bytes, bytearray)):
                                display_value = f"<binary data, {len(col_value)} bytes>"
                            elif isinstance(col_value, str) and len(col_value) > 100:
                                display_value = col_value[:100] + "..."
                            else:
                                display_value = str(col_value)

                            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                          f"  {col_name}: {display_value}\n")

                        results_count += 1
                    else:
                        self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                      f"  (No data in table)\n")

                    self.root.after(0, self.conversion_preview_text.insert, tk.END, f"\n")

                except Exception as e:
                    error_msg = str(e)
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"[{idx}/{total_tables}] {table_name}\n")
                    self.root.after(0, self.conversion_preview_text.insert, tk.END,
                                  f"  ❌ ERROR: {error_msg}\n\n")

            cursor.close()

            # Summary
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"{'=' * 80}\n")
            self.root.after(0, self.conversion_preview_text.insert, tk.END,
                          f"Sample data retrieved from {results_count} table(s)\n")

        except Exception as e:
            import traceback
            error_msg = f"Failed to get sample data:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
        finally:
            self.root.after(0, self.conversion_progress.stop)
            status_text = 'Sample data stopped' if self.conversion_stop_event.is_set() else f'Sample data complete for {results_count} table(s)'
            status_color = 'orange' if self.conversion_stop_event.is_set() else 'green'
            self.root.after(0, self.conversion_status_label.config,
                          {'text': status_text, 'foreground': status_color})
            self.root.after(0, self._end_conversion_operation)

    def stop_conversion_operation(self):
        """Stop the currently running conversion operation"""
        if self.conversion_running:
            if messagebox.askyesno("Confirm Stop", "Are you sure you want to stop the current operation?\n\nNote: The current batch will complete before stopping."):
                self.conversion_stop_event.set()
                self.conversion_status_label.config(text="Stopping operation...", foreground="orange")
                self.conversion_preview_text.insert(tk.END, "\n⚠️  Stop requested by user. Completing current batch...\n")
        else:
            messagebox.showinfo("Info", "No operation is currently running.")

    def _start_conversion_operation(self):
        """Called when starting any conversion operation"""
        self.conversion_running = True
        self.conversion_stop_event.clear()
        # Disable operation buttons
        self.preview_schema_btn.config(state=tk.DISABLED)
        self.row_counts_btn.config(state=tk.DISABLED)
        self.sample_data_btn.config(state=tk.DISABLED)
        self.convert_schema_btn.config(state=tk.DISABLED)
        self.transfer_data_btn.config(state=tk.DISABLED)
        self.stop_conversion_btn.config(state=tk.NORMAL)

    def _end_conversion_operation(self):
        """Called when conversion operation completes or is stopped"""
        self.conversion_running = False
        self.conversion_stop_event.clear()
        # Re-enable operation buttons
        self.preview_schema_btn.config(state=tk.NORMAL)
        self.row_counts_btn.config(state=tk.NORMAL)
        self.sample_data_btn.config(state=tk.NORMAL)
        self.convert_schema_btn.config(state=tk.NORMAL)
        self.transfer_data_btn.config(state=tk.NORMAL)
        self.stop_conversion_btn.config(state=tk.DISABLED)

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
        lines = text.strip().split('\n')
        all_commented = all(
            line.strip().startswith('/*') and line.strip().endswith('*/')
            for line in lines if line.strip()
        )

        if all_commented:
            # Extract SQL from within comments
            result = []
            for line in lines:
                line = line.strip()
                if line.startswith('/*') and line.endswith('*/'):
                    # Remove /* and */ and keep the SQL
                    sql_part = line[2:-2].strip()
                    if sql_part:
                        result.append(sql_part)
            return '\n'.join(result)

        # Check if text contains any markdown code fences
        has_markdown = '```' in text

        if not has_markdown:
            # No markdown detected - treat entire text as SQL, return as-is
            return text.strip()

        # Process markdown: extract code from fences, comment out other text
        result = []
        in_code_block = False
        lines = text.split('\n')

        for line in lines:
            line_stripped = line.strip()

            # Check if this is a code fence (``` or ```sql or ```SQL)
            if line_stripped.startswith('```'):
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
                        result.append('')

        # Join all lines
        final_sql = '\n'.join(result)

        # Clean up any "/* sql */" or "/* */" artifacts
        final_sql = re.sub(r'/\*\s*sql\s*\*/', '', final_sql, flags=re.IGNORECASE)
        final_sql = re.sub(r'/\*\s*\*/', '', final_sql)

        # Clean up multiple empty lines
        final_sql = re.sub(r'\n\n\n+', '\n\n', final_sql)

        return final_sql.strip()


def main():
    root = tk.Tk()

    # Check if any database modules are available
    if not DatabaseConfig.get_db_types():
        messagebox.showerror(
            "No Database Modules",
            "No database modules available!\n\nPlease ensure either conOracle.py or conMysql.py is available."
        )
        return

    app = UnifiedDBManagerUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
