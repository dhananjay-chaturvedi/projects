#-------------------------------------------------------------------------------
#description: Server monitor manager for the tool
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
import subprocess
import os
import tempfile
import re

from monitor_connection_manager import MonitorConnectionManager
from metrics_visualizer import MetricsVisualizer
from database_registry import DatabaseRegistry
from ui import bind_canvas_mousewheel
from config_loader import config, properties, console_print



class ServerMonitorUI:
    """Server Monitor UI Module - Manages SSH server monitoring and database performance monitoring"""
    
    def __init__(self, parent_frame, root, connection_manager, active_connections, 
                 update_status_callback, theme):
        """
        Initialize Server Monitor UI
        
        Args:
            parent_frame: tk.Frame to contain the monitor UI
            root: Main window reference for dialogs and after()
            connection_manager: ConnectionManager for database connections
            active_connections: Dict of active database connections
            update_status_callback: Callback function(msg, type) for status updates
            theme: ColorTheme class for styling
        """
        self.parent = parent_frame
        self.root = root
        self.connection_manager = connection_manager
        self.active_connections = active_connections
        self.update_status = update_status_callback
        self.theme = theme
        
        # Initialize monitor connection manager
        self.monitor_connection_manager = MonitorConnectionManager()

        # SSH configuration from config
        self.ssh_timeout = config.get_int('ssh.connection', 'ssh_timeout', default=30)
        self.ssh_test_timeout = config.get_int('ssh.connection', 'ssh_test_timeout', default=5)
        self.ssh_control_persist = config.get_int('ssh.connection', 'ssh_control_persist', default=600)

        # Monitoring refresh interval
        self.refresh_interval = config.get_int('monitoring', 'metrics_refresh_interval', default=5000)

        # Server monitoring state
        self.monitor_connections = {}  # {name: {'host': ..., 'username': ..., 'monitoring': bool}}
        self.servers_pending_removal = set()
        self.active_server_query_threads = {}
        
        # Database monitoring state
        self.monitored_databases = {}  # {db_name: db_manager}
        self.databases_pending_removal = set()
        self.active_db_query_threads = {}
        self._db_metrics_cache = {}
        
        # View modes
        self.os_view_mode = 'text'  # 'text' or 'graph'
        self.db_view_mode = 'text'  # 'text' or 'graph'
        
        # Update job
        self.monitor_update_job = None
        
        # Thread safety
        self.db_query_lock = threading.Lock()
        
        # UI widgets (initialized in create_ui)
        self.monitor_conn_listbox = None
        self.monitored_db_listbox = None
        self.monitor_status_label = None
        self.os_metrics_text = None
        self.os_view_container = None
        self.os_metrics_visualizer = None
        self.db_metrics_text = None
        self.db_view_container = None
        self.db_metrics_visualizer = None

    def create_ui(self):
        """Create the complete server monitor UI"""
        """Create UI for server monitoring tab"""
        # Main horizontal paned window
        main_paned = ttk.PanedWindow(self.parent, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # LEFT PANEL - Scrollable container for controls
        left_panel = ttk.Frame(main_paned)
        left_panel.config(width=250)  # Preferred width
        main_paned.add(left_panel, weight=1)  # Allow left panel to resize

        # Create scrollable container for left panel
        left_canvas = tk.Canvas(left_panel, highlightthickness=0, bd=0)
        left_scrollbar = ttk.Scrollbar(left_panel, orient=tk.VERTICAL, command=left_canvas.yview)
        left_scrollable = ttk.Frame(left_canvas)

        left_canvas_window = left_canvas.create_window((0, 0), window=left_scrollable, anchor=tk.NW)
        left_canvas.configure(yscrollcommand=left_scrollbar.set)

        def update_left_scroll(event=None):
            left_canvas.configure(scrollregion=left_canvas.bbox("all"))

        def on_left_canvas_resize(event):
            """Expand left_scrollable to canvas width when canvas resizes"""
            canvas_width = event.width
            left_canvas.itemconfig(left_canvas_window, width=canvas_width)
            # Update scroll region when canvas resizes to ensure scrollbar appears
            left_canvas.after(10, update_left_scroll)

        left_scrollable.bind("<Configure>", update_left_scroll)
        left_canvas.bind("<Configure>", on_left_canvas_resize)

        left_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        left_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Enable mousewheel scrolling (cross-platform)
        bind_canvas_mousewheel(left_canvas)

        # TOP: Server Monitor Connections
        server_frame = ttk.LabelFrame(left_scrollable, text="Server Connections", padding="10")
        server_frame.pack(fill=tk.X, pady=(0, 10))
        server_frame.config(height=300)  # Minimum height to ensure scrollbar appears

        # Connection list
        list_frame = ttk.Frame(server_frame, height=120)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        list_frame.pack_propagate(False)  # Maintain minimum height

        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL)
        self.monitor_conn_listbox = tk.Listbox(
            list_frame,
            yscrollcommand=scrollbar.set,
            bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_PRIMARY,
            selectbackground=self.theme.PRIMARY,
            selectforeground="white",
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=self.theme.BORDER
        )
        scrollbar.config(command=self.monitor_conn_listbox.yview)

        self.monitor_conn_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.monitor_conn_listbox.bind('<<ListboxSelect>>', self.on_monitor_connection_selected)

        # Server Buttons
        btn_frame = ttk.Frame(server_frame)
        btn_frame.pack(fill=tk.X)

        ttk.Button(btn_frame, text="Add Connection", command=self.add_monitor_connection, width=20).pack(pady=2, fill=tk.X)
        ttk.Button(btn_frame, text="Select Server", command=self.select_server_to_monitor, width=20, style="Success.TButton").pack(pady=2, fill=tk.X)
        ttk.Button(btn_frame, text="Remove Server", command=self.remove_monitor_connection, width=20, style="Warning.TButton").pack(pady=2, fill=tk.X)

        # BOTTOM: Database Management
        db_mgmt_frame = ttk.LabelFrame(left_scrollable, text="Database Monitoring", padding="10")
        db_mgmt_frame.pack(fill=tk.X, pady=(0, 10))
        db_mgmt_frame.config(height=280)  # Minimum height to ensure scrollbar appears

        # Monitored databases list
        db_list_frame = ttk.Frame(db_mgmt_frame, height=120)
        db_list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        db_list_frame.pack_propagate(False)  # Maintain minimum height

        db_scrollbar = ttk.Scrollbar(db_list_frame, orient=tk.VERTICAL)
        self.monitored_db_listbox = tk.Listbox(
            db_list_frame,
            yscrollcommand=db_scrollbar.set,
            bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_PRIMARY,
            selectbackground=self.theme.PRIMARY,
            selectforeground="white",
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=self.theme.BORDER
        )
        db_scrollbar.config(command=self.monitored_db_listbox.yview)

        self.monitored_db_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        db_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Database Buttons
        db_btn_frame = ttk.Frame(db_mgmt_frame)
        db_btn_frame.pack(fill=tk.X)

        ttk.Button(db_btn_frame, text="Select Database", command=self.add_db_to_monitor, width=20, style="Success.TButton").pack(pady=2, fill=tk.X)
        ttk.Button(db_btn_frame, text="Remove Database", command=self.remove_db_from_monitor, width=20, style="Warning.TButton").pack(pady=2, fill=tk.X)

        # RIGHT PANEL - Metrics Display
        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=3)  # Give more weight to expand more

        # Status bar at top right
        status_frame = ttk.Frame(right_frame)
        status_frame.pack(fill=tk.X, pady=(0, 5))

        self.monitor_status_label = ttk.Label(status_frame, text="Status: No active monitoring", foreground="gray")
        self.monitor_status_label.pack(side=tk.LEFT, padx=5)

        # Vertical split: OS metrics (top) and DB metrics (bottom)
        metrics_paned = ttk.PanedWindow(right_frame, orient=tk.VERTICAL)
        metrics_paned.pack(fill=tk.BOTH, expand=True)

        # OS Resources Frame
        os_frame = ttk.LabelFrame(metrics_paned, text="OS Resources", padding="10")
        metrics_paned.add(os_frame, weight=1)

        # OS Control buttons
        os_control_frame = ttk.Frame(os_frame)
        os_control_frame.pack(fill=tk.X, pady=(0, 5))

        ttk.Button(os_control_frame, text="📊 Show Graphs", command=lambda: self.toggle_os_view('graph')).pack(side=tk.LEFT, padx=2)
        ttk.Button(os_control_frame, text="📝 Show Text", command=lambda: self.toggle_os_view('text')).pack(side=tk.LEFT, padx=2)
        ttk.Button(os_control_frame, text="Clear Graphs", command=self.clear_os_graphs).pack(side=tk.LEFT, padx=2)
        ttk.Button(os_control_frame, text="Refresh", command=self.refresh_server_metrics).pack(side=tk.LEFT, padx=2)

        # Container for text/graph views
        self.os_view_container = ttk.Frame(os_frame)
        self.os_view_container.pack(fill=tk.BOTH, expand=True)

        # Text view
        self.os_metrics_text = scrolledtext.ScrolledText(
            self.os_view_container,
            wrap=tk.WORD,
            font=("Courier", 10),
            bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_PRIMARY,
            insertbackground=self.theme.PRIMARY,
            relief=tk.FLAT,
            borderwidth=1
        )
        self.os_metrics_text.pack(fill=tk.BOTH, expand=True)
        self.os_metrics_text.insert(1.0, "Select a monitoring connection and start monitoring to view OS resources...")
        self.os_metrics_text.config(state=tk.DISABLED)

        # Graph view (hidden initially)
        self.os_metrics_visualizer = MetricsVisualizer(self.os_view_container, title="OS Metrics")
        # Enable mousewheel scrolling for OS graphs
        bind_canvas_mousewheel(self.os_metrics_visualizer.canvas)

        # DB Resources Frame
        db_frame = ttk.LabelFrame(metrics_paned, text="Database Metrics", padding="10")
        metrics_paned.add(db_frame, weight=1)

        # DB View control buttons
        db_view_control_frame = ttk.Frame(db_frame)
        db_view_control_frame.pack(fill=tk.X, pady=(0, 5))

        ttk.Button(db_view_control_frame, text="📊 Show Graphs", command=lambda: self.toggle_db_view('graph')).pack(side=tk.LEFT, padx=2)
        ttk.Button(db_view_control_frame, text="📝 Show Text", command=lambda: self.toggle_db_view('text')).pack(side=tk.LEFT, padx=2)
        ttk.Button(db_view_control_frame, text="Clear Graphs", command=self.clear_db_graphs).pack(side=tk.LEFT, padx=2)
        ttk.Button(db_view_control_frame, text="Refresh", command=self.refresh_monitor_db_list).pack(side=tk.LEFT, padx=2)

        # Container for text/graph views
        self.db_view_container = ttk.Frame(db_frame)
        self.db_view_container.pack(fill=tk.BOTH, expand=True)

        # Text view
        self.db_metrics_text = scrolledtext.ScrolledText(
            self.db_view_container,
            wrap=tk.WORD,
            font=("Courier", 10),
            bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_PRIMARY,
            insertbackground=self.theme.PRIMARY,
            relief=tk.FLAT,
            borderwidth=1
        )
        self.db_metrics_text.pack(fill=tk.BOTH, expand=True)
        self.db_metrics_text.insert(1.0, "Click 'Add Database' to start monitoring databases...")
        self.db_metrics_text.config(state=tk.DISABLED)

        # Graph view (hidden initially)
        self.db_metrics_visualizer = MetricsVisualizer(self.db_view_container, title="Database Metrics")
        # Enable mousewheel scrolling for DB graphs
        bind_canvas_mousewheel(self.db_metrics_visualizer.canvas)

        # Clear any stale graphs from previous sessions (in case monitoring tab is recreated)
        if not self.monitored_databases:
            self.db_metrics_visualizer.clear_all()

        # Start periodic updates
        self.start_monitor_updates()

    def add_monitor_connection(self):
        """Add a new monitoring connection"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Monitor Connection")
        dialog.geometry("450x350")
        dialog.resizable(True, True)
        dialog.transient(self.root)
        dialog.grab_set()

        # Create scrollable container
        canvas = tk.Canvas(dialog, highlightthickness=0, bd=0)
        scrollbar = ttk.Scrollbar(dialog, orient=tk.VERTICAL, command=canvas.yview)
        main_frame = ttk.Frame(canvas, padding="20")

        canvas.create_window((0, 0), window=main_frame, anchor=tk.NW)
        canvas.configure(yscrollcommand=scrollbar.set)

        def on_frame_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        main_frame.bind("<Configure>", on_frame_configure)

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Enable mousewheel scrolling
        bind_canvas_mousewheel(canvas)

        ttk.Label(main_frame, text="Add Server Monitoring Connection", font=("Arial", 12, "bold")).pack(pady=(0, 20))

        # Connection Name
        ttk.Label(main_frame, text="Connection Name:").pack(anchor=tk.W, pady=(5, 2))
        name_entry = ttk.Entry(main_frame, width=40)
        name_entry.pack(fill=tk.X, pady=(0, 10))

        # SSH Details
        ttk.Label(main_frame, text="Hostname or IP:").pack(anchor=tk.W, pady=(5, 2))
        host_entry = ttk.Entry(main_frame, width=40)
        host_entry.pack(fill=tk.X, pady=(0, 10))
        host_entry.insert(0, "localhost")

        ttk.Label(main_frame, text="SSH Username:").pack(anchor=tk.W, pady=(5, 2))
        user_entry = ttk.Entry(main_frame, width=40)
        user_entry.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(main_frame, text="Password (optional):").pack(anchor=tk.W, pady=(5, 2))
        ttk.Label(main_frame, text="For SSH/jump server/sudo as needed", font=("Arial", 8), foreground="gray").pack(anchor=tk.W, pady=(0, 2))
        password_entry = ttk.Entry(main_frame, width=40, show="*")
        password_entry.pack(fill=tk.X, pady=(0, 10))

        def test_connection():
            """Test the SSH connection with provided credentials"""
            host = host_entry.get().strip()
            username = user_entry.get().strip()
            password = password_entry.get()

            if not all([host, username]):
                messagebox.showerror("Error", "Please fill hostname and username to test!")
                return

            self.update_status(f"Testing connection to {host}... (timeout: 30s)")

            # Build SSH command based on password configuration
            if password:
                # Single password - use sshpass
                ssh_cmd = [
                    'sshpass', '-p', password,
                    'ssh',
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    '-o', f'ConnectTimeout={self.ssh_timeout}',
                    f"{username}@{host}",
                    'echo SSH_OK'
                ]

                try:
                    result = subprocess.run(
                        ssh_cmd,
                        capture_output=True,
                        text=True,
                        timeout=self.ssh_timeout + 5
                    )

                    if result.returncode == 0 and 'SSH_OK' in result.stdout:
                        messagebox.showinfo("Success", f"✓ SSH connection successful to {username}@{host}")
                        self.update_status("Connection test successful", "success")
                    else:
                        error_msg = "SSH connection failed!"
                        if result.returncode != 0:
                            # Only show stderr if command actually failed (non-zero exit code)
                            if 'sshpass' in result.stderr:
                                error_msg += "\n\nNote: 'sshpass' not found. Install it using:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)"
                            elif result.stderr:
                                # Filter out common SSH warnings that aren't actual errors
                                stderr_filtered = '\n'.join([line for line in result.stderr.split('\n')
                                                            if not line.startswith('Warning: Permanently added')])
                                if stderr_filtered.strip():
                                    error_msg += f"\n\nError: {stderr_filtered[:300]}"
                        messagebox.showerror("Error", error_msg)
                        self.update_status("Connection test failed", "error")

                except subprocess.TimeoutExpired:
                    error_msg = f"SSH connection timeout after 30 seconds!\n\nThe server '{host}' is not responding."
                    messagebox.showerror("Connection Timeout", error_msg)
                    self.update_status(f"SSH test timeout for {host}", "error")
                except FileNotFoundError as e:
                    if 'sshpass' in str(e):
                        messagebox.showerror("Error", "sshpass not found!\n\nTo use passwords for testing, install sshpass:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)")
                    else:
                        messagebox.showerror("Error", f"SSH error: {str(e)}")
                    self.update_status("Connection test error", "error")
                except Exception as e:
                    messagebox.showerror("Error", f"SSH error: {str(e)}")
                    self.update_status("Connection test error", "error")

            else:
                # No password - will use SSH key or prompt
                ssh_cmd = [
                    'ssh',
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    '-o', f'ConnectTimeout={self.ssh_timeout}',
                    f"{username}@{host}",
                    'echo SSH_OK'
                ]

                try:
                    result = subprocess.run(
                        ssh_cmd,
                        capture_output=True,
                        text=True,
                        timeout=self.ssh_timeout
                    )

                    if result.returncode == 0 and 'SSH_OK' in result.stdout:
                        messagebox.showinfo("Success", f"✓ SSH connection successful to {username}@{host}")
                        self.update_status("Connection test successful", "success")
                    else:
                        error_msg = "SSH connection failed!"
                        if result.returncode != 0 and result.stderr:
                            # Only show stderr if command actually failed (non-zero exit code)
                            # Filter out common SSH warnings that aren't actual errors
                            stderr_filtered = '\n'.join([line for line in result.stderr.split('\n')
                                                        if not line.startswith('Warning: Permanently added')])
                            if stderr_filtered.strip():
                                error_msg += f"\n\nError: {stderr_filtered[:200]}"
                        messagebox.showerror("Error", error_msg)
                        self.update_status("Connection test failed", "error")

                except subprocess.TimeoutExpired:
                    error_msg = f"SSH connection timeout after 30 seconds!\n\nThe server '{host}' is not responding."
                    messagebox.showerror("Connection Timeout", error_msg)
                    self.update_status(f"SSH test timeout for {host}", "error")
                except Exception as e:
                    messagebox.showerror("Error", f"SSH error: {str(e)}")
                    self.update_status("Connection test error", "error")

        def add_connection_only():
            """Add connection to list and start monitoring immediately"""
            name = name_entry.get().strip()
            host = host_entry.get().strip()
            username = user_entry.get().strip()
            password = password_entry.get()  # Can be empty

            if not all([name, host, username]):
                messagebox.showerror("Error", "Please fill all required fields!")
                return

            if name in self.monitor_connections:
                messagebox.showerror("Error", f"Connection '{name}' already exists!")
                return

            self.monitor_connections[name] = {
                'host': host,
                'username': username,
                'password': password,
                'monitoring': False
            }

            self.monitor_conn_listbox.insert(tk.END, f"{name} [{username}@{host}]")
            dialog.destroy()

            # Start monitoring immediately
            self._start_monitoring_for_server(name)

        def save_connection_persistent():
            """Save connection to persistent storage only (not added to list)"""
            name = name_entry.get().strip()
            host = host_entry.get().strip()
            username = user_entry.get().strip()
            password = password_entry.get()  # Can be empty

            if not all([name, host, username]):
                messagebox.showerror("Error", "Please fill all required fields!")
                return

            # Save to persistent storage only
            success, msg = self.monitor_connection_manager.add_connection(
                name=name,
                host=host,
                username=username,
                password=password
            )

            dialog.destroy()

            if success:
                messagebox.showinfo("Success", f"Connection '{name}' saved!\n\nUse 'Select Server' to start monitoring this server.")
                self.update_status(f"✓ Connection '{name}' saved", "success")
            else:
                messagebox.showerror("Error", f"Failed to save connection:\n{msg}")

        # Buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(btn_frame, text="Test Connection", command=test_connection).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Add", command=add_connection_only, style="Primary.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Save Connection", command=save_connection_persistent).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT, padx=5)

        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")

    def edit_monitor_connection(self, conn_name):
        """Edit an existing saved monitor connection"""
        # Get connection details
        conn = self.monitor_connection_manager.get_connection(conn_name)
        if not conn:
            messagebox.showerror("Error", f"Connection '{conn_name}' not found!")
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Edit Monitor Connection")
        dialog.geometry("450x400")
        dialog.resizable(True, True)
        dialog.transient(self.root)
        dialog.grab_set()

        # Create scrollable container
        canvas = tk.Canvas(dialog, highlightthickness=0, bd=0)
        scrollbar = ttk.Scrollbar(dialog, orient=tk.VERTICAL, command=canvas.yview)
        main_frame = ttk.Frame(canvas, padding="20")

        canvas.create_window((0, 0), window=main_frame, anchor=tk.NW)
        canvas.configure(yscrollcommand=scrollbar.set)

        def on_frame_configure(event):
            canvas.configure(scrollregion=canvas.bbox("all"))

        main_frame.bind("<Configure>", on_frame_configure)

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Enable mousewheel scrolling
        bind_canvas_mousewheel(canvas)

        ttk.Label(main_frame, text="Edit Server Monitoring Connection", font=("Arial", 12, "bold")).pack(pady=(0, 20))

        # Connection Name (read-only)
        ttk.Label(main_frame, text="Connection Name:").pack(anchor=tk.W, pady=(5, 2))
        name_entry = ttk.Entry(main_frame, width=40)
        name_entry.insert(0, conn['name'])
        name_entry.config(state="readonly")
        name_entry.pack(fill=tk.X, pady=(0, 10))

        # SSH Details
        ttk.Label(main_frame, text="Hostname or IP:").pack(anchor=tk.W, pady=(5, 2))
        host_entry = ttk.Entry(main_frame, width=40)
        host_entry.pack(fill=tk.X, pady=(0, 10))
        host_entry.insert(0, conn['host'])

        ttk.Label(main_frame, text="SSH Username:").pack(anchor=tk.W, pady=(5, 2))
        user_entry = ttk.Entry(main_frame, width=40)
        user_entry.pack(fill=tk.X, pady=(0, 10))
        user_entry.insert(0, conn['username'])

        ttk.Label(main_frame, text="Password (optional):").pack(anchor=tk.W, pady=(5, 2))
        ttk.Label(main_frame, text="For SSH/jump server/sudo as needed", font=("Arial", 8), foreground="gray").pack(anchor=tk.W, pady=(0, 2))
        password_entry = ttk.Entry(main_frame, width=40, show="*")
        password_entry.pack(fill=tk.X, pady=(0, 10))
        if conn.get('password'):
            password_entry.insert(0, conn['password'])

        def save_changes():
            """Save edited connection"""
            name = name_entry.get().strip()
            host = host_entry.get().strip()
            username = user_entry.get().strip()
            password = password_entry.get()

            if not all([name, host, username]):
                messagebox.showerror("Error", "Please fill all required fields!")
                return

            # Update in persistent storage
            success, msg = self.monitor_connection_manager.update_connection(
                old_name=name,
                name=name,
                host=host,
                username=username,
                password=password
            )

            dialog.destroy()

            if success:
                messagebox.showinfo("Success", f"Connection '{name}' updated successfully!")
                self.update_status(f"✓ Connection '{name}' updated", "success")
            else:
                messagebox.showerror("Error", f"Failed to update connection:\n{msg}")

        # Buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(btn_frame, text="Save", command=save_changes, style="Primary.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT, padx=5)

        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")

    def test_monitor_connection(self):
        """Test selected monitoring connection"""
        selection = self.monitor_conn_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a connection to test!")
            return

        conn_text = self.monitor_conn_listbox.get(selection[0])
        conn_name = conn_text.split('[')[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]
        self.update_status(f"Testing connection to {conn['host']}... (timeout: 30s)")

        # Test connection - run simple echo command
        try:
            result = subprocess.run([
                'ssh',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'ConnectTimeout=30',
                f"{conn['username']}@{conn['host']}",
                'echo SSH_OK'
            ],
                capture_output=True,
                text=True,
                timeout=self.ssh_timeout
            )

            if result.returncode == 0 and 'SSH_OK' in result.stdout:
                messagebox.showinfo("Success", f"SSH connection successful to {conn['username']}@{conn['host']}")
                self.update_status("Connection test successful")
            else:
                error_msg = "SSH connection failed!"
                if result.returncode != 0 and result.stderr:
                    # Only show stderr if command actually failed
                    # Filter out common SSH warnings that aren't actual errors
                    stderr_filtered = '\n'.join([line for line in result.stderr.split('\n')
                                                if not line.startswith('Warning: Permanently added')])
                    if stderr_filtered.strip():
                        error_msg += f"\n\n{stderr_filtered[:200]}"
                messagebox.showerror("Error", error_msg)
                self.update_status("Connection test failed")

        except subprocess.TimeoutExpired:
            error_msg = f"SSH connection timeout after 30 seconds!\n\nThe server '{conn['host']}' is not responding.\n\nPossible causes:\n- Server is down or unreachable\n- Firewall blocking SSH port (default: 22)\n- Network connectivity issues\n- Wrong hostname/IP address"
            messagebox.showerror("Connection Timeout", error_msg)
            self.update_status(f"SSH test timeout for {conn['host']}")
            console_print(f"SSH test connection timeout after 30 seconds for {conn_name}")
        except Exception as e:
            messagebox.showerror("Error", f"SSH error: {str(e)}")
            self.update_status("Connection test error")

    def start_monitor_connection(self):
        """Start monitoring selected connection"""
        selection = self.monitor_conn_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a connection to start!")
            return

        conn_text = self.monitor_conn_listbox.get(selection[0])
        conn_name = conn_text.split('[')[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]

        if conn['monitoring']:
            messagebox.showinfo("Info", "Already monitoring this connection!")
            return

        # Create control socket path for SSH multiplexing
        control_path = os.path.join(tempfile.gettempdir(), f"ssh_monitor_{conn_name.replace(' ', '_')}")
        conn['control_path'] = control_path

        # Establish master SSH connection in background
        ssh_host = f"{conn['username']}@{conn['host']}"

        # Build SSH command with password if available
        if conn.get('password'):
            # Use sshpass for automatic password authentication
            master_cmd = [
                'sshpass', '-p', conn['password'],
                'ssh', '-M', '-N', '-f',
                '-o', 'ControlMaster=yes',
                '-o', f'ControlPath={control_path}',
                '-o', f'ControlPersist={self.ssh_control_persist}',
                '-o', 'StrictHostKeyChecking=no',  # Auto-accept host key
                ssh_host
            ]
        else:
            # Standard SSH (will prompt for password)
            master_cmd = [
                'ssh', '-M', '-N', '-f',
                '-o', 'ControlMaster=yes',
                '-o', f'ControlPath={control_path}',
                '-o', f'ControlPersist={self.ssh_control_persist}',
                ssh_host
            ]

        self.update_status(f"Establishing SSH connection to {conn['host']}... (timeout: {self.ssh_timeout}s)")

        try:
            # SSH timeout applies ONLY to establishing the SSH connection
            # Once connected, monitoring commands can run indefinitely
            result = subprocess.run(master_cmd, timeout=self.ssh_timeout, capture_output=True, text=True)

            if result.returncode == 0:
                conn['monitoring'] = True
                self.update_monitor_status_label()
                messagebox.showinfo("Success", f"Started monitoring '{conn_name}'")
            else:
                error_msg = "Failed to establish SSH connection!"
                if conn.get('password') and 'sshpass' in result.stderr:
                    error_msg += "\n\nNote: 'sshpass' not found. Install it using:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)"
                elif result.stderr:
                    error_msg += f"\n\nError: {result.stderr[:200]}"
                messagebox.showerror("Error", error_msg)
                self.update_status("SSH connection failed")

        except subprocess.TimeoutExpired:
            error_msg = f"SSH connection timeout after 30 seconds!\n\nThe server '{conn['host']}' is not responding.\n\nPossible causes:\n- Server is down or unreachable\n- Firewall blocking SSH port\n- Network connectivity issues\n- Wrong hostname/IP address"
            messagebox.showerror("Connection Timeout", error_msg)
            self.update_status(f"SSH connection timeout for {conn_name}")
            console_print(f"SSH connection timeout for {conn_name} after 30 seconds")
        except FileNotFoundError as e:
            if 'sshpass' in str(e):
                messagebox.showerror("Error", "sshpass not found!\n\nTo use saved passwords, install sshpass:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)\n\nAlternatively, leave password empty and enter it manually.")
            else:
                messagebox.showerror("Error", f"SSH error: {str(e)}")
            self.update_status("SSH connection error")
        except Exception as e:
            messagebox.showerror("Error", f"SSH error: {str(e)}")
            self.update_status("SSH connection error")

    def stop_monitor_connection(self):
        """Stop monitoring selected connection"""
        selection = self.monitor_conn_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a connection to stop!")
            return

        conn_text = self.monitor_conn_listbox.get(selection[0])
        conn_name = conn_text.split('[')[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]

        if not conn['monitoring']:
            messagebox.showinfo("Info", "This connection is not being monitored!")
            return

        # Close SSH master connection
        if 'control_path' in conn:
            ssh_host = f"{conn['username']}@{conn['host']}"
            try:
                subprocess.run([
                    'ssh', '-O', 'exit',
                    '-o', f"ControlPath={conn['control_path']}",
                    ssh_host
                ], timeout=self.ssh_test_timeout)
            except (subprocess.SubprocessError, OSError):
                pass  # Ignore SSH cleanup errors

        conn['monitoring'] = False
        self.update_monitor_status_label()
        messagebox.showinfo("Success", f"Stopped monitoring '{conn_name}'")

    def remove_monitor_connection(self):
        """Remove selected monitoring connection - stops immediately, cleanup async"""
        if not self.monitor_connections:
            messagebox.showwarning("Warning", "No server connections available to remove!")
            return

        selection = self.monitor_conn_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a server to remove!")
            return

        conn_text = self.monitor_conn_listbox.get(selection[0])
        conn_name = conn_text.split('[')[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]

        # Mark server as pending removal (stops new queries IMMEDIATELY)
        if conn['monitoring']:
            self.servers_pending_removal.add(conn_name)
            conn['monitoring'] = False  # Stop monitoring immediately
            console_print(f"✓ Marked {conn_name} for removal - monitoring stopped")

        # Update status immediately
        self.update_status(f"Removing '{conn_name}'...", "info")

        # Schedule async cleanup after brief delay to let current query finish
        self.root.after(100, self._complete_server_removal, conn_name, selection[0])

    def _complete_server_removal(self, conn_name, listbox_index):
        """Complete server removal after current query finishes"""
        if conn_name not in self.monitor_connections:
            return  # Already removed

        # Check if query is still running
        if conn_name in self.active_server_query_threads:
            thread = self.active_server_query_threads[conn_name]
            if thread and thread.is_alive():
                # Query still running, check again in 100ms
                console_print(f"Query still running for {conn_name}, checking again...")
                self.root.after(100, self._complete_server_removal, conn_name, listbox_index)
                return

        # Query finished, safe to close SSH and remove
        conn = self.monitor_connections[conn_name]

        # Close SSH master connection
        if 'control_path' in conn:
            ssh_host = f"{conn['username']}@{conn['host']}"
            try:
                subprocess.run([
                    'ssh', '-O', 'exit',
                    '-o', f"ControlPath={conn['control_path']}",
                    ssh_host
                ], timeout=self.ssh_test_timeout)
                console_print(f"✓ Closed SSH connection for {conn_name}")
            except (subprocess.SubprocessError, OSError) as e:
                console_print(f"SSH cleanup error for {conn_name}: {e}")


        # Remove from tracking
        self.servers_pending_removal.discard(conn_name)
        if conn_name in self.active_server_query_threads:
            del self.active_server_query_threads[conn_name]

        # Remove from list
        del self.monitor_connections[conn_name]

        # Refresh listbox to ensure correct removal
        self._refresh_monitor_conn_listbox()

        # Cleanup stale OS graphs
        self._cleanup_stale_os_graphs()

        self.update_monitor_status_label()
        self.update_status(f"✓ Removed '{conn_name}' from monitoring", "success")
        console_print(f"✓ Successfully removed {conn_name} from monitoring")

    def on_monitor_connection_selected(self, event=None):
        """Handle monitor connection selection"""
        selection = self.monitor_conn_listbox.curselection()
        if not selection:
            return

        conn_text = self.monitor_conn_listbox.get(selection[0])
        conn_name = conn_text.split('[')[0].strip()

        if conn_name in self.monitor_connections:
            conn = self.monitor_connections[conn_name]
            status = "Monitoring" if conn['monitoring'] else "Not monitoring"
            self.monitor_status_label.config(
                text=f"Status: {conn_name} - {status}",
                foreground="green" if conn['monitoring'] else "gray"
            )

    def save_monitor_connection(self):
        """Save selected monitor connection"""
        selection = self.monitor_conn_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a connection to save!")
            return

        conn_text = self.monitor_conn_listbox.get(selection[0])
        conn_name = conn_text.split('[')[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]

        # Save to monitor connection manager
        success, msg = self.monitor_connection_manager.add_connection(
            name=conn_name,
            host=conn['host'],
            username=conn['username'],
            password=conn.get('password')
        )

        if success:
            messagebox.showinfo("Success", f"Monitor connection '{conn_name}' saved successfully!")
            self.update_status(f"✓ Monitor connection '{conn_name}' saved", "success")
        else:
            messagebox.showerror("Error", msg)
            self.update_status(f"✗ Failed to save monitor connection", "error")

    def select_server_to_monitor(self):
        """Select a server to monitor from saved connections - starts monitoring immediately"""
        saved_connections = self.monitor_connection_manager.get_all_connections()

        if not saved_connections:
            messagebox.showinfo("Info", "No saved server connections found.\nPlease add a connection first.")
            return

        # Create dialog to select server
        dialog = tk.Toplevel(self.root)
        dialog.title("Select Server to Monitor")
        dialog.geometry("450x250")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Select Server to Monitor:", font=("Arial", 11, "bold")).pack(pady=20, padx=20)

        # Dropdown with saved connections
        server_combo = ttk.Combobox(dialog, state="readonly", width=50)
        server_combo.pack(pady=10, padx=20)

        # Build list of servers with status
        server_options = []
        conn_map = {}
        for conn in saved_connections:
            conn_name = conn['name']
            if conn_name in self.monitor_connections:
                status = " [Already Monitoring]"
            else:
                status = ""
            display_name = f"{conn_name} [{conn['username']}@{conn['host']}]{status}"
            server_options.append(display_name)
            conn_map[display_name] = conn

        server_combo['values'] = server_options
        if server_options:
            server_combo.current(0)

        def add_selected():
            selected = server_combo.get()
            if not selected:
                messagebox.showwarning("Warning", "Please select a server!")
                return

            conn = conn_map[selected]
            conn_name = conn['name']

            # Check if already monitoring
            if conn_name in self.monitor_connections:
                messagebox.showinfo("Info", f"'{conn_name}' is already being monitored!")
                dialog.destroy()
                return

            # Add to monitor connections
            self.monitor_connections[conn_name] = {
                'host': conn['host'],
                'username': conn['username'],
                'password': conn.get('password'),
                'monitoring': False
            }

            # Add to listbox
            self.monitor_conn_listbox.insert(tk.END, f"{conn_name} [{conn['username']}@{conn['host']}]")

            dialog.destroy()

            # Start monitoring immediately
            self._start_monitoring_for_server(conn_name)

        def edit_selected():
            """Edit the selected saved connection"""
            selected = server_combo.get()
            if not selected:
                messagebox.showwarning("Warning", "Please select a server to edit!")
                return

            conn = conn_map[selected]
            dialog.destroy()

            # Open edit dialog
            self.edit_monitor_connection(conn['name'])

        def delete_selected():
            """Delete the selected saved connection"""
            selected = server_combo.get()
            if not selected:
                messagebox.showwarning("Warning", "Please select a server to delete!")
                return

            conn = conn_map[selected]
            conn_name = conn['name']

            # Confirm deletion
            confirm = messagebox.askyesno(
                "Confirm Delete",
                f"Are you sure you want to delete connection '{conn_name}'?\n\nThis action cannot be undone."
            )

            if not confirm:
                return

            # If currently monitoring, stop it first
            if conn_name in self.monitor_connections:
                monitor_conn = self.monitor_connections[conn_name]

                # Mark as pending removal and stop monitoring
                if monitor_conn.get('monitoring'):
                    self.servers_pending_removal.add(conn_name)
                    monitor_conn['monitoring'] = False

                    # Close SSH master connection
                    if 'control_path' in monitor_conn:
                        ssh_host = f"{monitor_conn['username']}@{monitor_conn['host']}"
                        try:
                            subprocess.run([
                                'ssh', '-O', 'exit',
                                '-o', f"ControlPath={monitor_conn['control_path']}",
                                ssh_host
                            ], timeout=self.ssh_test_timeout)
                        except (subprocess.SubprocessError, OSError):
                            pass  # Ignore SSH cleanup errors

                # Remove from tracking
                self.servers_pending_removal.discard(conn_name)
                if conn_name in self.active_server_query_threads:
                    del self.active_server_query_threads[conn_name]

                # Remove from monitor connections
                del self.monitor_connections[conn_name]

                # Refresh monitor listbox
                self._refresh_monitor_conn_listbox()
                self.update_monitor_status_label()

            # Delete from persistent storage
            success, msg = self.monitor_connection_manager.delete_connection(conn_name)

            if success:
                messagebox.showinfo("Success", f"Connection '{conn_name}' deleted successfully!")
                self.update_status(f"✓ Connection '{conn_name}' deleted", "success")

                # Close dialog and reopen to refresh list
                dialog.destroy()
                self.select_server_to_monitor()
            else:
                messagebox.showerror("Error", f"Failed to delete connection:\n{msg}")

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=20)
        ttk.Button(btn_frame, text="Select", command=add_selected, width=12, style="Success.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Edit", command=edit_selected, width=12).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Delete", command=delete_selected, width=12, style="Error.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy, width=12).pack(side=tk.LEFT, padx=5)

    def _start_monitoring_for_server(self, conn_name):
        """Internal function to start monitoring for a specific server"""
        if conn_name not in self.monitor_connections:
            return

        conn = self.monitor_connections[conn_name]

        if conn['monitoring']:
            return  # Already monitoring

        # Create control socket path for SSH multiplexing
        control_path = os.path.join(tempfile.gettempdir(), f"ssh_monitor_{conn_name.replace(' ', '_')}")
        conn['control_path'] = control_path

        # Establish master SSH connection in background
        ssh_host = f"{conn['username']}@{conn['host']}"

        self.update_status(f"Establishing SSH connection to {conn['host']}... (timeout: 30s)")

        # Check if we need password authentication
        password = conn.get('password')

        if password:
            # Single password - use sshpass
            master_cmd = [
                'sshpass', '-p', password,
                'ssh', '-M', '-N', '-f',
                '-o', 'ControlMaster=yes',
                '-o', f'ControlPath={control_path}',
                '-o', f'ControlPersist={self.ssh_control_persist}',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                ssh_host
            ]

            try:
                result = subprocess.run(master_cmd, timeout=self.ssh_timeout + 5, capture_output=True, text=True)

                if result.returncode == 0:
                    conn['monitoring'] = True
                    self.update_monitor_status_label()
                    messagebox.showinfo("Success", f"Started monitoring '{conn_name}'")
                    self.update_status(f"✓ Monitoring {conn_name}", "success")
                else:
                    error_msg = "Failed to establish SSH connection!"
                    if 'sshpass' in result.stderr:
                        error_msg += "\n\nNote: 'sshpass' not found. Install it using:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)"
                    elif result.stderr:
                        error_msg += f"\n\nError: {result.stderr[:300]}"
                    messagebox.showerror("Error", error_msg)
                    self.update_status("SSH connection failed")
                    del self.monitor_connections[conn_name]
                    self._refresh_monitor_conn_listbox()

            except subprocess.TimeoutExpired:
                messagebox.showerror("Connection Timeout", f"SSH connection timeout!\n\nThe server '{conn['host']}' is not responding.")
                self.update_status(f"SSH connection timeout for {conn_name}")
                del self.monitor_connections[conn_name]
                self._refresh_monitor_conn_listbox()
            except FileNotFoundError as e:
                if 'sshpass' in str(e):
                    messagebox.showerror("Error", "sshpass not found!\n\nTo use passwords, install sshpass:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)")
                else:
                    messagebox.showerror("Error", f"SSH error: {str(e)}")
                self.update_status("SSH connection error")
                del self.monitor_connections[conn_name]
                self._refresh_monitor_conn_listbox()
            except Exception as e:
                messagebox.showerror("Error", f"SSH error: {str(e)}")
                self.update_status("SSH connection error")
                del self.monitor_connections[conn_name]
                self._refresh_monitor_conn_listbox()

        else:
            # No password - standard SSH (will prompt or use keys)
            master_cmd = [
                'ssh', '-M', '-N', '-f',
                '-o', 'ControlMaster=yes',
                '-o', f'ControlPath={control_path}',
                '-o', f'ControlPersist={self.ssh_control_persist}',
                '-o', 'StrictHostKeyChecking=no',
                ssh_host
            ]

            try:
                result = subprocess.run(master_cmd, timeout=self.ssh_timeout + 5, capture_output=True, text=True)

                if result.returncode == 0:
                    conn['monitoring'] = True
                    self.update_monitor_status_label()
                    messagebox.showinfo("Success", f"Started monitoring '{conn_name}'")
                    self.update_status(f"✓ Monitoring {conn_name}", "success")
                else:
                    error_msg = "Failed to establish SSH connection!"
                    if result.stderr:
                        error_msg += f"\n\nError: {result.stderr[:300]}"
                    messagebox.showerror("Error", error_msg)
                    self.update_status("SSH connection failed")
                    del self.monitor_connections[conn_name]
                    self._refresh_monitor_conn_listbox()

            except subprocess.TimeoutExpired:
                messagebox.showerror("Connection Timeout", f"SSH connection timeout!\n\nThe server '{conn['host']}' is not responding.")
                self.update_status(f"SSH connection timeout for {conn_name}")
                del self.monitor_connections[conn_name]
                self._refresh_monitor_conn_listbox()
            except Exception as e:
                messagebox.showerror("Error", f"SSH error: {str(e)}")
                self.update_status("SSH connection error")
                del self.monitor_connections[conn_name]
                self._refresh_monitor_conn_listbox()

    def _refresh_monitor_conn_listbox(self):
        """Refresh the server connection listbox"""
        self.monitor_conn_listbox.delete(0, tk.END)
        for conn_name, conn in self.monitor_connections.items():
            self.monitor_conn_listbox.insert(tk.END, f"{conn_name} [{conn['username']}@{conn['host']}]")

    def load_monitor_connections(self):
        """Load saved monitor connections - DEPRECATED, kept for compatibility"""
        saved_connections = self.monitor_connection_manager.get_all_connections()

        if not saved_connections:
            messagebox.showinfo("Info", "No saved monitor connections found.")
            return

        # Create dialog to select connections to load
        dialog = tk.Toplevel(self.root)
        dialog.title("Load Saved Monitor Connections")
        dialog.geometry("500x400")
        dialog.minsize(500, 300)  # Prevent resizing smaller to keep buttons visible
        dialog.transient(self.root)
        dialog.grab_set()

        main_frame = ttk.Frame(dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(main_frame, text="Select connections to load:", font=(self.ui_font[0], 12, "bold")).pack(side=tk.TOP, pady=(0, 10))

        # Buttons frame - pack FIRST at bottom to ensure it stays visible
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 0))

        # Listbox with saved connections - pack AFTER buttons to fill remaining space
        list_frame = ttk.Frame(main_frame)
        list_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL)
        conn_listbox = tk.Listbox(
            list_frame,
            height=15,
            font=self.ui_font,
            yscrollcommand=scrollbar.set,
            selectmode=tk.MULTIPLE,
            bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_PRIMARY,
            selectbackground=self.theme.PRIMARY,
            selectforeground="white"
        )
        scrollbar.config(command=conn_listbox.yview)

        conn_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Populate listbox
        for conn in saved_connections:
            conn_listbox.insert(tk.END, f"{conn['name']} [{conn['username']}@{conn['host']}]")

        def load_selected():
            selection = conn_listbox.curselection()
            if not selection:
                messagebox.showwarning("Warning", "Please select at least one connection!")
                return

            loaded_count = 0
            for idx in selection:
                conn = saved_connections[idx]

                # Check if already exists
                if conn['name'] in self.monitor_connections:
                    continue

                # Add to monitor connections
                self.monitor_connections[conn['name']] = {
                    'host': conn['host'],
                    'username': conn['username'],
                    'password': conn.get('password'),  # Load password if saved
                    'monitoring': False
                }

                # Add to listbox
                self.monitor_conn_listbox.insert(tk.END, f"{conn['name']} [{conn['username']}@{conn['host']}]")
                loaded_count += 1

            dialog.destroy()
            if loaded_count > 0:
                messagebox.showinfo("Success", f"Loaded {loaded_count} monitor connection(s)!")
                self.update_status(f"✓ Loaded {loaded_count} monitor connections", "success")
            else:
                messagebox.showinfo("Info", "All selected connections are already loaded.")

        def delete_selected():
            selection = conn_listbox.curselection()
            if not selection:
                messagebox.showwarning("Warning", "Please select at least one connection to delete!")
                return

            # Confirm deletion
            if not messagebox.askyesno("Confirm Delete",
                                       f"Are you sure you want to delete {len(selection)} saved connection(s)?\nThis cannot be undone."):
                return

            deleted_count = 0
            # Delete in reverse order to maintain indices
            for idx in reversed(selection):
                conn = saved_connections[idx]
                success, message = self.monitor_connection_manager.delete_connection(conn['name'])
                if success:
                    conn_listbox.delete(idx)
                    saved_connections.pop(idx)
                    deleted_count += 1

            if deleted_count > 0:
                messagebox.showinfo("Success", f"Deleted {deleted_count} saved connection(s)!")
                self.update_status(f"✓ Deleted {deleted_count} saved connections", "success")

                # Close dialog if no more connections
                if conn_listbox.size() == 0:
                    dialog.destroy()

        def edit_selected():
            selection = conn_listbox.curselection()
            if not selection:
                messagebox.showwarning("Warning", "Please select a connection to edit!")
                return

            if len(selection) > 1:
                messagebox.showwarning("Warning", "Please select only one connection to edit!")
                return

            idx = selection[0]
            conn = saved_connections[idx]

            # Create edit dialog
            edit_dialog = tk.Toplevel(self.root)
            edit_dialog.title("Edit Monitor Connection")
            edit_dialog.geometry("450x350")
            edit_dialog.resizable(True, True)
            edit_dialog.transient(dialog)
            edit_dialog.grab_set()

            # Create scrollable container
            edit_canvas = tk.Canvas(edit_dialog, highlightthickness=0, bd=0)
            edit_scrollbar = ttk.Scrollbar(edit_dialog, orient=tk.VERTICAL, command=edit_canvas.yview)
            edit_main_frame = ttk.Frame(edit_canvas, padding="20")

            edit_canvas.create_window((0, 0), window=edit_main_frame, anchor=tk.NW)
            edit_canvas.configure(yscrollcommand=edit_scrollbar.set)

            def on_edit_frame_configure(event):
                edit_canvas.configure(scrollregion=edit_canvas.bbox("all"))

            edit_main_frame.bind("<Configure>", on_edit_frame_configure)

            edit_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            edit_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

            # Enable mousewheel scrolling
            bind_canvas_mousewheel(edit_canvas)

            ttk.Label(edit_main_frame, text="Edit Server Monitoring Connection", font=("Arial", 12, "bold")).pack(pady=(0, 20))

            # Connection Name (read-only - shown but can't be changed to avoid key conflicts)
            ttk.Label(edit_main_frame, text="Connection Name:").pack(anchor=tk.W, pady=(5, 2))
            name_entry = ttk.Entry(edit_main_frame, width=40)
            name_entry.pack(fill=tk.X, pady=(0, 10))
            name_entry.insert(0, conn['name'])
            name_entry.config(state='readonly')

            # SSH Details
            ttk.Label(edit_main_frame, text="Hostname or IP:").pack(anchor=tk.W, pady=(5, 2))
            host_entry = ttk.Entry(edit_main_frame, width=40)
            host_entry.pack(fill=tk.X, pady=(0, 10))
            host_entry.insert(0, conn['host'])

            ttk.Label(edit_main_frame, text="SSH Username:").pack(anchor=tk.W, pady=(5, 2))
            user_entry = ttk.Entry(edit_main_frame, width=40)
            user_entry.pack(fill=tk.X, pady=(0, 10))
            user_entry.insert(0, conn['username'])

            ttk.Label(edit_main_frame, text="SSH Password (optional):").pack(anchor=tk.W, pady=(5, 2))
            password_entry = ttk.Entry(edit_main_frame, width=40, show="*")
            password_entry.pack(fill=tk.X, pady=(0, 5))
            if conn.get('password'):
                password_entry.insert(0, conn['password'])

            # Info label for password
            ttk.Label(edit_main_frame, text="(Clear to remove password, or enter new password to change)",
                     foreground="gray", font=('Arial', 9)).pack(anchor=tk.W, pady=(0, 20))

            def save_edited():
                host = host_entry.get().strip()
                username = user_entry.get().strip()
                password = password_entry.get()  # Can be empty - empty means no password

                if not all([host, username]):
                    messagebox.showerror("Error", "Please fill hostname and username!")
                    return

                # Use whatever is in the password field (even if empty)
                # Empty password field = no password saved

                # Update in monitor connection manager
                success, msg = self.monitor_connection_manager.update_connection(
                    old_name=conn['name'],
                    name=conn['name'],  # Name stays the same
                    host=host,
                    username=username,
                    password=password
                )

                if success:
                    # Update in saved_connections list
                    saved_connections[idx]['host'] = host
                    saved_connections[idx]['username'] = username
                    saved_connections[idx]['password'] = password

                    # Update listbox display
                    conn_listbox.delete(idx)
                    conn_listbox.insert(idx, f"{conn['name']} [{username}@{host}]")
                    conn_listbox.selection_set(idx)

                    edit_dialog.destroy()
                    messagebox.showinfo("Success", f"Connection '{conn['name']}' updated successfully!")
                    self.update_status(f"✓ Connection '{conn['name']}' updated", "success")
                else:
                    messagebox.showerror("Error", msg)

            # Buttons
            edit_btn_frame = ttk.Frame(edit_main_frame)
            edit_btn_frame.pack(fill=tk.X)
            ttk.Button(edit_btn_frame, text="Save Changes", command=save_edited, style="Primary.TButton").pack(side=tk.LEFT, padx=5)
            ttk.Button(edit_btn_frame, text="Cancel", command=edit_dialog.destroy).pack(side=tk.LEFT, padx=5)

            # Center dialog
            edit_dialog.update_idletasks()
            x = (edit_dialog.winfo_screenwidth() // 2) - (edit_dialog.winfo_width() // 2)
            y = (edit_dialog.winfo_screenheight() // 2) - (edit_dialog.winfo_height() // 2)
            edit_dialog.geometry(f"+{x}+{y}")

        # Buttons (already packed at bottom before listbox)
        ttk.Button(btn_frame, text="Load Selected", command=load_selected, style="Primary.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Edit Selected", command=edit_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Delete Selected", command=delete_selected, style="Error.TButton").pack(side=tk.RIGHT, padx=5)

        # Center dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")

    def update_monitored_db_listbox(self):
        """Update the monitored databases listbox display"""
        self.monitored_db_listbox.delete(0, tk.END)
        for db_name in self.monitored_databases.keys():
            self.monitored_db_listbox.insert(tk.END, db_name)

    def refresh_monitor_db_list(self):
        """Refresh monitoring - fetch metrics for all monitored databases"""
        if not self.monitored_databases:
            return

        # Fetch metrics immediately for all monitored databases
        for db_name in list(self.monitored_databases.keys()):
            self.fetch_db_metrics_for_db(db_name)

        # Update status to show complete monitoring state
        self.update_monitor_status_label()

    def add_db_to_monitor(self):
        """Add a database to the monitoring list from active and saved connections"""
        # Show dialog to select database
        dialog = tk.Toplevel(self.root)
        dialog.title("Select Database to Monitor")
        dialog.geometry("450x250")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(dialog, text="Select Database to Monitor:", font=("Arial", 11, "bold")).pack(pady=20, padx=20)

        # Dropdown with active connections first, then saved connections
        conn_combo = ttk.Combobox(dialog, state="readonly", width=40)

        # Build list: Active connections first, then saved connections
        conn_display_list = []
        conn_name_map = {}  # Maps display name to actual connection name
        conn_type_map = {}  # Maps display name to type: 'active' or 'saved'

        # First: Add all active connections
        for conn_name in self.active_connections.keys():
            if conn_name in self.monitored_databases:
                status = " [Already Monitoring]"
            else:
                status = " [Active]"

            display_name = f"{conn_name}{status}"
            conn_display_list.append(display_name)
            conn_name_map[display_name] = conn_name
            conn_type_map[display_name] = 'active'

        # Second: Add saved connections that are not already in active connections
        saved_connections = self.connection_manager.get_all_connections()
        for conn in saved_connections:
            conn_name = conn['name']

            # Skip if already in active connections (avoid duplicates)
            if conn_name in self.active_connections:
                continue

            if conn_name in self.monitored_databases:
                status = " [Already Monitoring]"
            else:
                status = " [Saved]"

            display_name = f"{conn_name}{status}"
            conn_display_list.append(display_name)
            conn_name_map[display_name] = conn_name
            conn_type_map[display_name] = 'saved'

        if not conn_display_list:
            ttk.Label(dialog, text="No connections available.\n\nPlease create an active connection or save a connection first.",
                     foreground="gray", justify="center").pack(pady=10)
            ttk.Button(dialog, text="Close", command=dialog.destroy, width=15).pack(pady=20)
            return

        conn_combo['values'] = conn_display_list
        conn_combo.current(0)
        conn_combo.pack(pady=10, padx=20)

        # Status label for connection progress
        status_label = ttk.Label(dialog, text="", foreground="blue")
        status_label.pack(pady=5)

        def add_selected():
            display_name = conn_combo.get()
            if not display_name:
                return

            conn_name = conn_name_map[display_name]
            conn_type = conn_type_map[display_name]

            # Check if already monitoring
            if conn_name in self.monitored_databases:
                messagebox.showinfo("Info", f"'{conn_name}' is already being monitored.")
                return

            # If it's an active connection, use it directly
            if conn_type == 'active':
                status_label.config(text=f"Using active connection...", foreground="blue")
                dialog.update()

                # Get the db_manager from active connections
                db_manager = self.active_connections[conn_name]

                # Add to monitoring with the active connection's db_manager
                self.monitored_databases[conn_name] = db_manager
                self.update_monitored_db_listbox()
                self.update_monitor_status_label()

                # Fetch metrics immediately for the new database
                self.fetch_db_metrics_for_db(conn_name)

                messagebox.showinfo("Success", f"'{conn_name}' added to monitoring (using active connection).")
                dialog.destroy()
                return

            # It's a saved connection - need to get details and possibly connect
            conn_details = self.connection_manager.get_connection(conn_name)
            if not conn_details:
                messagebox.showerror("Error", f"Connection '{conn_name}' not found.")
                return

            # Check if password is available, if not prompt for it
            password = conn_details.get('password', '')
            if not password:
                # Password not saved, prompt user for it
                password_dialog = tk.Toplevel(dialog)
                password_dialog.title("Password Required")
                password_dialog.geometry("400x180")
                password_dialog.transient(dialog)
                password_dialog.grab_set()

                ttk.Label(password_dialog, text=f"Password required for '{conn_name}'",
                         font=("Arial", 11, "bold")).pack(pady=20, padx=20)

                ttk.Label(password_dialog, text="Database Password:", font=("Arial", 10)).pack(anchor=tk.W, padx=20, pady=(0, 5))
                password_entry = ttk.Entry(password_dialog, width=40, show="*")
                password_entry.pack(padx=20, pady=(0, 10))
                password_entry.focus()

                ttk.Label(password_dialog, text="(Password will be used for this session only, not saved)",
                         foreground="gray", font=('Arial', 9)).pack(padx=20, pady=(0, 10))

                entered_password = {'value': None}

                def submit_password():
                    pwd = password_entry.get()
                    if not pwd:
                        messagebox.showwarning("Warning", "Password cannot be empty!", parent=password_dialog)
                        return
                    entered_password['value'] = pwd
                    password_dialog.destroy()

                def cancel_password():
                    password_dialog.destroy()

                # Bind Enter key to submit
                password_entry.bind('<Return>', lambda e: submit_password())

                pwd_btn_frame = ttk.Frame(password_dialog)
                pwd_btn_frame.pack(pady=10)
                ttk.Button(pwd_btn_frame, text="OK", command=submit_password, style="Primary.TButton", width=12).pack(side=tk.LEFT, padx=5)
                ttk.Button(pwd_btn_frame, text="Cancel", command=cancel_password, width=12).pack(side=tk.LEFT, padx=5)

                # Center password dialog
                password_dialog.update_idletasks()
                x = (password_dialog.winfo_screenwidth() // 2) - (password_dialog.winfo_width() // 2)
                y = (password_dialog.winfo_screenheight() // 2) - (password_dialog.winfo_height() // 2)
                password_dialog.geometry(f"+{x}+{y}")

                # Wait for password dialog to close
                password_dialog.wait_window()

                # Check if password was entered
                if entered_password['value'] is None:
                    status_label.config(text="Cancelled - password required", foreground="red")
                    return

                password = entered_password['value']

            # Create a dedicated monitoring connection (not added to active_connections)
            status_label.config(text=f"Connecting to '{conn_name}' for monitoring...", foreground="blue")
            dialog.update()

            try:
                # Import DatabaseManager (lazy import to avoid circular dependency)
                from conDbUi import DatabaseManager

                # Create DatabaseManager for monitoring
                db_type = conn_details['db_type']
                db_manager = DatabaseManager(db_type)

                # Prepare connection parameters
                conn_params = {
                    'host': conn_details['host'],
                    'port': conn_details['port'],
                    'username': conn_details['username'],
                    'password': password  # Use either saved password or user-entered password
                }

                # Add database/service parameter based on db_type
                if db_type == "Oracle":
                    conn_params['service'] = conn_details['service_or_db']
                else:
                    conn_params['database'] = conn_details['service_or_db']

                # Attempt connection
                conn = db_manager.connect(**conn_params)

                if conn:
                    status_label.config(text=f"Connected successfully!", foreground="green")
                    dialog.update()
                else:
                    messagebox.showerror("Connection Failed", f"Failed to connect to '{conn_name}' for monitoring.")
                    status_label.config(text="", foreground="blue")
                    return

            except Exception as e:
                messagebox.showerror("Connection Error", f"Failed to connect to '{conn_name}' for monitoring:\n{str(e)}")
                status_label.config(text="", foreground="blue")
                return

            # Add to monitoring with the dedicated db_manager
            self.monitored_databases[conn_name] = db_manager
            self.update_monitored_db_listbox()
            self.update_monitor_status_label()

            # Fetch metrics immediately for the new database
            self.fetch_db_metrics_for_db(conn_name)

            messagebox.showinfo("Success", f"'{conn_name}' added to monitoring.")
            dialog.destroy()

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=20)
        ttk.Button(btn_frame, text="Select", command=add_selected, width=12, style="Success.TButton").pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy, width=12).pack(side=tk.LEFT, padx=5)

    def remove_db_from_monitor(self):
        """Remove selected database from monitoring - stops immediately, cleanup async"""
        if not self.monitored_databases:
            messagebox.showwarning("Warning", "No databases are currently being monitored!")
            return

        # Get selected database from listbox
        selection = self.monitored_db_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a database from the list to remove!")
            return

        # Get the selected database name
        selected = self.monitored_db_listbox.get(selection[0])

        if selected and selected in self.monitored_databases:
            # Mark database as pending removal (stops new queries IMMEDIATELY)
            self.databases_pending_removal.add(selected)
            console_print(f"✓ Marked {selected} for removal - monitoring stopped")

            # Update status immediately
            self.update_status(f"Removing '{selected}'...", "info")

            # Schedule async cleanup after brief delay to let current query finish
            self.root.after(100, self._complete_db_removal, selected)

    def _complete_db_removal(self, db_name):
        """Complete database removal after current query finishes"""
        if db_name not in self.monitored_databases:
            return  # Already removed

        # Check if query is still running
        if db_name in self.active_db_query_threads:
            thread = self.active_db_query_threads[db_name]
            if thread and thread.is_alive():
                # Query still running, check again in 100ms
                console_print(f"Query still running for {db_name}, checking again...")
                self.root.after(100, self._complete_db_removal, db_name)
                return

        # Query finished, safe to disconnect and remove
        db_manager = self.monitored_databases[db_name]
        try:
            db_manager.disconnect()
            console_print(f"✓ Disconnected {db_name}")
        except Exception as e:
            console_print(f"Error disconnecting monitoring connection for {db_name}: {e}")

        # Remove from all tracking structures
        del self.monitored_databases[db_name]
        self.databases_pending_removal.discard(db_name)
        if db_name in self.active_db_query_threads:
            del self.active_db_query_threads[db_name]
        if db_name in self._db_metrics_cache:
            del self._db_metrics_cache[db_name]

        # Update UI
        self.update_monitored_db_listbox()
        self.update_monitor_status_label()

        # Clear graphs if no databases are being monitored
        if not self.monitored_databases:
            self.clear_db_graphs()
            self.db_metrics_text.config(state=tk.NORMAL)
            self.db_metrics_text.delete(1.0, tk.END)
            self.db_metrics_text.insert(1.0, "Click 'Select Database' to start monitoring databases...")
            self.db_metrics_text.config(state=tk.DISABLED)
        else:
            # Trigger cleanup to remove graphs for this database
            self._cleanup_stale_graphs()

        self.update_status(f"✓ Removed '{db_name}' from monitoring", "success")
        console_print(f"✓ Successfully removed {db_name} from monitoring")

    def toggle_os_view(self, mode):
        """Toggle between text and graph view for OS metrics"""
        self.os_view_mode = mode

        if mode == 'text':
            # Show text, hide graphs
            self.os_metrics_visualizer.canvas.pack_forget()
            self.os_metrics_visualizer.v_scrollbar.pack_forget()
            self.os_metrics_visualizer.h_scrollbar.pack_forget()
            self.os_metrics_text.pack(fill=tk.BOTH, expand=True)
            self.update_status("OS view: Text mode", "info")
        else:
            # Show graphs, hide text
            self.os_metrics_text.pack_forget()
            self.os_metrics_visualizer.h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
            self.os_metrics_visualizer.v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            self.os_metrics_visualizer.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            self.update_status("OS view: Graph mode", "info")

    def toggle_db_view(self, mode):
        """Toggle between text and graph view for DB metrics"""
        self.db_view_mode = mode

        if mode == 'text':
            # Show text, hide graphs
            self.db_metrics_visualizer.canvas.pack_forget()
            self.db_metrics_visualizer.v_scrollbar.pack_forget()
            self.db_metrics_visualizer.h_scrollbar.pack_forget()
            self.db_metrics_text.pack(fill=tk.BOTH, expand=True)
            self.update_status("DB view: Text mode", "info")
        else:
            # Show graphs, hide text
            self.db_metrics_text.pack_forget()
            self.db_metrics_visualizer.h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
            self.db_metrics_visualizer.v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            self.db_metrics_visualizer.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            self.update_status("DB view: Graph mode", "info")

    def fetch_db_metrics_now(self):
        """Fetch database metrics for all monitored databases - runs in background"""
        if not self.monitored_databases:
            return

        # Fetch metrics for each monitored database
        for db_name in list(self.monitored_databases.keys()):
            self.fetch_db_metrics_for_db(db_name)

    def fetch_db_metrics_for_db(self, db_name):
        """Fetch metrics for a specific database in background"""
        if db_name not in self.monitored_databases:
            return

        # Run in background thread to avoid UI freeze
        thread = threading.Thread(
            target=self._fetch_db_metrics_thread,
            args=(db_name,),
            daemon=True
        )
        thread.start()

    def _fetch_db_metrics_thread(self, db_name):
        """Background thread to fetch DB metrics"""
        try:
            if db_name not in self.monitored_databases:
                return

            # Skip if pending removal
            if db_name in self.databases_pending_removal:
                console_print(f"Skipping fetch for {db_name} (pending removal)")
                return

            # Track this thread
            self.active_db_query_threads[db_name] = threading.current_thread()

            try:
                db_manager = self.monitored_databases[db_name]
                db_stats = self.get_db_metrics(db_manager)

                # Update UI on main thread
                self.root.after(0, self._update_db_metrics_ui, db_name, db_stats)
            finally:
                # Remove from active threads
                if db_name in self.active_db_query_threads:
                    del self.active_db_query_threads[db_name]

        except Exception as e:
            console_print(f"Error fetching DB metrics in thread: {e}")
            import traceback
            traceback.print_exc()

    def _cleanup_stale_graphs(self):
        """Remove graphs for databases that are no longer being monitored"""
        if not hasattr(self, 'db_metrics_visualizer'):
            return

        # Get list of databases that are stale (have graphs but not monitored)
        stale_databases = set()
        for graph_key in list(self.db_metrics_visualizer.graphs.keys()):
            # Extract database name from graph key (format: "db_name_metric_name")
            db_name_found = None
            for monitored_db in self.monitored_databases.keys():
                if graph_key.startswith(f"{monitored_db}_"):
                    db_name_found = monitored_db
                    break

            # If no matching monitored database found, add to stale list
            if db_name_found is None:
                # Extract the database name from the key (everything before the last underscore)
                parts = graph_key.split('_')
                if len(parts) >= 2:
                    # Reconstruct db name (in case it has underscores)
                    for i in range(len(parts), 0, -1):
                        potential_db_name = '_'.join(parts[:i])
                        if potential_db_name not in self.monitored_databases:
                            stale_databases.add(potential_db_name)
                            break

        # Remove all graphs and UI elements for stale databases
        if stale_databases:
            console_print(f"Removing stale DB graphs: {stale_databases}")

            # Completely rebuild the visualizer to release space properly
            # Clear all widgets from scrollable frame
            for widget in self.db_metrics_visualizer.scrollable_frame.winfo_children():
                widget.destroy()

            # Clear all graph references
            self.db_metrics_visualizer.graphs.clear()
            self.db_metrics_visualizer.separators_added.clear()

            # Reset row tracking
            self.db_metrics_visualizer.current_row_frame = None
            self.db_metrics_visualizer.metrics_in_current_row = 0

            # Update scroll region
            self.db_metrics_visualizer.scrollable_frame.update_idletasks()
            self.db_metrics_visualizer.canvas.configure(
                scrollregion=self.db_metrics_visualizer.canvas.bbox("all")
            )

            console_print("✓ DB graphs visualizer rebuilt")

    def _cleanup_stale_os_graphs(self):
        """Remove graphs for servers that are no longer being monitored"""
        if not hasattr(self, 'os_metrics_visualizer'):
            return

        # Get list of servers that are stale (have graphs but not monitored)
        stale_servers = set()
        monitored_servers = set(conn_name for conn_name, conn in self.monitor_connections.items()
                                if conn.get('monitoring', False))

        for graph_key in list(self.os_metrics_visualizer.graphs.keys()):
            # Extract server name from graph key (format: "server_name - Metric Name")
            if ' - ' in graph_key:
                server_name = graph_key.split(' - ')[0].strip()
                # Check if this server is still being monitored
                if server_name not in monitored_servers:
                    stale_servers.add(server_name)

        # Remove all graphs and UI elements for stale servers
        if stale_servers:
            console_print(f"Removing stale OS graphs: {stale_servers}")

            # Completely rebuild the visualizer to release space properly
            # Clear all widgets from scrollable frame
            for widget in self.os_metrics_visualizer.scrollable_frame.winfo_children():
                widget.destroy()

            # Clear all graph references
            self.os_metrics_visualizer.graphs.clear()
            self.os_metrics_visualizer.separators_added.clear()

            # Reset row tracking
            self.os_metrics_visualizer.current_row_frame = None
            self.os_metrics_visualizer.metrics_in_current_row = 0

            # Update scroll region
            self.os_metrics_visualizer.scrollable_frame.update_idletasks()
            self.os_metrics_visualizer.canvas.configure(
                scrollregion=self.os_metrics_visualizer.canvas.bbox("all")
            )

            console_print("✓ OS graphs visualizer rebuilt")

    def _update_db_metrics_ui(self, db_name, db_stats):
        """Update DB metrics UI for a specific database (runs on main thread)"""
        if db_name not in self.monitored_databases:
            return

        # Clean up stale graphs (graphs for databases no longer being monitored)
        self._cleanup_stale_graphs()

        # Store metrics for this database
        if not hasattr(self, '_db_metrics_cache'):
            self._db_metrics_cache = {}
        self._db_metrics_cache[db_name] = {
            'stats': db_stats,
            'timestamp': time.strftime('%H:%M:%S')
        }

        # Update text view - show all monitored databases
        self.db_metrics_text.config(state=tk.NORMAL)
        self.db_metrics_text.delete(1.0, tk.END)

        for monitored_db in self.monitored_databases.keys():
            if monitored_db in self._db_metrics_cache:
                cached = self._db_metrics_cache[monitored_db]
                stats = cached['stats']
                timestamp = cached['timestamp']

                if stats:
                    db_text = f"{'=' * 60}\n"
                    db_text += f"Database: {monitored_db}\n"
                    db_text += f"Last Update: {timestamp}\n"
                    db_text += f"{'=' * 60}\n"
                    for key, value in stats.items():
                        db_text += f"  {key:30s}: {value}\n"
                    db_text += "\n"
                    self.db_metrics_text.insert(tk.END, db_text)
                else:
                    self.db_metrics_text.insert(tk.END, f"\n{monitored_db}: No metrics available\n\n")

        self.db_metrics_text.config(state=tk.DISABLED)

        # Update graph view - add separator for each database
        if db_stats:
            console_print(f"\n=== Updating DB Graphs for {db_name} ===")
            console_print(f"DB view mode: {self.db_view_mode}")
            console_print(f"Number of metrics: {len(db_stats)}")

            # Add separator label for this database (will be added only once)
            self.db_metrics_visualizer.add_separator(label=f"📊 {db_name}")

            # Update graph view for each metric
            for metric_name, value in db_stats.items():
                # Use prefixed metric name to avoid conflicts between databases
                graph_key = f"{db_name}_{metric_name}"
                console_print(f"  Adding/Updating graph for: {graph_key} = {value}")

                # Add metric graph if it doesn't exist
                if graph_key not in self.db_metrics_visualizer.graphs:
                    # 60 points * 5 seconds = 300 seconds = 5 minutes
                    self.db_metrics_visualizer.add_metric(metric_name)
                    # Store the graph under the prefixed key
                    graph = self.db_metrics_visualizer.graphs[metric_name]
                    del self.db_metrics_visualizer.graphs[metric_name]
                    self.db_metrics_visualizer.graphs[graph_key] = graph
                    console_print(f"    Created new graph for {graph_key}")

                # Update metric value
                self.db_metrics_visualizer.update_metric(graph_key, value)
                console_print(f"    Updated graph for {graph_key}")

            console_print(f"  Total graphs now: {len(self.db_metrics_visualizer.graphs)}")
            console_print("=" * 40)

    def update_monitor_status_label(self):
        """Update the monitor status label and main status bar based on active monitoring"""
        # Check if any server connections are being monitored
        server_monitoring = any(conn.get('monitoring', False) for conn in self.monitor_connections.values())

        # Check if any databases are being monitored
        db_monitoring = len(self.monitored_databases) > 0

        if server_monitoring and db_monitoring:
            server_count = sum(1 for conn in self.monitor_connections.values() if conn.get('monitoring', False))
            db_count = len(self.monitored_databases)
            status_text = f"Monitoring {server_count} server(s) & {db_count} database(s)"
            self.monitor_status_label.config(
                text=f"Status: {status_text}",
                foreground="green"
            )
            self.update_status(f"✓ {status_text}", "success")
        elif server_monitoring:
            server_count = sum(1 for conn in self.monitor_connections.values() if conn.get('monitoring', False))
            status_text = f"Monitoring {server_count} server(s)"
            self.monitor_status_label.config(
                text=f"Status: {status_text}",
                foreground="green"
            )
            self.update_status(f"✓ {status_text}", "success")
        elif db_monitoring:
            db_count = len(self.monitored_databases)
            status_text = f"Monitoring {db_count} database(s)"
            self.monitor_status_label.config(
                text=f"Status: {status_text}",
                foreground="green"
            )
            self.update_status(f"✓ {status_text}", "success")
        else:
            self.monitor_status_label.config(
                text="Status: No active monitoring",
                foreground="gray"
            )
            self.update_status("No active monitoring", "info")

    def clear_os_graphs(self):
        """Clear all OS resource graphs"""
        if hasattr(self, 'os_metrics_visualizer'):
            self.os_metrics_visualizer.clear_all()
            self.update_status("OS resource graphs cleared", "success")

    def clear_db_graphs(self):
        """Clear all database graphs"""
        if hasattr(self, 'db_metrics_visualizer'):
            self.db_metrics_visualizer.clear_all()
            self.update_status("Database graphs cleared", "success")

    def start_monitor_updates(self):
        """Start periodic metric updates"""
        # Initialize database listbox display
        self.update_monitored_db_listbox()
        # Start metrics update loop
        self.update_monitor_metrics()

    def update_monitor_metrics(self):
        """Update metrics display for all active monitors - runs in background"""
        # Run in background thread to avoid UI freeze
        thread = threading.Thread(
            target=self._update_monitor_metrics_thread,
            daemon=True
        )
        thread.start()

    def _update_monitor_metrics_thread(self):
        """Background thread to update monitor metrics"""
        try:
            # Update OS metrics
            os_text = ""
            db_text = ""

            for conn_name, conn in list(self.monitor_connections.items()):
                # Skip servers pending removal
                if conn_name in self.servers_pending_removal:
                    console_print(f"Skipping metrics for {conn_name} (pending removal)")
                    continue

                if conn['monitoring']:
                    # Track this query thread
                    self.active_server_query_threads[conn_name] = threading.current_thread()
                    # Run SSH commands to get metrics using existing connection
                    ssh_host = f"{conn['username']}@{conn['host']}"

                    # Check control socket if exists
                    if 'control_path' in conn:
                        control_path = conn['control_path']
                        if not os.path.exists(control_path):
                            os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                            os_text += f"  Error: SSH master connection lost\n"
                            os_text += f"  Please restart monitoring for this server\n\n"
                            conn['monitoring'] = False
                            continue

                    # Detect OS type once and cache it
                    if 'os_type' not in conn:
                        os_detect_cmd = "uname -s"
                        os_type = "Linux"  # default

                        try:
                            # Use ControlPath for connections
                            ssh_cmd_detect = ['ssh']
                            if 'control_path' in conn:
                                ssh_cmd_detect.extend(['-o', f"ControlPath={conn['control_path']}"])
                                ssh_cmd_detect.extend(['-o', 'ControlMaster=auto'])
                            ssh_cmd_detect.extend(['-o', f'ConnectTimeout={self.ssh_test_timeout}'])
                            ssh_cmd_detect.extend([ssh_host, os_detect_cmd])

                            result_os = subprocess.run(ssh_cmd_detect, capture_output=True, text=True, timeout=self.ssh_test_timeout + 10)
                            if result_os.returncode == 0:
                                os_type = result_os.stdout.strip()
                        except (subprocess.SubprocessError, OSError):
                            pass  # Use default OS type

                        conn['os_type'] = os_type
                    else:
                        os_type = conn['os_type']

                    # Build OS-specific commands
                    if os_type == "Darwin":  # macOS
                        cmd = """
                        echo "===CPU==="
                        top -l 1 | grep "CPU usage"
                        echo "===MEM==="
                        vm_stat | grep -E "Pages (free|active|inactive|speculative|wired down)"
                        echo "===MEMTOTAL==="
                        sysctl hw.memsize
                        echo "===DISK==="
                        df -h / | tail -1
                        echo "===LOAD==="
                        uptime
                        echo "===PROC==="
                        ps -e | wc -l
                        echo "===END==="
                        """
                    else:  # Linux
                        cmd = """
                        echo "===CPU==="
                        top -bn1 | grep -i "cpu" | head -1
                        echo "===MEM==="
                        free -m | grep "^Mem:"
                        echo "===DISK==="
                        df -h / | tail -1
                        echo "===LOAD==="
                        uptime
                        echo "===PROC==="
                        ps -e | wc -l
                        echo "===END==="
                        """

                    # Execute monitoring command
                    try:
                        # Use ControlPath for connections
                        ssh_cmd = ['ssh']
                        if 'control_path' in conn:
                            ssh_cmd.extend(['-o', f"ControlPath={conn['control_path']}"])
                            ssh_cmd.extend(['-o', 'ControlMaster=auto'])
                        ssh_cmd.extend(['-o', f'ConnectTimeout={self.ssh_test_timeout}'])
                        ssh_cmd.extend([ssh_host, cmd])

                        result = subprocess.run(
                            ssh_cmd,
                            capture_output=True,
                            text=True,
                            timeout=self.ssh_timeout
                        )

                        if result.returncode == 0:
                            output = result.stdout

                            # Parse the output
                            try:
                                # Extract CPU
                                cpu_match = output.split('===CPU===')[1].split('===MEM===')[0].strip()

                                if os_type == "Darwin":  # macOS
                                    # macOS format: "CPU usage: 12.34% user, 56.78% sys, 30.88% idle"
                                    idle_match = re.search(r'(\d+\.?\d*)%\s+idle', cpu_match)
                                    if idle_match:
                                        idle = float(idle_match.group(1))
                                        cpu_usage = round(100 - idle, 1)
                                    else:
                                        cpu_usage = "N/A"
                                else:  # Linux
                                    # Try to extract idle percentage and calculate usage
                                    if 'id' in cpu_match or 'idle' in cpu_match:
                                        # Look for pattern like "23.4 id" or "23.4%id"
                                        idle_match = re.search(r'(\d+\.?\d*)\s*%?\s*i?d', cpu_match)
                                        if idle_match:
                                            idle = float(idle_match.group(1))
                                            cpu_usage = round(100 - idle, 1)
                                        else:
                                            cpu_usage = "N/A"
                                    else:
                                        cpu_usage = "N/A"

                                # Extract Memory
                                if os_type == "Darwin":  # macOS
                                    mem_match = output.split('===MEM===')[1].split('===MEMTOTAL===')[0].strip()
                                    memtotal_match = output.split('===MEMTOTAL===')[1].split('===DISK===')[0].strip()

                                    # Parse vm_stat output (pages are in 4KB chunks, numbers may have trailing dots)
                                    pages_free = re.search(r'Pages free:\s+(\d+)', mem_match)
                                    pages_active = re.search(r'Pages active:\s+(\d+)', mem_match)
                                    pages_inactive = re.search(r'Pages inactive:\s+(\d+)', mem_match)
                                    pages_speculative = re.search(r'Pages speculative:\s+(\d+)', mem_match)
                                    pages_wired = re.search(r'Pages wired down:\s+(\d+)', mem_match)

                                    # Parse total memory
                                    memtotal = re.search(r'hw\.memsize:\s+(\d+)', memtotal_match)

                                    if all([pages_free, pages_active, pages_wired, memtotal]):
                                        page_size = 4096  # 4KB
                                        free_pages = int(pages_free.group(1))
                                        active_pages = int(pages_active.group(1))
                                        inactive_pages = int(pages_inactive.group(1)) if pages_inactive else 0
                                        speculative_pages = int(pages_speculative.group(1)) if pages_speculative else 0
                                        wired_pages = int(pages_wired.group(1))

                                        total_bytes = int(memtotal.group(1))
                                        total_mb = total_bytes / (1024 * 1024)

                                        used_pages = active_pages + wired_pages
                                        used_mb = (used_pages * page_size) / (1024 * 1024)

                                        mem_total = round(total_mb)
                                        mem_used = round(used_mb)
                                        mem_percent = round((used_mb / total_mb) * 100, 1)
                                    else:
                                        mem_total = mem_used = mem_percent = "N/A"
                                else:  # Linux
                                    mem_match = output.split('===MEM===')[1].split('===DISK===')[0].strip()
                                    mem_parts = mem_match.split()
                                    if len(mem_parts) >= 3:
                                        mem_total = mem_parts[1]
                                        mem_used = mem_parts[2]
                                        try:
                                            mem_percent = round((float(mem_used) / float(mem_total)) * 100, 1)
                                        except (ValueError, ZeroDivisionError, TypeError):
                                            mem_percent = "N/A"
                                    else:
                                        mem_total = mem_used = mem_percent = "N/A"

                                # Extract Disk
                                disk_match = output.split('===DISK===')[1].split('===LOAD===')[0].strip()
                                disk_parts = disk_match.split()
                                if len(disk_parts) >= 5:
                                    disk_size = disk_parts[1]
                                    disk_used = disk_parts[2]
                                    disk_percent = disk_parts[4].rstrip('%')
                                else:
                                    disk_size = disk_used = disk_percent = "N/A"

                                # Extract Load
                                load_match = output.split('===LOAD===')[1].split('===PROC===')[0].strip()
                                if 'load average' in load_match.lower():
                                    # Extract just the numbers after "load average:" or "load averages:"
                                    load_split = re.split(r'load averages?:', load_match, flags=re.IGNORECASE)
                                    if len(load_split) > 1:
                                        load_avg = load_split[-1].strip()
                                    else:
                                        load_avg = "N/A"
                                else:
                                    load_avg = "N/A"

                                # Extract Process count
                                proc_match = output.split('===PROC===')[1].split('===END===')[0].strip()
                                try:
                                    # Subtract 1 for the header line from ps command
                                    process_count = int(proc_match) - 1
                                except (ValueError, TypeError):
                                    process_count = proc_match

                                # Display metrics in text
                                os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                                os_text += f"  CPU Usage:       {cpu_usage}%\n"
                                os_text += f"  Memory Usage:    {mem_percent}% ({mem_used}/{mem_total} MB)\n"
                                os_text += f"  Disk Usage:      {disk_percent}% ({disk_used}/{disk_size})\n"
                                os_text += f"  Load Average:    {load_avg}\n"
                                os_text += f"  Processes:       {process_count}\n"
                                os_text += f"  Last Update:     {time.strftime('%H:%M:%S')}\n\n"

                                # Update OS graphs
                                try:
                                    # Check if this is the first metric for this host (add separator/header)
                                    first_metric_for_host = f"{conn_name} - CPU %"
                                    if first_metric_for_host not in self.os_metrics_visualizer.graphs:
                                        # Add separator/header for this host (always add for new hosts)
                                        self.os_metrics_visualizer.add_separator(f"=== {conn_name} ({conn['host']}) ===")

                                    if isinstance(cpu_usage, (int, float)):
                                        metric_name = f"{conn_name} - CPU %"
                                        if metric_name not in self.os_metrics_visualizer.graphs:
                                            # 60 points * 5 seconds = 5 minutes
                                            self.os_metrics_visualizer.add_metric(metric_name)
                                        self.os_metrics_visualizer.update_metric(metric_name, cpu_usage)

                                    if isinstance(mem_percent, (int, float)):
                                        metric_name = f"{conn_name} - Memory %"
                                        if metric_name not in self.os_metrics_visualizer.graphs:
                                            # 60 points * 5 seconds = 5 minutes
                                            self.os_metrics_visualizer.add_metric(metric_name)
                                        self.os_metrics_visualizer.update_metric(metric_name, mem_percent)

                                    if isinstance(process_count, int):
                                        metric_name = f"{conn_name} - Processes"
                                        if metric_name not in self.os_metrics_visualizer.graphs:
                                            # 60 points * 5 seconds = 5 minutes
                                            self.os_metrics_visualizer.add_metric(metric_name)
                                        self.os_metrics_visualizer.update_metric(metric_name, process_count)
                                except Exception as graph_error:
                                    console_print(f"Error updating OS graphs: {graph_error}")

                            except Exception as parse_error:
                                os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                                os_text += f"  Parse Error: {str(parse_error)}\n"
                                os_text += f"  Raw Output (first 500 chars):\n{output[:500]}\n\n"
                        else:
                            os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                            os_text += f"  SSH Command Failed\n"
                            os_text += f"  Return Code: {result.returncode}\n"
                            if result.stderr:
                                os_text += f"  Error: {result.stderr[:300]}\n"
                            if result.stdout:
                                os_text += f"  Output: {result.stdout[:300]}\n"
                            os_text += "\n"

                    except subprocess.TimeoutExpired:
                        os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                        os_text += f"  Error: SSH timeout\n\n"
                    except Exception as e:
                        os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                        os_text += f"  Error: {str(e)}\n\n"
                    finally:
                        # Remove from active threads when done
                        if conn_name in self.active_server_query_threads:
                            del self.active_server_query_threads[conn_name]

            if not os_text:
                os_text = "No active monitoring connections.\nClick 'Select Server' to start monitoring."

            # Also query monitored databases
            # Fetch metrics for all monitored databases
            for db_name, db_manager in list(self.monitored_databases.items()):
                # Skip databases pending removal
                if db_name in self.databases_pending_removal:
                    console_print(f"Skipping metrics for {db_name} (pending removal)")
                    continue

                # Track this query in active threads
                self.active_db_query_threads[db_name] = threading.current_thread()

                try:
                    db_stats = self.get_db_metrics(db_manager)
                    if db_stats:
                        # Update UI on main thread for each database
                        self.root.after(0, self._update_db_metrics_ui, db_name, db_stats)
                finally:
                    # Remove from active threads when done
                    if db_name in self.active_db_query_threads:
                        del self.active_db_query_threads[db_name]

            # Update OS UI on main thread
            self.root.after(0, self._update_monitor_os_ui, os_text)

        except Exception as e:
            console_print(f"Error in monitor metrics thread: {e}")
            import traceback
            traceback.print_exc()

    def _update_monitor_os_ui(self, os_text):
        """Update OS monitor UI (runs on main thread)"""
        try:
            # Clean up stale graphs (graphs for servers no longer being monitored)
            self._cleanup_stale_os_graphs()

            # Update OS display
            self.os_metrics_text.config(state=tk.NORMAL)
            self.os_metrics_text.delete(1.0, tk.END)
            self.os_metrics_text.insert(1.0, os_text)
            self.os_metrics_text.config(state=tk.DISABLED)

        except Exception as e:
            console_print(f"Error updating OS monitor UI: {e}")

        # Schedule next update (every 5 seconds)
        self.monitor_update_job = self.root.after(self.refresh_interval, self.update_monitor_metrics)

    def get_db_metrics(self, db_manager):
        """Get database-level metrics"""
        try:
            metrics = {}
            db_type = db_manager.db_type

            console_print(f"\n=== Getting DB Metrics for {db_type} ===")

            if db_type == "MySQL" or db_type == "MariaDB":
                # MySQL/MariaDB comprehensive metrics
                queries = {
                    # Connection metrics
                    'Active Connections': "SELECT COUNT(*) AS active_conn FROM information_schema.PROCESSLIST WHERE COMMAND != 'Sleep'",
                    'Total Connections': "SELECT COUNT(*) AS total_conn FROM information_schema.PROCESSLIST",
                    'Max Connections': "SHOW VARIABLES LIKE 'max_connections'",

                    # Query metrics
                    'Queries Per Second': "SHOW GLOBAL STATUS LIKE 'Questions'",
                    'Slow Queries': "SHOW GLOBAL STATUS LIKE 'Slow_queries'",
                    'Threads Running': "SHOW GLOBAL STATUS LIKE 'Threads_running'",

                    # Lock metrics
                    'Table Locks Waited': "SHOW GLOBAL STATUS LIKE 'Table_locks_waited'",
                    'Innodb Row Lock Waits': "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'",

                    # Buffer pool metrics
                    'Buffer Pool Size': "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_pages_total'",
                    'Buffer Pool Used': "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_pages_data'",

                    # Cache metrics
                    'Query Cache Hit Rate': "SHOW GLOBAL STATUS LIKE 'Qcache_hits'",

                    # Traffic metrics
                    'Bytes Received': "SHOW GLOBAL STATUS LIKE 'Bytes_received'",
                    'Bytes Sent': "SHOW GLOBAL STATUS LIKE 'Bytes_sent'",
                }

                for metric_name, query in queries.items():
                    try:
                        console_print(f"  Executing: {metric_name}")
                        console_print(f"  Query: {query[:80]}...")

                        # execute_query returns (result, error) tuple
                        # Use lock to ensure thread-safe database access
                        with self.db_query_lock:
                            result_data, error = db_manager.execute_query(query)

                        if error:
                            console_print(f"  ✗ Error: {error}")
                            continue

                        if not result_data:
                            console_print(f"  ✗ No result data")
                            continue

                        # Check if it's a dict with 'rows' key (from query result)
                        if isinstance(result_data, dict):
                            rows = None
                            if 'rows' in result_data:
                                rows = result_data['rows']
                            elif 'data' in result_data:
                                rows = result_data['data']

                            if rows and len(rows) > 0:
                                # Check if it's SHOW STATUS format (Variable_name, Value)
                                if len(rows[0]) == 2:
                                    # SHOW STATUS/VARIABLES format
                                    value = rows[0][1]
                                else:
                                    # COUNT or simple SELECT format
                                    value = rows[0][0]

                                # Try to convert to numeric
                                try:
                                    if isinstance(value, str):
                                        value = float(value) if '.' in value else int(value)
                                except (ValueError, TypeError):
                                    pass  # Keep original value if conversion fails

                                metrics[metric_name] = value
                                console_print(f"  ✓ {metric_name} = {value}")
                            else:
                                console_print(f"  ✗ No rows in result")
                        elif isinstance(result_data, list):
                            if len(result_data) > 0:
                                value = result_data[0][1] if len(result_data[0]) > 1 else result_data[0][0]
                                metrics[metric_name] = value
                                console_print(f"  ✓ {metric_name} = {value}")
                        else:
                            console_print(f"  ✗ Unexpected result type: {type(result_data)}")

                    except Exception as e:
                        console_print(f"  ✗ Exception: {e}")
                        import traceback
                        traceback.print_exc()

            elif db_type == "Oracle":
                # Oracle comprehensive metrics
                queries = {
                    # Session metrics
                    'Active Sessions': "SELECT COUNT(*) FROM v$session WHERE status = 'ACTIVE'",
                    'Total Sessions': "SELECT COUNT(*) FROM v$session",
                    'Inactive Sessions': "SELECT COUNT(*) FROM v$session WHERE status = 'INACTIVE'",

                    # Query metrics
                    'Executions Per Sec': "SELECT value FROM v$sysmetric WHERE metric_name = 'Executions Per Sec' AND group_id = 2",
                    'User Calls Per Sec': "SELECT value FROM v$sysmetric WHERE metric_name = 'User Calls Per Sec' AND group_id = 2",

                    # I/O metrics
                    'Physical Reads': "SELECT value FROM v$sysstat WHERE name = 'physical reads'",
                    'Physical Writes': "SELECT value FROM v$sysstat WHERE name = 'physical writes'",
                    'DB Block Gets': "SELECT value FROM v$sysstat WHERE name = 'db block gets'",

                    # Lock metrics
                    'Enqueue Waits': "SELECT value FROM v$sysstat WHERE name = 'enqueue waits'",
                    'Lock Wait Time': "SELECT SUM(wait_time + time_waited) FROM v$session_wait WHERE wait_class != 'Idle'",

                    # Memory metrics
                    'SGA Size (MB)': "SELECT ROUND(SUM(value)/1024/1024, 2) FROM v$sga",
                    'PGA Used (MB)': "SELECT ROUND(value/1024/1024, 2) FROM v$pgastat WHERE name = 'total PGA allocated'",

                    # Performance metrics
                    'Buffer Cache Hit%': "SELECT ROUND((1 - (phy.value / (log.value + phy.value))) * 100, 2) FROM v$sysstat phy, v$sysstat log WHERE phy.name = 'physical reads' AND log.name = 'db block gets'",
                }

                # Check if connection is still valid before executing queries (once per cycle)
                try:
                    if not db_manager.conn or not hasattr(db_manager.conn, 'ping'):
                        console_print("  ✗ Oracle connection not valid (no connection or ping method)")
                        return metrics

                    # Ping the connection once at the start to ensure it's alive
                    db_manager.conn.ping()
                    console_print("  ✓ Oracle connection validated (checked once per cycle)")
                except Exception as ping_error:
                    console_print(f"  ✗ Oracle connection validation failed: {ping_error}")
                    return metrics  # Return empty metrics if connection is dead

                for metric_name, query in queries.items():
                    try:
                        console_print(f"  Executing: {metric_name}")

                        # Use lock to ensure thread-safe database access
                        with self.db_query_lock:
                            result_data, error = db_manager.execute_query(query)

                        console_print(f"  Result: {result_data}, Error: {error}")

                        # Check for connection errors and stop if detected
                        if error and ('DPI-1010' in str(error) or 'not connected' in str(error).lower()):
                            console_print(f"  ✗ Connection lost during query execution: {error}")
                            console_print(f"  ℹ Stopping remaining queries to prevent segfault")
                            break  # Stop executing remaining queries

                        if result_data and not error:
                            if isinstance(result_data, dict):
                                rows = None
                                if 'rows' in result_data:
                                    rows = result_data['rows']
                                elif 'data' in result_data:
                                    rows = result_data['data']

                                if rows and len(rows) > 0:
                                    metrics[metric_name] = rows[0][0]
                                    console_print(f"  ✓ {metric_name} = {rows[0][0]}")
                            elif isinstance(result_data, list) and len(result_data) > 0:
                                metrics[metric_name] = result_data[0][0]
                                console_print(f"  ✓ {metric_name} = {result_data[0][0]}")
                        else:
                            console_print(f"  ✗ No valid result for {metric_name}")

                    except Exception as e:
                        # Catch any cx_Oracle errors that might cause segfault
                        error_msg = str(e)
                        console_print(f"  ✗ Error getting metric {metric_name}: {error_msg}")

                        # If connection error, stop processing remaining metrics
                        if 'DPI-1010' in error_msg or 'not connected' in error_msg.lower():
                            console_print(f"  ✗ Connection error detected, stopping metric collection")
                            break

                        import traceback
                        traceback.print_exc()

            elif db_type == "PostgreSQL":
                # PostgreSQL comprehensive metrics
                queries = {
                    # Connection metrics
                    'Active Connections': "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'",
                    'Total Connections': "SELECT count(*) FROM pg_stat_activity",
                    'Idle Connections': "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'",
                    'Max Connections': "SELECT setting::int FROM pg_settings WHERE name = 'max_connections'",

                    # Database size
                    'Database Size (MB)': "SELECT ROUND(pg_database_size(current_database())::numeric/1024/1024, 2)",

                    # Transaction metrics
                    'Transactions Committed': "SELECT SUM(xact_commit) FROM pg_stat_database WHERE datname = current_database()",
                    'Transactions Rolled Back': "SELECT SUM(xact_rollback) FROM pg_stat_database WHERE datname = current_database()",

                    # Lock metrics
                    'Active Locks': "SELECT count(*) FROM pg_locks WHERE granted = true",
                    'Waiting Locks': "SELECT count(*) FROM pg_locks WHERE granted = false",

                    # I/O metrics
                    'Blocks Read': "SELECT SUM(blks_read) FROM pg_stat_database WHERE datname = current_database()",
                    'Blocks Hit': "SELECT SUM(blks_hit) FROM pg_stat_database WHERE datname = current_database()",

                    # Performance metrics
                    'Cache Hit Ratio %': "SELECT ROUND(100.0 * SUM(blks_hit) / NULLIF(SUM(blks_hit) + SUM(blks_read), 0), 2) FROM pg_stat_database WHERE datname = current_database()",
                    'Deadlocks': "SELECT SUM(deadlocks) FROM pg_stat_database WHERE datname = current_database()",

                    # Table stats
                    'Total Tuples': "SELECT SUM(n_live_tup) FROM pg_stat_user_tables",
                    'Dead Tuples': "SELECT SUM(n_dead_tup) FROM pg_stat_user_tables",
                }

                for metric_name, query in queries.items():
                    try:
                        console_print(f"  Executing: {metric_name}")
                        # Use lock to ensure thread-safe database access
                        with self.db_query_lock:
                            result_data, error = db_manager.execute_query(query)
                        console_print(f"  Result: {result_data}, Error: {error}")

                        if result_data and not error:
                            if isinstance(result_data, dict):
                                rows = None
                                if 'rows' in result_data:
                                    rows = result_data['rows']
                                elif 'data' in result_data:
                                    rows = result_data['data']

                                if rows and len(rows) > 0:
                                    metrics[metric_name] = rows[0][0]
                                    console_print(f"  ✓ {metric_name} = {rows[0][0]}")
                            elif isinstance(result_data, list) and len(result_data) > 0:
                                metrics[metric_name] = result_data[0][0]
                                console_print(f"  ✓ {metric_name} = {result_data[0][0]}")
                    except Exception as e:
                        console_print(f"  ✗ Error getting metric {metric_name}: {e}")
                        import traceback
                        traceback.print_exc()

            console_print(f"  Total metrics collected: {len(metrics)}")
            console_print(f"  Metrics: {metrics}")
            console_print("=====================================\n")

            return metrics if metrics else None

        except Exception as e:
            console_print(f"✗ Error in get_db_metrics: {e}")
            import traceback
            traceback.print_exc()
            return None

    def refresh_monitor_metrics(self):
        """Manually refresh metrics"""
        self.update_monitor_metrics()
        # Update status to show complete monitoring state
        self.update_monitor_status_label()

    def refresh_server_metrics(self):
        """Manually refresh only server/OS metrics"""
        # Run in background thread to avoid UI freeze
        thread = threading.Thread(
            target=self._refresh_server_metrics_thread,
            daemon=True
        )
        thread.start()

    def _refresh_server_metrics_thread(self):
        """Background thread to refresh only OS metrics"""
        try:
            os_text = ""

            for conn_name, conn in self.monitor_connections.items():
                if conn['monitoring']:
                    # Run SSH commands to get metrics using existing connection
                    ssh_host = f"{conn['username']}@{conn['host']}"

                    # Detect OS type once and cache it
                    if 'os_type' not in conn:
                        os_detect_cmd = "uname -s"
                        os_type = "Linux"  # default

                        try:
                            # Use ControlPath for connections
                            ssh_cmd_detect = ['ssh']
                            if 'control_path' in conn:
                                ssh_cmd_detect.extend(['-o', f"ControlPath={conn['control_path']}"])
                                ssh_cmd_detect.extend(['-o', 'ControlMaster=auto'])
                            ssh_cmd_detect.extend(['-o', f'ConnectTimeout={self.ssh_test_timeout}'])
                            ssh_cmd_detect.extend([ssh_host, os_detect_cmd])

                            result_os = subprocess.run(ssh_cmd_detect, capture_output=True, text=True, timeout=self.ssh_test_timeout + 10)
                            if result_os.returncode == 0:
                                os_type = result_os.stdout.strip()
                        except (subprocess.SubprocessError, OSError):
                            pass  # Use default OS type

                        conn['os_type'] = os_type
                    else:
                        os_type = conn['os_type']

                    # Build OS-specific commands
                    if os_type == "Darwin":  # macOS
                        cmd = """
                        echo "===CPU==="
                        top -l 1 | grep "CPU usage"
                        echo "===MEM==="
                        vm_stat | grep -E "Pages (free|active|inactive|speculative|wired down)"
                        echo "===MEMTOTAL==="
                        sysctl hw.memsize
                        echo "===DISK==="
                        df -h / | tail -1
                        echo "===LOAD==="
                        uptime
                        echo "===PROC==="
                        ps -e | wc -l
                        echo "===END==="
                        """
                    else:  # Linux
                        cmd = """
                        echo "===CPU==="
                        top -bn1 | grep -i "cpu" | head -1
                        echo "===MEM==="
                        free -m | grep "^Mem:"
                        echo "===DISK==="
                        df -h / | tail -1
                        echo "===LOAD==="
                        uptime
                        echo "===PROC==="
                        ps -e | wc -l
                        echo "===END==="
                        """

                    # Execute monitoring command
                    try:
                        # Use ControlPath for connections
                        ssh_cmd = ['ssh']
                        if 'control_path' in conn:
                            ssh_cmd.extend(['-o', f"ControlPath={conn['control_path']}"])
                            ssh_cmd.extend(['-o', 'ControlMaster=auto'])
                        ssh_cmd.extend(['-o', f'ConnectTimeout={self.ssh_test_timeout}'])
                        ssh_cmd.extend([ssh_host, cmd])

                        result = subprocess.run(
                            ssh_cmd,
                            capture_output=True,
                            text=True,
                            timeout=self.ssh_timeout
                        )

                        if result.returncode == 0:
                            output = result.stdout

                            # Parse the output and update OS metrics (same logic as in _update_monitor_metrics_thread)
                            # ... (copy the parsing logic from _update_monitor_metrics_thread for OS metrics)
                            # For brevity, I'll just append raw output for now
                            os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                            os_text += f"  Last refreshed: {time.strftime('%H:%M:%S')}\n"

                            # Extract key metrics
                            try:
                                cpu_match = output.split('===CPU===')[1].split('===MEM===')[0].strip()
                                # Format CPU output - each line indented
                                for line in cpu_match.split('\n'):
                                    if line.strip():
                                        os_text += f"  CPU: {line.strip()}\n"

                                mem_match = output.split('===MEM===')[1].split('===DISK===')[0] if 'Darwin' not in os_type else output.split('===MEM===')[1].split('===MEMTOTAL===')[0]
                                # Format Memory output - each line indented
                                mem_lines = mem_match.strip().split('\n')
                                if mem_lines:
                                    os_text += f"  Memory: {mem_lines[0].strip()}\n"
                                    for line in mem_lines[1:]:
                                        if line.strip():
                                            os_text += f"          {line.strip()}\n"

                                disk_match = output.split('===DISK===')[1].split('===LOAD===')[0].strip()
                                os_text += f"  Disk: {disk_match}\n"

                                load_match = output.split('===LOAD===')[1].split('===PROC===')[0].strip()
                                os_text += f"  Load: {load_match}\n"

                                os_text += "\n"
                            except Exception as parse_error:
                                os_text += f"  Parse Error: {str(parse_error)}\n\n"
                        else:
                            os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                            os_text += f"  SSH Command Failed\n"
                            os_text += f"  Return Code: {result.returncode}\n"
                            if result.stderr:
                                os_text += f"  Error: {result.stderr[:300]}\n"
                            if result.stdout:
                                os_text += f"  Output: {result.stdout[:300]}\n"
                            os_text += "\n"

                    except subprocess.TimeoutExpired:
                        os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                        os_text += f"  Error: SSH timeout\n\n"
                    except Exception as e:
                        os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                        os_text += f"  Error: {str(e)}\n\n"
                    finally:
                        # Remove from active threads when done
                        if conn_name in self.active_server_query_threads:
                            del self.active_server_query_threads[conn_name]

            if not os_text:
                os_text = "No active monitoring connections.\nClick 'Select Server' to start monitoring."

            # Update OS UI on main thread
            self.root.after(0, self._update_os_text_only, os_text)

        except Exception as e:
            console_print(f"Error in server metrics refresh thread: {e}")
            import traceback
            traceback.print_exc()

    def _update_os_text_only(self, os_text):
        """Update only OS text display (runs on main thread)"""
        try:
            self.os_metrics_text.config(state=tk.NORMAL)
            self.os_metrics_text.delete(1.0, tk.END)
            self.os_metrics_text.insert(1.0, os_text)
            self.os_metrics_text.config(state=tk.DISABLED)
            self.update_monitor_status_label()
        except Exception as e:
            console_print(f"Error updating OS text: {e}")


