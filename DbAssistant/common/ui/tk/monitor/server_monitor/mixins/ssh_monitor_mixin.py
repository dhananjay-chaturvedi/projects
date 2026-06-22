"""SSHMonitorMixin — ServerMonitorUI mixin."""

from __future__ import annotations

from common.ui.tk.monitor.server_monitor.mixins._shared import *  # noqa: F403

class SSHMonitorMixin:
    def add_monitor_connection(self):
        """Add a new monitoring connection"""


        dialog = tk.Toplevel(self.root)
        dialog.title("Add Monitor Connection")
        dialog.geometry("450x350")
        dialog.resizable(True, True)
        dialog.transient(self.root)
        dialog.grab_set()

        main_frame = make_scrollable(dialog)
        main_frame.configure(padding=20)

        ttk.Label(
            main_frame, text="Add Monitoring Connection", font=("Arial", 12, "bold")
        ).pack(pady=(0, 20))

        ttk.Label(main_frame, text="Target type:").pack(anchor=tk.W, pady=(5, 2))
        target_var = tk.StringVar(value="vm")
        target_frame = ttk.Frame(main_frame)
        target_frame.pack(fill=tk.X, pady=(0, 10))
        for val, lbl in (
            ("vm", "VM / host (SSH metrics)"),
            ("db_server", "DB server (SSH to DB host)"),
            ("service", "Other service (SSH)"),
        ):
            ttk.Radiobutton(
                target_frame, text=lbl, variable=target_var, value=val
            ).pack(anchor=tk.W)

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

        ttk.Label(main_frame, text="Password (optional):").pack(
            anchor=tk.W, pady=(5, 2)
        )
        ttk.Label(
            main_frame,
            text="For SSH/jump server/sudo as needed",
            font=("Arial", 8),
            foreground="gray",
        ).pack(anchor=tk.W, pady=(0, 2))
        password_entry = ttk.Entry(main_frame, width=40, show="*")
        password_entry.pack(fill=tk.X, pady=(0, 10))

        def test_connection():
            """Test the SSH connection with provided credentials"""
            host = host_entry.get().strip()
            username = user_entry.get().strip()
            password = password_entry.get()

            if not all([host, username]):
                messagebox.showerror(
                    "Error", "Please fill hostname and username to test!"
                )
                return

            self.update_status(f"Testing connection to {host}... (timeout: 30s)")

            # Build SSH command based on password configuration
            if password:
                # Single password - use sshpass
                ssh_cmd = [
                    "sshpass",
                    "-p",
                    password,
                    "ssh",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "-o",
                    f"ConnectTimeout={self.ssh_timeout}",
                    f"{username}@{host}",
                    "echo SSH_OK",
                ]

                try:
                    result = subprocess.run(
                        ssh_cmd,
                        capture_output=True,
                        text=True,
                        timeout=self.ssh_timeout + 5,
                    )

                    if result.returncode == 0 and "SSH_OK" in result.stdout:
                        messagebox.showinfo(
                            "Success",
                            f"✓ SSH connection successful to {username}@{host}",
                        )
                        self.update_status("Connection test successful", "success")
                    else:
                        error_msg = "SSH connection failed!"
                        if result.returncode != 0:
                            # Only show stderr if command actually failed (non-zero exit code)
                            if "sshpass" in result.stderr:
                                error_msg += "\n\nNote: 'sshpass' not found. Install it using:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)"
                            elif result.stderr:
                                # Filter out common SSH warnings that aren't actual errors
                                stderr_filtered = "\n".join(
                                    [
                                        line
                                        for line in result.stderr.split("\n")
                                        if not line.startswith(
                                            "Warning: Permanently added"
                                        )
                                    ]
                                )
                                if stderr_filtered.strip():
                                    error_msg += f"\n\nError: {stderr_filtered[:300]}"
                        messagebox.showerror("Error", error_msg)
                        self.update_status("Connection test failed", "error")

                except subprocess.TimeoutExpired:
                    error_msg = f"SSH connection timeout after 30 seconds!\n\nThe server '{host}' is not responding."
                    messagebox.showerror("Connection Timeout", error_msg)
                    self.update_status(f"SSH test timeout for {host}", "error")
                except FileNotFoundError as e:
                    if "sshpass" in str(e):
                        messagebox.showerror(
                            "Error",
                            "sshpass not found!\n\nTo use passwords for testing, install sshpass:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)",
                        )
                    else:
                        messagebox.showerror("Error", f"SSH error: {str(e)}")
                    self.update_status("Connection test error", "error")
                except Exception as e:
                    messagebox.showerror("Error", f"SSH error: {str(e)}")
                    self.update_status("Connection test error", "error")

            else:
                # No password - will use SSH key or prompt
                ssh_cmd = [
                    "ssh",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "-o",
                    f"ConnectTimeout={self.ssh_timeout}",
                    f"{username}@{host}",
                    "echo SSH_OK",
                ]

                try:
                    result = subprocess.run(
                        ssh_cmd,
                        capture_output=True,
                        text=True,
                        timeout=self.ssh_timeout,
                    )

                    if result.returncode == 0 and "SSH_OK" in result.stdout:
                        messagebox.showinfo(
                            "Success",
                            f"✓ SSH connection successful to {username}@{host}",
                        )
                        self.update_status("Connection test successful", "success")
                    else:
                        error_msg = "SSH connection failed!"
                        if result.returncode != 0 and result.stderr:
                            # Only show stderr if command actually failed (non-zero exit code)
                            # Filter out common SSH warnings that aren't actual errors
                            stderr_filtered = "\n".join(
                                [
                                    line
                                    for line in result.stderr.split("\n")
                                    if not line.startswith("Warning: Permanently added")
                                ]
                            )
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
                "host": host,
                "username": username,
                "password": password,
                "monitoring": False,
                "target_type": target_var.get(),
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
                password=password,
                target_type=target_var.get(),
            )

            dialog.destroy()

            if success:
                messagebox.showinfo(
                    "Success",
                    f"Connection '{name}' saved!\n\nUse 'Select Server' to start monitoring this server.",
                )
                self.update_status(f"✓ Connection '{name}' saved", "success")
            else:
                messagebox.showerror("Error", f"Failed to save connection:\n{msg}")

        # Buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(btn_frame, text="Test Connection", command=test_connection).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(
            btn_frame, text="Add", command=add_connection_only, style="Primary.TButton"
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            btn_frame, text="Save Connection", command=save_connection_persistent
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(
            side=tk.LEFT, padx=5
        )

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

        main_frame = make_scrollable(dialog)
        main_frame.configure(padding=20)

        ttk.Label(
            main_frame, text="Edit Monitoring Connection", font=("Arial", 12, "bold")
        ).pack(pady=(0, 20))

        # Connection Name (read-only)
        ttk.Label(main_frame, text="Connection Name:").pack(anchor=tk.W, pady=(5, 2))
        name_entry = ttk.Entry(main_frame, width=40)
        name_entry.insert(0, conn["name"])
        name_entry.config(state="readonly")
        name_entry.pack(fill=tk.X, pady=(0, 10))

        # SSH Details
        ttk.Label(main_frame, text="Hostname or IP:").pack(anchor=tk.W, pady=(5, 2))
        host_entry = ttk.Entry(main_frame, width=40)
        host_entry.pack(fill=tk.X, pady=(0, 10))
        host_entry.insert(0, conn["host"])

        ttk.Label(main_frame, text="SSH Username:").pack(anchor=tk.W, pady=(5, 2))
        user_entry = ttk.Entry(main_frame, width=40)
        user_entry.pack(fill=tk.X, pady=(0, 10))
        user_entry.insert(0, conn["username"])

        ttk.Label(main_frame, text="Password (optional):").pack(
            anchor=tk.W, pady=(5, 2)
        )
        ttk.Label(
            main_frame,
            text="For SSH/jump server/sudo as needed",
            font=("Arial", 8),
            foreground="gray",
        ).pack(anchor=tk.W, pady=(0, 2))
        password_entry = ttk.Entry(main_frame, width=40, show="*")
        password_entry.pack(fill=tk.X, pady=(0, 10))
        if conn.get("password"):
            password_entry.insert(0, conn["password"])

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
                password=password,
            )

            dialog.destroy()

            if success:
                messagebox.showinfo(
                    "Success", f"Connection '{name}' updated successfully!"
                )
                self.update_status(f"✓ Connection '{name}' updated", "success")
            else:
                messagebox.showerror("Error", f"Failed to update connection:\n{msg}")

        # Buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(
            btn_frame, text="Save", command=save_changes, style="Primary.TButton"
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(
            side=tk.LEFT, padx=5
        )

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
        conn_name = conn_text.split("[")[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]
        self.update_status(f"Testing connection to {conn['host']}... (timeout: 30s)")

        # Test connection - run simple echo command
        try:
            result = subprocess.run(
                [
                    "ssh",
                    "-o",
                    "StrictHostKeyChecking=no",
                    "-o",
                    "UserKnownHostsFile=/dev/null",
                    "-o",
                    "ConnectTimeout=30",
                    f"{conn['username']}@{conn['host']}",
                    "echo SSH_OK",
                ],
                capture_output=True,
                text=True,
                timeout=self.ssh_timeout,
            )

            if result.returncode == 0 and "SSH_OK" in result.stdout:
                messagebox.showinfo(
                    "Success",
                    f"SSH connection successful to {conn['username']}@{conn['host']}",
                )
                self.update_status("Connection test successful")
            else:
                error_msg = "SSH connection failed!"
                if result.returncode != 0 and result.stderr:
                    # Only show stderr if command actually failed
                    # Filter out common SSH warnings that aren't actual errors
                    stderr_filtered = "\n".join(
                        [
                            line
                            for line in result.stderr.split("\n")
                            if not line.startswith("Warning: Permanently added")
                        ]
                    )
                    if stderr_filtered.strip():
                        error_msg += f"\n\n{stderr_filtered[:200]}"
                messagebox.showerror("Error", error_msg)
                self.update_status("Connection test failed")

        except subprocess.TimeoutExpired:
            error_msg = f"SSH connection timeout after 30 seconds!\n\nThe server '{conn['host']}' is not responding.\n\nPossible causes:\n- Server is down or unreachable\n- Firewall blocking SSH port (default: 22)\n- Network connectivity issues\n- Wrong hostname/IP address"
            messagebox.showerror("Connection Timeout", error_msg)
            self.update_status(f"SSH test timeout for {conn['host']}")
            console_print(
                f"SSH test connection timeout after 30 seconds for {conn_name}"
            )
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
        conn_name = conn_text.split("[")[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]

        if conn["monitoring"]:
            messagebox.showinfo("Info", "Already monitoring this connection!")
            return

        # Create control socket path for SSH multiplexing
        control_path = os.path.join(
            tempfile.gettempdir(), f"ssh_monitor_{conn_name.replace(' ', '_')}"
        )
        conn["control_path"] = control_path

        # Establish master SSH connection in background
        ssh_host = f"{conn['username']}@{conn['host']}"

        # Build SSH command with password if available
        if conn.get("password"):
            # Use sshpass for automatic password authentication
            master_cmd = [
                "sshpass",
                "-p",
                conn["password"],
                "ssh",
                "-M",
                "-N",
                "-f",
                "-o",
                "ControlMaster=yes",
                "-o",
                f"ControlPath={control_path}",
                "-o",
                f"ControlPersist={self.ssh_control_persist}",
                "-o",
                "StrictHostKeyChecking=no",  # Auto-accept host key
                ssh_host,
            ]
        else:
            # Standard SSH (will prompt for password)
            master_cmd = [
                "ssh",
                "-M",
                "-N",
                "-f",
                "-o",
                "ControlMaster=yes",
                "-o",
                f"ControlPath={control_path}",
                "-o",
                f"ControlPersist={self.ssh_control_persist}",
                ssh_host,
            ]

        self.update_status(
            f"Establishing SSH connection to {conn['host']}... (timeout: {self.ssh_timeout}s)"
        )

        try:
            # SSH timeout applies ONLY to establishing the SSH connection
            # Once connected, monitoring commands can run indefinitely
            result = subprocess.run(
                master_cmd, timeout=self.ssh_timeout, capture_output=True, text=True
            )

            if result.returncode == 0:
                conn["monitoring"] = True
                self._start_ssh_keepalive()
                self.update_monitor_status_label()
                messagebox.showinfo("Success", f"Started monitoring '{conn_name}'")
            else:
                error_msg = "Failed to establish SSH connection!"
                if conn.get("password") and "sshpass" in result.stderr:
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
            if "sshpass" in str(e):
                messagebox.showerror(
                    "Error",
                    "sshpass not found!\n\nTo use saved passwords, install sshpass:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)\n\nAlternatively, leave password empty and enter it manually.",
                )
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
        conn_name = conn_text.split("[")[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]

        if not conn["monitoring"]:
            messagebox.showinfo("Info", "This connection is not being monitored!")
            return

        # Close SSH master connection
        if "control_path" in conn:
            ssh_host = f"{conn['username']}@{conn['host']}"
            try:
                subprocess.run(
                    [
                        "ssh",
                        "-O",
                        "exit",
                        "-o",
                        f"ControlPath={conn['control_path']}",
                        ssh_host,
                    ],
                    timeout=self.ssh_test_timeout,
                )
            except (subprocess.SubprocessError, OSError):
                pass  # Ignore SSH cleanup errors

        conn["monitoring"] = False
        self.update_monitor_status_label()
        messagebox.showinfo("Success", f"Stopped monitoring '{conn_name}'")

    def remove_monitor_connection(self):
        """Remove selected monitoring connection - stops immediately, cleanup async"""
        if not self.monitor_connections:
            messagebox.showwarning(
                "Warning", "No server connections available to remove!"
            )
            return

        selection = self.monitor_conn_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a server to remove!")
            return

        conn_text = self.monitor_conn_listbox.get(selection[0])
        conn_name = conn_text.split("[")[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]

        # Mark server as pending removal (stops new queries IMMEDIATELY)
        if conn["monitoring"]:
            self.servers_pending_removal.add(conn_name)
            conn["monitoring"] = False  # Stop monitoring immediately
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
                self.root.after(
                    100, self._complete_server_removal, conn_name, listbox_index
                )
                return

        # Query finished, safe to close SSH and remove
        conn = self.monitor_connections[conn_name]

        # Close SSH master connection
        if "control_path" in conn:
            ssh_host = f"{conn['username']}@{conn['host']}"
            try:
                subprocess.run(
                    [
                        "ssh",
                        "-O",
                        "exit",
                        "-o",
                        f"ControlPath={conn['control_path']}",
                        ssh_host,
                    ],
                    timeout=self.ssh_test_timeout,
                )
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
        conn_name = conn_text.split("[")[0].strip()

        if conn_name in self.monitor_connections:
            conn = self.monitor_connections[conn_name]
            status = "Monitoring" if conn["monitoring"] else "Not monitoring"
            self.monitor_status_label.config(
                text=f"Status: {conn_name} - {status}",
                foreground="green" if conn["monitoring"] else "gray",
            )

    def save_monitor_connection(self):
        """Save selected monitor connection"""
        selection = self.monitor_conn_listbox.curselection()
        if not selection:
            messagebox.showwarning("Warning", "Please select a connection to save!")
            return

        conn_text = self.monitor_conn_listbox.get(selection[0])
        conn_name = conn_text.split("[")[0].strip()

        if conn_name not in self.monitor_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        conn = self.monitor_connections[conn_name]

        # Save to monitor connection manager
        success, msg = self.monitor_connection_manager.add_connection(
            name=conn_name,
            host=conn["host"],
            username=conn["username"],
            password=conn.get("password"),
        )

        if success:
            messagebox.showinfo(
                "Success", f"Monitor connection '{conn_name}' saved successfully!"
            )
            self.update_status(f"✓ Monitor connection '{conn_name}' saved", "success")
        else:
            messagebox.showerror("Error", msg)
            self.update_status(f"✗ Failed to save monitor connection", "error")

    def select_server_to_monitor(self):
        """Select a server to monitor from saved connections - starts monitoring immediately"""
        saved_connections = self.monitor_connection_manager.get_all_connections()

        if not saved_connections:
            messagebox.showinfo(
                "Info",
                "No saved server connections found.\nPlease add a connection first.",
            )
            return

        # Create dialog to select server
        dialog = tk.Toplevel(self.root)
        dialog.title("Select Server to Monitor")
        dialog.geometry("450x250")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(
            dialog, text="Select Server to Monitor:", font=("Arial", 11, "bold")
        ).pack(pady=20, padx=20)

        # Dropdown with saved connections
        server_combo = ttk.Combobox(dialog, state="readonly", width=50)
        server_combo.pack(pady=10, padx=20)

        # Build list of servers with status
        server_options = []
        conn_map = {}
        for conn in saved_connections:
            conn_name = conn["name"]
            if conn_name in self.monitor_connections:
                status = " [Already Monitoring]"
            else:
                status = ""
            display_name = f"{conn_name} [{conn['username']}@{conn['host']}]{status}"
            server_options.append(display_name)
            conn_map[display_name] = conn

        server_combo["values"] = server_options
        if server_options:
            server_combo.current(0)

        def add_selected():
            selected = server_combo.get()
            if not selected:
                messagebox.showwarning("Warning", "Please select a server!")
                return

            conn = conn_map[selected]
            conn_name = conn["name"]

            # Check if already monitoring
            if conn_name in self.monitor_connections:
                messagebox.showinfo(
                    "Info", f"'{conn_name}' is already being monitored!"
                )
                dialog.destroy()
                return

            # Add to monitor connections
            self.monitor_connections[conn_name] = {
                "host": conn["host"],
                "username": conn["username"],
                "password": conn.get("password"),
                "monitoring": False,
            }

            # Add to listbox
            self.monitor_conn_listbox.insert(
                tk.END, f"{conn_name} [{conn['username']}@{conn['host']}]"
            )

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
            self.edit_monitor_connection(conn["name"])

        def delete_selected():
            """Delete the selected saved connection"""
            selected = server_combo.get()
            if not selected:
                messagebox.showwarning("Warning", "Please select a server to delete!")
                return

            conn = conn_map[selected]
            conn_name = conn["name"]

            # Confirm deletion
            confirm = messagebox.askyesno(
                "Confirm Delete",
                f"Are you sure you want to delete connection '{conn_name}'?\n\nThis action cannot be undone.",
            )

            if not confirm:
                return

            # If currently monitoring, stop it first
            if conn_name in self.monitor_connections:
                monitor_conn = self.monitor_connections[conn_name]

                # Mark as pending removal and stop monitoring
                if monitor_conn.get("monitoring"):
                    self.servers_pending_removal.add(conn_name)
                    monitor_conn["monitoring"] = False

                    # Close SSH master connection
                    if "control_path" in monitor_conn:
                        ssh_host = f"{monitor_conn['username']}@{monitor_conn['host']}"
                        try:
                            subprocess.run(
                                [
                                    "ssh",
                                    "-O",
                                    "exit",
                                    "-o",
                                    f"ControlPath={monitor_conn['control_path']}",
                                    ssh_host,
                                ],
                                timeout=self.ssh_test_timeout,
                            )
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
                messagebox.showinfo(
                    "Success", f"Connection '{conn_name}' deleted successfully!"
                )
                self.update_status(f"✓ Connection '{conn_name}' deleted", "success")

                # Close dialog and reopen to refresh list
                dialog.destroy()
                self.select_server_to_monitor()
            else:
                messagebox.showerror("Error", f"Failed to delete connection:\n{msg}")

        # Buttons
        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=20)
        ttk.Button(
            btn_frame,
            text="Select",
            command=add_selected,
            width=12,
            style="Success.TButton",
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Edit", command=edit_selected, width=12).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(
            btn_frame,
            text="Delete",
            command=delete_selected,
            width=12,
            style="Error.TButton",
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy, width=12).pack(
            side=tk.LEFT, padx=5
        )

    def _start_monitoring_for_server(self, conn_name):
        """Internal function to start monitoring for a specific server"""
        if conn_name not in self.monitor_connections:
            return

        conn = self.monitor_connections[conn_name]

        if conn["monitoring"]:
            return  # Already monitoring

        # Create control socket path for SSH multiplexing
        control_path = os.path.join(
            tempfile.gettempdir(), f"ssh_monitor_{conn_name.replace(' ', '_')}"
        )
        conn["control_path"] = control_path

        # Establish master SSH connection in background
        ssh_host = f"{conn['username']}@{conn['host']}"

        self.update_status(
            f"Establishing SSH connection to {conn['host']}... (timeout: 30s)"
        )

        # Check if we need password authentication
        password = conn.get("password")

        if password:
            # Single password - use sshpass
            master_cmd = [
                "sshpass",
                "-p",
                password,
                "ssh",
                "-M",
                "-N",
                "-f",
                "-o",
                "ControlMaster=yes",
                "-o",
                f"ControlPath={control_path}",
                "-o",
                f"ControlPersist={self.ssh_control_persist}",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                ssh_host,
            ]

            try:
                result = subprocess.run(
                    master_cmd,
                    timeout=self.ssh_timeout + 5,
                    capture_output=True,
                    text=True,
                )

                if result.returncode == 0:
                    conn["monitoring"] = True
                    self._start_ssh_keepalive()
                    self.update_monitor_status_label()
                    messagebox.showinfo("Success", f"Started monitoring '{conn_name}'")
                    self.update_status(f"✓ Monitoring {conn_name}", "success")
                else:
                    error_msg = "Failed to establish SSH connection!"
                    if "sshpass" in result.stderr:
                        error_msg += "\n\nNote: 'sshpass' not found. Install it using:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)"
                    elif result.stderr:
                        error_msg += f"\n\nError: {result.stderr[:300]}"
                    messagebox.showerror("Error", error_msg)
                    self.update_status("SSH connection failed")
                    del self.monitor_connections[conn_name]
                    self._refresh_monitor_conn_listbox()

            except subprocess.TimeoutExpired:
                messagebox.showerror(
                    "Connection Timeout",
                    f"SSH connection timeout!\n\nThe server '{conn['host']}' is not responding.",
                )
                self.update_status(f"SSH connection timeout for {conn_name}")
                del self.monitor_connections[conn_name]
                self._refresh_monitor_conn_listbox()
            except FileNotFoundError as e:
                if "sshpass" in str(e):
                    messagebox.showerror(
                        "Error",
                        "sshpass not found!\n\nTo use passwords, install sshpass:\n  brew install sshpass (macOS)\n  apt-get install sshpass (Linux)",
                    )
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
                "ssh",
                "-M",
                "-N",
                "-f",
                "-o",
                "ControlMaster=yes",
                "-o",
                f"ControlPath={control_path}",
                "-o",
                f"ControlPersist={self.ssh_control_persist}",
                "-o",
                "StrictHostKeyChecking=no",
                ssh_host,
            ]

            try:
                result = subprocess.run(
                    master_cmd,
                    timeout=self.ssh_timeout + 5,
                    capture_output=True,
                    text=True,
                )

                if result.returncode == 0:
                    conn["monitoring"] = True
                    self._start_ssh_keepalive()
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
                messagebox.showerror(
                    "Connection Timeout",
                    f"SSH connection timeout!\n\nThe server '{conn['host']}' is not responding.",
                )
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
            self.monitor_conn_listbox.insert(
                tk.END, f"{conn_name} [{conn['username']}@{conn['host']}]"
            )

