"""DBMonitorMixin — ServerMonitorUI mixin."""

from __future__ import annotations

from common.ui.tk.monitor.server_monitor.mixins._shared import *  # noqa: F403

class DBMonitorMixin:
    def add_monitor_db_connection(self, remote: bool = False):
        """Open the shared Add-database-connection dialog, saving into the
        Monitor-tab-only store (``monitor_db.json``).

        Connections created here are isolated to Monitoring: other tabs read
        the core ``db.json`` and never see them. After saving, the new profile
        is immediately available under "Select Database".

        ``remote=True`` preselects the "Remote host (SSH tunnel)" location so a
        database reachable only through a bastion / jump host can be monitored.
        """
        from common.ui.tk.db_connection_form import open_db_connection_form

        def _on_saved(name):
            self.update_status(
                f"Saved monitoring database connection '{name}'.", "success"
            )

        open_db_connection_form(
            self.root,
            self.monitor_db_connection_manager,
            title="Add Database (Monitoring only)",
            theme=getattr(self, "theme", None),
            on_saved=_on_saved,
            remote=remote,
        )

    def update_monitored_db_listbox(self):
        """Update the monitored databases listbox display"""


        self.monitored_db_listbox.delete(0, tk.END)
        for db_name in self.monitored_databases.keys():
            self.monitored_db_listbox.insert(tk.END, db_name)

    def refresh_monitor_db_list(self):
        """Refresh monitoring - reload thresholds then fetch metrics for all monitored databases"""
        if self._threshold_checker:
            try:
                self._threshold_checker.reload()
                console_print("[Monitor] monitor_thresholds.ini reloaded on Refresh")
            except Exception as _reload_err:
                print(f"[Monitor] Could not reload thresholds: {_reload_err}", file=sys.stderr)

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

        ttk.Label(
            dialog, text="Select Database to Monitor:", font=("Arial", 11, "bold")
        ).pack(pady=20, padx=20)

        # Dropdown with active connections first, then saved connections
        conn_combo = ttk.Combobox(dialog, state="readonly", width=40)

        # Build list: Active connections first, then saved connections
        conn_display_list = []
        conn_name_map = {}  # Maps display name to actual connection name
        conn_type_map = {}  # Maps display name to type: 'active' or 'saved'
        conn_mgr_map = {}   # Maps display name to the manager for saved lookups

        # First: Add all active connections
        for conn_name in self.active_connections.keys():
            if conn_name in self.monitored_databases:
                status = " [Already Monitoring]"
            else:
                status = " [Active]"

            display_name = f"{conn_name}{status}"
            conn_display_list.append(display_name)
            conn_name_map[display_name] = conn_name
            conn_type_map[display_name] = "active"

        # Second: Add saved connections that are not already in active connections.
        # We pull from two stores:
        #   * the core Connections-tab store (shared, read-only here), and
        #   * the Monitor-tab-only store (isolated to Monitoring).
        saved_sources = [
            (" [Saved]", self.connection_manager),
            (" [Monitoring]", getattr(self, "monitor_db_connection_manager", None)),
        ]
        for tag, mgr in saved_sources:
            if mgr is None:
                continue
            for conn in mgr.get_all_connections():
                conn_name = conn["name"]

                # Skip if already shown as an active connection (avoid duplicates)
                if conn_name in self.active_connections:
                    continue

                status = " [Already Monitoring]" if conn_name in self.monitored_databases else tag
                display_name = f"{conn_name}{status}"
                conn_display_list.append(display_name)
                conn_name_map[display_name] = conn_name
                conn_type_map[display_name] = "saved"
                conn_mgr_map[display_name] = mgr

        if not conn_display_list:
            ttk.Label(
                dialog,
                text="No connections available.\n\nPlease create an active connection or save a connection first.",
                foreground="gray",
                justify="center",
            ).pack(pady=10)
            ttk.Button(dialog, text="Close", command=dialog.destroy, width=15).pack(
                pady=20
            )
            return

        conn_combo["values"] = conn_display_list
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
                messagebox.showinfo(
                    "Info", f"'{conn_name}' is already being monitored."
                )
                return

            # If it's an active connection, use it directly
            if conn_type == "active":
                status_label.config(
                    text=f"Using active connection...", foreground="blue"
                )
                dialog.update()

                # Get the db_manager from active connections
                db_manager = self.active_connections[conn_name]
                self._ensure_db_monitor_reconnect_params(conn_name, db_manager)

                # Add to monitoring with the active connection's db_manager
                self.monitored_databases[conn_name] = db_manager
                self._start_db_keepalive()
                self.update_monitored_db_listbox()
                self.update_monitor_status_label()

                # Fetch metrics immediately for the new database
                self.fetch_db_metrics_for_db(conn_name)

                messagebox.showinfo(
                    "Success",
                    f"'{conn_name}' added to monitoring (using active connection).",
                )
                dialog.destroy()
                return

            # It's a saved connection - need to get details and possibly connect.
            # Route the lookup to the store the entry came from (core vs monitor).
            saved_mgr = conn_mgr_map.get(display_name, self.connection_manager)
            conn_details = saved_mgr.get_connection(conn_name)
            if not conn_details:
                messagebox.showerror("Error", f"Connection '{conn_name}' not found.")
                return

            # Check if password is available, if not prompt for it
            password = conn_details.get("password", "")
            if not password:
                # Password not saved, prompt user for it
                password_dialog = tk.Toplevel(dialog)
                password_dialog.title("Password Required")
                password_dialog.geometry("400x180")
                password_dialog.transient(dialog)
                password_dialog.grab_set()

                ttk.Label(
                    password_dialog,
                    text=f"Password required for '{conn_name}'",
                    font=("Arial", 11, "bold"),
                ).pack(pady=20, padx=20)

                ttk.Label(
                    password_dialog, text="Database Password:", font=("Arial", 10)
                ).pack(anchor=tk.W, padx=20, pady=(0, 5))
                password_entry = ttk.Entry(password_dialog, width=40, show="*")
                password_entry.pack(padx=20, pady=(0, 10))
                password_entry.focus()

                ttk.Label(
                    password_dialog,
                    text="(Password will be used for this session only, not saved)",
                    foreground="gray",
                    font=("Arial", 9),
                ).pack(padx=20, pady=(0, 10))

                entered_password: dict[str, str | None] = {"value": None}

                def submit_password():
                    pwd = password_entry.get()
                    if not pwd:
                        messagebox.showwarning(
                            "Warning",
                            "Password cannot be empty!",
                            parent=password_dialog,
                        )
                        return
                    entered_password["value"] = pwd
                    password_dialog.destroy()

                def cancel_password():
                    password_dialog.destroy()

                # Bind Enter key to submit
                password_entry.bind("<Return>", lambda e: submit_password())

                pwd_btn_frame = ttk.Frame(password_dialog)
                pwd_btn_frame.pack(pady=10)
                ttk.Button(
                    pwd_btn_frame,
                    text="OK",
                    command=submit_password,
                    style="Primary.TButton",
                    width=12,
                ).pack(side=tk.LEFT, padx=5)
                ttk.Button(
                    pwd_btn_frame, text="Cancel", command=cancel_password, width=12
                ).pack(side=tk.LEFT, padx=5)

                # Center password dialog
                password_dialog.update_idletasks()
                x = (password_dialog.winfo_screenwidth() // 2) - (
                    password_dialog.winfo_width() // 2
                )
                y = (password_dialog.winfo_screenheight() // 2) - (
                    password_dialog.winfo_height() // 2
                )
                password_dialog.geometry(f"+{x}+{y}")

                # Wait for password dialog to close
                password_dialog.wait_window()

                # Check if password was entered
                if entered_password["value"] is None:
                    status_label.config(
                        text="Cancelled - password required", foreground="red"
                    )
                    return

                password = entered_password["value"]

            # Create a dedicated monitoring connection (not added to active_connections)
            status_label.config(
                text=f"Connecting to '{conn_name}' for monitoring...", foreground="blue"
            )
            dialog.update()

            try:
                # Import DatabaseManager (lazy import to avoid circular dependency)
                from common.db_manager import DatabaseManager

                # Create DatabaseManager for monitoring
                db_type = conn_details["db_type"]
                db_manager = DatabaseManager(db_type)

                # Prepare connection parameters
                conn_params = {
                    "host": conn_details["host"],
                    "port": conn_details["port"],
                    "username": conn_details["username"],
                    "password": password,  # Use either saved password or user-entered password
                }

                # Add database/service parameter based on db_type
                if db_type == "Oracle":
                    conn_params["service"] = conn_details["service_or_db"]
                else:
                    conn_params["database"] = conn_details["service_or_db"]

                # Forward SSL/TLS and SSH-tunnel settings from the saved profile
                # so remote (bastion) and TLS-protected DBs connect correctly.
                for _k in (
                    "ssl_mode", "ssl_ca", "ssl_cert", "ssl_key", "wallet_location",
                    "tls", "tls_ca_file", "ssh_tunnel",
                ):
                    if conn_details.get(_k) not in (None, ""):
                        conn_params[_k] = conn_details[_k]

                # Attempt connection
                conn = db_manager.connect(**conn_params)

                if conn:
                    status_label.config(
                        text=f"Connected successfully!", foreground="green"
                    )
                    dialog.update()
                else:
                    messagebox.showerror(
                        "Connection Failed",
                        f"Failed to connect to '{conn_name}' for monitoring.",
                    )
                    status_label.config(text="", foreground="blue")
                    return

            except Exception as e:
                messagebox.showerror(
                    "Connection Error",
                    f"Failed to connect to '{conn_name}' for monitoring:\n{str(e)}",
                )
                status_label.config(text="", foreground="blue")
                return

            # Add to monitoring with the dedicated db_manager
            self.monitored_databases[conn_name] = db_manager
            self._start_db_keepalive()
            self.update_monitored_db_listbox()
            self.update_monitor_status_label()

            # Fetch metrics immediately for the new database
            self.fetch_db_metrics_for_db(conn_name)

            messagebox.showinfo("Success", f"'{conn_name}' added to monitoring.")
            dialog.destroy()

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=20)
        ttk.Button(
            btn_frame,
            text="Select",
            command=add_selected,
            width=12,
            style="Success.TButton",
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy, width=12).pack(
            side=tk.LEFT, padx=5
        )

    def remove_db_from_monitor(self):
        """Remove selected database from monitoring - stops immediately, cleanup async"""
        if not self.monitored_databases:
            messagebox.showwarning(
                "Warning", "No databases are currently being monitored!"
            )
            return

        # Get selected database from listbox
        selection = self.monitored_db_listbox.curselection()
        if not selection:
            messagebox.showwarning(
                "Warning", "Please select a database from the list to remove!"
            )
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
            console_print(
                f"Error disconnecting monitoring connection for {db_name}: {e}"
            )

        # Remove from all tracking structures
        del self.monitored_databases[db_name]
        self.databases_pending_removal.discard(db_name)
        if db_name in self.active_db_query_threads:
            del self.active_db_query_threads[db_name]
        if db_name in self._db_metrics_cache:
            del self._db_metrics_cache[db_name]
        self._db_last_metric_at.pop(db_name, None)

        # Update UI
        self.update_monitored_db_listbox()
        self.update_monitor_status_label()

        # Clear graphs if no databases are being monitored
        if not self.monitored_databases:
            self.clear_db_graphs()
            self.db_metrics_text.config(state=tk.NORMAL)
            self.db_metrics_text.delete(1.0, tk.END)
            self.db_metrics_text.insert(
                1.0, "Click 'Select Database' to start monitoring databases..."
            )
            self.db_metrics_text.config(state=tk.DISABLED)
        else:
            # Trigger cleanup to remove graphs for this database
            self._cleanup_stale_graphs()

        self.update_status(f"✓ Removed '{db_name}' from monitoring", "success")
        console_print(f"✓ Successfully removed {db_name} from monitoring")

    def toggle_os_view(self, mode):
        """Toggle between text and graph view for OS metrics"""
        self.os_view_mode = mode

        if mode == "text":
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
            self.os_metrics_visualizer.canvas.pack(
                side=tk.LEFT, fill=tk.BOTH, expand=True
            )
            self.update_status("OS view: Graph mode", "info")

    def toggle_db_view(self, mode):
        """Toggle between text and graph view for DB metrics"""
        self.db_view_mode = mode

        if mode == "text":
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
            self.db_metrics_visualizer.canvas.pack(
                side=tk.LEFT, fill=tk.BOTH, expand=True
            )
            self.update_status("DB view: Graph mode", "info")

    def toggle_cloud_view(self, mode):
        """Toggle between text and graph view for cloud metrics"""
        self.cloud_view_mode = mode

        if mode == "text":
            self.cloud_metrics_visualizer.canvas.pack_forget()
            self.cloud_metrics_visualizer.v_scrollbar.pack_forget()
            self.cloud_metrics_visualizer.h_scrollbar.pack_forget()
            self.cloud_metrics_text.pack(fill=tk.BOTH, expand=True)
            self.update_status("Cloud view: Text mode", "info")
        else:
            self.cloud_metrics_text.pack_forget()
            self.cloud_metrics_visualizer.h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
            self.cloud_metrics_visualizer.v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            self.cloud_metrics_visualizer.canvas.pack(
                side=tk.LEFT, fill=tk.BOTH, expand=True
            )
            self.update_status("Cloud view: Graph mode", "info")

