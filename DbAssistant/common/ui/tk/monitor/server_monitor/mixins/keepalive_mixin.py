"""KeepaliveMixin — ServerMonitorUI mixin."""

from __future__ import annotations

from common.ui.tk.monitor.server_monitor.mixins._shared import *  # noqa: F403

class KeepaliveMixin:
    def _start_ssh_keepalive(self):
        """Start the SSH control-socket keepalive loop (idempotent)."""


        if self._ssh_keepalive_active:
            return
        self._ssh_keepalive_active = True
        threading.Thread(
            target=self._ssh_keepalive_loop, daemon=True, name="SSHKeepalive"
        ).start()
        console_print(
            f"[Keepalive] SSH monitor keepalive started "
            f"(interval={self._ssh_keepalive_interval}s)"
        )

    def _ssh_keepalive_loop(self):
        """Periodically check/reopen SSH multiplex master sockets."""
        interval = self._ssh_keepalive_interval
        while self._ssh_keepalive_active:
            for _ in range(interval):
                if not self._ssh_keepalive_active:
                    return
                time.sleep(1)

            connections = list(self.monitor_connections.items())
            if not connections:
                continue

            refresh_s = self.refresh_interval / 1000.0
            for conn_name, conn in connections:
                if not conn.get("monitoring"):
                    continue
                if self._should_skip_liveness(
                    self._ssh_last_cmd_ok_at.get(conn_name, 0.0),
                    refresh_s,
                    self._ssh_keepalive_skip_if_used_within,
                ):
                    continue
                try:
                    self._check_or_reopen_ssh_master(conn_name, conn)
                    console_print(f"[Keepalive] {conn_name}: SSH session OK")
                except Exception as exc:
                    msg = f"'{conn_name}' SSH keepalive failed: {exc}"
                    console_print(f"[Keepalive] ERROR: {msg}")
                    self.root.after(
                        0,
                        lambda m=msg: self.update_status(m, "error"),
                    )

    def _check_or_reopen_ssh_master(self, conn_name: str, conn: dict):
        """Check an SSH control master and reopen it if the socket died."""
        control_path = conn.get("control_path")
        if not control_path:
            control_path = os.path.join(
                tempfile.gettempdir(), f"ssh_monitor_{conn_name.replace(' ', '_')}"
            )
            conn["control_path"] = control_path

        ssh_host = f"{conn['username']}@{conn['host']}"
        check_cmd = [
            "ssh",
            "-O",
            "check",
            "-o",
            f"ControlPath={control_path}",
            ssh_host,
        ]
        result = subprocess.run(
            check_cmd,
            timeout=self.ssh_test_timeout,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return

        reopen_cmd = self._build_ssh_master_command(conn, control_path, ssh_host)
        result = subprocess.run(
            reopen_cmd,
            timeout=self.ssh_timeout + 5,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(err[:300] or "could not reopen SSH master")

    def _build_ssh_master_command(self, conn: dict, control_path: str, ssh_host: str) -> list:
        """Build an SSH multiplex master command with TCP keepalive options."""
        base = [
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
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=3",
            "-o",
            "StrictHostKeyChecking=no",
        ]
        if conn.get("password"):
            return ["sshpass", "-p", conn["password"]] + base + [
                "-o",
                "UserKnownHostsFile=/dev/null",
                ssh_host,
            ]
        return base + [ssh_host]

    def _start_db_keepalive(self):
        """Start the monitored database keepalive loop (idempotent)."""
        if self._db_keepalive_interval <= 0:
            console_print("[Keepalive] Database keepalive disabled (interval=0)")
            return
        if self._db_keepalive_active:
            return
        self._db_keepalive_active = True
        threading.Thread(
            target=self._db_keepalive_loop, daemon=True, name="DBKeepalive"
        ).start()
        console_print(
            f"[Keepalive] Database connection keepalive started "
            f"(interval={self._db_keepalive_interval}s)"
        )

    def _db_keepalive_loop(self):
        """Periodically ping/reconnect every monitored database connection."""
        interval = self._db_keepalive_interval
        while self._db_keepalive_active:
            for _ in range(interval):
                if not self._db_keepalive_active:
                    return
                time.sleep(1)

            db_items = list(self.monitored_databases.items())
            if not db_items:
                continue

            for db_name, db_manager in db_items:
                if db_name in self.databases_pending_removal:
                    continue
                if self.monitored_databases.get(db_name) is not db_manager:
                    continue
                if self._db_keepalive_skip_if_polled_within > 0:
                    last_metric = self._db_last_metric_at.get(db_name, 0.0)
                    if time.time() - last_metric < self._db_keepalive_skip_if_polled_within:
                        continue

                try:
                    with self._get_db_lock(db_name):
                        self._ping_or_reconnect_database(db_name, db_manager)
                    console_print(f"[Keepalive] {db_name}: database session OK")
                except Exception as exc:
                    msg = f"'{db_name}' database keepalive failed: {exc}"
                    console_print(f"[Keepalive] ERROR: {msg}")
                    self.root.after(
                        0,
                        lambda m=msg: self.update_status(m, "error"),
                    )

    def _ping_or_reconnect_database(self, db_name: str, db_manager):
        """Ping a monitored DB session; reconnect when the ping fails."""
        if not hasattr(db_manager, "ping_or_reconnect"):
            self._ping_database_connection(db_manager)
            return
        self._ensure_db_monitor_reconnect_params(db_name, db_manager)
        if not db_manager.ping_or_reconnect():
            self._db_last_metric_at.pop(db_name, None)
            raise RuntimeError("ping and reconnect both failed")
        self.monitored_databases[db_name] = db_manager
        if db_name in self.active_connections:
            self.active_connections[db_name] = db_manager

    def _ping_database_connection(self, db_manager):
        """Engine-aware lightweight liveness check."""
        conn = getattr(db_manager, "conn", None)
        db_type = getattr(db_manager, "db_type", "")
        if conn is None:
            raise RuntimeError("not connected")

        if db_type in ("MySQL", "MariaDB"):
            if hasattr(conn, "ping"):
                try:
                    conn.ping(reconnect=True, attempts=1, delay=0)
                except TypeError:
                    conn.ping()
            elif hasattr(conn, "is_connected") and not conn.is_connected():
                raise RuntimeError("connection is not connected")
            return

        if db_type == "Oracle":
            if not hasattr(conn, "ping"):
                raise RuntimeError("Oracle connection has no ping()")
            conn.ping()
            return

        if db_type == "PostgreSQL":
            if getattr(conn, "closed", 0):
                raise RuntimeError("PostgreSQL connection is closed")
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1")
                cur.fetchone()
            finally:
                cur.close()
                try:
                    conn.rollback()
                except Exception:
                    pass
            return

        # SQLite and any future DB fallback.
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            try:
                cur.fetchone()
            except Exception:
                pass
        finally:
            cur.close()

    def _ensure_db_monitor_reconnect_params(self, db_name: str, db_manager) -> bool:
        """Attach saved reconnect params to a DB manager when available."""
        if getattr(db_manager, "_last_connect_params", None):
            return True
        params = self._get_db_reconnect_params(db_name, db_manager)
        if params:
            db_manager._last_connect_params = params
            return True
        return False

    def _get_db_reconnect_params(self, db_name: str, db_manager) -> dict | None:
        """Return reconnect params from the manager or saved connection registry."""
        params = getattr(db_manager, "_last_connect_params", None)
        if params:
            return dict(params)

        try:
            conn_details = self.connection_manager.get_connection(db_name)
        except Exception:
            conn_details = None
        if not conn_details:
            return None

        password = conn_details.get("password", "")
        if not password:
            return None

        conn_params = {
            "host": conn_details["host"],
            "port": conn_details["port"],
            "username": conn_details["username"],
            "password": password,
        }
        if conn_details.get("db_type") == "Oracle":
            conn_params["service"] = conn_details["service_or_db"]
        else:
            conn_params["database"] = conn_details["service_or_db"]
        return conn_params

    def _start_cloud_keepalive(self):
        """Start the 5-minute keepalive loop (idempotent — safe to call multiple times)."""
        if self._cloud_keepalive_active:
            return
        self._cloud_keepalive_active = True
        threading.Thread(
            target=self._cloud_keepalive_loop, daemon=True, name="CloudKeepalive"
        ).start()
        console_print(
            f"[Keepalive] Cloud connection keepalive started (interval={self._cloud_keepalive_interval}s)"
        )

    def _cloud_keepalive_loop(self):
        """Background thread: periodically refreshes or health-checks every active
        cloud monitor so credentials never silently expire.

        Providers refresh credentials in-place when possible (GCP ADC /
        service-account / authorized_user, Azure TokenCredential).  Older or
        static-key providers may rebuild the monitor as a fallback.
        """
        interval = self._cloud_keepalive_interval
        while self._cloud_keepalive_active:
            # Sleep in small slices so the thread responds quickly to shutdown
            for _ in range(interval):
                if not self._cloud_keepalive_active:
                    return
                time.sleep(1)

            monitors = list(self.active_cloud_monitors.items())
            if not monitors:
                continue

            for display_name, monitor in monitors:
                # Guard: entry may have been removed while we slept
                entry = self.active_cloud_databases.get(display_name)
                if not entry:
                    continue

                provider = entry.get("provider", "?")
                if not self._cloud_should_refresh_keepalive(
                    display_name, entry, monitor
                ):
                    continue

                try:
                    new_monitor, err = CloudProviderRegistry.refresh_monitor(
                        entry, monitor, sso_callback=None
                    )
                    if err:
                        raise RuntimeError(err)
                    if new_monitor is not None:
                        self.active_cloud_monitors[display_name] = new_monitor
                    self._cloud_consecutive_failures[display_name] = 0
                    self._cloud_needs_refresh[display_name] = False
                    console_print(
                        f"[Keepalive] {display_name} ({provider}): session refreshed OK"
                    )
                except Exception as rb_err:
                    fails = self._cloud_consecutive_failures.get(display_name, 0) + 1
                    self._cloud_consecutive_failures[display_name] = fails
                    self._cloud_needs_refresh[display_name] = True
                    msg = (
                        f"'{display_name}' ({provider}) keepalive refresh failed: "
                        f"{rb_err}"
                    )
                    console_print(f"[Keepalive] ERROR: {msg}")
                    if fails >= 3:
                        reconnect_msg = (
                            f"Reconnect required for '{display_name}' ({provider})"
                        )
                        self.root.after(
                            0,
                            lambda m=reconnect_msg: self.update_status(m, "error"),
                        )
                    else:
                        self.root.after(
                            0,
                            lambda m=msg: self.update_status(m, "error"),
                        )

    def _on_cloud_db_connected(self, display_name: str):
        """Called after a cloud resource is added to the monitoring list.
        Builds the monitor object in a background thread so authentication
        (which may involve network calls) doesn't block the UI.
        """
        self.update_cloud_db_listbox()
        keys = list(self.active_cloud_databases.keys())
        if display_name in keys:
            idx = keys.index(display_name)
            self.cloud_db_listbox.selection_clear(0, tk.END)
            self.cloud_db_listbox.selection_set(idx)
            self.cloud_db_listbox.see(idx)

        entry = self.active_cloud_databases.get(display_name, {})
        provider = entry.get("provider", "")
        resource = entry.get("resource_name", display_name)
        self.update_status(f"'{display_name}' ({provider}) — authenticating…", "info")

        def _auth_thread():
            monitor, err = self._build_cloud_monitor(entry)

            def _done():
                if err:
                    self.active_cloud_databases.pop(display_name, None)
                    self.update_cloud_db_listbox()
                    self.update_monitor_status_label()
                    self.update_status(f"'{display_name}' auth failed: {err}", "error")
                    messagebox.showerror(
                        "Authentication Failed",
                        f"Could not connect to {provider} for '{display_name}':\n\n{err}",
                    )
                else:
                    self.active_cloud_monitors[display_name] = monitor
                    self.update_monitor_status_label()
                    self.update_status(
                        f"'{display_name}' ({provider}/{resource}) connected — monitoring active.",
                        "success",
                    )
                    # Start keepalive loop on first successful cloud connection
                    self._start_cloud_keepalive()

            self.root.after(0, _done)

        threading.Thread(target=_auth_thread, daemon=True).start()

    def remove_cloud_database(self):
        """Remove the selected entry from the monitoring list (connection stays saved)."""
        selection = self.cloud_db_listbox.curselection()
        if not selection:
            messagebox.showwarning(
                "No Selection",
                "Please select a cloud database to remove from monitoring.",
            )
            return

        display_name = self._cloud_db_key_from_index(selection[0])
        if not display_name:
            return

        self.active_cloud_databases.pop(display_name, None)
        self.active_cloud_monitors.pop(display_name, None)
        self._cloud_metrics_cache.pop(display_name, None)
        self._clear_cloud_liveness_state(display_name)
        # Clear ephemeral MFA code from the registry entry too
        if display_name in self.cloud_databases:
            self.cloud_databases[display_name].pop("_mfa_code", None)
        self.update_cloud_db_listbox()
        self.update_monitor_status_label()
        if not self.active_cloud_monitors:
            self.clear_cloud_graphs()
            self.cloud_metrics_text.config(state=tk.NORMAL)
            self.cloud_metrics_text.delete(1.0, tk.END)
            self.cloud_metrics_text.insert(
                1.0,
                "Click 'Select Resource' to start monitoring cloud resources...",
            )
            self.cloud_metrics_text.config(state=tk.DISABLED)
        else:
            self._redraw_cloud_panel_from_cache()
        self.update_status(f"'{display_name}' removed from cloud monitoring.", "info")
