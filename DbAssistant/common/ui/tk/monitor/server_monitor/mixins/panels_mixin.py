"""MonitorPanelsMixin — ServerMonitorUI mixin."""

from __future__ import annotations

from common.ui.tk.monitor.server_monitor.mixins._shared import *  # noqa: F403

class MonitorPanelsMixin:
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
            target=self._fetch_db_metrics_thread, args=(db_name,), daemon=True
        )
        thread.start()

    def _fetch_db_metrics_thread(self, db_name):
        """Background thread to fetch metrics for a single DB (manual refresh path)."""
        try:
            if db_name not in self.monitored_databases:
                return

            if db_name in self.databases_pending_removal:
                console_print(f"Skipping fetch for {db_name} (pending removal)")
                return

            self.active_db_query_threads[db_name] = threading.current_thread()

            try:
                db_manager = self.monitored_databases[db_name]
                db_stats = self.get_db_metrics(db_manager, db_name=db_name)
                if db_stats:
                    if not hasattr(self, "_db_metrics_cache"):
                        self._db_metrics_cache = {}
                    self._db_metrics_cache[db_name] = {
                        "stats": db_stats,
                        "timestamp": display_time_str(),
                    }
                # Trigger a full unified panel redraw using current cache + cloud data
                self.root.after(0, self._redraw_db_panel_from_cache)
            finally:
                if db_name in self.active_db_query_threads:
                    del self.active_db_query_threads[db_name]

        except Exception as e:
            console_print(f"Error fetching DB metrics in thread: {e}")
            import traceback

            traceback.print_exc()

    def _cleanup_stale_graphs(self):
        """Remove graphs for databases that are no longer being monitored"""
        if not hasattr(self, "db_metrics_visualizer"):
            return

        # All known live prefixes: local DB names only
        known_prefixes = set(self.monitored_databases.keys())

        # Get list of databases that are stale (have graphs but not monitored)
        stale_databases = set()
        for graph_key in list(self.db_metrics_visualizer.graphs.keys()):
            # Extract database name from graph key (format: "db_name_metric_name")
            db_name_found = None
            for known in known_prefixes:
                if graph_key.startswith(f"{known}_"):
                    db_name_found = known
                    break

            # If no matching monitored database found, add to stale list
            if db_name_found is None:
                # Extract the database name from the key (everything before the last underscore)
                parts = graph_key.split("_")
                if len(parts) >= 2:
                    # Reconstruct db name (in case it has underscores)
                    for i in range(len(parts), 0, -1):
                        potential_db_name = "_".join(parts[:i])
                        if potential_db_name not in known_prefixes:
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
            self.db_metrics_visualizer.sections_order.clear()

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
        if not hasattr(self, "os_metrics_visualizer"):
            return

        # Get list of servers that are stale (have graphs but not monitored)
        stale_servers = set()
        monitored_servers = set(
            conn_name
            for conn_name, conn in self.monitor_connections.items()
            if conn.get("monitoring", False)
        )

        for graph_key in list(self.os_metrics_visualizer.graphs.keys()):
            # Extract server name from graph key (format: "server_name - Metric Name")
            if " - " in graph_key:
                server_name = graph_key.split(" - ")[0].strip()
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
            self.os_metrics_visualizer.sections_order.clear()

            # Reset row tracking
            self.os_metrics_visualizer.current_row_frame = None
            self.os_metrics_visualizer.metrics_in_current_row = 0

            # Update scroll region
            self.os_metrics_visualizer.scrollable_frame.update_idletasks()
            self.os_metrics_visualizer.canvas.configure(
                scrollregion=self.os_metrics_visualizer.canvas.bbox("all")
            )

            console_print("✓ OS graphs visualizer rebuilt")

    # ── DB and Cloud panel renderers ───────────────────────────────────────────

    def _update_db_panel(self, local_db_results: dict):
        """Rewrite the local database metrics text panel and graph visualizer."""
        if not hasattr(self, "_db_metrics_cache"):
            self._db_metrics_cache = {}
        now_ts = display_time_str()
        for db_name, stats in local_db_results.items():
            self._db_metrics_cache[db_name] = {"stats": stats, "timestamp": now_ts}

        ordered_local = list(self.monitored_databases.keys())
        self._cleanup_stale_graphs()
        self.db_metrics_text.config(state=tk.NORMAL)
        self.db_metrics_text.delete(1.0, tk.END)

        W = self._BLOCK_WIDTH
        WAITING = "  Waiting for first poll…\n"

        if not ordered_local:
            self.db_metrics_text.insert(
                1.0, "Click 'Select Database' to start monitoring databases..."
            )
        else:
            for db_name in ordered_local:
                cached = self._db_metrics_cache.get(db_name)
                if not cached:
                    self.db_metrics_text.insert(
                        tk.END,
                        f"{'═'*W}\n Database  : {db_name}\n{'═'*W}\n{WAITING}\n",
                    )
                    continue
                ts = cached["timestamp"]
                db_mgr = self.monitored_databases.get(db_name)
                db_type = getattr(db_mgr, "db_type", "") if db_mgr else ""
                sections = (
                    getattr(self, "_db_sections_cache", {}).get(db_name) or []
                )
                if not sections:
                    stats = cached["stats"]
                    sections, note = self._group_local_metrics(db_type, stats)
                else:
                    note = getattr(self, "_db_os_note_cache", {}).get(db_name, "")
                    if not note:
                        note = "Host CPU / memory / disk: use SSH Server Monitoring tab"
                block = self._format_metric_block(
                    db_name, db_type or "DB", ts, sections, note=note
                )
                self.db_metrics_text.insert(tk.END, block)

        self.db_metrics_text.config(state=tk.DISABLED)
        self._sync_db_graphs(ordered_local)

    def _sync_db_graphs(self, ordered_local: list[str]):
        """Update local DB graph visualizer in listbox order."""
        visualizer = self.db_metrics_visualizer
        desired_sections = [
            f"📊 {db_name}"
            for db_name in ordered_local
            if self._db_metrics_cache.get(db_name)
        ]
        current_sections = [
            s for s in visualizer.sections_order if s in set(desired_sections)
        ]

        if current_sections != desired_sections:
            saved_data: dict[str, object] = {
                graph_key: graph.snapshot()
                for graph_key, graph in visualizer.graphs.items()
            }
            for widget in visualizer.scrollable_frame.winfo_children():
                widget.destroy()
            visualizer.graphs.clear()
            visualizer.separators_added.clear()
            visualizer.sections_order.clear()
            visualizer.current_row_frame = None
            visualizer.metrics_in_current_row = 0

            for db_name in ordered_local:
                cached = self._db_metrics_cache.get(db_name)
                if not cached:
                    continue
                pairs = [(f"{db_name}_{m}", v) for m, v in cached["stats"].items()]
                self._rebuild_graph_section(
                    visualizer, f"📊 {db_name}", pairs, db_name, saved_data
                )
            return

        for db_name in ordered_local:
            cached = self._db_metrics_cache.get(db_name)
            if not cached:
                continue
            for metric_name, value in cached["stats"].items():
                graph_key = f"{db_name}_{metric_name}"
                if graph_key not in visualizer.graphs:
                    visualizer.add_separator(label=f"📊 {db_name}")
                    visualizer.add_metric(metric_name)
                    if metric_name in visualizer.graphs:
                        graph = visualizer.graphs.pop(metric_name)
                        visualizer.graphs[graph_key] = graph
                visualizer.update_metric(graph_key, value)

    def _update_cloud_panel(
        self, cloud_text_dict: dict, cloud_graph_data: dict
    ):
        """Rewrite the cloud resource metrics text panel and graph visualizer."""
        if not hasattr(self, "_cloud_metrics_cache"):
            self._cloud_metrics_cache = {}
        now_ts = display_time_str()
        for name, text in cloud_text_dict.items():
            self._cloud_metrics_cache[name] = {"text": text, "timestamp": now_ts}

        ordered_cloud = list(self.active_cloud_monitors.keys())
        self._cleanup_stale_cloud_graphs()
        self.cloud_metrics_text.config(state=tk.NORMAL)
        self.cloud_metrics_text.delete(1.0, tk.END)

        W = self._BLOCK_WIDTH
        WAITING = "  Waiting for first poll…\n"

        if not ordered_cloud:
            self.cloud_metrics_text.insert(
                1.0,
                "Click 'Select Resource' to start monitoring cloud resources...",
            )
        else:
            for name in ordered_cloud:
                cached = self._cloud_metrics_cache.get(name)
                if cached:
                    self.cloud_metrics_text.insert(tk.END, cached["text"])
                else:
                    entry = self.active_cloud_databases.get(name, {})
                    provider = entry.get("provider", "Cloud")
                    self.cloud_metrics_text.insert(
                        tk.END,
                        f"{'═'*W}\n Resource  : {name}\n Type      : {provider}\n{'═'*W}\n{WAITING}\n",
                    )

        self.cloud_metrics_text.config(state=tk.DISABLED)
        self._sync_cloud_graphs(ordered_cloud, cloud_graph_data)

    def _sync_cloud_graphs(
        self, ordered_cloud: list[str], cloud_graph_data: dict
    ):
        """Update cloud graph visualizer in listbox order."""
        visualizer = self.cloud_metrics_visualizer
        desired_sections = []
        for display_name in ordered_cloud:
            entry = self.active_cloud_databases.get(display_name, {})
            provider = entry.get("provider", "Cloud")
            if (
                cloud_graph_data.get(display_name)
                or display_name in visualizer.separators_added
            ):
                desired_sections.append(f"☁ [{provider}] {display_name}")

        current_sections = [
            s for s in visualizer.sections_order if s in set(desired_sections)
        ]

        if current_sections != desired_sections:
            saved_data: dict[str, object] = {
                graph_key: graph.snapshot()
                for graph_key, graph in visualizer.graphs.items()
            }
            for widget in visualizer.scrollable_frame.winfo_children():
                widget.destroy()
            visualizer.graphs.clear()
            visualizer.separators_added.clear()
            visualizer.sections_order.clear()
            visualizer.current_row_frame = None
            visualizer.metrics_in_current_row = 0

            for display_name in ordered_cloud:
                gdata = cloud_graph_data.get(display_name) or {}
                entry = self.active_cloud_databases.get(display_name, {})
                provider = entry.get("provider", "Cloud")
                pairs = [(gk, gv) for gk, gv in gdata.items()]
                if pairs:
                    self._rebuild_graph_section(
                        visualizer,
                        f"☁ [{provider}] {display_name}",
                        pairs,
                        display_name,
                        saved_data,
                    )
            return

        for display_name in ordered_cloud:
            gdata = cloud_graph_data.get(display_name)
            if not gdata:
                continue
            entry = self.active_cloud_databases.get(display_name, {})
            provider = entry.get("provider", "Cloud")
            sep_label = f"☁ [{provider}] {display_name}"
            for graph_key, value in gdata.items():
                metric_name = graph_key[len(display_name) + 1 :]
                if graph_key not in visualizer.graphs:
                    visualizer.add_separator(label=sep_label)
                    visualizer.add_metric(metric_name)
                    if metric_name in visualizer.graphs:
                        graph = visualizer.graphs.pop(metric_name)
                        visualizer.graphs[graph_key] = graph
                visualizer.update_metric(graph_key, value)

    def _rebuild_graph_section(
        self,
        visualizer,
        sep_label: str,
        graph_keys_values: list[tuple[str, float]],
        prefix: str,
        saved_data: dict[str, object],
    ):
        """Rebuild one graph section inside a metrics visualizer."""
        visualizer.add_separator(label=sep_label)
        for graph_key, value in graph_keys_values:
            metric_name = (
                graph_key[len(prefix) + 1 :]
                if graph_key.startswith(prefix + "_")
                else graph_key
            )
            visualizer.add_metric(metric_name)
            if metric_name in visualizer.graphs:
                graph = visualizer.graphs.pop(metric_name)
                visualizer.graphs[graph_key] = graph
            if graph_key in saved_data:
                visualizer.graphs[graph_key].restore(saved_data[graph_key])
            visualizer.update_metric(graph_key, value)

    def _redraw_db_panel_from_cache(self):
        """Re-render local DB text + graphs from cached data."""
        local_results = {}
        for db_name, cached in self._db_metrics_cache.items():
            if db_name in self.monitored_databases:
                local_results[db_name] = cached["stats"]
        self._update_db_panel(local_results)

    def _redraw_cloud_panel_from_cache(self):
        """Re-render cloud text + graphs from cached data."""
        cloud_text_dict = {
            n: c["text"]
            for n, c in getattr(self, "_cloud_metrics_cache", {}).items()
            if n in self.active_cloud_monitors
        }
        self._update_cloud_panel(cloud_text_dict, {})

    def update_monitor_status_label(self):
        """Update the monitor status label and main status bar based on active monitoring"""
        server_count = sum(
            1
            for conn in self.monitor_connections.values()
            if conn.get("monitoring", False)
        )
        # Local DBs + cloud DBs both count toward the database total
        db_count = len(self.monitored_databases) + len(
            getattr(self, "active_cloud_databases", {})
        )

        server_monitoring = server_count > 0
        db_monitoring = db_count > 0

        # Refresh the top-right "resources monitored" badge.
        badge = getattr(self, "monitor_resources_label", None)
        if badge is not None:
            total = server_count + db_count
            try:
                badge.config(text=f"Monitoring {total} resource"
                                  f"{'' if total == 1 else 's'}")
            except Exception:
                pass

        if server_monitoring and db_monitoring:
            status_text = (
                f"Monitoring {server_count} server(s) & {db_count} database(s)"
            )
            self.monitor_status_label.config(
                text=f"Status: {status_text}", foreground="green"
            )
            self.update_status(f"✓ {status_text}", "success")
        elif server_monitoring:
            status_text = f"Monitoring {server_count} server(s)"
            self.monitor_status_label.config(
                text=f"Status: {status_text}", foreground="green"
            )
            self.update_status(f"✓ {status_text}", "success")
        elif db_monitoring:
            status_text = f"Monitoring {db_count} database(s)"
            self.monitor_status_label.config(
                text=f"Status: {status_text}", foreground="green"
            )
            self.update_status(f"✓ {status_text}", "success")
        else:
            self.monitor_status_label.config(
                text="Status: No active monitoring", foreground="gray"
            )
            self.update_status("No active monitoring", "info")

    def clear_os_graphs(self):
        """Clear all OS resource graphs"""
        if hasattr(self, "os_metrics_visualizer"):
            self.os_metrics_visualizer.clear_all()
            self.update_status("OS resource graphs cleared", "success")

    def clear_db_graphs(self):
        """Clear all database graphs"""
        if hasattr(self, "db_metrics_visualizer"):
            self.db_metrics_visualizer.clear_all()
            self.update_status("Database graphs cleared", "success")

    def clear_cloud_graphs(self):
        """Clear all cloud resource graphs"""
        if hasattr(self, "cloud_metrics_visualizer"):
            self.cloud_metrics_visualizer.clear_all()
            self.update_status("Cloud resource graphs cleared", "success")

    def refresh_cloud_metrics(self):
        """Refresh cloud monitoring — reload thresholds then fetch cloud metrics."""
        if self._threshold_checker:
            try:
                self._threshold_checker.reload()
                console_print("[Monitor] monitor_thresholds.ini reloaded on Refresh")
            except Exception as _reload_err:
                print(
                    f"[Monitor] Could not reload thresholds: {_reload_err}",
                    file=sys.stderr,
                )

        if not self.active_cloud_monitors:
            return

        threading.Thread(
            target=self._fetch_cloud_metrics_now_thread, daemon=True
        ).start()
        self.update_monitor_status_label()

    def _fetch_cloud_metrics_now_thread(self):
        """Background fetch for all active cloud monitors (manual refresh path)."""
        cloud_text_dict: dict[str, str] = {}
        cloud_graph_data: dict[str, dict[str, float]] = {}
        for display_name, monitor in list(self.active_cloud_monitors.items()):
            entry = self.active_cloud_databases.get(display_name, {})
            try:
                text, gdata = self._fetch_cloud_metrics(
                    display_name, entry, monitor
                )
                cloud_text_dict[display_name] = text
                if gdata:
                    cloud_graph_data[display_name] = gdata
            except Exception as exc:
                cloud_text_dict[display_name] = (
                    f"=== {display_name} ===\n  Error: {exc}\n\n"
                )

        def _apply():
            self._update_cloud_panel(cloud_text_dict, cloud_graph_data)

        self.root.after(0, _apply)

    def _cleanup_stale_cloud_graphs(self):
        """Remove graphs for cloud resources that are no longer being monitored."""
        if not hasattr(self, "cloud_metrics_visualizer"):
            return

        known_prefixes = set(self.active_cloud_monitors.keys())
        stale_resources = set()
        for graph_key in list(self.cloud_metrics_visualizer.graphs.keys()):
            db_name_found = None
            for known in known_prefixes:
                if graph_key.startswith(f"{known}_"):
                    db_name_found = known
                    break
            if db_name_found is None:
                parts = graph_key.split("_")
                if len(parts) >= 2:
                    for i in range(len(parts), 0, -1):
                        potential_name = "_".join(parts[:i])
                        if potential_name not in known_prefixes:
                            stale_resources.add(potential_name)
                            break

        if stale_resources:
            console_print(f"Removing stale cloud graphs: {stale_resources}")
            for widget in self.cloud_metrics_visualizer.scrollable_frame.winfo_children():
                widget.destroy()
            self.cloud_metrics_visualizer.graphs.clear()
            self.cloud_metrics_visualizer.separators_added.clear()
            self.cloud_metrics_visualizer.sections_order.clear()
            self.cloud_metrics_visualizer.current_row_frame = None
            self.cloud_metrics_visualizer.metrics_in_current_row = 0
            self.cloud_metrics_visualizer.scrollable_frame.update_idletasks()
            self.cloud_metrics_visualizer.canvas.configure(
                scrollregion=self.cloud_metrics_visualizer.canvas.bbox("all")
            )
            console_print("✓ Cloud graphs visualizer rebuilt")
