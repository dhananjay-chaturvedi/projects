"""MetricsLoopMixin — ServerMonitorUI mixin."""

from __future__ import annotations

from common.ui.tk.monitor.server_monitor.mixins._shared import *  # noqa: F403

class MetricsLoopMixin:
    def _get_db_lock(self, db_name: str) -> threading.Lock:
        """Return (creating if necessary) the per-database query lock for *db_name*."""


        with self._db_locks_meta:
            if db_name not in self._db_locks:
                self._db_locks[db_name] = threading.Lock()
            return self._db_locks[db_name]

    def _fire_alerts(self, alerts: list, origin: str = "db") -> None:
        """Log alerts, update badge counts, print to console, and forward to Teams.

        *origin* (``"os"`` / ``"db"`` / ``"cloud"``) is the pane the alerts came
        from, passed explicitly by the caller. It is stored on each entry and
        used for badge counts and the per-pane Alerts window, so attribution no
        longer relies on fragile substring matching of the message (cloud alert
        messages carry the RDS resource id, not the cloud display name).
        """
        if not alerts:
            return
        origin = origin if origin in ("os", "db", "cloud") else "db"
        for item in alerts:
            # Support both AlertResult namedtuples and plain strings (legacy)
            if hasattr(item, "severity"):
                severity, message = item.severity, item.message
            else:
                severity, message = CRITICAL, str(item)

            entry = {
                "time":     display_time_str(),
                "severity": severity,
                "message":  message,
                "origin":   origin,
            }
            self._alert_log.append(entry)

            # Bump the badge for the originating pane. Lock guards the
            # read-modify-write so a context switch mid-operation cannot lose
            # an increment.
            with self._alert_counter_lock:
                if origin == "os":
                    self._alert_unread_os += 1
                elif origin == "cloud":
                    self._alert_unread_cloud += 1
                else:
                    self._alert_unread_db += 1

            console_print(f"\n{'='*60}\n[ALERT] [{severity}] {message}\n{'='*60}")

        # Refresh badge labels on the main thread
        self.root.after(0, self._refresh_alert_badges)

        # Forward all to Teams as one message
        try:
            combined = "\n".join(
                f"[{a['severity']}] {a['message']}" for a in
                [{"severity": (i.severity if hasattr(i, "severity") else CRITICAL),
                  "message":  (i.message  if hasattr(i, "severity") else str(i))}
                 for i in alerts]
            )
            send_alert(combined)
        except Exception as _sa_err:
            console_print(f"[Monitor] send_alert error: {_sa_err}")

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
            target=self._update_monitor_metrics_thread, daemon=True
        )
        thread.start()

    def _update_monitor_metrics_thread(self):
        """Background thread to update monitor metrics"""
        try:
            # Update OS metrics
            os_text = ""

            for conn_name, conn in list(self.monitor_connections.items()):
                # Skip servers pending removal
                if conn_name in self.servers_pending_removal:
                    console_print(f"Skipping metrics for {conn_name} (pending removal)")
                    continue

                if conn["monitoring"]:
                    # Track this query thread
                    self.active_server_query_threads[conn_name] = (
                        threading.current_thread()
                    )
                    # Run SSH commands to get metrics using existing connection
                    ssh_host = f"{conn['username']}@{conn['host']}"

                    # Check control socket if exists
                    if "control_path" in conn:
                        control_path = conn["control_path"]
                        if not os.path.exists(control_path):
                            os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                            os_text += f"  Error: SSH master connection lost\n"
                            os_text += (
                                f"  Please restart monitoring for this server\n\n"
                            )
                            conn["monitoring"] = False
                            continue

                    # Detect OS type once and cache it
                    if "os_type" not in conn:
                        os_detect_cmd = "uname -s"
                        os_type = "Linux"  # default

                        try:
                            # Use ControlPath for connections
                            ssh_cmd_detect = ["ssh"]
                            if "control_path" in conn:
                                ssh_cmd_detect.extend(
                                    ["-o", f"ControlPath={conn['control_path']}"]
                                )
                                ssh_cmd_detect.extend(["-o", "ControlMaster=auto"])
                            ssh_cmd_detect.extend(
                                ["-o", f"ConnectTimeout={self.ssh_test_timeout}"]
                            )
                            ssh_cmd_detect.extend([ssh_host, os_detect_cmd])

                            result_os = subprocess.run(
                                ssh_cmd_detect,
                                capture_output=True,
                                text=True,
                                timeout=self.ssh_test_timeout + 10,
                            )
                            if result_os.returncode == 0:
                                os_type = result_os.stdout.strip()
                        except (subprocess.SubprocessError, OSError):
                            pass  # Use default OS type

                        conn["os_type"] = os_type
                    else:
                        os_type = conn["os_type"]

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
                        ssh_cmd = ["ssh"]
                        if "control_path" in conn:
                            ssh_cmd.extend(
                                ["-o", f"ControlPath={conn['control_path']}"]
                            )
                            ssh_cmd.extend(["-o", "ControlMaster=auto"])
                        ssh_cmd.extend(
                            ["-o", f"ConnectTimeout={self.ssh_test_timeout}"]
                        )
                        ssh_cmd.extend([ssh_host, cmd])

                        result = subprocess.run(
                            ssh_cmd,
                            capture_output=True,
                            text=True,
                            timeout=self.ssh_timeout,
                        )

                        if result.returncode == 0:
                            output = result.stdout

                            # Parse the output
                            try:
                                # Extract CPU
                                cpu_match = (
                                    output.split("===CPU===")[1]
                                    .split("===MEM===")[0]
                                    .strip()
                                )

                                if os_type == "Darwin":  # macOS
                                    # macOS format: "CPU usage: 12.34% user, 56.78% sys, 30.88% idle"
                                    idle_match = re.search(
                                        r"(\d+\.?\d*)%\s+idle", cpu_match
                                    )
                                    if idle_match:
                                        idle = float(idle_match.group(1))
                                        cpu_usage = round(100 - idle, 1)
                                    else:
                                        cpu_usage = "N/A"
                                else:  # Linux
                                    # Try to extract idle percentage and calculate usage
                                    if "id" in cpu_match or "idle" in cpu_match:
                                        # Look for pattern like "23.4 id" or "23.4%id"
                                        idle_match = re.search(
                                            r"(\d+\.?\d*)\s*%?\s*i?d", cpu_match
                                        )
                                        if idle_match:
                                            idle = float(idle_match.group(1))
                                            cpu_usage = round(100 - idle, 1)
                                        else:
                                            cpu_usage = "N/A"
                                    else:
                                        cpu_usage = "N/A"

                                # Extract Memory
                                if os_type == "Darwin":  # macOS
                                    mem_match = (
                                        output.split("===MEM===")[1]
                                        .split("===MEMTOTAL===")[0]
                                        .strip()
                                    )
                                    memtotal_match = (
                                        output.split("===MEMTOTAL===")[1]
                                        .split("===DISK===")[0]
                                        .strip()
                                    )

                                    # Parse vm_stat output (pages are in 4KB chunks, numbers may have trailing dots)
                                    pages_free = re.search(
                                        r"Pages free:\s+(\d+)", mem_match
                                    )
                                    pages_active = re.search(
                                        r"Pages active:\s+(\d+)", mem_match
                                    )
                                    pages_wired = re.search(
                                        r"Pages wired down:\s+(\d+)", mem_match
                                    )

                                    # Parse total memory
                                    memtotal = re.search(
                                        r"hw\.memsize:\s+(\d+)", memtotal_match
                                    )

                                    if (
                                        pages_free
                                        and pages_active
                                        and pages_wired
                                        and memtotal
                                    ):
                                        page_size = 4096  # 4KB
                                        active_pages = int(pages_active.group(1))
                                        wired_pages = int(pages_wired.group(1))

                                        total_bytes = int(memtotal.group(1))
                                        total_mb = total_bytes / (1024 * 1024)

                                        used_pages = active_pages + wired_pages
                                        used_mb = (used_pages * page_size) / (
                                            1024 * 1024
                                        )

                                        mem_total = round(total_mb)
                                        mem_used = round(used_mb)
                                        mem_percent = round(
                                            (used_mb / total_mb) * 100, 1
                                        )
                                    else:
                                        mem_total = mem_used = mem_percent = "N/A"
                                else:  # Linux
                                    mem_match = (
                                        output.split("===MEM===")[1]
                                        .split("===DISK===")[0]
                                        .strip()
                                    )
                                    mem_parts = mem_match.split()
                                    if len(mem_parts) >= 3:
                                        mem_total = mem_parts[1]
                                        mem_used = mem_parts[2]
                                        try:
                                            mem_percent = round(
                                                (float(mem_used) / float(mem_total))
                                                * 100,
                                                1,
                                            )
                                        except (
                                            ValueError,
                                            ZeroDivisionError,
                                            TypeError,
                                        ):
                                            mem_percent = "N/A"
                                    else:
                                        mem_total = mem_used = mem_percent = "N/A"

                                # Extract Disk
                                disk_match = (
                                    output.split("===DISK===")[1]
                                    .split("===LOAD===")[0]
                                    .strip()
                                )
                                disk_parts = disk_match.split()
                                if len(disk_parts) >= 5:
                                    disk_size = disk_parts[1]
                                    disk_used = disk_parts[2]
                                    disk_avail = disk_parts[3]
                                    disk_percent = disk_parts[4].rstrip("%")
                                else:
                                    disk_size = disk_used = disk_avail = disk_percent = "N/A"

                                # Extract Load
                                load_match = (
                                    output.split("===LOAD===")[1]
                                    .split("===PROC===")[0]
                                    .strip()
                                )
                                if "load average" in load_match.lower():
                                    # Extract just the numbers after "load average:" or "load averages:"
                                    load_split = re.split(
                                        r"load averages?:",
                                        load_match,
                                        flags=re.IGNORECASE,
                                    )
                                    if len(load_split) > 1:
                                        load_avg = load_split[-1].strip()
                                    else:
                                        load_avg = "N/A"
                                else:
                                    load_avg = "N/A"

                                # Extract Process count
                                proc_match = (
                                    output.split("===PROC===")[1]
                                    .split("===END===")[0]
                                    .strip()
                                )
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
                                os_text += f"  Last Update:     {display_time_str()}\n\n"
                                self._ssh_last_cmd_ok_at[conn_name] = time.time()

                                # ── threshold evaluation ──────────────────────────────
                                if self._threshold_checker:
                                    _os_m: dict = {}
                                    if isinstance(cpu_usage, (int, float)):
                                        _os_m["cpu_utilization"] = float(cpu_usage)
                                    try:
                                        _os_m["free_memory_mb"] = float(mem_total) - float(mem_used)
                                    except (ValueError, TypeError):
                                        pass
                                    try:
                                        _load1m = float(str(load_avg).split(",")[0].split()[-1])
                                        _os_m["load_avg_1m"] = _load1m
                                    except (ValueError, TypeError, IndexError):
                                        pass
                                    try:
                                        _da = str(disk_avail).rstrip("i")  # strip Gi → G
                                        if _da.endswith("T"):
                                            _os_m["free_disk_gb"] = float(_da[:-1]) * 1024
                                        elif _da.endswith("G"):
                                            _os_m["free_disk_gb"] = float(_da[:-1])
                                        elif _da.endswith("M"):
                                            _os_m["free_disk_gb"] = float(_da[:-1]) / 1024
                                        elif _da.endswith("K"):
                                            _os_m["free_disk_gb"] = float(_da[:-1]) / (1024 * 1024)
                                        else:
                                            _os_m["free_disk_gb"] = float(_da)
                                    except (ValueError, TypeError):
                                        pass
                                    if _os_m:
                                        _alerts = self._threshold_checker.check_many(
                                            "os", _os_m, instance_id=conn_name
                                        )
                                        self._fire_alerts(_alerts, origin="os")

                                # Update OS graphs
                                try:
                                    # Check if this is the first metric for this host (add separator/header)
                                    first_metric_for_host = f"{conn_name} - CPU %"
                                    if (
                                        first_metric_for_host
                                        not in self.os_metrics_visualizer.graphs
                                    ):
                                        # Add separator/header for this host (always add for new hosts)
                                        self.os_metrics_visualizer.add_separator(
                                            f"=== {conn_name} ({conn['host']}) ==="
                                        )

                                    if isinstance(cpu_usage, (int, float)):
                                        metric_name = f"{conn_name} - CPU %"
                                        if (
                                            metric_name
                                            not in self.os_metrics_visualizer.graphs
                                        ):
                                            # 60 points * 5 seconds = 5 minutes
                                            self.os_metrics_visualizer.add_metric(
                                                metric_name
                                            )
                                        self.os_metrics_visualizer.update_metric(
                                            metric_name, cpu_usage
                                        )

                                    if isinstance(mem_percent, (int, float)):
                                        metric_name = f"{conn_name} - Memory %"
                                        if (
                                            metric_name
                                            not in self.os_metrics_visualizer.graphs
                                        ):
                                            # 60 points * 5 seconds = 5 minutes
                                            self.os_metrics_visualizer.add_metric(
                                                metric_name
                                            )
                                        self.os_metrics_visualizer.update_metric(
                                            metric_name, mem_percent
                                        )

                                    if isinstance(process_count, int):
                                        metric_name = f"{conn_name} - Processes"
                                        if (
                                            metric_name
                                            not in self.os_metrics_visualizer.graphs
                                        ):
                                            # 60 points * 5 seconds = 5 minutes
                                            self.os_metrics_visualizer.add_metric(
                                                metric_name
                                            )
                                        self.os_metrics_visualizer.update_metric(
                                            metric_name, process_count
                                        )
                                except Exception as graph_error:
                                    console_print(
                                        f"Error updating OS graphs: {graph_error}"
                                    )

                            except Exception as parse_error:
                                self._ssh_last_cmd_ok_at.pop(conn_name, None)
                                os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                                os_text += f"  Parse Error: {str(parse_error)}\n"
                                os_text += f"  Raw Output (first 500 chars):\n{output[:500]}\n\n"
                        else:
                            self._ssh_last_cmd_ok_at.pop(conn_name, None)
                            os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                            os_text += f"  SSH Command Failed\n"
                            os_text += f"  Return Code: {result.returncode}\n"
                            if result.stderr:
                                os_text += f"  Error: {result.stderr[:300]}\n"
                            if result.stdout:
                                os_text += f"  Output: {result.stdout[:300]}\n"
                            os_text += "\n"

                    except subprocess.TimeoutExpired:
                        self._ssh_last_cmd_ok_at.pop(conn_name, None)
                        os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                        os_text += f"  Error: SSH timeout\n\n"
                    except Exception as e:
                        self._ssh_last_cmd_ok_at.pop(conn_name, None)
                        os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                        os_text += f"  Error: {str(e)}\n\n"
                    finally:
                        # Remove from active threads when done
                        if conn_name in self.active_server_query_threads:
                            del self.active_server_query_threads[conn_name]

            if not os_text:
                os_text = "No active monitoring connections.\nClick 'Select Server' to start monitoring."

            # Collect local DB metrics (all DBs, then single UI update)
            local_db_results: dict[str, dict] = {}
            for db_name, db_manager in list(self.monitored_databases.items()):
                if db_name in self.databases_pending_removal:
                    console_print(f"Skipping metrics for {db_name} (pending removal)")
                    continue
                self.active_db_query_threads[db_name] = threading.current_thread()
                try:
                    db_stats = self.get_db_metrics(db_manager, db_name=db_name)
                    if db_stats:
                        local_db_results[db_name] = db_stats
                        # ── threshold evaluation for local DB ────────────────
                        if self._threshold_checker:
                            _db_m: dict = {}
                            if "Active Connections" in db_stats:
                                _db_m["active_connections"] = float(db_stats["Active Connections"])
                            if "Slow Queries" in db_stats:
                                _db_m["slow_query_count"] = float(db_stats["Slow Queries"])
                            try:
                                _bp_total = float(db_stats.get("Buffer Pool Size", 0))
                                _bp_used = float(db_stats.get("Buffer Pool Used", 0))
                                if _bp_total > 0:
                                    _db_m["cache_hit_ratio"] = _bp_used / _bp_total
                            except (ValueError, TypeError):
                                pass
                            try:
                                _waits = float(db_stats.get("Table Locks Waited", 0))
                                _qps = float(db_stats.get("Queries Per Second", 0))
                                if _qps > 0:
                                    _db_m["table_lock_wait_ratio"] = _waits / _qps
                            except (ValueError, TypeError):
                                pass
                            if _db_m:
                                db_type = getattr(
                                    self.monitored_databases.get(db_name),
                                    "db_type",
                                    "",
                                )
                                from monitoring.db_metric_config import db_type_path

                                _alerts = self._threshold_checker.check_many(
                                    "db",
                                    _db_m,
                                    instance_id=db_name,
                                    path=db_type_path(db_type),
                                    fallback_to_empty=True,
                                )
                                self._fire_alerts(_alerts, origin="db")
                finally:
                    if db_name in self.active_db_query_threads:
                        del self.active_db_query_threads[db_name]

            # Collect cloud database metrics
            cloud_text_dict: dict[str, str] = {}  # display_name → text block
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

            # Single UI update — ordered, no flicker
            self.root.after(0, self._update_db_panel, local_db_results)
            self.root.after(
                0, self._update_cloud_panel, cloud_text_dict, cloud_graph_data
            )

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
        self.monitor_update_job = self.root.after(
            self.refresh_interval, self.update_monitor_metrics
        )

    def _get_db_host(self, db_name: str) -> str:
        """Return the host string for a monitored database, or '' for unknown.

        Checks core Connections store, Monitor-tab-only store, then the
        active connection manager's cached reconnect params.
        """
        for mgr in (
            getattr(self, "connection_manager", None),
            getattr(self, "monitor_db_connection_manager", None),
        ):
            if mgr is None:
                continue
            try:
                conn_detail = mgr.get_connection(db_name)
                if conn_detail:
                    return conn_detail.get("host", "") or ""
            except Exception:
                pass

        db_manager = self.monitored_databases.get(db_name)
        if db_manager is not None:
            params = getattr(db_manager, "_last_connect_params", None) or {}
            host = params.get("host")
            if host:
                return str(host)

        return ""

    def _liveness_window_seconds(
        self, refresh_interval_s: float, override_window_s: float
    ) -> float:
        """Return the skip window for liveness checks (seconds)."""
        if override_window_s > 0:
            return float(override_window_s)
        return refresh_interval_s * 2 + 5

    def _should_skip_liveness(
        self,
        last_ok_at: float,
        refresh_interval_s: float,
        override_window_s: float,
    ) -> bool:
        """True when recent successful metric/API activity proves the channel is alive."""
        if last_ok_at <= 0:
            return False
        window = self._liveness_window_seconds(refresh_interval_s, override_window_s)
        return (time.time() - last_ok_at) < window

    def _seconds_until_expiry(self, provider: str, monitor) -> float | None:
        """Seconds until cloud credentials expire, or None if unknown/permanent."""
        provider = (provider or "").upper()
        try:
            if provider == "GCP":
                credentials = getattr(monitor, "credentials", None)
                if credentials is None:
                    return None
                expiry = getattr(credentials, "expiry", None)
                if expiry is None:
                    return None
                from datetime import datetime, timezone

                now = datetime.now(timezone.utc)
                if getattr(expiry, "tzinfo", None) is None:
                    expiry = expiry.replace(tzinfo=timezone.utc)
                return (expiry - now).total_seconds()

            if provider == "AZURE":
                expires_on = getattr(monitor, "_token_expires_on", None)
                if expires_on is None:
                    credential = getattr(monitor, "credential", None)
                    if credential is None:
                        return None
                    tok = credential.get_token(
                        "https://management.azure.com/.default"
                    )
                    expires_on = tok.expires_on
                    monitor._token_expires_on = expires_on
                return expires_on - time.time()

            if provider == "AWS":
                client = getattr(monitor, "rds", None) or getattr(monitor, "cw", None)
                if client is None:
                    return None
                creds = client._request_signer._credentials
                expiry = getattr(creds, "_expiry_time", None)
                if expiry is None:
                    return None
                from datetime import datetime, timezone

                now = datetime.now(timezone.utc)
                if expiry.tzinfo is None:
                    expiry = expiry.replace(tzinfo=timezone.utc)
                return (expiry - now).total_seconds()
        except Exception:
            pass
        return None

    def _cloud_should_refresh_keepalive(
        self, display_name: str, entry: dict, monitor
    ) -> bool:
        """True when the cloud keepalive loop should refresh this monitor."""
        if self._cloud_needs_refresh.get(display_name):
            return True
        if self._cloud_consecutive_failures.get(display_name, 0) >= 1:
            return True

        secs_left = self._seconds_until_expiry(entry.get("provider", ""), monitor)
        if secs_left is not None and secs_left <= 300:
            return True

        if self._cloud_force_refresh_interval > 0:
            last_ok = self._cloud_last_ok_at.get(display_name, 0.0)
            if last_ok <= 0:
                return True
            if time.time() - last_ok > self._cloud_force_refresh_interval:
                return True

        return False

    def _clear_cloud_liveness_state(self, display_name: str):
        """Drop cached cloud liveness timestamps/flags for a monitor."""
        self._cloud_last_ok_at.pop(display_name, None)
        self._cloud_consecutive_failures.pop(display_name, None)
        self._cloud_needs_refresh.pop(display_name, None)

    def get_db_metrics(self, db_manager, db_name: str = ""):
        """
        Collect all enabled metrics for *db_manager* and return a flat dict
        {display_name: raw_value} for backward compatibility with the threshold
        evaluation code.  The actual structured collection is done by
        db_metric_config.collect_metrics().
        """
        db_type = getattr(db_manager, "db_type", "")
        _lock_key = db_name or db_type
        refresh_s = self.refresh_interval / 1000.0

        # Skip redundant ping when a recent metric cycle proved the DB is alive.
        if not self._should_skip_liveness(
            self._db_last_metric_at.get(db_name, 0.0),
            refresh_s,
            self._db_metric_skip_ping_if_used_within,
        ):
            try:
                with self._get_db_lock(_lock_key):
                    if hasattr(db_manager, "ping_or_reconnect"):
                        if not db_manager.ping_or_reconnect():
                            console_print(f"  ✗ {db_type} connection not recoverable")
                            self._db_last_metric_at.pop(db_name, None)
                            return None
            except Exception as ping_err:
                console_print(f"  ✗ {db_type} connection validation failed: {ping_err}")
                self._db_last_metric_at.pop(db_name, None)
                return None

        host = self._get_db_host(db_name)
        console_print(f"\n=== Collecting metrics for {db_type} ({db_name or 'unnamed'}) ===")

        # Wrap execute_query to run under the per-db lock and count successful SQL.
        original_execute = db_manager.execute_query
        sql_ok_count = 0

        def _locked_execute(query):
            nonlocal sql_ok_count
            with self._get_db_lock(_lock_key):
                result, error = original_execute(query)
                if error is None:
                    sql_ok_count += 1
                return result, error

        db_manager.execute_query = _locked_execute
        try:
            sections, raw_floats, os_note = _collect_db_metrics(
                db_manager, host=host, checker=self._threshold_checker
            )
        except Exception as collect_err:
            console_print(f"  ✗ {db_type} metric collection failed: {collect_err}")
            self._db_last_metric_at.pop(db_name, None)
            return None
        finally:
            db_manager.execute_query = original_execute

        console_print(f"  Collected {len(raw_floats)} numeric metrics")

        # Store sections for the display renderer (keyed by db_name)
        if not hasattr(self, "_db_sections_cache"):
            self._db_sections_cache = {}
        self._db_sections_cache[db_name] = sections
        if sql_ok_count > 0:
            self._db_last_metric_at[db_name] = time.time()
        else:
            self._db_last_metric_at.pop(db_name, None)
        # store the os_note so the display can append it
        if not hasattr(self, "_db_os_note_cache"):
            self._db_os_note_cache = {}
        self._db_os_note_cache[db_name] = os_note

        # Return flat dict for backward-compatible threshold evaluation
        return raw_floats if raw_floats else None

    def refresh_monitor_metrics(self):
        """Manually refresh metrics — reloads monitor_thresholds.ini first"""
        if self._threshold_checker:
            try:
                self._threshold_checker.reload()
                console_print("[Monitor] monitor_thresholds.ini reloaded on Refresh")
            except Exception as _reload_err:
                print(f"[Monitor] Could not reload thresholds: {_reload_err}", file=sys.stderr)
        self.update_monitor_metrics()
        self.update_monitor_status_label()

    def refresh_server_metrics(self):
        """Manually refresh only server/OS metrics — reloads monitor_thresholds.ini first"""
        if self._threshold_checker:
            try:
                self._threshold_checker.reload()
                console_print("[Monitor] monitor_thresholds.ini reloaded on Refresh")
            except Exception as _reload_err:
                print(f"[Monitor] Could not reload thresholds: {_reload_err}", file=sys.stderr)
        # Run in background thread to avoid UI freeze
        thread = threading.Thread(
            target=self._refresh_server_metrics_thread, daemon=True
        )
        thread.start()

    def _refresh_server_metrics_thread(self):
        """Background thread to refresh only OS metrics — INI already reloaded by caller"""
        try:
            os_text = ""

            for conn_name, conn in self.monitor_connections.items():
                if conn["monitoring"]:
                    # Run SSH commands to get metrics using existing connection
                    ssh_host = f"{conn['username']}@{conn['host']}"

                    # Detect OS type once and cache it
                    if "os_type" not in conn:
                        os_detect_cmd = "uname -s"
                        os_type = "Linux"  # default

                        try:
                            # Use ControlPath for connections
                            ssh_cmd_detect = ["ssh"]
                            if "control_path" in conn:
                                ssh_cmd_detect.extend(
                                    ["-o", f"ControlPath={conn['control_path']}"]
                                )
                                ssh_cmd_detect.extend(["-o", "ControlMaster=auto"])
                            ssh_cmd_detect.extend(
                                ["-o", f"ConnectTimeout={self.ssh_test_timeout}"]
                            )
                            ssh_cmd_detect.extend([ssh_host, os_detect_cmd])

                            result_os = subprocess.run(
                                ssh_cmd_detect,
                                capture_output=True,
                                text=True,
                                timeout=self.ssh_test_timeout + 10,
                            )
                            if result_os.returncode == 0:
                                os_type = result_os.stdout.strip()
                        except (subprocess.SubprocessError, OSError):
                            pass  # Use default OS type

                        conn["os_type"] = os_type
                    else:
                        os_type = conn["os_type"]

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
                        ssh_cmd = ["ssh"]
                        if "control_path" in conn:
                            ssh_cmd.extend(
                                ["-o", f"ControlPath={conn['control_path']}"]
                            )
                            ssh_cmd.extend(["-o", "ControlMaster=auto"])
                        ssh_cmd.extend(
                            ["-o", f"ConnectTimeout={self.ssh_test_timeout}"]
                        )
                        ssh_cmd.extend([ssh_host, cmd])

                        result = subprocess.run(
                            ssh_cmd,
                            capture_output=True,
                            text=True,
                            timeout=self.ssh_timeout,
                        )

                        if result.returncode == 0:
                            output = result.stdout

                            # Parse the output and update OS metrics (same logic as in _update_monitor_metrics_thread)
                            # ... (copy the parsing logic from _update_monitor_metrics_thread for OS metrics)
                            # For brevity, I'll just append raw output for now
                            os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                            os_text += (
                                f"  Last refreshed: {display_time_str()}\n"
                            )

                            # Extract key metrics
                            try:
                                cpu_match = (
                                    output.split("===CPU===")[1]
                                    .split("===MEM===")[0]
                                    .strip()
                                )
                                # Format CPU output - each line indented
                                for line in cpu_match.split("\n"):
                                    if line.strip():
                                        os_text += f"  CPU: {line.strip()}\n"

                                mem_match = (
                                    output.split("===MEM===")[1].split("===DISK===")[0]
                                    if "Darwin" not in os_type
                                    else output.split("===MEM===")[1].split(
                                        "===MEMTOTAL==="
                                    )[0]
                                )
                                # Format Memory output - each line indented
                                mem_lines = mem_match.strip().split("\n")
                                if mem_lines:
                                    os_text += f"  Memory: {mem_lines[0].strip()}\n"
                                    for line in mem_lines[1:]:
                                        if line.strip():
                                            os_text += f"          {line.strip()}\n"

                                disk_match = (
                                    output.split("===DISK===")[1]
                                    .split("===LOAD===")[0]
                                    .strip()
                                )
                                os_text += f"  Disk: {disk_match}\n"

                                load_match = (
                                    output.split("===LOAD===")[1]
                                    .split("===PROC===")[0]
                                    .strip()
                                )
                                os_text += f"  Load: {load_match}\n"

                                os_text += "\n"
                                self._ssh_last_cmd_ok_at[conn_name] = time.time()
                            except Exception as parse_error:
                                self._ssh_last_cmd_ok_at.pop(conn_name, None)
                                os_text += f"  Parse Error: {str(parse_error)}\n\n"
                        else:
                            self._ssh_last_cmd_ok_at.pop(conn_name, None)
                            os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                            os_text += f"  SSH Command Failed\n"
                            os_text += f"  Return Code: {result.returncode}\n"
                            if result.stderr:
                                os_text += f"  Error: {result.stderr[:300]}\n"
                            if result.stdout:
                                os_text += f"  Output: {result.stdout[:300]}\n"
                            os_text += "\n"

                    except subprocess.TimeoutExpired:
                        self._ssh_last_cmd_ok_at.pop(conn_name, None)
                        os_text += f"=== {conn_name} ({conn['host']}) ===\n"
                        os_text += f"  Error: SSH timeout\n\n"
                    except Exception as e:
                        self._ssh_last_cmd_ok_at.pop(conn_name, None)
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

    # -------------------------------------------------------------------------
    # Cloud Database Monitoring
    # -------------------------------------------------------------------------

