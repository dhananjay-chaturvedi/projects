"""Monitoring screen — three per-category target lists with concurrent polling.

Mirrors the Tk Monitoring tab: a status bar (Monitor Settings / Alert Thresholds)
over three sections — Server / Database / Cloud — each with its OWN saved-targets
list, Add / Select / Remove controls and a live metrics panel. "Select" starts
monitoring a target (adds it to that section's active set; many can run at once);
"Remove" stops monitoring an active target or deletes a saved one. A single
refresh tick polls every active target across all three sections at once.
"""

from __future__ import annotations

from textual.containers import Horizontal
from textual.widgets import (
    Button, Checkbox, Collapsible, DataTable, Label, OptionList, Static,
)
from textual.widgets.option_list import Option

from common.ui.shared import specs
from common.ui.textual.screens.base import BaseScreen

# (section id, source values that feed this section's saved list)
_CATEGORIES = (
    ("server", ("monitor",)),
    ("database", ("monitor-db", "db")),
    ("cloud", ("cloud",)),
)
_LOCAL_OS = "(local OS)"


class MonitoringScreen(BaseScreen):
    """DB/OS/cloud metrics, thresholds and alerts (requires monitor module)."""

    NAV_ID = "monitor"

    def __init__(self, svc, **kwargs) -> None:
        super().__init__(svc, **kwargs)
        # Saved targets and active (monitored) sets, per category.
        self._saved: dict[str, list[dict]] = {"server": [], "database": [], "cloud": []}
        self._active: dict[str, set[str]] = {"server": set(), "database": set(), "cloud": set()}
        self._metrics_cache: dict[tuple[str, str], dict] = {}
        self._target_source: dict[str, str] = {}
        self._threshold_rules: list[dict] = []
        self._auto_timer = None
        self._interval = 5.0
        self._view_mode: dict[str, str] = {"server": "text", "database": "text", "cloud": "text"}
        self._graph_history: dict[str, list[float]] = {}

    def screen_title(self) -> str:
        return "Monitoring"

    # ------------------------------------------------------------------ #
    # Layout
    # ------------------------------------------------------------------ #
    def compose_body(self):
        spec = specs.monitoring_payload()
        top = {a["id"]: a["label"] for a in spec["topActions"]}
        sections = {s["id"]: s for s in spec["sections"]}
        thr = {a["id"]: a["label"] for a in spec["thresholdActions"]}
        view = {a["id"]: a["label"] for a in spec["viewActions"]}

        def ta(section_id: str, action_id: str) -> str:
            for action in sections.get(section_id, {}).get("targetActions", []):
                if action["id"] == action_id:
                    return action["label"]
            return ""

        with Horizontal(classes="actions-row"):
            yield Button(top["settings"], id="mon-settings", classes="mini")
            yield Button("Notifications", id="mon-notifications", classes="mini")
            yield Button(top["thresholds_settings"], id="mon-threshold-settings", classes="mini")
            yield Button("Refresh now", id="mon-refresh-all", variant="primary")
            yield Checkbox("Auto refresh", id="mon-auto-refresh")

        # One collapsible per category: saved list + controls + metrics panel.
        for cat in ("server", "database", "cloud"):
            sec = sections[cat]
            with Collapsible(title=sec["title"], id=f"mon-sec-{cat}", collapsed=False):
                yield OptionList(id=f"mon-{cat}-list")
                with Horizontal(classes="actions-row"):
                    yield Button(ta(cat, "add"), id=f"mon-{cat}-add", classes="mini")
                    yield Button(ta(cat, "select"), id=f"mon-{cat}-select", classes="mini")
                    yield Button(ta(cat, "remove"), id=f"mon-{cat}-remove", classes="mini")
                    yield Button("Refresh", id=f"mon-{cat}-refresh", classes="mini")
                    if cat == "server":
                        yield Button("Local OS", id="mon-server-localos", classes="mini")
                with Horizontal(classes="actions-row"):
                    yield Button(view["show_graphs"], id=f"mon-{cat}-show-graphs", classes="mini")
                    yield Button(view["show_text"], id=f"mon-{cat}-show-text", classes="mini")
                    yield Button(view["clear_graphs"], id=f"mon-{cat}-clear-graphs", classes="mini")
                yield Label(sec["metricsTitle"], classes="hint")
                yield DataTable(id=f"mon-{cat}-metrics", zebra_stripes=True)
                yield Static("", id=f"mon-{cat}-graphs", classes="mon-graphs")

        with Collapsible(title="Alert thresholds", id="mon-sec-thresholds", collapsed=True):
            with Horizontal(classes="actions-row"):
                yield Button(thr["load"], id="mon-thresholds")
                yield Button(thr["edit"], id="mon-thr-edit", classes="mini")
                yield Button(thr["check"], id="mon-thr-check", classes="mini")
            yield DataTable(id="mon-thr-table", zebra_stripes=True)

        with Collapsible(title="Alerts", id="mon-sec-alerts", collapsed=False):
            with Horizontal(classes="actions-row"):
                yield Button("Reload alerts", id="mon-alerts", classes="mini")
                yield Button(thr["clear_alerts"], id="mon-alerts-clear", variant="error")
            yield DataTable(id="mon-alerts-table", zebra_stripes=True)

        yield Static("", id="mon-status", classes="status")

    def on_mount(self) -> None:
        for cat in ("server", "database", "cloud"):
            self.query_one(f"#mon-{cat}-graphs", Static).display = False
        try:
            self._reload_saved()
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------ #
    # Saved targets + lists
    # ------------------------------------------------------------------ #
    def _reload_saved(self) -> None:
        """Reload the three saved-target lists from the service layer."""
        rows: list[dict] = []
        if hasattr(self.svc, "list_all_connections"):
            rows = [r for r in self.svc.list_all_connections(source="all") if not r.get("error")]
        elif hasattr(self.svc, "list_connections"):
            rows = [{"name": c.get("name", ""), "source": "db", "kind": c.get("db_type", ""),
                     "host": c.get("host", "")} for c in self.svc.list_connections()]

        self._saved = {"server": [], "database": [], "cloud": []}
        self._target_source.clear()
        src_to_cat = {src: cat for cat, srcs in _CATEGORIES for src in srcs}
        for r in rows:
            name = str(r.get("name") or "")
            if not name:
                continue
            src = r.get("source") or "db"
            self._target_source[name] = src
            cat = src_to_cat.get(src)
            if cat:
                self._saved[cat].append(r)
        for cat in ("server", "database", "cloud"):
            self._populate_list(cat)

    @staticmethod
    def _row_label(cat: str, row: dict) -> str:
        name = str(row.get("name") or "")
        kind = str(row.get("kind") or "")
        host = str(row.get("host") or "")
        if cat == "server":
            return f"{name}  [{kind or 'vm'}@{host}]" if host else f"{name}  [{kind or 'vm'}]"
        if cat == "cloud":
            return f"{name}  [{kind or 'cloud'}]"
        tag = "monitor" if row.get("source") == "monitor-db" else "db"
        return f"{name}  [{kind or tag}]"

    def _populate_list(self, cat: str) -> None:
        ol = self.query_one(f"#mon-{cat}-list", OptionList)
        highlighted = ol.highlighted
        ol.clear_options()
        options: list[Option] = []
        for row in self._saved[cat]:
            name = str(row.get("name") or "")
            marker = "● " if name in self._active[cat] else "  "
            options.append(Option(marker + self._row_label(cat, row), id=name))
        if cat == "server" and _LOCAL_OS in self._active["server"]:
            options.append(Option("● " + _LOCAL_OS, id=_LOCAL_OS))
        if options:
            ol.add_options(options)
            if highlighted is not None and highlighted < len(options):
                ol.highlighted = highlighted
            else:
                ol.highlighted = 0

    def _selected_name(self, cat: str) -> str:
        ol = self.query_one(f"#mon-{cat}-list", OptionList)
        if ol.highlighted is None:
            return ""
        try:
            opt = ol.get_option_at_index(ol.highlighted)
        except Exception:  # noqa: BLE001
            return ""
        return str(opt.id or "")

    # ------------------------------------------------------------------ #
    # Metrics helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _flatten_sections(sections) -> list[tuple]:
        """Normalise sections ([name, [[metric, value], …]], …) to rows."""
        rows: list[tuple] = []
        if isinstance(sections, list):
            for sec in sections:
                if isinstance(sec, (list, tuple)) and len(sec) == 2 and isinstance(sec[1], list):
                    name, pairs = sec
                    for pair in pairs:
                        if isinstance(pair, (list, tuple)) and len(pair) == 2:
                            rows.append((str(name), str(pair[0]), str(pair[1])))
                elif isinstance(sec, dict):
                    name = sec.get("title") or sec.get("name") or ""
                    for k, v in (sec.get("metrics") or {}).items():
                        rows.append((str(name), str(k), str(v)))
        elif isinstance(sections, dict):
            for name, metrics in sections.items():
                if isinstance(metrics, dict):
                    for k, v in metrics.items():
                        rows.append((str(name), str(k), str(v)))
        return rows

    def _status(self, msg: str) -> None:
        self.query_one("#mon-status", Static).update(msg)

    def _collect(self, targets: list[tuple[str, str]]) -> dict:
        """Poll every active target (runs in a thread worker). Pure I/O."""
        results: dict = {"metrics": {}, "alerts": []}
        for cat, name in targets:
            try:
                if name == _LOCAL_OS:
                    r = self.svc.get_os_metrics() if hasattr(self.svc, "get_os_metrics") else {}
                    if r.get("error"):
                        results["metrics"][(cat, name)] = {"error": r["error"]}
                        continue
                    m = r.get("metrics") or {}
                    results["metrics"][(cat, name)] = {
                        "sections": [["OS", [[str(k), str(v)] for k, v in m.items()]]]}
                    continue
                if not hasattr(self.svc, "monitor_any"):
                    results["metrics"][(cat, name)] = {"error": "Monitor module not available."}
                    continue
                r = self.svc.monitor_any(name)
                results["metrics"][(cat, name)] = r
                for a in r.get("alerts") or []:
                    results["alerts"].append({**a, "source": a.get("source") or name})
            except Exception as exc:  # noqa: BLE001
                results["metrics"][(cat, name)] = {"error": str(exc)}
        return results

    def _apply_results(self, results: dict) -> None:
        metrics = results.get("metrics") or {}
        for (cat, name), res in metrics.items():
            self._metrics_cache[(cat, name)] = res
        for cat in ("server", "database", "cloud"):
            self._render_category(cat)
        alerts = results.get("alerts") or []
        if alerts:
            self._render_alerts(alerts)
        total = sum(len(self._active[c]) for c in self._active)
        self._status(f"Polled {total} target(s); {len(alerts)} alert(s).")

    @staticmethod
    def _sparkline(values: list[float], width: int = 24) -> str:
        if not values:
            return ""
        lo, hi = min(values), max(values)
        span = hi - lo or 1.0
        chars = "▁▂▃▄▅▆▇█"
        recent = values[-width:]
        return "".join(
            chars[min(len(chars) - 1, int((v - lo) / span * (len(chars) - 1)))]
            for v in recent
        )

    def _update_graph_history(self, cat: str) -> None:
        for name in sorted(self._active[cat]):
            res = self._metrics_cache.get((cat, name)) or {}
            floats = res.get("raw_floats") or {}
            if not floats:
                for _sec, metric, value in self._flatten_sections(res.get("sections")):
                    try:
                        floats[metric] = float(value)
                    except (TypeError, ValueError):
                        continue
            for metric, val in floats.items():
                key = f"{cat}|{name}|{metric}"
                hist = self._graph_history.setdefault(key, [])
                try:
                    hist.append(float(val))
                except (TypeError, ValueError):
                    continue
                self._graph_history[key] = hist[-40:]

    def _graph_text(self, cat: str) -> str:
        lines: list[str] = []
        for name in sorted(self._active[cat]):
            prefix = f"{cat}|{name}|"
            keys = sorted(k for k in self._graph_history if k.startswith(prefix))
            if not keys:
                continue
            lines.append(f"=== {name} ===")
            for key in keys:
                metric = key.split("|", 2)[-1]
                hist = self._graph_history[key]
                latest = hist[-1] if hist else 0
                lines.append(f"  {metric}: {self._sparkline(hist)} ({latest:g})")
            lines.append("")
        return "\n".join(lines).strip() or "No graph data yet — select targets and refresh."

    def _render_category(self, cat: str) -> None:
        table = self.query_one(f"#mon-{cat}-metrics", DataTable)
        graphs = self.query_one(f"#mon-{cat}-graphs", Static)
        mode = self._view_mode.get(cat, "text")
        table.display = mode == "text"
        graphs.display = mode == "graph"
        if mode == "graph":
            self._update_graph_history(cat)
            graphs.update(self._graph_text(cat))
            return
        table.clear(columns=True)
        table.add_columns("target", "section", "metric", "value")
        active = sorted(self._active[cat])
        if not active:
            table.add_row("(no targets monitored)", "", "", "")
            return
        for name in active:
            res = self._metrics_cache.get((cat, name))
            if not res:
                table.add_row(name, "", "(waiting…)", "")
                continue
            if res.get("error"):
                table.add_row(name, "", "error", str(res["error"])[:60])
                continue
            rows = self._flatten_sections(res.get("sections"))
            if not rows:
                table.add_row(name, "", "(no metrics)", "")
            for sec, metric, value in rows:
                table.add_row(name, sec, metric, value)

    def _render_alerts(self, alerts: list) -> None:
        out = self.query_one("#mon-alerts-table", DataTable)
        out.clear(columns=True)
        out.add_columns("time", "severity", "source", "message")
        for a in alerts or []:
            out.add_row(
                str(a.get("timestamp", a.get("time", ""))), str(a.get("severity", "")),
                str(a.get("source", "")), str(a.get("message", ""))[:80])

    async def _refresh_now(self) -> None:
        targets = [(c, n) for c in ("server", "database", "cloud")
                   for n in sorted(self._active[c])]
        if not targets:
            for cat in ("server", "database", "cloud"):
                self._render_category(cat)
            self._status("No targets monitored. Highlight a saved target and press Select.")
            return
        worker = self.run_worker(lambda: self._collect(targets), thread=True,
                                 exclusive=True, group="mon-collect", exit_on_error=False)
        try:
            results = await worker.wait()
        except Exception as exc:  # noqa: BLE001
            self._status(f"Polling failed: {exc}")
            return
        if isinstance(results, dict):
            self._apply_results(results)

    # ------------------------------------------------------------------ #
    # Events
    # ------------------------------------------------------------------ #
    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        if (event.checkbox.id or "") != "mon-auto-refresh":
            return
        if event.value:
            self._auto_timer = self.set_interval(self._interval, self._tick)
            self._status("Auto refresh enabled.")
        else:
            if self._auto_timer is not None:
                self._auto_timer.stop()
                self._auto_timer = None
            self._status("Auto refresh disabled.")

    def _tick(self) -> None:
        self.run_worker(self._refresh_now, exclusive=True, group="mon-poll")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""

        if bid == "mon-settings":
            self._monitor_settings()
            return
        if bid == "mon-notifications":
            self._notification_settings()
            return
        if bid == "mon-threshold-settings":
            self._status("Alert Thresholds: use Thresholds to load current rules.")
            return
        if bid in ("mon-refresh-all", "mon-server-refresh", "mon-database-refresh",
                   "mon-cloud-refresh"):
            self.run_worker(self._refresh_now, exclusive=True, group="mon-poll")
            return
        if bid == "mon-server-localos":
            self._toggle_local_os()
            return

        for cat in ("server", "database", "cloud"):
            if bid == f"mon-{cat}-show-graphs":
                self._view_mode[cat] = "graph"
                self._render_category(cat)
                self._status(f"{cat.title()} metrics: graph view.")
                return
            if bid == f"mon-{cat}-show-text":
                self._view_mode[cat] = "text"
                self._render_category(cat)
                self._status(f"{cat.title()} metrics: text view.")
                return
            if bid == f"mon-{cat}-clear-graphs":
                drop = [k for k in self._graph_history if k.startswith(f"{cat}|")]
                for k in drop:
                    del self._graph_history[k]
                if self._view_mode.get(cat) == "graph":
                    self._render_category(cat)
                self._status(f"{cat.title()} graphs cleared.")
                return

        # Per-section Add / Select / Remove.
        for cat in ("server", "database", "cloud"):
            if bid == f"mon-{cat}-add":
                self._add_target(cat)
                return
            if bid == f"mon-{cat}-select":
                self._start_monitoring(cat)
                return
            if bid == f"mon-{cat}-remove":
                self._remove(cat)
                return

        if bid == "mon-thresholds":
            self._load_thresholds()
        elif bid == "mon-thr-edit":
            self._edit_threshold()
        elif bid == "mon-thr-check":
            self._check_threshold()
        elif bid == "mon-alerts":
            self._reload_alerts()
        elif bid == "mon-alerts-clear":
            if hasattr(self.svc, "clear_alerts"):
                self.svc.clear_alerts()
                self._render_alerts([])
                self._status("Alerts cleared.")

    # ------------------------------------------------------------------ #
    # Monitoring lifecycle
    # ------------------------------------------------------------------ #
    def _start_monitoring(self, cat: str) -> None:
        name = self._selected_name(cat)
        if not name:
            self._status("Highlight a saved target first.")
            return
        if name in self._active[cat]:
            self._status(f"'{name}' is already being monitored.")
            return
        self._active[cat].add(name)
        self._populate_list(cat)
        self._status(f"Monitoring '{name}'.")
        self.run_worker(self._refresh_now, exclusive=True, group="mon-poll")

    def _toggle_local_os(self) -> None:
        if _LOCAL_OS in self._active["server"]:
            self._active["server"].discard(_LOCAL_OS)
            self._metrics_cache.pop(("server", _LOCAL_OS), None)
            self._status("Stopped local OS metrics.")
        else:
            self._active["server"].add(_LOCAL_OS)
            self._status("Monitoring local OS metrics.")
        self._populate_list("server")
        self.run_worker(self._refresh_now, exclusive=True, group="mon-poll")

    def _remove(self, cat: str) -> None:
        """Stop monitoring an active target, or delete a saved one (source-aware)."""
        name = self._selected_name(cat)
        if not name:
            self._status("Highlight a target first.")
            return
        if name in self._active[cat]:
            self._active[cat].discard(name)
            self._metrics_cache.pop((cat, name), None)
            self._populate_list(cat)
            self._render_category(cat)
            self._status(f"Stopped monitoring '{name}'.")
            return
        self._delete_saved(name)

    def _delete_saved(self, name: str) -> None:
        src = self._target_source.get(name, "db")
        try:
            if src == "monitor-db" and hasattr(self.svc, "remove_monitor_db_connection"):
                r = self.svc.remove_monitor_db_connection(name)
            elif src == "monitor" and hasattr(self.svc, "remove_monitor_connection"):
                r = self.svc.remove_monitor_connection(name)
            elif src == "cloud" and hasattr(self.svc, "remove_cloud_connection"):
                r = self.svc.remove_cloud_connection(name)
            else:
                self._status("This is a Connections-tab DB profile; remove it from Connections.")
                return
        except Exception as exc:  # noqa: BLE001
            self._status(str(exc))
            return
        if not r.get("ok"):
            self._status(r.get("message", "Could not delete target."))
            return
        self._reload_saved()
        self._status(f"Deleted saved target '{name}'.")

    # ------------------------------------------------------------------ #
    # Add forms (per category)
    # ------------------------------------------------------------------ #
    def _add_target(self, cat: str) -> None:
        if cat == "server":
            self._add_ssh_connection()
        elif cat == "database":
            self._add_db_connection()
        else:
            self._add_cloud_connection()

    def _add_ssh_connection(self) -> None:
        if not hasattr(self.svc, "add_monitor_connection"):
            self._status("Monitor connection management not available.")
            return
        from common.ui.textual.screens.form_modal import FormModal

        fields = [
            {"name": "target_type", "label": "Target type", "type": "select", "value": "vm",
             "options": [("vm", "VM / host (SSH)"), ("db_server", "DB server (SSH)"),
                         ("service", "Other service (SSH)")]},
            {"name": "name", "label": "Connection name"},
            {"name": "host", "label": "Hostname or IP", "value": "localhost"},
            {"name": "username", "label": "SSH username"},
            {"name": "password", "label": "Password (optional)", "type": "password"},
        ]

        def _done(v: dict | None) -> None:
            if not v:
                return
            if not (v.get("name") and v.get("host") and v.get("username")):
                self._status("Name, host and username are required.")
                return
            r = self.svc.add_monitor_connection(
                v["name"], v["host"], v["username"],
                password=v.get("password", ""), target_type=v.get("target_type", "vm"))
            if not r.get("ok"):
                self._status(r.get("message", "Could not add connection."))
                return
            self._reload_saved()
            self._status(f"Monitor connection '{v['name']}' added.")

        self.app.push_screen(FormModal("Add Monitoring Connection (SSH)", fields,
                                       submit_label="Add connection"), _done)

    def _add_db_connection(self) -> None:
        if not hasattr(self.svc, "add_monitor_db_connection"):
            self._status("Monitor DB management not available.")
            return
        from common.ui.textual.screens.form_modal import FormModal

        fields = [
            {"name": "name", "label": "Connection name"},
            {"name": "db_type", "label": "Database type", "type": "select", "value": "MariaDB",
             "options": ["MariaDB", "MySQL", "PostgreSQL", "Oracle"]},
            {"name": "host", "label": "Host", "value": "localhost"},
            {"name": "port", "label": "Port"},
            {"name": "database", "label": "Database (non-Oracle)"},
            {"name": "service", "label": "Service name (Oracle)"},
            {"name": "username", "label": "Username"},
            {"name": "password", "label": "Password", "type": "password"},
        ]

        def _done(v: dict | None) -> None:
            if not v:
                return
            if not (v.get("name") and v.get("db_type") and v.get("host")):
                self._status("Name, type and host are required.")
                return
            from common.connection_params import ConnectionParams

            r = self.svc.add_monitor_db_connection(
                ConnectionParams.from_mapping({
                    "name": v["name"],
                    "db_type": v["db_type"],
                    "host": v["host"],
                    "port": v.get("port", ""),
                    "user": v.get("username", ""),
                    "password": v.get("password", ""),
                    "database": v.get("database", ""),
                    "service": v.get("service", ""),
                }))
            if not r.get("ok"):
                self._status(r.get("message", "Could not add database."))
                return
            self._reload_saved()
            self._status(f"Monitor database '{v['name']}' added.")

        self.app.push_screen(FormModal("Add Database (Monitoring only)", fields,
                                       submit_label="Add database"), _done)

    def _add_cloud_connection(self) -> None:
        if not hasattr(self.svc, "add_cloud_connection"):
            self._status("Cloud connection management not available.")
            return
        from common.ui.textual.screens.form_modal import FormModal

        fields = [
            {"name": "name", "label": "Connection name"},
            {"name": "provider", "label": "Provider", "type": "select", "value": "AWS",
             "options": ["AWS", "Azure", "GCP", "Other"]},
            {"name": "region", "label": "Region / location"},
            {"name": "access_key", "label": "Access key / client id"},
            {"name": "secret_key", "label": "Secret key", "type": "password"},
        ]

        def _done(v: dict | None) -> None:
            if not v:
                return
            if not (v.get("name") and v.get("provider")):
                self._status("Name and provider are required.")
                return
            profile = {"provider": v["provider"], "region": v.get("region", "")}
            if v.get("access_key"):
                profile["access_key"] = v["access_key"]
            if v.get("secret_key"):
                profile["secret_key"] = v["secret_key"]
            r = self.svc.add_cloud_connection(v["name"], profile)
            if not r.get("ok"):
                self._status(r.get("message", "Could not add cloud resource."))
                return
            self._reload_saved()
            self._status(f"Cloud resource '{v['name']}' added.")

        self.app.push_screen(FormModal("Add Cloud Resource", fields,
                                       submit_label="Add cloud resource"), _done)

    # ------------------------------------------------------------------ #
    # Settings / thresholds / alerts
    # ------------------------------------------------------------------ #
    def _monitor_settings(self) -> None:
        if hasattr(self.svc, "get_monitor_config"):
            import json
            r = self.svc.get_monitor_config()
            self._status(json.dumps(r, indent=2, default=str)[:500] or "Monitor settings loaded.")
        else:
            self._status("Monitor settings not available.")

    def _notification_settings(self) -> None:
        """Edit notification channels (Teams + SMTP email), mirroring the Tk
        Monitor Settings notifications group. Config keys persist via
        set_notification_config; secrets via set_notification_secret (write-only)."""
        if not hasattr(self.svc, "get_notification_config"):
            self._status("Notification settings not available.")
            return
        from common.ui.textual.screens.form_modal import FormModal

        cfg = self.svc.get_notification_config() or {}
        teams_set = " (configured)" if cfg.get("teams_webhook_url_set") else " (not set)"
        smtp_pw_set = " (configured)" if cfg.get("smtp_password_set") else " (not set)"
        fields = [
            {"name": "enabled", "label": "Enable alert notifications",
             "type": "checkbox", "value": bool(cfg.get("enabled"))},
            {"name": "min_severity", "label": "Minimum severity", "type": "select",
             "value": cfg.get("min_severity") or "WARNING",
             "options": ["INFO", "WARNING", "CRITICAL"]},
            {"name": "teams_enabled", "label": "Send to Microsoft Teams",
             "type": "checkbox", "value": bool(cfg.get("teams_enabled"))},
            {"name": "teams_webhook_url", "label": f"Teams webhook URL{teams_set}",
             "type": "password", "value": ""},
            {"name": "email_enabled", "label": "Send email alerts",
             "type": "checkbox", "value": bool(cfg.get("email_enabled"))},
            {"name": "smtp_host", "label": "SMTP host", "value": cfg.get("smtp_host") or ""},
            {"name": "smtp_port", "label": "SMTP port",
             "value": str(cfg.get("smtp_port") or 587)},
            {"name": "smtp_use_tls", "label": "Use STARTTLS",
             "type": "checkbox", "value": cfg.get("smtp_use_tls") is not False},
            {"name": "smtp_username", "label": "SMTP username",
             "value": cfg.get("smtp_username") or ""},
            {"name": "smtp_password", "label": f"SMTP password{smtp_pw_set}",
             "type": "password", "value": ""},
            {"name": "email_from", "label": "From address", "value": cfg.get("email_from") or ""},
            {"name": "email_to", "label": "Recipient(s), comma-separated",
             "value": cfg.get("email_to") or ""},
        ]
        secret_keys = {"teams_webhook_url", "smtp_password"}
        bool_keys = {"enabled", "teams_enabled", "email_enabled", "smtp_use_tls"}

        def _done(v: dict | None) -> None:
            if not v:
                return
            errors = []
            for key, raw in v.items():
                if key in secret_keys:
                    continue
                value = ("true" if raw else "false") if key in bool_keys else str(raw)
                r = self.svc.set_notification_config(key, value)
                if not r.get("ok"):
                    errors.append(r.get("message", f"{key} failed"))
            for key in secret_keys:
                if v.get(key) and hasattr(self.svc, "set_notification_secret"):
                    r = self.svc.set_notification_secret(key, v[key])
                    if not r.get("ok"):
                        errors.append(r.get("message", f"{key} failed"))
            self._status("; ".join(errors) if errors else "Notification settings saved.")

        self.app.push_screen(
            FormModal("Notification settings", fields, submit_label="Save notifications"),
            _done,
        )

    def _reload_alerts(self) -> None:
        if not hasattr(self.svc, "list_alerts"):
            self._status("Monitor module not available.")
            return
        r = self.svc.list_alerts(limit=50)
        self._render_alerts(r.get("alerts") or [])
        self._status(f"{r.get('total', len(r.get('alerts') or []))} alerts")

    def _load_thresholds(self) -> None:
        if not hasattr(self.svc, "list_thresholds"):
            self._status("Monitor module not available.")
            return
        r = self.svc.list_thresholds(enabled_only=False)
        rules = r if isinstance(r, list) else (r.get("thresholds") or r.get("rules") or [])
        self._threshold_rules = [t for t in rules if isinstance(t, dict)]
        table = self.query_one("#mon-thr-table", DataTable)
        table.clear(columns=True)
        table.add_columns("source", "metric", "warning", "critical", "enabled")
        for t in self._threshold_rules:
            table.add_row(
                str(t.get("source") or t.get("api") or ""),
                str(t.get("metric") or t.get("name") or ""),
                str(t.get("warning", t.get("warn", ""))),
                str(t.get("critical", t.get("crit", ""))),
                "yes" if t.get("enabled", True) else "no")
        self._status(f"{len(self._threshold_rules)} threshold rule(s).")

    def _selected_threshold(self) -> dict | None:
        if not self._threshold_rules:
            self._status("Load thresholds first.")
            return None
        table = self.query_one("#mon-thr-table", DataTable)
        row = table.cursor_row
        if row is None or row < 0 or row >= len(self._threshold_rules):
            self._status("Select a threshold row first.")
            return None
        return self._threshold_rules[row]

    def _edit_threshold(self) -> None:
        t = self._selected_threshold()
        if not t:
            return
        if not hasattr(self.svc, "update_threshold"):
            self._status("Threshold editing is not available.")
            return
        from common.ui.textual.screens.form_modal import FormModal

        fields = [
            {"name": "warning", "label": "Warning", "value": str(t.get("warning", ""))},
            {"name": "critical", "label": "Critical", "value": str(t.get("critical", ""))},
            {"name": "info", "label": "Info", "value": str(t.get("info", ""))},
            {"name": "operator", "label": "Operator", "type": "select",
             "value": t.get("operator", ">="), "options": [">=", ">", "<=", "<", "==", "!="]},
            {"name": "window", "label": "Window", "value": str(t.get("window", ""))},
            {"name": "enabled", "label": "Enabled", "type": "checkbox",
             "value": t.get("enabled", True)},
            {"name": "description", "label": "Description",
             "value": str(t.get("description", ""))},
        ]

        def _done(v: dict | None) -> None:
            if not v:
                return
            changes = {"enabled": bool(v.get("enabled"))}
            for key in ("warning", "critical", "info"):
                if str(v.get(key, "")).strip():
                    changes[key] = float(v[key])
            for key in ("operator", "window", "description"):
                if str(v.get(key, "")).strip():
                    changes[key] = v[key]
            r = self.svc.update_threshold(
                t.get("source") or t.get("api") or "",
                t.get("metric") or t.get("name") or "",
                changes,
                path=t.get("path") or None,
            )
            self._status(r.get("message", "Threshold updated." if r.get("ok") else "Update failed."))
            if r.get("ok"):
                self._load_thresholds()

        self.app.push_screen(FormModal("Edit threshold", fields,
                                       submit_label="Save threshold"), _done)

    def _check_threshold(self) -> None:
        t = self._selected_threshold()
        if not t:
            return
        if not hasattr(self.svc, "check_threshold"):
            self._status("Threshold checks are not available.")
            return
        from common.ui.textual.screens.form_modal import FormModal

        fields = [
            {"name": "value", "label": "Metric value"},
            {"name": "instance", "label": "Instance", "value": "manual"},
        ]

        def _done(v: dict | None) -> None:
            if not v:
                return
            try:
                value = float(v.get("value", ""))
            except ValueError:
                self._status("Metric value must be numeric.")
                return
            alerts = self.svc.check_threshold(
                t.get("source") or t.get("api") or "",
                t.get("metric") or t.get("name") or "",
                value,
                instance_id=v.get("instance") or "manual",
                path=t.get("path") or None,
            )
            self._render_alerts(alerts)
            self._status(f"{len(alerts)} threshold alert(s).")

        self.app.push_screen(FormModal("Check threshold", fields,
                                       submit_label="Check value"), _done)
