"""AlertsMixin — ServerMonitorUI mixin."""

from __future__ import annotations

from common.ui.tk.monitor.server_monitor.mixins._shared import *  # noqa: F403

class AlertsMixin:
    def _alert_is_cloud(self, message: str) -> bool:
        return any(name in message for name in self.active_cloud_databases)

    def _alert_origin(self, entry: dict) -> str:
        """Resolve the pane an alert entry belongs to.

        Prefers the explicit ``origin`` recorded when the alert fired; falls
        back to the legacy message-substring heuristic for entries logged
        before origins were tracked.
        """
        origin = entry.get("origin")
        if origin in ("os", "db", "cloud"):
            return origin
        msg = entry.get("message", "")
        if "[OS]" in msg.upper():
            return "os"
        if self._alert_is_cloud(msg):
            return "cloud"
        return "db"

    def _refresh_alert_badges(self) -> None:
        """Update the Alerts button text with unread count badges."""


        try:
            os_lbl = f"Alerts" + (f" ({self._alert_unread_os})" if self._alert_unread_os else "")
            db_lbl = f"Alerts" + (f" ({self._alert_unread_db})" if self._alert_unread_db else "")
            cloud_lbl = f"Alerts" + (
                f" ({self._alert_unread_cloud})" if self._alert_unread_cloud else ""
            )
            if hasattr(self, "_btn_alerts_os"):
                self._btn_alerts_os.config(text=os_lbl)
            if hasattr(self, "_btn_alerts_db"):
                self._btn_alerts_db.config(text=db_lbl)
            if hasattr(self, "_btn_alerts_cloud"):
                self._btn_alerts_cloud.config(text=cloud_lbl)
        except Exception:
            pass

    def _show_alerts_window(self, source_filter: str) -> None:
        """Open a coloured alert log window for a given pane (os / db / cloud / all)."""
        # Reset unread counter for this pane (under lock for consistency with _fire_alerts)
        with self._alert_counter_lock:
            if source_filter == "os":
                self._alert_unread_os = 0
            elif source_filter == "cloud":
                self._alert_unread_cloud = 0
            elif source_filter == "db":
                self._alert_unread_db = 0
        self._refresh_alert_badges()

        win = tk.Toplevel(self.root)
        win.title(f"Alert Log - {source_filter.upper() if source_filter != 'all' else 'All'}")
        win.geometry("820x480")
        win.resizable(True, True)
        win.transient(self.root)

        # ── toolbar ──────────────────────────────────────────────────────────
        toolbar = ttk.Frame(win)
        toolbar.pack(fill=tk.X, padx=8, pady=(6, 2))
        ttk.Label(toolbar, text="Severity:").pack(side=tk.LEFT)
        filter_var = tk.StringVar(value="ALL")
        for lbl in ("ALL", CRITICAL, WARNING, INFO):
            ttk.Radiobutton(
                toolbar, text=lbl, variable=filter_var, value=lbl,
                command=lambda: _populate(),
            ).pack(side=tk.LEFT, padx=4)
        ttk.Button(toolbar, text="Clear All", command=lambda: _clear()).pack(side=tk.RIGHT, padx=4)

        # ── scrolled text ─────────────────────────────────────────────────────
        txt_frame = ttk.Frame(win)
        txt_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(2, 8))
        vsb = ttk.Scrollbar(txt_frame, orient=tk.VERTICAL)
        txt = tk.Text(
            txt_frame,
            yscrollcommand=vsb.set,
            wrap=tk.WORD,
            state=tk.DISABLED,
            font=("Courier", 10),
            bg="#1E1E1E",
            fg="#FFFFFF",
            relief=tk.FLAT,
            padx=6, pady=4,
        )
        vsb.config(command=txt.yview)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Configure colour tags
        txt.tag_configure("CRITICAL", foreground=self._ALERT_COLOURS[CRITICAL], font=("Courier", 10, "bold"))
        txt.tag_configure("WARNING",  foreground=self._ALERT_COLOURS[WARNING])
        txt.tag_configure("INFO",     foreground=self._ALERT_COLOURS[INFO])
        txt.tag_configure("time",     foreground="#888888")

        def _populate():
            sev_filter = filter_var.get()
            # Filter entries relevant to this pane
            entries = [
                e for e in self._alert_log
                if (
                    source_filter == "all"
                    or self._alert_origin(e) == source_filter
                )
                and (sev_filter == "ALL" or e["severity"] == sev_filter)
            ]
            txt.config(state=tk.NORMAL)
            txt.delete("1.0", tk.END)
            if not entries:
                txt.insert(tk.END, "No alerts recorded yet.")
            else:
                for e in reversed(entries):   # newest first
                    txt.insert(tk.END, f"[{e['time']}] ", "time")
                    txt.insert(tk.END, f"[{e['severity']}] ", e["severity"])
                    txt.insert(tk.END, f"{e['message']}\n")
            txt.config(state=tk.DISABLED)

        def _clear():
            # Remove entries belonging to this pane (keep the rest).
            to_keep = [
                e for e in self._alert_log
                if source_filter != "all" and self._alert_origin(e) != source_filter
            ]
            self._alert_log.clear()
            for e in to_keep:
                self._alert_log.append(e)
            _populate()

        _populate()

