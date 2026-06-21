import tkinter as tk
from tkinter import ttk, scrolledtext
import threading

from monitoring.monitor_connection_manager import MonitorConnectionManager
from monitoring.monitor_db_connection_manager import MonitorDBConnectionManager
from common.cloud.connection_manager import CloudConnectionManager
from common.ui.tk.monitor.metrics_visualizer import MetricsVisualizer
from common.ui.shared import specs as shared_specs
from common.ui.tk import bind_canvas_mousewheel, default_ui_font
from common.config_loader import console_print
from monitoring import monitor_config
from monitoring.threshold_checker import ThresholdChecker

# Re-exports — patched at this module path by tests (test_get_db_metrics_gating,
# test_server_monitor_ui_helpers). Keep here even though not referenced directly.
from monitoring.cloud_provider_registry import CloudProviderRegistry  # noqa: F401
from monitoring.db_metric_config import collect_metrics as _collect_db_metrics  # noqa: F401

from common.ui.tk.monitor.server_monitor.mixins.ssh_monitor_mixin import SSHMonitorMixin
from common.ui.tk.monitor.server_monitor.mixins.db_monitor_mixin import DBMonitorMixin
from common.ui.tk.monitor.server_monitor.mixins.panels_mixin import MonitorPanelsMixin
from common.ui.tk.monitor.server_monitor.mixins.metric_helpers_mixin import MetricHelpersMixin
from common.ui.tk.monitor.server_monitor.mixins.metrics_loop_mixin import MetricsLoopMixin
from common.ui.tk.monitor.server_monitor.mixins.cloud_monitor_mixin import CloudMonitorMixin
from common.ui.tk.monitor.server_monitor.mixins.alerts_mixin import AlertsMixin
from common.ui.tk.monitor.server_monitor.mixins.keepalive_mixin import KeepaliveMixin


class ServerMonitorUI(SSHMonitorMixin, DBMonitorMixin, MonitorPanelsMixin, MetricHelpersMixin, MetricsLoopMixin, CloudMonitorMixin, AlertsMixin, KeepaliveMixin):
    def __init__(
        self,
        parent_frame,
        root,
        connection_manager,
        active_connections,
        update_status_callback,
        theme,
    ):
        """
        Initialize Monitor UI

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
        self.ui_font = default_ui_font()

        # Initialize monitor connection manager (SSH/host targets)
        self.monitor_connection_manager = MonitorConnectionManager()

        # Monitor-tab-only DB connection store. Isolated from the core
        # Connections-tab store (db.json) so DB profiles added from the Monitor
        # tab are NOT visible to the SQL Editor / Data Migration / AI Query
        # tabs. The Monitor tab still reads the core ``connection_manager`` too.
        self.monitor_db_connection_manager = MonitorDBConnectionManager()

        # Cloud connection persistence
        self.cloud_connection_manager = CloudConnectionManager()

        # Threshold checker — reload() is called explicitly on every Refresh button click,
        # so reload_on_check=False avoids an extra file read on every alert evaluation tick.
        try:
            self._threshold_checker = ThresholdChecker(reload_on_check=False)
        except Exception as _tc_err:
            console_print(f"[Monitor] Threshold checker unavailable: {_tc_err}")
            self._threshold_checker = None

        # SSH configuration from the module-owned monitor_config.ini
        self.ssh_timeout = monitor_config.get_int("ssh.connection", "ssh_timeout", default=30)
        self.ssh_test_timeout = monitor_config.get_int(
            "ssh.connection", "ssh_test_timeout", default=5
        )
        self.ssh_control_persist = monitor_config.get_int(
            "ssh.connection", "ssh_control_persist", default=600
        )

        # Monitoring refresh interval
        self.refresh_interval = monitor_config.get_int(
            "monitoring", "metrics_refresh_interval", default=5000
        )

        # Monitoring state
        self.monitor_connections = (
            {}
        )  # {name: {'host': ..., 'username': ..., 'monitoring': bool}}
        self.servers_pending_removal = set()
        self.active_server_query_threads = {}

        # Database monitoring state
        self.monitored_databases = {}  # {db_name: db_manager}
        self.databases_pending_removal = set()
        self.active_db_query_threads = {}
        self._db_metrics_cache = {}

        # Cloud database monitoring state
        self.cloud_databases = (
            self.cloud_connection_manager.load_cloud_databases()
        )  # registry: all saved cloud connections
        self.active_cloud_databases = {}  # monitoring list: only selected/active ones
        self.active_cloud_monitors = (
            {}
        )  # {display_name: AWSMonitor/AzureMonitor/GCPMonitor}
        self._cloud_metrics_cache = (
            {}
        )  # {display_name: {'text': str, 'timestamp': str}}

        # Keepalive config (seconds)
        self._cloud_keepalive_interval = monitor_config.get_int(
            "monitoring", "cloud_keepalive_interval", default=300
        )
        self._cloud_keepalive_active = False
        self._db_keepalive_interval = monitor_config.get_int(
            "monitoring", "db_keepalive_interval", default=120
        )
        self._db_keepalive_skip_if_polled_within = monitor_config.get_int(
            "monitoring", "db_keepalive_skip_if_polled_within", default=60
        )
        self._db_keepalive_active = False
        self._ssh_keepalive_interval = monitor_config.get_int(
            "monitoring", "ssh_keepalive_interval", default=240
        )
        self._ssh_keepalive_active = False
        self._db_metric_skip_ping_if_used_within = monitor_config.get_int(
            "monitoring", "db_metric_skip_ping_if_used_within", default=0
        )
        self._cloud_health_skip_if_used_within = monitor_config.get_int(
            "monitoring", "cloud_health_skip_if_used_within", default=0
        )
        self._ssh_keepalive_skip_if_used_within = monitor_config.get_int(
            "monitoring", "ssh_keepalive_skip_if_used_within", default=0
        )
        self._cloud_force_refresh_interval = monitor_config.get_int(
            "monitoring", "cloud_force_refresh_interval", default=1800
        )
        self._cloud_wizard_opts: dict = {}

        # Alert log — shared across all sources, max 500 entries
        # Each entry: {'time': str, 'severity': str, 'source': str, 'message': str}
        import collections
        self._alert_log: collections.deque = collections.deque(maxlen=500)
        # Unread counts per pane for badge display.
        # _alert_counter_lock serialises all read-modify-write operations on these
        # integers because += is not atomic across a thread context-switch.
        self._alert_unread_os: int = 0
        self._alert_unread_db: int = 0
        self._alert_unread_cloud: int = 0
        self._alert_counter_lock = threading.Lock()

        # Per-db structured metric sections (populated by collect_metrics → get_db_metrics)
        self._db_sections_cache: dict[str, list] = {}
        self._db_os_note_cache: dict[str, str] = {}
        self._db_last_metric_at: dict[str, float] = {}
        self._ssh_last_cmd_ok_at: dict[str, float] = {}
        self._cloud_last_ok_at: dict[str, float] = {}
        self._cloud_consecutive_failures: dict[str, int] = {}
        self._cloud_needs_refresh: dict[str, bool] = {}

        # View modes
        self.os_view_mode = "text"  # 'text' or 'graph'
        self.db_view_mode = "text"  # 'text' or 'graph'
        self.cloud_view_mode = "text"  # 'text' or 'graph'

        # Update job
        self.monitor_update_job = None

        # Per-database query locks — one Lock per monitored DB so independent
        # databases can be queried concurrently instead of serialising through a
        # single shared lock.  Keys are db_name strings, locks are created lazily.
        self._db_locks: dict[str, threading.Lock] = {}
        self._db_locks_meta = threading.Lock()  # guards _db_locks dict mutations

        # UI widgets (initialized in create_ui)
        self.monitor_conn_listbox: tk.Listbox
        self.monitored_db_listbox: tk.Listbox
        self.cloud_db_listbox: tk.Listbox
        self.monitor_status_label: ttk.Label
        self.os_metrics_text: scrolledtext.ScrolledText
        self.os_view_container: ttk.Frame
        self.os_metrics_visualizer: MetricsVisualizer
        self.db_metrics_text: scrolledtext.ScrolledText
        self.db_view_container: ttk.Frame
        self.db_metrics_visualizer: MetricsVisualizer
        self.cloud_metrics_text: scrolledtext.ScrolledText
        self.cloud_view_container: ttk.Frame
        self.cloud_metrics_visualizer: MetricsVisualizer

        self._monitor_rows_paned: ttk.PanedWindow | None = None
        self._monitor_row_collapsed: list[bool] = [False, False, False]
        self._monitor_display_frames: list[ttk.Frame] = []
        self._monitor_left_content_frames: list[ttk.Frame] = []
        self._monitor_collapse_btns: list[ttk.Button] = []
        self._monitor_pane_saved_sash: list[tuple[int, int] | None] = [None, None, None]

    _MONITOR_SECTION_TITLE_FONT = ("Arial", 12, "bold")

    def _add_monitor_pane(
        self,
        row_idx: int,
        left_title: str,
        right_title: str,
    ) -> dict:
        """Add one resizable coupled row to the vertical PanedWindow."""
        pane = ttk.Frame(self._monitor_rows_paned)
        self._monitor_rows_paned.add(pane, weight=1)
        pane.columnconfigure(0, weight=1)
        pane.columnconfigure(1, weight=3)
        pane.rowconfigure(0, weight=1)

        left = ttk.LabelFrame(
            pane,
            text=left_title,
            padding="10",
            style="Monitor.TLabelframe",
        )
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 6))

        right = ttk.LabelFrame(
            pane,
            text=right_title,
            padding="10",
            style="Monitor.TLabelframe",
        )
        right.grid(row=0, column=1, sticky="nsew")

        left_content = ttk.Frame(left)
        left_content.pack(fill=tk.BOTH, expand=True)

        right_toolbar = ttk.Frame(right)
        right_toolbar.pack(fill=tk.X, pady=(0, 5))

        display = ttk.Frame(right)
        display.pack(fill=tk.BOTH, expand=True)

        collapse_btn = ttk.Button(
            right_toolbar,
            text="Collapse",
            width=10,
            command=lambda idx=row_idx: self._toggle_monitor_display(idx),
        )
        collapse_btn.pack(side=tk.RIGHT, padx=(4, 0))

        self._monitor_display_frames.append(display)
        self._monitor_left_content_frames.append(left_content)
        self._monitor_collapse_btns.append(collapse_btn)

        return {
            "left": left_content,
            "right_toolbar": right_toolbar,
            "display": display,
        }

    def _toggle_monitor_display(self, row_idx: int) -> None:
        """Collapse or expand a whole monitor row, leaving only the button row."""
        collapsed = not self._monitor_row_collapsed[row_idx]
        self._monitor_row_collapsed[row_idx] = collapsed
        display = self._monitor_display_frames[row_idx]
        left_content = self._monitor_left_content_frames[row_idx]
        btn = self._monitor_collapse_btns[row_idx]
        if collapsed:
            display.pack_forget()
            left_content.pack_forget()
            btn.config(text="Expand")
        else:
            display.pack(fill=tk.BOTH, expand=True)
            left_content.pack(fill=tk.BOTH, expand=True)
            btn.config(text="Collapse")
        self.root.after_idle(lambda idx=row_idx: self._rebalance_monitor_pane(idx))

    def _rebalance_monitor_pane(self, row_idx: int) -> None:
        """Shrink or restore vertical pane size when display is collapsed."""
        paned = self._monitor_rows_paned
        if paned is None:
            return
        try:
            panes = paned.panes()
        except tk.TclError:
            return
        if row_idx >= len(panes):
            return

        total_h = paned.winfo_height()
        if total_h <= 1:
            return

        # Collapse down to just the button-row toolbar: measure the right
        # LabelFrame's required height now that its display area is hidden
        # (toolbar height + LabelFrame title + padding).
        min_collapsed = 60
        try:
            right_lf = self._monitor_collapse_btns[row_idx].master.master
            right_lf.update_idletasks()
            req = right_lf.winfo_reqheight()
            if req > 1:
                min_collapsed = req + 6
        except Exception:
            pass
        sash_count = len(panes) - 1

        if self._monitor_row_collapsed[row_idx]:
            saved = []
            for i in range(sash_count):
                try:
                    saved.append(paned.sashpos(i))
                except tk.TclError:
                    saved.append(0)
            self._monitor_pane_saved_sash[row_idx] = tuple(saved)

            if row_idx == 0:
                if sash_count >= 1:
                    paned.sashpos(0, min(min_collapsed, total_h // 3))
            elif row_idx == len(panes) - 1:
                if sash_count >= 1:
                    paned.sashpos(sash_count - 1, max(min_collapsed, total_h - min_collapsed))
            else:
                top = paned.sashpos(row_idx - 1) if row_idx > 0 else 0
                if row_idx < sash_count:
                    paned.sashpos(row_idx, min(top + min_collapsed, total_h - min_collapsed))
            return

        saved = self._monitor_pane_saved_sash[row_idx]
        if saved:
            for i, pos in enumerate(saved):
                if i < sash_count:
                    try:
                        paned.sashpos(i, pos)
                    except tk.TclError:
                        pass
            self._monitor_pane_saved_sash[row_idx] = None

    def open_monitor_settings(self):
        """Open monitor_config.ini editor (refresh, keepalive, SSH, notifications)."""
        from common.ui.tk.monitor.monitor_settings_ui import open_monitor_settings

        open_monitor_settings(self.root, on_change=self._on_monitor_config_saved)

    def _on_monitor_config_saved(self):
        """Reload intervals from monitor_config.ini after a settings save."""
        from monitoring import monitor_config

        monitor_config.reload()
        self.refresh_interval = monitor_config.get_int(
            "monitoring", "metrics_refresh_interval", default=5000
        )
        self._cloud_keepalive_interval = monitor_config.get_int(
            "monitoring", "cloud_keepalive_interval", default=300
        )
        self._db_keepalive_interval = monitor_config.get_int(
            "monitoring", "db_keepalive_interval", default=120
        )
        self._ssh_keepalive_interval = monitor_config.get_int(
            "monitoring", "ssh_keepalive_interval", default=240
        )
        self._cloud_force_refresh_interval = monitor_config.get_int(
            "monitoring", "cloud_force_refresh_interval", default=1800
        )

    def open_threshold_settings(self):
        """Open the alert-threshold editor (monitor_thresholds.ini)."""
        from common.ui.tk.monitor.server_monitor.threshold_editor import open_threshold_editor

        def _reload():
            if self._threshold_checker:
                try:
                    self._threshold_checker.reload()
                except Exception:
                    pass

        open_threshold_editor(self.root, self._threshold_checker, on_change=_reload)

    def create_ui(self):
        """Create the complete Monitor UI"""
        """Create UI for Monitoring tab"""
        # Section titles, target-action labels and the view toolbar are
        # single-sourced from the shared spec so Tk, TUI and Web stay in sync.
        spec = shared_specs.monitoring_payload()
        top_labels = {a["id"]: a["label"] for a in spec["topActions"]}
        section_specs = {s["id"]: s for s in spec["sections"]}
        view_labels = {a["id"]: a["label"] for a in spec["viewActions"]}

        def _ta(section_id: str, action_id: str) -> str:
            for action in section_specs.get(section_id, {}).get("targetActions", []):
                if action["id"] == action_id:
                    return action["label"]
            return ""

        outer = ttk.Frame(self.parent)
        outer.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        status_frame = ttk.Frame(outer)
        status_frame.pack(fill=tk.X, pady=(0, 5))

        self.monitor_status_label = ttk.Label(
            status_frame, text="Status: No active monitoring", foreground="gray"
        )
        self.monitor_status_label.pack(side=tk.LEFT, padx=5)

        # Top-right: module settings + threshold editor + live badge.
        self._btn_monitor_settings = ttk.Button(
            status_frame, text=top_labels["settings"],
            command=self.open_monitor_settings,
        )
        self._btn_monitor_settings.pack(side=tk.RIGHT, padx=(6, 0))
        self._btn_threshold_settings = ttk.Button(
            status_frame, text=top_labels["thresholds_settings"],
            command=self.open_threshold_settings,
        )
        self._btn_threshold_settings.pack(side=tk.RIGHT, padx=(6, 0))
        self.monitor_resources_label = ttk.Label(
            status_frame, text="Monitoring 0 resources", foreground="#1565c0",
        )
        self.monitor_resources_label.pack(side=tk.RIGHT, padx=6)

        section_style = ttk.Style()
        section_style.configure(
            "Monitor.TLabelframe.Label", font=self._MONITOR_SECTION_TITLE_FONT
        )

        self._monitor_rows_paned = ttk.PanedWindow(outer, orient=tk.VERTICAL)
        self._monitor_rows_paned.pack(fill=tk.BOTH, expand=True)
        self._monitor_row_collapsed = [False, False, False]
        self._monitor_display_frames = []
        self._monitor_left_content_frames = []
        self._monitor_collapse_btns = []
        self._monitor_pane_saved_sash = [None, None, None]

        server_row = self._add_monitor_pane(
            0,
            section_specs["server"]["title"],
            section_specs["server"]["metricsTitle"],
        )
        db_row = self._add_monitor_pane(
            1,
            section_specs["database"]["title"],
            section_specs["database"]["metricsTitle"],
        )
        cloud_row = self._add_monitor_pane(
            2,
            section_specs["cloud"]["title"],
            section_specs["cloud"]["metricsTitle"],
        )

        # ── Row 0: Server / SSH controls (buttons on top, always visible) ─────
        btn_frame = ttk.Frame(server_row["left"])
        btn_frame.pack(fill=tk.X, pady=(0, 8))

        ttk.Button(
            btn_frame,
            text=_ta("server", "add"),
            command=self.add_monitor_connection,
            width=20,
        ).pack(pady=2, fill=tk.X)
        ttk.Button(
            btn_frame,
            text=_ta("server", "select"),
            command=self.select_server_to_monitor,
            width=20,
            style="Success.TButton",
        ).pack(pady=2, fill=tk.X)
        ttk.Button(
            btn_frame,
            text=_ta("server", "remove"),
            command=self.remove_monitor_connection,
            width=20,
            style="Warning.TButton",
        ).pack(pady=2, fill=tk.X)

        list_frame = ttk.Frame(server_row["left"])
        list_frame.pack(fill=tk.BOTH, expand=True)

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
            highlightbackground=self.theme.BORDER,
        )
        scrollbar.config(command=self.monitor_conn_listbox.yview)

        self.monitor_conn_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.monitor_conn_listbox.bind(
            "<<ListboxSelect>>", self.on_monitor_connection_selected
        )

        # ── Row 1: Database controls (buttons on top) ─────────────────────────
        db_btn_frame = ttk.Frame(db_row["left"])
        db_btn_frame.pack(fill=tk.X, pady=(0, 8))

        # "Add Database" opens the connection form, which already lets the user
        # choose a localhost/direct DB or a remote DB reached over an SSH tunnel.
        ttk.Button(
            db_btn_frame,
            text=_ta("database", "add"),
            command=self.add_monitor_db_connection,
            width=20,
        ).pack(pady=2, fill=tk.X)
        ttk.Button(
            db_btn_frame,
            text=_ta("database", "select"),
            command=self.add_db_to_monitor,
            width=20,
            style="Success.TButton",
        ).pack(pady=2, fill=tk.X)
        ttk.Button(
            db_btn_frame,
            text=_ta("database", "remove"),
            command=self.remove_db_from_monitor,
            width=20,
            style="Warning.TButton",
        ).pack(pady=2, fill=tk.X)

        db_list_frame = ttk.Frame(db_row["left"])
        db_list_frame.pack(fill=tk.BOTH, expand=True)

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
            highlightbackground=self.theme.BORDER,
        )
        db_scrollbar.config(command=self.monitored_db_listbox.yview)

        self.monitored_db_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        db_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # ── Row 2: Cloud controls (buttons on top) ────────────────────────────
        cloud_db_btn_frame = ttk.Frame(cloud_row["left"])
        cloud_db_btn_frame.pack(fill=tk.X, pady=(0, 8))

        ttk.Button(
            cloud_db_btn_frame,
            text=_ta("cloud", "add"),
            command=self.add_cloud_database,
            width=20,
        ).pack(pady=2, fill=tk.X)
        ttk.Button(
            cloud_db_btn_frame,
            text=_ta("cloud", "select"),
            command=self.select_cloud_database,
            width=20,
            style="Success.TButton",
        ).pack(pady=2, fill=tk.X)
        ttk.Button(
            cloud_db_btn_frame,
            text=_ta("cloud", "remove"),
            command=self.remove_cloud_database,
            width=20,
            style="Warning.TButton",
        ).pack(pady=2, fill=tk.X)

        cloud_db_list_frame = ttk.Frame(cloud_row["left"])
        cloud_db_list_frame.pack(fill=tk.BOTH, expand=True)

        cloud_db_scrollbar = ttk.Scrollbar(cloud_db_list_frame, orient=tk.VERTICAL)
        self.cloud_db_listbox = tk.Listbox(
            cloud_db_list_frame,
            yscrollcommand=cloud_db_scrollbar.set,
            bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_PRIMARY,
            selectbackground=self.theme.PRIMARY,
            selectforeground="white",
            relief=tk.FLAT,
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=self.theme.BORDER,
        )
        cloud_db_scrollbar.config(command=self.cloud_db_listbox.yview)

        self.cloud_db_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        cloud_db_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # ── Row 0: OS metrics ──────────────────────────────────────────────────
        os_frame = server_row["display"]
        os_control_frame = server_row["right_toolbar"]

        ttk.Button(
            os_control_frame,
            text=view_labels["show_graphs"],
            command=lambda: self.toggle_os_view("graph"),
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            os_control_frame,
            text=view_labels["show_text"],
            command=lambda: self.toggle_os_view("text"),
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            os_control_frame, text=view_labels["clear_graphs"], command=self.clear_os_graphs
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            os_control_frame, text=view_labels["refresh"], command=self.refresh_server_metrics
        ).pack(side=tk.LEFT, padx=2)
        self._btn_alerts_os = ttk.Button(
            os_control_frame,
            text=view_labels["alerts"],
            command=lambda: self._show_alerts_window("os"),
            style="Warning.TButton",
        )
        self._btn_alerts_os.pack(side=tk.RIGHT, padx=2)

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
            borderwidth=1,
        )
        self.os_metrics_text.pack(fill=tk.BOTH, expand=True)
        self.os_metrics_text.insert(
            1.0,
            "Select a monitoring connection and start monitoring to view OS resources...",
        )
        self.os_metrics_text.config(state=tk.DISABLED)

        # Graph view (hidden initially)
        self.os_metrics_visualizer = MetricsVisualizer(
            self.os_view_container, title="OS Metrics"
        )
        bind_canvas_mousewheel(self.os_metrics_visualizer.canvas)

        # ── Row 1: Database metrics ────────────────────────────────────────────
        db_frame = db_row["display"]
        db_view_control_frame = db_row["right_toolbar"]

        ttk.Button(
            db_view_control_frame,
            text=view_labels["show_graphs"],
            command=lambda: self.toggle_db_view("graph"),
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            db_view_control_frame,
            text=view_labels["show_text"],
            command=lambda: self.toggle_db_view("text"),
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            db_view_control_frame, text=view_labels["clear_graphs"], command=self.clear_db_graphs
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            db_view_control_frame, text=view_labels["refresh"], command=self.refresh_monitor_db_list
        ).pack(side=tk.LEFT, padx=2)
        self._btn_alerts_db = ttk.Button(
            db_view_control_frame,
            text=view_labels["alerts"],
            command=lambda: self._show_alerts_window("db"),
            style="Warning.TButton",
        )
        self._btn_alerts_db.pack(side=tk.RIGHT, padx=2)

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
            borderwidth=1,
        )
        self.db_metrics_text.pack(fill=tk.BOTH, expand=True)
        self.db_metrics_text.insert(
            1.0, "Click 'Select Database' to start monitoring databases..."
        )
        self.db_metrics_text.config(state=tk.DISABLED)

        # Graph view (hidden initially)
        self.db_metrics_visualizer = MetricsVisualizer(
            self.db_view_container, title="Database Metrics"
        )
        bind_canvas_mousewheel(self.db_metrics_visualizer.canvas)

        # ── Row 2: Cloud metrics ──────────────────────────────────────────────
        cloud_frame = cloud_row["display"]
        cloud_view_control_frame = cloud_row["right_toolbar"]

        ttk.Button(
            cloud_view_control_frame,
            text=view_labels["show_graphs"],
            command=lambda: self.toggle_cloud_view("graph"),
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            cloud_view_control_frame,
            text=view_labels["show_text"],
            command=lambda: self.toggle_cloud_view("text"),
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            cloud_view_control_frame, text=view_labels["clear_graphs"], command=self.clear_cloud_graphs
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            cloud_view_control_frame,
            text=view_labels["refresh"],
            command=self.refresh_cloud_metrics,
        ).pack(side=tk.LEFT, padx=2)
        self._btn_alerts_cloud = ttk.Button(
            cloud_view_control_frame,
            text=view_labels["alerts"],
            command=lambda: self._show_alerts_window("cloud"),
            style="Warning.TButton",
        )
        self._btn_alerts_cloud.pack(side=tk.RIGHT, padx=2)

        self.cloud_view_container = ttk.Frame(cloud_frame)
        self.cloud_view_container.pack(fill=tk.BOTH, expand=True)

        self.cloud_metrics_text = scrolledtext.ScrolledText(
            self.cloud_view_container,
            wrap=tk.WORD,
            font=("Courier", 10),
            bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_PRIMARY,
            insertbackground=self.theme.PRIMARY,
            relief=tk.FLAT,
            borderwidth=1,
        )
        self.cloud_metrics_text.pack(fill=tk.BOTH, expand=True)
        self.cloud_metrics_text.insert(
            1.0,
            "Click 'Select Resource' to start monitoring cloud resources...",
        )
        self.cloud_metrics_text.config(state=tk.DISABLED)

        self.cloud_metrics_visualizer = MetricsVisualizer(
            self.cloud_view_container, title="Cloud Metrics"
        )
        bind_canvas_mousewheel(self.cloud_metrics_visualizer.canvas)

        # Clear any stale graphs from previous sessions (in case monitoring tab is recreated)
        if not self.monitored_databases:
            self.db_metrics_visualizer.clear_all()
        if not self.active_cloud_databases:
            self.cloud_metrics_visualizer.clear_all()

        # Start periodic updates
        self.start_monitor_updates()

