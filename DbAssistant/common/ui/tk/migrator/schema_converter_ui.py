# ---------------------------------------------------------------------
# description: Schema conversion UI module for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

# Allow ``python schema_converter/schema_converter_ui.py`` from project root.
if __name__ == "__main__":
    import sys
    from pathlib import Path

    _root = Path(__file__).resolve().parents[1]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading

from common.database_registry import DatabaseRegistry
from schema_converter.ui_kit import (
    bind_canvas_mousewheel,
    create_horizontal_scrollable,
    make_scrollable,
)
from common.config_loader import config, console_print, get_compare_sample_size

from schema_converter import module_config
from schema_converter.converter import (
    SchemaConverter,
    DataConverter,
    ConversionValidator,
    SchemaComparer,
    DataComparer,
)


class SchemaConverterUI:
    """Schema Conversion UI Module - Database schema and data conversion"""

    def __init__(
        self,
        parent_frame,
        root,
        get_connections_callback,
        update_status_callback,
        theme,
        fonts,
    ):
        self.parent = parent_frame
        self.root = root
        self.get_connections = get_connections_callback
        self.update_status = update_status_callback
        self.theme = theme
        self.ui_font = fonts["ui"]
        self.ui_font_mono = fonts["mono"]

        self.conversion_stop_event = threading.Event()
        self.conversion_running = False

        # UI widgets (initialized in create_ui)
        self.source_conn_combo = None
        self.target_conn_combo = None
        self.table_checkboxes_frame = None
        self.table_checkbox_vars = {}
        self.target_prefix_entry = None
        self.target_suffix_entry = None
        self.create_indexes_var = None
        self.drop_if_exists_var = None
        self.batch_size_entry = None
        self.preview_schema_btn = None
        self.row_counts_btn = None
        self.sample_data_btn = None
        self.dump_schema_btn = None
        self.convert_schema_btn = None
        self.transfer_data_btn = None
        self.compare_data_btn = None
        self.stop_conversion_btn = None
        self.conversion_preview_text = None
        self.conversion_progress = None
        self.conversion_status_label = None

    @property
    def active_connections(self):
        return self.get_connections()

    def _open_migrator_settings(self):
        from common.ui.tk.migrator.migrator_settings_ui import open_migrator_settings

        open_migrator_settings(self.root)

    def create_ui(self):
        """Create UI for schema conversion tab"""
        # The tab is split into two stacked regions: a scrollable controls area on
        # top and the results pane on the bottom. The large results ScrolledText is
        # deliberately kept OUT of the outer scroll canvas — nesting a self-scrolling
        # widget inside a Canvas caused stale-pixel "smearing" on macOS (the status
        # bar text bleeding into the Conversion and Migration results window).
        self.parent.rowconfigure(0, weight=3)
        self.parent.rowconfigure(1, weight=2)
        self.parent.columnconfigure(0, weight=1)

        controls_container = ttk.Frame(self.parent)
        controls_container.grid(row=0, column=0, sticky="nsew")

        self._results_container = ttk.Frame(self.parent)
        self._results_container.grid(row=1, column=0, sticky="nsew")

        scrollable_frame = make_scrollable(controls_container)

        title_font = (self.ui_font[0], self.ui_font[1] + 3, "bold")
        # Title
        title_row = ttk.Frame(scrollable_frame)
        title_row.pack(fill=tk.X, padx=10, pady=10)
        title_label = ttk.Label(
            title_row,
            text="Data conversion and migration services",
            font=title_font,
        )
        title_label.pack(side=tk.LEFT)
        ttk.Button(
            title_row, text="\u2699 Migration Settings",
            command=self._open_migrator_settings,
        ).pack(side=tk.RIGHT)

        # Source (left) and Target (right) side by side
        connections_row = ttk.Frame(scrollable_frame)
        connections_row.pack(fill=tk.BOTH, expand=False, padx=10, pady=5)

        source_frame = ttk.LabelFrame(
            connections_row, text="Source Database", padding="10"
        )
        source_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        source_frame.columnconfigure(1, weight=1)

        ttk.Label(source_frame, text="Select Source Connection:").grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.source_conn_combo = ttk.Combobox(source_frame, width=28, state="readonly")
        self.source_conn_combo.grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        self.source_conn_combo.bind(
            "<<ComboboxSelected>>", self.on_source_connection_changed
        )

        ttk.Button(
            source_frame, text="Refresh", command=self.refresh_connections
        ).grid(row=0, column=2, padx=5, pady=5)

        ttk.Label(source_frame, text="Source database/schema:").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.source_database_combo = ttk.Combobox(source_frame, width=26)
        self.source_database_combo.grid(
            row=1, column=1, padx=5, pady=5, sticky=tk.EW
        )
        self.source_database_combo.bind(
            "<<ComboboxSelected>>", self.on_source_database_changed
        )

        ttk.Label(source_frame, text="Select Tables:").grid(
            row=2, column=0, sticky=tk.NW, padx=5, pady=5
        )

        # Table selection with checkboxes
        table_outer = ttk.Frame(source_frame)
        table_outer.grid(row=2, column=1, padx=5, pady=5, sticky=tk.EW)

        # Scrollable canvas for checkboxes
        table_canvas = tk.Canvas(
            table_outer,
            height=150,
            bg=self.theme.BG_SECONDARY,
            highlightthickness=1,
            highlightbackground="#ccc",
        )
        table_scroll = ttk.Scrollbar(
            table_outer, orient=tk.VERTICAL, command=table_canvas.yview
        )
        self.table_checkboxes_frame = ttk.Frame(table_canvas)

        # Debounced table canvas scrollregion update
        self._table_resize_timer = None

        def _update_table_scrollregion(event=None):
            if self._table_resize_timer:
                self.root.after_cancel(self._table_resize_timer)
            self._table_resize_timer = self.root.after(
                150,
                lambda: table_canvas.configure(scrollregion=table_canvas.bbox("all")),
            )

        self.table_checkboxes_frame.bind("<Configure>", _update_table_scrollregion)

        table_canvas.create_window(
            (0, 0), window=self.table_checkboxes_frame, anchor="nw"
        )
        table_canvas.configure(yscrollcommand=table_scroll.set)

        table_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        table_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Bind mousewheel to table canvas
        bind_canvas_mousewheel(table_canvas)

        # Store checkbox variables
        self.table_checkbox_vars = {}  # {table_name: BooleanVar}

        # Buttons for table selection
        table_btn_frame = ttk.Frame(source_frame)
        table_btn_frame.grid(row=2, column=2, padx=5, pady=5, sticky=tk.N)

        ttk.Button(
            table_btn_frame,
            text="Load Tables",
            command=self.load_source_tables,
            width=12,
        ).pack(pady=2)
        ttk.Button(
            table_btn_frame, text="Check All", command=self.check_all_tables, width=12
        ).pack(pady=2)
        ttk.Button(
            table_btn_frame,
            text="Uncheck All",
            command=self.uncheck_all_tables,
            width=12,
        ).pack(pady=2)

        target_frame = ttk.LabelFrame(
            connections_row, text="Target Database", padding="10"
        )
        target_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))
        target_frame.columnconfigure(1, weight=1)

        ttk.Label(target_frame, text="Select Target Connection:").grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.target_conn_combo = ttk.Combobox(target_frame, width=28, state="readonly")
        self.target_conn_combo.grid(row=0, column=1, columnspan=3, padx=5, pady=5, sticky=tk.EW)
        self.target_conn_combo.bind(
            "<<ComboboxSelected>>", self.on_target_connection_changed
        )

        ttk.Label(target_frame, text="Target database/schema:").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.target_database_combo = ttk.Combobox(target_frame, width=28)
        self.target_database_combo.grid(
            row=1, column=1, columnspan=3, padx=5, pady=5, sticky=tk.EW
        )
        ttk.Label(
            target_frame,
            text="Required for MySQL/MariaDB when the selected connection has no default database",
            foreground="gray",
            font=("Arial", 9),
        ).grid(row=2, column=0, columnspan=4, sticky=tk.W, padx=5, pady=(0, 5))

        # Table naming options
        ttk.Label(
            target_frame,
            text="Table Naming:",
            font=(self.ui_font[0], self.ui_font[1], "bold"),
        ).grid(row=3, column=0, sticky=tk.W, padx=5, pady=(10, 5))

        ttk.Label(target_frame, text="Prefix:").grid(
            row=4, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.target_prefix_entry = ttk.Entry(target_frame, width=15)
        self.target_prefix_entry.grid(row=4, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(
            target_frame, text="(optional)", foreground="gray", font=("Arial", 9)
        ).grid(row=4, column=2, sticky=tk.W, padx=5, pady=5)

        ttk.Label(target_frame, text="Suffix:").grid(
            row=5, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.target_suffix_entry = ttk.Entry(target_frame, width=15)
        self.target_suffix_entry.grid(row=5, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(
            target_frame, text="(optional)", foreground="gray", font=("Arial", 9)
        ).grid(row=5, column=2, sticky=tk.W, padx=5, pady=5)

        ttk.Label(
            target_frame,
            text="Example: If table is 'users' with prefix 'new_' and suffix '_bak', target will be 'new_users_bak'",
            foreground="blue",
            font=("Arial", 9),
        ).grid(row=6, column=0, columnspan=4, sticky=tk.W, padx=5, pady=(5, 5))

        # Options Frame
        options_frame = ttk.LabelFrame(
            scrollable_frame, text="Conversion and Migration options (optional)", padding="10"
        )
        options_frame.pack(fill=tk.X, padx=10, pady=5)

        self.create_indexes_var = tk.BooleanVar(value=True)
        self.drop_if_exists_var = tk.BooleanVar(value=False)

        ttk.Checkbutton(
            options_frame,
            text="Create Indexes (with schema)",
            variable=self.create_indexes_var,
        ).grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Checkbutton(
            options_frame,
            text="Drop Table If Exists (before schema conversion)",
            variable=self.drop_if_exists_var,
        ).grid(row=0, column=1, sticky=tk.W, padx=5, pady=2)

        ttk.Label(options_frame, text="Batch Size (for data transfer):").grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=5
        )
        self.batch_size_entry = ttk.Entry(options_frame, width=15)
        self.batch_size_entry.insert(
            0,
            str(
                config.get_int(
                    "database.performance", "transfer_batch_size", default=1000
                )
            ),
        )
        self.batch_size_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)

        self.parallel_transfer_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            options_frame,
            text="Run data transfer in parallel",
            variable=self.parallel_transfer_var,
        ).grid(row=2, column=0, sticky=tk.W, padx=5, pady=2)

        ttk.Label(options_frame, text="Parallel Workers:").grid(
            row=2, column=1, sticky=tk.E, padx=5, pady=5
        )
        self.parallel_workers_entry = ttk.Entry(options_frame, width=8)
        self.parallel_workers_entry.insert(
            0,
            str(
                module_config.get_int(
                    "schema.conversion", "parallel_workers", default=1
                )
            ),
        )
        self.parallel_workers_entry.grid(row=2, column=2, sticky=tk.W, padx=5, pady=5)

        ttk.Label(
            options_frame,
            text='Type mapping rules (e.g. "varchar2:text,int:decimal"):',
        ).grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        self.type_map_entry = ttk.Entry(options_frame, width=50)
        default_type_map = module_config.get(
            "schema.conversion", "type_overrides", default=""
        )
        if default_type_map:
            self.type_map_entry.insert(0, default_type_map)
        self.type_map_entry.grid(
            row=3, column=1, columnspan=2, sticky=tk.W, padx=5, pady=5
        )

        # --- Per-run transfer options (G1/G2/G9/G10) ------------------------
        # Row filter (WHERE) and column subset apply to a single table only, so
        # they are disabled automatically when more than one table is selected
        # (see _update_single_table_options_state). Column rename and the row
        # limit apply to every selected table. Fixed-value policies (continue-on-error,
        # overflow, null/bool, timezone, sequence reset) now live in the
        # ⚙ Migration Settings dialog (saved config defaults).
        self.where_label = ttk.Label(
            options_frame, text="Row filter (WHERE, single table):"
        )
        self.where_label.grid(row=4, column=0, sticky=tk.W, padx=5, pady=2)
        self.where_entry = ttk.Entry(options_frame, width=50)
        self.where_entry.grid(row=4, column=1, columnspan=2, sticky=tk.W, padx=5, pady=2)

        ttk.Label(options_frame, text="Row limit (per table):").grid(
            row=5, column=0, sticky=tk.W, padx=5, pady=2
        )
        self.limit_entry = ttk.Entry(options_frame, width=15)
        self.limit_entry.grid(row=5, column=1, sticky=tk.W, padx=5, pady=2)

        self.columns_label = ttk.Label(
            options_frame, text="Columns (subset, single table):"
        )
        self.columns_label.grid(row=6, column=0, sticky=tk.W, padx=5, pady=2)
        self.columns_entry = ttk.Entry(options_frame, width=50)
        self.columns_entry.grid(row=6, column=1, columnspan=2, sticky=tk.W, padx=5, pady=2)

        self.column_map_label = ttk.Label(
            options_frame, text='Column rename ("src:tgt,...", all tables):'
        )
        self.column_map_label.grid(row=7, column=0, sticky=tk.W, padx=5, pady=2)
        self.column_map_entry = ttk.Entry(options_frame, width=50)
        self.column_map_entry.grid(row=7, column=1, columnspan=2, sticky=tk.W, padx=5, pady=2)

        self.checkpoint_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            options_frame,
            text="Checkpoint / resume",
            variable=self.checkpoint_var,
        ).grid(row=8, column=0, sticky=tk.W, padx=5, pady=2)

        ttk.Label(options_frame, text="Report file:").grid(
            row=9, column=0, sticky=tk.W, padx=5, pady=2
        )
        self.report_path_entry = ttk.Entry(options_frame, width=50)
        self.report_path_entry.grid(
            row=9, column=1, columnspan=2, sticky=tk.W, padx=5, pady=2
        )

        ttk.Label(
            options_frame,
            text=(
                "Tip: error handling, overflow, NULL/boolean, timezone and "
                "sequence-reset policies are configured in \u2699 Migration Settings."
            ),
            foreground="gray",
            font=("Arial", 9),
            wraplength=560,
        ).grid(row=10, column=0, columnspan=3, sticky=tk.W, padx=5, pady=(6, 2))

        # Entries gated to single-table transfers. Column rename applies to all
        # selected tables, so it is intentionally not gated here.
        self._single_table_option_widgets = (
            "where_entry",
            "columns_entry",
        )
        self._single_table_option_labels = (
            "where_label",
            "columns_label",
        )
        self._update_single_table_options_state()

        # Action Buttons Frame (split into two rows) with horizontal scrolling (optimized)
        action_frame_outer = ttk.Frame(scrollable_frame)
        action_frame_outer.pack(fill=tk.X, padx=10, pady=10)
        action_frame = create_horizontal_scrollable(action_frame_outer)

        # First row - Analysis buttons
        row1_frame = ttk.Frame(action_frame)
        row1_frame.pack(fill=tk.X, pady=(0, 5))

        self.preview_schema_btn = ttk.Button(
            row1_frame,
            text="Preview Schema",
            command=self.preview_schema_conversion,
            width=16,
        )
        self.preview_schema_btn.pack(side=tk.LEFT, padx=5)

        self.row_counts_btn = ttk.Button(
            row1_frame, text="Row Counts", command=self.show_row_counts, width=14
        )
        self.row_counts_btn.pack(side=tk.LEFT, padx=5)

        self.sample_data_btn = ttk.Button(
            row1_frame, text="Sample Data", command=self.show_sample_data, width=14
        )
        self.sample_data_btn.pack(side=tk.LEFT, padx=5)

        self.dump_schema_btn = ttk.Button(
            row1_frame, text="Dump Schema", command=self.dump_schema_ddl, width=14
        )
        self.dump_schema_btn.pack(side=tk.LEFT, padx=5)

        self.validate_btn = ttk.Button(
            row1_frame,
            text="Validate (Dry-run)",
            command=self.validate_migration_dry_run,
            width=16,
        )
        self.validate_btn.pack(side=tk.LEFT, padx=5)

        ttk.Button(
            row1_frame,
            text="Clear Preview",
            command=self.clear_conversion_preview,
            width=14,
        ).pack(side=tk.LEFT, padx=5)

        # Second row - Conversion buttons
        row2_frame = ttk.Frame(action_frame)
        row2_frame.pack(fill=tk.X)

        self.convert_schema_btn = ttk.Button(
            row2_frame,
            text="Convert Schema",
            command=self.convert_schema_only,
            width=16,
        )
        self.convert_schema_btn.pack(side=tk.LEFT, padx=5)

        self.transfer_data_btn = ttk.Button(
            row2_frame, text="Transfer Data", command=self.transfer_data_only, width=14
        )
        self.transfer_data_btn.pack(side=tk.LEFT, padx=5)

        self.compare_data_btn = ttk.Button(
            row2_frame, text="Compare Data", command=self.compare_data_only, width=14
        )
        self.compare_data_btn.pack(side=tk.LEFT, padx=5)

        self.stop_conversion_btn = ttk.Button(
            row2_frame,
            text="Stop",
            command=self.stop_conversion_operation,
            width=14,
            style="Error.TButton",
        )
        # Stop is shown only while an operation is running (_start_conversion_operation)

        # Preview/Results Frame — lives in its own container (not the scroll canvas).
        preview_frame = ttk.LabelFrame(
            self._results_container,
            text="Conversion and Migration results",
            padding="10",
        )
        preview_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Preview Text
        self.conversion_preview_text = scrolledtext.ScrolledText(
            preview_frame, wrap=tk.WORD, height=12, font=self.ui_font_mono
        )
        self.conversion_preview_text.pack(fill=tk.BOTH, expand=True)

        # Progress Frame
        progress_frame = ttk.Frame(self._results_container)
        progress_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(progress_frame, text="Progress:").pack(side=tk.LEFT, padx=5)
        self.conversion_progress = ttk.Progressbar(progress_frame, mode="indeterminate")
        self.conversion_progress.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        self.conversion_status_label = ttk.Label(
            progress_frame, text="Ready", foreground="blue"
        )
        self.conversion_status_label.pack(side=tk.LEFT, padx=5)

        # Initialize connection combos
        self.refresh_connections()

    def _migration_connection_names(self):
        names = []
        for name, mgr in self.active_connections.items():
            caps = DatabaseRegistry.get_capabilities(mgr.db_type)
            if caps.supports_schema_conversion or caps.supports_document_query:
                names.append(name)
        return sorted(names)

    def _validate_migration_pair(self, source_conn_name, target_conn_name, *, operation="transfer"):
        from schema_converter.adapters import validate_migration_pair

        source_mgr = self.active_connections.get(source_conn_name)
        target_mgr = self.active_connections.get(target_conn_name)
        if not source_mgr or not target_mgr:
            return "Select both source and target connections."
        return validate_migration_pair(
            source_mgr.db_type, target_mgr.db_type, operation=operation
        )

    def refresh_connections(self):
        """Refresh connection dropdowns in conversion tab"""
        # Check if conversion tab has been initialized
        if not hasattr(self, "source_conn_combo") or not hasattr(
            self, "target_conn_combo"
        ):
            return

        connection_names = self._migration_connection_names()
        self.source_conn_combo["values"] = connection_names
        self.target_conn_combo["values"] = connection_names

        source_selected = self.source_conn_combo.get().strip()
        target_selected = self.target_conn_combo.get().strip()

        if source_selected and source_selected not in connection_names:
            self.source_conn_combo.set("")
        if target_selected and target_selected not in connection_names:
            self.target_conn_combo.set("")

        if not connection_names:
            self.source_conn_combo.set("")
            self.target_conn_combo.set("")

    def on_source_connection_changed(self, event=None):
        """Handle source connection change"""
        # Clear existing checkboxes when connection changes
        for widget in self.table_checkboxes_frame.winfo_children():
            widget.destroy()
        self.table_checkbox_vars.clear()
        self.clear_conversion_preview()

        source_conn_name = self.source_conn_combo.get() if self.source_conn_combo else ""
        manager = self.active_connections.get(source_conn_name)
        self._populate_namespace_combo(
            getattr(self, "source_database_combo", None), manager
        )

    def on_source_database_changed(self, event=None):
        """Reload tables when the source database/schema selection changes."""
        self.clear_conversion_preview()
        if self.source_conn_combo.get():
            self.load_source_tables()

    def on_target_connection_changed(self, event=None):
        """Populate the target database/schema field from the target connection."""
        self.clear_conversion_preview()
        target_conn_name = self.target_conn_combo.get() if self.target_conn_combo else ""
        manager = self.active_connections.get(target_conn_name)
        self._populate_namespace_combo(
            getattr(self, "target_database_combo", None), manager
        )

    def _list_namespaces_for_manager(self, manager):
        """Return databases (MySQL/MariaDB) or schemas (other engines).

        Used to populate the source/target database-schema dropdowns so the
        user can pick the namespace that qualifies the migration tables.
        """
        if not manager or not getattr(manager, "conn", None):
            return []
        db_type = manager.db_type
        operation = (
            "getDatabases" if db_type in ("MySQL", "MariaDB") else "getSchemas"
        )
        try:
            items = (
                DatabaseRegistry.execute_operation(db_type, operation, manager.conn)
                or []
            )
        except Exception:
            items = []
        if db_type in ("MySQL", "MariaDB"):
            system = {"information_schema", "performance_schema", "mysql", "sys"}
            items = [i for i in items if str(i).strip().lower() not in system]
        return [str(i).strip() for i in items if str(i).strip()]

    def _populate_namespace_combo(self, combo, manager):
        """Fill *combo* with namespaces and pre-select the connection default."""
        if combo is None:
            return
        names = self._list_namespaces_for_manager(manager)
        combo["values"] = names
        default = self._default_namespace_for_manager(manager) if manager else ""
        if default:
            # Keep editable so a value not present in the list still shows.
            combo.set(default)
        else:
            combo.set("")

    def _default_namespace_for_manager(self, manager):
        """Return the namespace to pre-select for *manager*.

        For MySQL/MariaDB this is the connection's database. For schema-based
        engines (PostgreSQL, SQL Server, Oracle) it is the live default schema,
        which is what qualifies table names, so it matches the dropdown entries.
        """
        if not manager or not getattr(manager, "conn", None):
            return ""
        db_type = manager.db_type
        if db_type in ("MySQL", "MariaDB"):
            return self._current_database_for_manager(manager)
        schema_sql = {
            "PostgreSQL": "SELECT current_schema()",
            "SQLServer": "SELECT SCHEMA_NAME()",
            "Oracle": "SELECT USER FROM dual",
        }.get(db_type)
        if not schema_sql:
            return self._current_database_for_manager(manager)
        cur = None
        try:
            cur = manager.conn.cursor()
            cur.execute(schema_sql)
            row = cur.fetchone()
            return str(row[0]).strip() if row and row[0] else ""
        except Exception:
            return self._current_database_for_manager(manager)
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass

    def load_source_tables(self):
        """Load tables from source connection"""
        source_conn_name = self.source_conn_combo.get()
        if not source_conn_name:
            messagebox.showwarning(
                "Warning", "Please select a source connection first!"
            )
            return

        if source_conn_name not in self.active_connections:
            messagebox.showerror("Error", "Source connection not found!")
            return

        db_manager = self.active_connections[source_conn_name]

        try:
            # Get tables using registry (works for any registered database type)
            tables = (
                DatabaseRegistry.execute_operation(
                    db_manager.db_type, "getTables", db_manager.conn
                )
                or []
            )

            tables = self._filter_tables_by_source_namespace(tables)

            console_print(
                f"Schema Conversion: Loaded {len(tables)} tables from {source_conn_name} ({db_manager.db_type})"
            )

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
                    variable=var,
                    command=self._update_single_table_options_state,
                )
                cb.pack(anchor=tk.W, padx=5, pady=2)

            self._update_single_table_options_state()

            if not tables:
                messagebox.showwarning(
                    "No Tables",
                    f"No tables found in {source_conn_name}.\n\nMake sure:\n1. Database is selected\n2. User has SELECT privileges\n3. Database contains tables",
                )

        except Exception as e:
            import traceback

            error_detail = traceback.format_exc()
            console_print(f"Error loading tables: {error_detail}")
            messagebox.showerror(
                "Error",
                f"Failed to load tables:\n{str(e)}\n\nCheck console for details.",
            )

    def check_all_tables(self):
        """Check all table checkboxes"""
        if not self.table_checkbox_vars:
            messagebox.showinfo("Info", "Please load tables first!")
            return

        for var in self.table_checkbox_vars.values():
            var.set(True)
        self._update_single_table_options_state()
        messagebox.showinfo(
            "Selected", f"Checked all {len(self.table_checkbox_vars)} tables"
        )

    def uncheck_all_tables(self):
        """Uncheck all table checkboxes"""
        if not self.table_checkbox_vars:
            messagebox.showinfo("Info", "Please load tables first!")
            return

        for var in self.table_checkbox_vars.values():
            var.set(False)
        self._update_single_table_options_state()
        messagebox.showinfo(
            "Deselected", f"Unchecked all {len(self.table_checkbox_vars)} tables"
        )

    def get_selected_tables(self):
        """Get list of selected tables from checkboxes"""
        selected = []
        for table_name, var in self.table_checkbox_vars.items():
            if var.get():
                selected.append(table_name)
        return selected

    def _update_single_table_options_state(self):
        """Enable single-table-only options only when at most one table is
        selected; disable (non-editable) them when more than one is selected.

        Row filter (WHERE) and column subset only make sense for a single source
        table, so they are greyed out for multi-table runs. Column rename is not
        gated here because it applies to every selected table.
        """
        try:
            selected_count = len(self.get_selected_tables())
        except Exception:
            selected_count = 0
        disabled = selected_count > 1
        entry_state = "disabled" if disabled else "normal"
        for name in getattr(self, "_single_table_option_widgets", ()):
            widget = getattr(self, name, None)
            if widget is not None:
                try:
                    widget.configure(state=entry_state)
                except Exception:
                    pass
        for name in getattr(self, "_single_table_option_labels", ()):
            label = getattr(self, name, None)
            if label is not None:
                try:
                    label.state(["disabled"] if disabled else ["!disabled"])
                except Exception:
                    pass

    def get_target_table_name(self, source_table, target_manager=None):
        """Generate target table name with prefix/suffix.

        Source tables may be schema-qualified (for example PostgreSQL
        ``public.orders``). The target name uses the base table name plus the
        target database/schema selected in this UI, so source and target SQL are
        qualified independently (``source_schema.table`` -> ``target_db.table``).
        """
        from schema_converter.table_naming import qualify_target_table

        prefix = self.target_prefix_entry.get().strip()
        suffix = self.target_suffix_entry.get().strip()
        target_db = self._target_database_name(target_manager)
        return qualify_target_table(source_table, target_db, prefix, suffix)

    def _get_type_overrides(self):
        from schema_converter.type_overrides import resolve_type_overrides

        return resolve_type_overrides(self.type_map_entry.get().strip())

    def _get_transfer_options(self, single_table: bool = True):
        """Build a :class:`TransferOptions` for a transfer run.

        Fixed-value policies (continue-on-error, overflow, NULL/boolean,
        timezone, sequence reset) come from the saved Migration Settings
        (``config.ini``). Per-run row filter (WHERE) and column subset are read
        from the UI only for single-table transfers; column rename and the row
        limit apply to every selected table.
        """
        from schema_converter.transfer_options import (
            options_from_config,
            parse_column_map,
            parse_columns,
        )

        def _entry(name, default=""):
            widget = getattr(self, name, None)
            if widget is None:
                return default
            try:
                return widget.get().strip()
            except Exception:
                return default

        def _var(name, default):
            var = getattr(self, name, None)
            if var is None:
                return default
            try:
                return var.get()
            except Exception:
                return default

        options = options_from_config()

        limit_text = _entry("limit_entry", "")
        try:
            options.limit = int(limit_text) if limit_text else None
        except ValueError:
            options.limit = None

        # Column rename applies to all selected tables (no-op for tables that
        # lack a listed source column); WHERE and column subset are single-table.
        options.column_map = parse_column_map(_entry("column_map_entry", ""))
        if single_table:
            options.where = _entry("where_entry", "")
            options.columns = parse_columns(_entry("columns_entry", ""))

        options.checkpoint = bool(_var("checkpoint_var", False))
        options.report_path = _entry("report_path_entry", "")
        options.__post_init__()
        return options

    def _make_checkpoint_store(self, options, source_conn_name, target_conn_name):
        if not getattr(options, "checkpoint", False):
            return None
        from schema_converter.migration_report import CheckpointStore

        return CheckpointStore(
            CheckpointStore.default_path(source_conn_name, target_conn_name)
        )

    def _maybe_write_report(self, options, source_conn_name, target_conn_name, entries):
        path = getattr(options, "report_path", "")
        if not path:
            return None
        from schema_converter.migration_report import MigrationReport

        report = MigrationReport(
            path, source_conn=source_conn_name, target_conn=target_conn_name
        )
        for entry in entries:
            report.tables.append(entry)
        written = report.write()
        if written:
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"\nMigration report written to: {written}\n",
            )
        return written

    def validate_migration_dry_run(self):
        """G5: run a read-only pre-migration validation and show the report."""
        source_conn_name = self.source_conn_combo.get() if self.source_conn_combo else ""
        target_conn_name = self.target_conn_combo.get() if self.target_conn_combo else ""
        if not source_conn_name or not target_conn_name:
            messagebox.showwarning(
                "Validate", "Select both a source and a target connection first."
            )
            return
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            messagebox.showwarning("Validate", "Select at least one table to validate.")
            return
        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]
        except KeyError:
            messagebox.showerror("Validate", "Selected connection is not active.")
            return

        pair_err = self._validate_migration_pair(source_conn_name, target_conn_name)
        if pair_err:
            messagebox.showerror("Validate", pair_err)
            return

        from schema_converter.migration_validation import validate_migration

        pairs = [
            (t, self.get_target_table_name(t, target_manager)) for t in selected_tables
        ]
        report = validate_migration(
            source_manager,
            target_manager,
            pairs,
            type_overrides=self._get_type_overrides(),
        )

        self.conversion_preview_text.insert(tk.END, f"\n{'=' * 80}\n")
        self.conversion_preview_text.insert(
            tk.END, "PRE-MIGRATION VALIDATION (dry-run — no rows moved)\n"
        )
        self.conversion_preview_text.insert(tk.END, f"{'=' * 80}\n")
        for table in report.get("tables", []):
            self.conversion_preview_text.insert(
                tk.END,
                f"\n{table['source_table']} → {table['target_table']} "
                f"({'OK' if table['ok'] else 'ISSUES'})\n",
            )
            for issue in table.get("issues", []):
                self.conversion_preview_text.insert(
                    tk.END,
                    f"  [{issue['severity'].upper()}] {issue['category']}"
                    + (f" ({issue['column']})" if issue.get("column") else "")
                    + f": {issue['message']}\n",
                )
        summary = report.get("summary", {})
        self.conversion_preview_text.insert(
            tk.END,
            f"\nSummary: tables={summary.get('tables', 0)} "
            f"errors={summary.get('errors', 0)} "
            f"warnings={summary.get('warnings', 0)}\n",
        )
        self.conversion_preview_text.see(tk.END)
        if report.get("ok"):
            messagebox.showinfo(
                "Validation passed",
                f"No blocking issues found across {summary.get('tables', 0)} table(s).\n"
                f"Warnings: {summary.get('warnings', 0)}",
            )
        else:
            messagebox.showwarning(
                "Validation found issues",
                f"{summary.get('errors', 0)} error(s) and "
                f"{summary.get('warnings', 0)} warning(s) found.\n"
                "See the preview area for details.",
            )

    def _filter_tables_by_source_namespace(self, tables):
        """Filter schema-qualified tables by the selected source namespace.

        Engines such as PostgreSQL return ``schema.table`` names. When the user
        picks a source schema/database, only the matching tables are shown.
        Unqualified table lists (e.g. MySQL current database) are returned as-is.
        """
        combo = getattr(self, "source_database_combo", None)
        selected = ""
        if combo is not None:
            try:
                selected = combo.get().strip()
            except tk.TclError:
                selected = ""
        if not selected:
            return tables
        qualified = [str(t) for t in tables if "." in str(t)]
        if not qualified:
            # No qualified names to filter against; keep the full list.
            return tables
        filtered = [
            t for t in qualified if str(t).split(".")[0].strip() == selected
        ]
        return filtered if filtered else tables

    @staticmethod
    def _target_base_table_name(source_table):
        """Return the table identifier without source schema/database prefix."""
        from schema_converter.table_naming import base_table_name

        return base_table_name(source_table)

    def _target_database_name(self, target_manager=None):
        """Return the target DB/schema qualifier for target-side SQL."""
        value = ""
        if getattr(self, "target_database_combo", None) is not None:
            try:
                value = self.target_database_combo.get().strip()
            except tk.TclError:
                value = ""
        if not value:
            value = self._current_database_for_manager(target_manager)
        if (
            target_manager
            and target_manager.db_type in ("MySQL", "MariaDB")
            and not value
        ):
            raise ValueError(
                "Target database/schema is required for MySQL/MariaDB. "
                "Enter it in the Target database/schema field (for example: test) "
                "or reconnect to the target with a default database selected."
            )
        return value

    @staticmethod
    def _current_database_for_manager(manager):
        if not manager or not getattr(manager, "conn", None):
            return ""
        params = getattr(manager, "_last_connect_params", {}) or {}
        for key in ("database", "service"):
            if params.get(key):
                return str(params[key]).strip()
        sql_by_type = {
            "MySQL": "SELECT DATABASE()",
            "MariaDB": "SELECT DATABASE()",
            "PostgreSQL": "SELECT current_schema()",
            "SQLServer": "SELECT DB_NAME()",
            "Oracle": "SELECT USER FROM dual",
        }
        sql = sql_by_type.get(manager.db_type)
        if not sql:
            return ""
        cur = None
        try:
            cur = manager.conn.cursor()
            cur.execute(sql)
            row = cur.fetchone()
            return str(row[0]).strip() if row and row[0] else ""
        except Exception:
            return ""
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass

    def _build_table_name_map(self, selected_tables, target_manager=None):
        return {
            name: self.get_target_table_name(name, target_manager)
            for name in selected_tables
        }

    def _execute_all_schema_ddl(self, target_manager, ddl_statements, create_indexes=True):
        executed = 0
        errors = []
        table_created = False
        for sql in ddl_statements:
            upper = sql.strip().upper()
            if not create_indexes and (
                upper.startswith("CREATE INDEX")
                or upper.startswith("CREATE UNIQUE INDEX")
            ):
                continue
            if not table_created and (
                upper.startswith("CREATE INDEX")
                or upper.startswith("CREATE UNIQUE INDEX")
                or upper.startswith("ALTER TABLE")
            ):
                continue
            try:
                cursor = target_manager.conn.cursor()
                cursor.execute(sql)
                target_manager.conn.commit()
                cursor.close()
                executed += 1
                if upper.startswith("CREATE TABLE"):
                    table_created = True
            except Exception as exc:
                errors.append((sql, str(exc)))
                if upper.startswith("CREATE TABLE"):
                    break
        return executed, errors, table_created

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
            messagebox.showwarning(
                "Warning", "Please select source and target connections!"
            )
            return

        if (
            source_conn_name not in self.active_connections
            or target_conn_name not in self.active_connections
        ):
            messagebox.showerror("Error", "Connection not found!")
            return

        schema_err = self._validate_migration_pair(
            source_conn_name, target_conn_name, operation="schema"
        )
        if schema_err:
            messagebox.showerror("Unsupported migration", schema_err)
            return

        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]

            # Check if converting between different DB types
            if source_manager.db_type == target_manager.db_type:
                messagebox.showinfo(
                    "Info",
                    f"Both connections are {source_manager.db_type}. No type conversion needed.",
                )

            # Start operation
            self._start_conversion_operation()

            self.conversion_status_label.config(
                text=f"Analyzing schema for {len(selected_tables)} table(s)...",
                foreground="blue",
            )
            self.conversion_progress.start()

            # Clear preview area first
            self.conversion_preview_text.delete(1.0, tk.END)

            # Run in thread
            thread = threading.Thread(
                target=self._preview_multiple_schemas_thread,
                args=(source_manager, target_manager, selected_tables),
            )
            thread.daemon = True
            thread.start()

        except Exception as e:
            messagebox.showerror("Error", f"Preview failed:\n{str(e)}")
            self.conversion_status_label.config(text="Preview failed", foreground="red")
            self._end_conversion_operation()

    def _preview_multiple_schemas_thread(
        self, source_manager, target_manager, selected_tables
    ):
        """Thread for previewing multiple table schemas"""
        total_tables = len(selected_tables)

        try:
            converter = SchemaConverter(source_manager, target_manager)

            # Header
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"SCHEMA CONVERSION PREVIEW\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Previewing {total_tables} table(s)\n",
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n\n"
            )

            for idx, source_table in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"\nPreview stopped by user at table {idx} of {total_tables}\n",
                    )
                    break

                try:
                    target_table = self.get_target_table_name(source_table, target_manager)

                    # Update status
                    self.root.after(
                        0,
                        self.conversion_status_label.config,
                        {
                            "text": f"Analyzing {idx} of {total_tables}: {source_table}",
                            "foreground": "blue",
                        },
                    )

                    # Get source schema
                    source_schema = converter.get_table_schema(source_table)
                    if not source_schema:
                        self.root.after(
                            0,
                            self.conversion_preview_text.insert,
                            tk.END,
                            f"[{idx}/{total_tables}]  Could not retrieve schema for {source_table}\n\n",
                        )
                        continue

                    # Convert schema
                    converted_schema = converter.convert_schema(
                        source_schema,
                        table_name_map={
                            source_table: target_table,
                            **self._build_table_name_map(selected_tables, target_manager),
                        },
                        type_overrides=self._get_type_overrides(),
                    )
                    converted_schema["table_name"] = target_table

                    all_ddl = converter.generate_all_table_ddl(converted_schema)
                    create_table_ddl = all_ddl[0] if all_ddl else ""
                    indexes_ddl = all_ddl[1:] if len(all_ddl) > 1 else []

                    # Validate
                    validator = ConversionValidator()
                    issues = validator.validate_schema_conversion(
                        source_schema, converted_schema
                    )

                    # Display this table's preview
                    self.root.after(
                        0,
                        self._append_schema_preview,
                        idx,
                        total_tables,
                        source_schema,
                        converted_schema,
                        create_table_ddl,
                        indexes_ddl,
                        issues,
                    )

                except Exception as e:
                    import traceback

                    error_detail = str(e)
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"[{idx}/{total_tables}]  Preview failed for {source_table}\n",
                    )
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"Error: {error_detail}\n\n",
                    )

            # Final summary
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Preview complete for {total_tables} table(s)!\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Click 'Convert Schema' to create tables, then 'Transfer Data' to copy data.\n",
            )

        except Exception as e:
            import traceback

            error_msg = f"Schema preview failed:\n{str(e)}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
        finally:
            self.root.after(0, self.conversion_progress.stop)
            status_text = (
                "Preview stopped"
                if self.conversion_stop_event.is_set()
                else f"Preview complete for {total_tables} table(s)"
            )
            status_color = "orange" if self.conversion_stop_event.is_set() else "green"
            self.root.after(
                0,
                self.conversion_status_label.config,
                {"text": status_text, "foreground": status_color},
            )
            self.root.after(0, self._end_conversion_operation)

    def _append_schema_preview(
        self,
        idx,
        total_tables,
        source_schema,
        converted_schema,
        ddl,
        indexes_ddl,
        issues,
    ):
        """Append a single table's schema preview to the text widget"""
        # Table header
        self.conversion_preview_text.insert(
            tk.END,
            f"\n[{idx}/{total_tables}] {source_schema['table_name']}  {converted_schema['table_name']}\n",
        )
        self.conversion_preview_text.insert(tk.END, "-" * 80 + "\n")

        # Source Info
        self.conversion_preview_text.insert(
            tk.END, f"Columns: {len(source_schema['columns'])}\n"
        )
        self.conversion_preview_text.insert(
            tk.END,
            f"Primary Key: {', '.join(source_schema['primary_key']) if source_schema['primary_key'] else 'None'}\n",
        )
        self.conversion_preview_text.insert(
            tk.END, f"Indexes: {len(source_schema.get('indexes') or [])}\n"
        )
        self.conversion_preview_text.insert(
            tk.END,
            f"Foreign Keys: {len(source_schema.get('foreign_keys') or [])}\n",
        )
        self.conversion_preview_text.insert(
            tk.END,
            f"Unique Constraints: {len(source_schema.get('unique_constraints') or [])}\n",
        )
        self.conversion_preview_text.insert(
            tk.END,
            f"Check Constraints: {len(source_schema.get('check_constraints') or [])}\n",
        )
        if source_schema.get("table_comment"):
            self.conversion_preview_text.insert(
                tk.END, f"Table Comment: {source_schema['table_comment'][:80]}\n"
            )
        self.conversion_preview_text.insert(tk.END, "\n")

        conv_warnings = converted_schema.get("conversion_warnings") or []
        if conv_warnings:
            self.conversion_preview_text.insert(tk.END, "CONVERSION NOTES:\n")
            for note in conv_warnings:
                self.conversion_preview_text.insert(tk.END, f"  - {note}\n")
            self.conversion_preview_text.insert(tk.END, "\n")

        # Validation Issues
        if issues:
            self.conversion_preview_text.insert(tk.END, "VALIDATION WARNINGS:\n")
            for issue in issues:
                self.conversion_preview_text.insert(tk.END, f"  - {issue}\n")
            self.conversion_preview_text.insert(tk.END, "\n")

        # Column Mapping (abbreviated)
        self.conversion_preview_text.insert(tk.END, "COLUMN TYPE MAPPINGS:\n")
        for src_col, tgt_col in zip(
            source_schema["columns"], converted_schema["columns"]
        ):
            nullable = "NULL" if tgt_col["nullable"] else "NOT NULL"
            self.conversion_preview_text.insert(
                tk.END,
                f"  {src_col['name']}: {src_col['type']}  {tgt_col['type']} ({nullable})\n",
            )

        # DDL (collapsed for multi-table view)
        self.conversion_preview_text.insert(
            tk.END, f"\nGENERATED DDL ({1 + len(indexes_ddl)} statement(s)):\n"
        )
        self.conversion_preview_text.insert(tk.END, ddl + "\n")

        if indexes_ddl:
            for extra_ddl in indexes_ddl[:5]:
                self.conversion_preview_text.insert(tk.END, extra_ddl + "\n")
            if len(indexes_ddl) > 5:
                self.conversion_preview_text.insert(
                    tk.END, f"... and {len(indexes_ddl) - 5} more statement(s)\n"
                )

        self.conversion_preview_text.insert(tk.END, "\n")

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
            messagebox.showwarning(
                "Warning", "Please select source and target connections!"
            )
            return

        schema_err = self._validate_migration_pair(
            source_conn_name, target_conn_name, operation="schema"
        )
        if schema_err:
            messagebox.showerror("Unsupported migration", schema_err)
            return

        # Show table names with prefix/suffix applied
        prefix = self.target_prefix_entry.get().strip()
        suffix = self.target_suffix_entry.get().strip()
        naming_info = ""
        if prefix or suffix:
            naming_info = f"\nTarget naming: {prefix}<table>{suffix}"

        table_count = len(selected_tables)
        if not messagebox.askyesno(
            "Confirm",
            f"Convert schema for {table_count} table(s)?\n\nTables: {', '.join(selected_tables[:5])}{'...' if table_count > 5 else ''}{naming_info}\n\nNote: This will create table structures only (no data).",
        ):
            return

        # Start operation
        self._start_conversion_operation()

        self.conversion_status_label.config(
            text=f"Converting {table_count} table(s)...", foreground="blue"
        )
        self.conversion_progress.start()

        thread = threading.Thread(
            target=self._convert_multiple_schemas_thread,
            args=(source_conn_name, target_conn_name, selected_tables),
        )
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
            messagebox.showwarning(
                "Warning", "Please select source and target connections!"
            )
            return

        transfer_err = self._validate_migration_pair(
            source_conn_name, target_conn_name, operation="transfer"
        )
        if transfer_err:
            messagebox.showerror("Unsupported migration", transfer_err)
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
        if not messagebox.askyesno(
            "Confirm",
            f"Transfer data for {table_count} table(s)?\n\nTables: {', '.join(selected_tables[:5])}{'...' if table_count > 5 else ''}{naming_info}\n\nNote: Target tables must already exist.\nThis may take a while for large tables.",
        ):
            return

        # Start operation
        self._start_conversion_operation()

        self.conversion_status_label.config(
            text=f"Transferring data for {table_count} table(s)...", foreground="blue"
        )
        self.conversion_progress.start()

        thread = threading.Thread(
            target=self._transfer_multiple_data_thread,
            args=(source_conn_name, target_conn_name, selected_tables),
        )
        thread.daemon = True
        thread.start()

    def compare_data_only(self):
        """Compare schema and data between source and target for selected tables."""
        selected_tables = self.get_selected_tables()
        if not selected_tables:
            messagebox.showwarning("Warning", "Please check at least one table!")
            return

        source_conn_name = self.source_conn_combo.get()
        target_conn_name = self.target_conn_combo.get()

        if not all([source_conn_name, target_conn_name]):
            messagebox.showwarning(
                "Warning", "Please select source and target connections!"
            )
            return

        if (
            source_conn_name not in self.active_connections
            or target_conn_name not in self.active_connections
        ):
            messagebox.showerror("Error", "Connection not found!")
            return

        compare_mode = self._ask_compare_data_mode()
        if compare_mode is None:
            return

        table_count = len(selected_tables)
        sample_rows = get_compare_sample_size()
        mode_label = (
            f"sample ({sample_rows} rows per table)"
            if compare_mode == "sample"
            else "full (all rows)"
        )
        if not messagebox.askyesno(
            "Confirm",
            f"Compare schema and data for {table_count} table(s)?\n\n"
            f"Mode: {mode_label}\n\n"
            f"Tables: {', '.join(selected_tables[:5])}"
            f"{'...' if table_count > 5 else ''}",
        ):
            return

        self._start_conversion_operation()
        self.conversion_status_label.config(
            text=f"Comparing {table_count} table(s)...", foreground="blue"
        )
        self.conversion_progress.start()
        self.conversion_preview_text.delete(1.0, tk.END)

        thread = threading.Thread(
            target=self._compare_multiple_data_thread,
            args=(
                source_conn_name,
                target_conn_name,
                selected_tables,
                compare_mode,
                sample_rows,
            ),
        )
        thread.daemon = True
        thread.start()

    def _ask_compare_data_mode(self):
        """Return 'sample' or 'full', or None if cancelled."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Compare Data")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.resizable(False, False)

        choice = {"mode": None}
        sample_rows = get_compare_sample_size()

        ttk.Label(
            dialog,
            text="Choose comparison scope:",
            font=(self.ui_font[0], self.ui_font[1] + 1, "bold"),
        ).pack(padx=20, pady=(15, 10))

        mode_var = tk.StringVar(value="sample")
        ttk.Radiobutton(
            dialog,
            text=f"Sample — compare first {sample_rows} rows per table",
            variable=mode_var,
            value="sample",
        ).pack(anchor=tk.W, padx=25, pady=2)
        ttk.Radiobutton(
            dialog,
            text="Full — compare all rows (may be slow on large tables)",
            variable=mode_var,
            value="full",
        ).pack(anchor=tk.W, padx=25, pady=2)

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=15)

        def _ok():
            choice["mode"] = mode_var.get()
            dialog.destroy()

        def _cancel():
            dialog.destroy()

        ttk.Button(btn_frame, text="Compare", command=_ok, width=12).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(btn_frame, text="Cancel", command=_cancel, width=12).pack(
            side=tk.LEFT, padx=5
        )

        dialog.update_idletasks()
        x = self.root.winfo_rootx() + (self.root.winfo_width() // 2) - 180
        y = self.root.winfo_rooty() + (self.root.winfo_height() // 2) - 80
        dialog.geometry(f"+{x}+{y}")

        self.root.wait_window(dialog)
        return choice["mode"]

    def _compare_multiple_data_thread(
        self, source_conn_name, target_conn_name, selected_tables, compare_mode, sample_size
    ):
        """Thread for schema + data comparison across multiple tables."""
        import traceback

        total_tables = len(selected_tables)
        schema_matches = 0
        data_matches = 0
        failed = []

        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]
            data_comparer = DataComparer(source_manager, target_manager)

            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                "SCHEMA & DATA COMPARISON\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Mode: {compare_mode}  |  Tables: {total_tables}\n",
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n\n"
            )

            for idx, source_table in enumerate(selected_tables, 1):
                if self.conversion_stop_event.is_set():
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"\nComparison stopped at table {idx} of {total_tables}\n",
                    )
                    break

                target_table = self.get_target_table_name(source_table, target_manager)
                self.root.after(
                    0,
                    self.conversion_status_label.config,
                    {
                        "text": f"Comparing {idx} of {total_tables}: {source_table}",
                        "foreground": "blue",
                    },
                )

                try:
                    schema_result = SchemaComparer.compare_tables(
                        source_manager, target_manager, source_table, target_table
                    )
                    data_result = data_comparer.compare_table_data(
                        source_table,
                        target_table,
                        mode=compare_mode,
                        sample_size=sample_size,
                        stop_event=self.conversion_stop_event,
                    )

                    schema_ok = schema_result.get("match", False)
                    data_ok = data_result.get("match", False)
                    if schema_ok:
                        schema_matches += 1
                    if data_ok:
                        data_matches += 1

                    self.root.after(
                        0,
                        self._append_compare_result,
                        idx,
                        total_tables,
                        source_table,
                        target_table,
                        schema_result,
                        data_result,
                    )
                except Exception as e:
                    failed.append((source_table, str(e)))
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"[{idx}/{total_tables}] FAILED: {source_table}\n",
                    )
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"  Error: {e}\n\n",
                    )

            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                "COMPARISON SUMMARY\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Schema matches: {schema_matches}/{total_tables}\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Data matches: {data_matches}/{total_tables}\n",
            )
            if failed:
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    f"Failed: {len(failed)}\n",
                )

        except Exception as e:
            error_msg = f"Comparison failed:\n{e}\n\n{traceback.format_exc()}"
            self.root.after(0, messagebox.showerror, "Error", error_msg)
        finally:
            self.root.after(0, self.conversion_progress.stop)
            if self.conversion_stop_event.is_set():
                status_text = "Comparison stopped"
                status_color = "orange"
            elif schema_matches == total_tables and data_matches == total_tables:
                status_text = f"All {total_tables} table(s) match"
                status_color = "green"
            else:
                status_text = (
                    f"Comparison complete: schema {schema_matches}/{total_tables}, "
                    f"data {data_matches}/{total_tables}"
                )
                status_color = "orange"
            self.root.after(
                0,
                self.conversion_status_label.config,
                {"text": status_text, "foreground": status_color},
            )
            self.root.after(0, self._end_conversion_operation)

    def _append_compare_result(
        self,
        idx,
        total,
        source_table,
        target_table,
        schema_result,
        data_result,
    ):
        """Append one table's schema/data comparison to the preview pane."""
        self.conversion_preview_text.insert(
            tk.END, f"[{idx}/{total}] {source_table} → {target_table}\n"
        )
        self.conversion_preview_text.insert(tk.END, f"{'-' * 80}\n")

        if schema_result.get("error"):
            self.conversion_preview_text.insert(
                tk.END, f"  Schema: ERROR — {schema_result['error']}\n"
            )
        elif schema_result.get("match"):
            self.conversion_preview_text.insert(tk.END, "  Schema: MATCH\n")
        else:
            self.conversion_preview_text.insert(tk.END, "  Schema: MISMATCH\n")
            for issue in schema_result.get("issues") or []:
                self.conversion_preview_text.insert(tk.END, f"    - {issue}\n")

        if data_result.get("error"):
            self.conversion_preview_text.insert(
                tk.END, f"  Data: ERROR — {data_result['error']}\n"
            )
        else:
            src_cnt = data_result.get("source_row_count", "?")
            tgt_cnt = data_result.get("target_row_count", "?")
            compared = data_result.get("rows_compared", 0)
            self.conversion_preview_text.insert(
                tk.END,
                f"  Data: rows source={src_cnt}, target={tgt_cnt}, "
                f"compared={compared}\n",
            )
            if data_result.get("row_count_message"):
                self.conversion_preview_text.insert(
                    tk.END, f"    {data_result['row_count_message']}\n"
                )
            if data_result.get("match"):
                self.conversion_preview_text.insert(tk.END, "  Data: MATCH\n")
            else:
                self.conversion_preview_text.insert(tk.END, "  Data: MISMATCH\n")
                for mm in data_result.get("mismatched_rows") or []:
                    self.conversion_preview_text.insert(
                        tk.END, f"    Row {mm['row_number']}:\n"
                    )
                    for d in mm.get("differences") or []:
                        self.conversion_preview_text.insert(
                            tk.END,
                            f"      {d['column']}: source={d['source']!r} "
                            f"target={d['target']!r}\n",
                        )
                extra = len(data_result.get("mismatched_rows") or [])
                if not data_result.get("match") and compared > extra:
                    self.conversion_preview_text.insert(
                        tk.END,
                        "    (Additional mismatches may exist beyond shown rows)\n",
                    )

        if data_result.get("stopped") or schema_result.get("stopped"):
            self.conversion_preview_text.insert(tk.END, "  (Stopped by user)\n")

        self.conversion_preview_text.insert(tk.END, "\n")

    def _convert_schema_thread(
        self, source_conn_name, target_conn_name, source_table, target_table
    ):
        """Thread for schema conversion only"""
        ok = False
        try:
            source_manager = self.active_connections[source_conn_name]
            target_manager = self.active_connections[target_conn_name]

            converter = SchemaConverter(source_manager, target_manager)

            # Get and convert schema
            source_schema = converter.get_table_schema(source_table)
            if not source_schema:
                self.root.after(
                    0,
                    messagebox.showerror,
                    "Error",
                    f"Could not retrieve schema for {source_table}",
                )
                return

            converted_schema = converter.convert_schema(
                source_schema,
                table_name_map={source_table: target_table},
                type_overrides=self._get_type_overrides(),
            )
            converted_schema["table_name"] = target_table  # Use target table name

            # Drop table if option selected
            if self.drop_if_exists_var.get():
                try:
                    target_cursor = target_manager.conn.cursor()
                    target_cursor.execute(f"DROP TABLE IF EXISTS {target_table}")
                    target_manager.conn.commit()
                    target_cursor.close()
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"\n Dropped existing table: {target_table}\n",
                    )
                except Exception:
                    pass  # Table might not exist

            # Create table and related DDL
            all_ddl = converter.generate_all_table_ddl(converted_schema)
            executed, ddl_errors, table_created = self._execute_all_schema_ddl(
                target_manager,
                all_ddl,
                create_indexes=self.create_indexes_var.get(),
            )

            if ddl_errors or not table_created:
                err_msgs = "; ".join(err for _sql, err in ddl_errors)
                raise Exception(
                    err_msgs or "CREATE TABLE did not run successfully"
                )

            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"\n Created table: {target_table} ({executed} DDL statement(s))\n",
            )

            # Success
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"\n{'=' * 80}\n SCHEMA CONVERSION COMPLETE!\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Table structure created. Click 'Transfer Data' to copy data.\n",
            )
            self.root.after(
                0,
                messagebox.showinfo,
                "Success",
                f"Schema converted successfully!\nTarget table: {target_table}\n\nYou can now transfer data using the 'Transfer Data' button.",
            )
            ok = True

        except Exception as e:
            import traceback

            error_msg = (
                f"Schema conversion failed:\n{str(e)}\n\n{traceback.format_exc()}"
            )
            self.root.after(0, messagebox.showerror, "Error", error_msg)
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"\n ERROR: {str(e)}\n"
            )
        finally:
            self.root.after(0, self.conversion_progress.stop)
            status_text = (
                "Schema conversion complete" if ok else "Schema conversion failed"
            )
            status_color = "green" if ok else "red"
            self.root.after(
                0,
                self.conversion_status_label.config,
                {"text": status_text, "foreground": status_color},
            )

    def _convert_multiple_schemas_thread(
        self, source_conn_name, target_conn_name, selected_tables
    ):
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

            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"BATCH SCHEMA CONVERSION\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Converting {total_tables} table(s)...\n",
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n\n"
            )

            for idx, source_table in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"\nPreview stopped by user at table {idx} of {total_tables}\n",
                    )
                    break

                try:
                    # Generate target table name with prefix/suffix
                    target_table = self.get_target_table_name(source_table, target_manager)

                    # Update status
                    self.root.after(
                        0,
                        self.conversion_status_label.config,
                        {
                            "text": f"Converting table {idx} of {total_tables}: {source_table}",
                            "foreground": "blue",
                        },
                    )
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"[{idx}/{total_tables}] {source_table}  {target_table}\n",
                    )

                    # Get and convert schema
                    source_schema = converter.get_table_schema(source_table)
                    if not source_schema:
                        raise Exception(f"Could not retrieve schema for {source_table}")

                    converted_schema = converter.convert_schema(
                        source_schema,
                        table_name_map=self._build_table_name_map(selected_tables, target_manager),
                        type_overrides=self._get_type_overrides(),
                    )
                    converted_schema["table_name"] = (
                        target_table  # Use target name with prefix/suffix
                    )

                    # Drop table if option selected
                    if self.drop_if_exists_var.get():
                        try:
                            target_cursor = target_manager.conn.cursor()
                            target_cursor.execute(
                                f"DROP TABLE IF EXISTS {target_table}"
                            )
                            target_manager.conn.commit()
                            target_cursor.close()
                            self.root.after(
                                0,
                                self.conversion_preview_text.insert,
                                tk.END,
                                f"   Dropped existing table: {target_table}\n",
                            )
                        except Exception:
                            pass  # Table might not exist

                    all_ddl = converter.generate_all_table_ddl(converted_schema)
                    executed, ddl_errors, table_created = self._execute_all_schema_ddl(
                        target_manager,
                        all_ddl,
                        create_indexes=self.create_indexes_var.get(),
                    )

                    if ddl_errors or not table_created:
                        err_msgs = "; ".join(err for _sql, err in ddl_errors)
                        raise Exception(
                            err_msgs or "CREATE TABLE did not run successfully"
                        )

                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"   Created table: {target_table} ({executed} DDL statement(s))\n",
                    )

                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"   SUCCESS: {source_table}  {target_table}\n\n",
                    )
                    successful.append(f"{source_table}  {target_table}")

                except Exception as e:
                    import traceback

                    error_detail = str(e)
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"   FAILED: {source_table}\n",
                    )
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"     Error: {error_detail}\n\n",
                    )
                    failed.append((source_table, error_detail))

            # Final summary
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"BATCH CONVERSION COMPLETE\n",
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Total tables: {total_tables}\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f" Successful: {len(successful)}\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f" Failed: {len(failed)}\n",
            )

            if successful:
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    f"\nSuccessful tables:\n",
                )
                for table in successful:
                    self.root.after(
                        0, self.conversion_preview_text.insert, tk.END, f"   {table}\n"
                    )

            if failed:
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    f"\nFailed tables:\n",
                )
                for table, error in failed:
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"   {table}: {error}\n",
                    )

            # Show summary dialog
            if len(failed) == 0:
                self.root.after(
                    0,
                    messagebox.showinfo,
                    "Batch Conversion Complete",
                    f"All {len(successful)} table(s) converted successfully!\n\nYou can now transfer data using the 'Transfer Data' button.",
                )
            else:
                self.root.after(
                    0,
                    messagebox.showwarning,
                    "Batch Conversion Complete",
                    f"Conversion complete with some errors:\n\n Successful: {len(successful)}\n Failed: {len(failed)}\n\nCheck the preview area for details.",
                )

        except Exception as e:
            import traceback

            error_msg = (
                f"Batch schema conversion failed:\n{str(e)}\n\n{traceback.format_exc()}"
            )
            self.root.after(0, messagebox.showerror, "Error", error_msg)
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"\n BATCH ERROR: {str(e)}\n",
            )
        finally:
            self.root.after(0, self.conversion_progress.stop)
            if self.conversion_stop_event.is_set():
                status_text = (
                    f"Conversion stopped: {len(successful)} completed before stop"
                )
                status_color = "orange"
            else:
                status_text = f"Batch conversion complete: {len(successful)} successful, {len(failed)} failed"
                status_color = "green" if len(failed) == 0 else "orange"
            self.root.after(
                0,
                self.conversion_status_label.config,
                {"text": status_text, "foreground": status_color},
            )
            self.root.after(0, self._end_conversion_operation)

    def _clone_manager_for_worker(self, manager):
        """Open a fresh, task-scoped DB session from an active manager."""
        from common.db_manager import DatabaseManager

        params = getattr(manager, "_last_connect_params", None)
        if not params:
            raise ValueError(
                "Cannot open a worker session because this connection has no "
                "saved connect parameters."
            )
        worker = DatabaseManager(manager.db_type)
        worker.connect(**params)
        return worker

    def _transfer_multiple_data_parallel(
        self,
        source_conn_name,
        target_conn_name,
        selected_tables,
        source_manager,
        target_manager,
        batch_size,
        workers,
    ):
        """Parallel data transfer path. Schema/DDL conversion remains serial."""
        from schema_converter.parallel_transfer import (
            build_transfer_specs,
            run_parallel_transfer,
        )
        from schema_converter.transfer_options import ParallelTransferContext

        total_tables = len(selected_tables)
        self.root.after(
            0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
        )
        self.root.after(
            0, self.conversion_preview_text.insert, tk.END, "BATCH DATA TRANSFER\n"
        )
        self.root.after(
            0,
            self.conversion_preview_text.insert,
            tk.END,
            (
                f"Transferring data for {total_tables} table(s) "
                f"(batch size: {batch_size}, workers: {workers})...\n"
            ),
        )
        self.root.after(
            0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n\n"
        )

        specs = build_transfer_specs(
            selected_tables,
            lambda source_table: self.get_target_table_name(source_table, target_manager),
        )

        for idx, spec in enumerate(specs, 1):
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"[{idx}/{total_tables}] {spec.source_table}  {spec.target_table}\n",
            )

        def _progress(source_table, rows, total_rows):
            if total_rows:
                pct = (rows / total_rows) * 100
                text = f"{source_table}: {rows:,} / ~{total_rows:,} rows ({pct:.1f}%)"
            else:
                text = f"{source_table}: {rows:,} rows"
            self.root.after(
                0,
                self.conversion_status_label.config,
                {"text": text, "foreground": "blue"},
            )

        options = self._get_transfer_options(single_table=len(selected_tables) == 1)
        checkpoint_store = self._make_checkpoint_store(
            options, source_conn_name, target_conn_name
        )
        context = ParallelTransferContext(
            source_conn=source_conn_name,
            target_conn=target_conn_name,
            source_manager_factory=lambda _name: self._clone_manager_for_worker(source_manager),
            target_manager_factory=lambda _name: self._clone_manager_for_worker(target_manager),
            batch_size=batch_size,
            workers=workers,
            progress_callback=_progress,
            stop_event=self.conversion_stop_event,
            checkpoint_store=checkpoint_store,
        )
        result = run_parallel_transfer(
            specs,
            context,
            options=options,
        )
        self._maybe_write_report(
            options,
            source_conn_name,
            target_conn_name,
            [
                {
                    "source_table": row.get("source_table"),
                    "target_table": row.get("target_table"),
                    "ok": row.get("ok"),
                    "rows_transferred": row.get("rows_transferred", 0),
                    "skipped": row.get("skipped", 0),
                    "error_count": row.get("error_count", 0),
                    "duration_seconds": row.get("duration_seconds"),
                    "source_count": row.get("source_count"),
                    "target_count": row.get("target_count"),
                    "error": row.get("error"),
                }
                for row in (result.get("tables") or [])
            ],
        )

        successful = []
        failed = []
        total_rows = 0
        for row in result.get("tables") or []:
            source_table = row.get("source_table")
            target_table = row.get("target_table")
            if row.get("ok"):
                rows = int(row.get("rows_transferred") or 0)
                total_rows += rows
                successful.append((f"{source_table}  {target_table}", rows))
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    (
                        f"   {source_table}  {target_table}: "
                        f"transferred {rows:,} rows successfully "
                        f"(Source: {row.get('source_count')}, "
                        f"Target: {row.get('target_count')})\n"
                    ),
                )
            else:
                error = row.get("error") or "Transfer failed"
                failed.append((f"{source_table}  {target_table}", error))
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    f"   FAILED: {source_table}  {target_table}\n     Error: {error}\n",
                )

        self.root.after(
            0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
        )
        self.root.after(
            0,
            self.conversion_preview_text.insert,
            tk.END,
            "BATCH DATA TRANSFER COMPLETE\n",
        )
        self.root.after(
            0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
        )
        self.root.after(
            0,
            self.conversion_preview_text.insert,
            tk.END,
            (
                f"Total tables: {total_tables}\n"
                f" Successful: {len(successful)}\n"
                f" Failed: {len(failed)}\n"
                f"Total rows transferred: {total_rows:,}\n"
                f"Workers used: {result.get('workers', workers)}\n"
            ),
        )

        if failed:
            self.root.after(
                0,
                messagebox.showwarning,
                "Batch Transfer Complete",
                (
                    "Transfer complete with some errors:\n\n"
                    f" Successful: {len(successful)}\n"
                    f" Failed/Warning: {len(failed)}\n\n"
                    f"Total rows transferred: {total_rows:,}\n\n"
                    "Check the preview area for details."
                ),
            )
        else:
            self.root.after(
                0,
                messagebox.showinfo,
                "Batch Transfer Complete",
                (
                    f"All {len(successful)} table(s) transferred successfully!\n\n"
                    f"Total rows: {total_rows:,}"
                ),
            )
        return successful, failed, total_rows

    def _transfer_multiple_data_thread(
        self, source_conn_name, target_conn_name, selected_tables
    ):
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
            parallel_var = getattr(self, "parallel_transfer_var", None)
            parallel_enabled = bool(parallel_var.get()) if parallel_var is not None else False
            try:
                workers = int(self.parallel_workers_entry.get())
            except Exception:
                workers = module_config.get_int(
                    "schema.conversion", "parallel_workers", default=1
                )
            workers = max(1, workers)

            if parallel_enabled:
                successful, failed, total_rows = self._transfer_multiple_data_parallel(
                    source_conn_name,
                    target_conn_name,
                    selected_tables,
                    source_manager,
                    target_manager,
                    batch_size,
                    workers,
                )
                return

            options = self._get_transfer_options(
                single_table=len(selected_tables) == 1
            )
            checkpoint_store = self._make_checkpoint_store(
                options, source_conn_name, target_conn_name
            )
            from schema_converter.transfer_options import TransferRuntime

            report_entries = []

            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"BATCH DATA TRANSFER\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Transferring data for {total_tables} table(s) (batch size: {batch_size})...\n",
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n\n"
            )

            for idx, source_table in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"\nPreview stopped by user at table {idx} of {total_tables}\n",
                    )
                    break

                try:
                    # Generate target table name with prefix/suffix
                    target_table = self.get_target_table_name(source_table, target_manager)

                    # Initial status
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"[{idx}/{total_tables}] {source_table}  {target_table}\n",
                    )

                    # Progress callback for this table
                    def progress_callback(rows_transferred_current, total_rows_current):
                        if total_rows_current:
                            percentage = (
                                rows_transferred_current / total_rows_current
                            ) * 100
                            # Show ~ to indicate estimated total
                            status_text = f"[{idx}/{total_tables}] {source_table}: {rows_transferred_current:,} / ~{total_rows_current:,} rows ({percentage:.1f}%)"
                        else:
                            status_text = f"[{idx}/{total_tables}] {source_table}: {rows_transferred_current:,} rows"
                        self.root.after(
                            0,
                            self.conversion_status_label.config,
                            {"text": status_text, "foreground": "blue"},
                        )

                    # Transfer data with progress callback and stop event
                    rows_transferred = data_converter.transfer_table_data(
                        source_table,
                        target_table,
                        runtime=TransferRuntime(
                            batch_size=batch_size,
                            progress_callback=progress_callback,
                            stop_event=self.conversion_stop_event,
                            options=options,
                            checkpoint_store=checkpoint_store,
                        ),
                    )
                    stats = getattr(data_converter, "last_transfer_stats", {}) or {}
                    if checkpoint_store is not None:
                        checkpoint_store.clear(source_table, target_table)

                    # Validate
                    source_count = data_converter.get_row_count(
                        source_table, is_source=True
                    )
                    target_count = data_converter.get_row_count(
                        target_table, is_source=False
                    )

                    report_entries.append(
                        {
                            "source_table": source_table,
                            "target_table": target_table,
                            "ok": True,
                            "rows_transferred": rows_transferred,
                            "skipped": stats.get("skipped", 0),
                            "error_count": stats.get("error_count", 0),
                            "duration_seconds": stats.get("duration_seconds"),
                            "source_count": source_count,
                            "target_count": target_count,
                        }
                    )

                    if (
                        options.where
                        or options.limit is not None
                        or int(stats.get("skipped") or 0) > 0
                    ):
                        validation_msg = None
                    else:
                        validation_msg = ConversionValidator.validate_data_transfer(
                            source_count, target_count
                        )

                    if validation_msg:
                        self.root.after(
                            0,
                            self.conversion_preview_text.insert,
                            tk.END,
                            f"  {validation_msg}\n",
                        )
                        self.root.after(
                            0,
                            self.conversion_preview_text.insert,
                            tk.END,
                            f"  Rows transferred: {rows_transferred} (Source: {source_count}, Target: {target_count})\n\n",
                        )
                        failed.append(
                            (f"{source_table}  {target_table}", validation_msg)
                        )
                    else:
                        self.root.after(
                            0,
                            self.conversion_preview_text.insert,
                            tk.END,
                            f"   Transferred {rows_transferred} rows successfully\n",
                        )
                        self.root.after(
                            0,
                            self.conversion_preview_text.insert,
                            tk.END,
                            f"   Validation passed (Source: {source_count}, Target: {target_count})\n\n",
                        )
                        successful.append(
                            (f"{source_table}  {target_table}", rows_transferred)
                        )
                        total_rows += rows_transferred

                except Exception as e:
                    import traceback

                    error_detail = str(e)
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"   FAILED: {source_table}\n",
                    )
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"     Error: {error_detail}\n\n",
                    )
                    failed.append((source_table, error_detail))

            self._maybe_write_report(
                options, source_conn_name, target_conn_name, report_entries
            )

            # Final summary
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"BATCH DATA TRANSFER COMPLETE\n",
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Total tables: {total_tables}\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f" Successful: {len(successful)}\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f" Failed: {len(failed)}\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Total rows transferred: {total_rows:,}\n",
            )

            if successful:
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    f"\nSuccessful tables:\n",
                )
                for table, rows in successful:
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"   {table}: {rows:,} rows\n",
                    )

            if failed:
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    f"\nFailed/Warning tables:\n",
                )
                for table, error in failed:
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"   {table}: {error}\n",
                    )

            # Show summary dialog
            if len(failed) == 0:
                self.root.after(
                    0,
                    messagebox.showinfo,
                    "Batch Transfer Complete",
                    f"All {len(successful)} table(s) transferred successfully!\n\nTotal rows: {total_rows:,}",
                )
            else:
                self.root.after(
                    0,
                    messagebox.showwarning,
                    "Batch Transfer Complete",
                    f"Transfer complete with some errors:\n\n Successful: {len(successful)}\n Failed/Warning: {len(failed)}\n\nTotal rows transferred: {total_rows:,}\n\nCheck the preview area for details.",
                )

        except Exception as e:
            import traceback

            error_msg = (
                f"Batch data transfer failed:\n{str(e)}\n\n{traceback.format_exc()}"
            )
            self.root.after(0, messagebox.showerror, "Error", error_msg)
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"\n BATCH ERROR: {str(e)}\n",
            )
        finally:
            self.root.after(0, self.conversion_progress.stop)
            if self.conversion_stop_event.is_set():
                status_text = f"Transfer stopped: {len(successful)} completed, {total_rows:,} rows transferred"
                status_color = "orange"
            else:
                status_text = f"Batch transfer complete: {len(successful)} successful, {len(failed)} failed"
                status_color = "green" if len(failed) == 0 else "orange"
            self.root.after(
                0,
                self.conversion_status_label.config,
                {"text": status_text, "foreground": status_color},
            )
            self.root.after(0, self._end_conversion_operation)

    def clear_conversion_preview(self):
        """Clear conversion preview text"""
        self.conversion_preview_text.delete(1.0, tk.END)
        self.conversion_status_label.config(text="Ready", foreground="blue")

    def dump_schema_ddl(self):
        """Dump native CREATE TABLE/INDEX DDL for the selected tables (or all
        tables when none are selected), then offer to save it to a .sql file.

        Reuses the shared ``SchemaService.dump`` so the DDL is byte-for-byte the
        same as the CLI (``migrator dump``) and API (``GET .../dump``) surfaces.
        """
        source_conn_name = self.source_conn_combo.get()
        if not source_conn_name:
            messagebox.showwarning("Warning", "Please select source connection!")
            return
        if source_conn_name not in self.active_connections:
            messagebox.showerror("Error", "Connection not found!")
            return

        selected_tables = self.get_selected_tables()

        self._start_conversion_operation()
        self.conversion_status_label.config(
            text="Dumping schema DDL...", foreground="blue"
        )
        self.conversion_progress.start()
        self.conversion_preview_text.delete(1.0, tk.END)

        thread = threading.Thread(
            target=self._dump_schema_thread,
            args=(source_conn_name, selected_tables),
        )
        thread.daemon = True
        thread.start()

    def _dump_schema_thread(self, source_conn_name, selected_tables):
        """Worker for :meth:`dump_schema_ddl` (keeps the UI responsive)."""
        from schema_converter.service import SchemaService

        def _ui(fn, *a):
            self.root.after(0, fn, *a)

        try:
            svc = SchemaService(connect=lambda n: self.active_connections[n])
            statements: list[str] = []
            tables = selected_tables or [None]
            for tbl in tables:
                r = svc.dump(source_conn_name, table=tbl)
                if r.get("error"):
                    raise RuntimeError(r["error"])
                ddl = (r.get("ddl") or "").strip()
                if ddl:
                    statements.append(ddl)
            ddl_text = "\n\n".join(statements)

            scope = (
                f"{len(selected_tables)} selected table(s)"
                if selected_tables
                else "all tables"
            )
            _ui(
                self.conversion_preview_text.insert,
                tk.END,
                f"{'=' * 80}\nSCHEMA DDL DUMP — {source_conn_name} ({scope})\n{'=' * 80}\n\n",
            )
            _ui(
                self.conversion_preview_text.insert,
                tk.END,
                ddl_text + ("\n" if ddl_text else "(no DDL produced)\n"),
            )
            _ui(
                self.conversion_status_label.config,
                {"text": "Schema dump complete.", "foreground": "green"},
            )
            _ui(self._offer_save_dump, ddl_text, source_conn_name)
        except Exception as exc:  # noqa: BLE001
            _ui(
                self.conversion_preview_text.insert,
                tk.END,
                f"Schema dump failed: {exc}\n",
            )
            _ui(
                self.conversion_status_label.config,
                {"text": "Schema dump failed", "foreground": "red"},
            )
        finally:
            _ui(self._end_conversion_operation)
            _ui(self.conversion_progress.stop)

    def _offer_save_dump(self, ddl_text, source_conn_name):
        """Prompt to save dumped DDL to a .sql file (no-op if empty/declined)."""
        if not ddl_text:
            return
        try:
            from tkinter import filedialog

            path = filedialog.asksaveasfilename(
                title="Save schema dump",
                defaultextension=".sql",
                initialfile=f"{source_conn_name}_schema.sql",
                filetypes=[("SQL files", "*.sql"), ("All files", "*.*")],
            )
            if not path:
                return
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(ddl_text + "\n")
            self.conversion_status_label.config(
                text=f"Schema dump saved to {path}", foreground="green"
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Error", f"Failed to save dump:\n{exc}")

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

            self.conversion_status_label.config(
                text=f"Getting row counts for {len(selected_tables)} table(s)...",
                foreground="blue",
            )
            self.conversion_progress.start()

            # Clear preview area first
            self.conversion_preview_text.delete(1.0, tk.END)

            # Run in thread
            thread = threading.Thread(
                target=self._get_row_counts_thread,
                args=(source_manager, selected_tables),
            )
            thread.daemon = True
            thread.start()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to get row counts:\n{str(e)}")
            self.conversion_status_label.config(
                text="Failed to get row counts", foreground="red"
            )
            self._end_conversion_operation()

    def _get_row_counts_thread(self, source_manager, selected_tables):
        """Thread for getting row counts"""
        total_tables = len(selected_tables)
        results = []

        try:
            # Header
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"ROW COUNTS\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Checking {total_tables} table(s)\n",
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n\n"
            )

            cursor = source_manager.conn.cursor()

            for idx, table_name in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"\nPreview stopped by user at table {idx} of {total_tables}\n",
                    )
                    break

                try:
                    # Update status
                    self.root.after(
                        0,
                        self.conversion_status_label.config,
                        {
                            "text": f"Counting rows {idx} of {total_tables}: {table_name}",
                            "foreground": "blue",
                        },
                    )

                    # Get row count
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    result = cursor.fetchone()
                    row_count = result[0] if result else 0

                    results.append((table_name, row_count))

                    # Display result
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"[{idx}/{total_tables}] {table_name}: {row_count:,} rows\n",
                    )

                except Exception as e:
                    error_msg = str(e)
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"[{idx}/{total_tables}] {table_name}:  ERROR - {error_msg}\n",
                    )

            cursor.close()

            # Summary
            if results:
                total_rows = sum(count for _, count in results)
                self.root.after(
                    0, self.conversion_preview_text.insert, tk.END, f"\n{'=' * 80}\n"
                )
                self.root.after(
                    0, self.conversion_preview_text.insert, tk.END, f"SUMMARY\n"
                )
                self.root.after(
                    0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
                )
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    f"Total tables: {len(results)}\n",
                )
                self.root.after(
                    0,
                    self.conversion_preview_text.insert,
                    tk.END,
                    f"Total rows: {total_rows:,}\n",
                )

        except Exception as e:
            import traceback

            error_msg = (
                f"Failed to get row counts:\n{str(e)}\n\n{traceback.format_exc()}"
            )
            self.root.after(0, messagebox.showerror, "Error", error_msg)
        finally:
            self.root.after(0, self.conversion_progress.stop)
            status_text = (
                "Row counts stopped"
                if self.conversion_stop_event.is_set()
                else f"Row counts complete for {len(results)} table(s)"
            )
            status_color = "orange" if self.conversion_stop_event.is_set() else "green"
            self.root.after(
                0,
                self.conversion_status_label.config,
                {"text": status_text, "foreground": status_color},
            )
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

            self.conversion_status_label.config(
                text=f"Getting sample data for {len(selected_tables)} table(s)...",
                foreground="blue",
            )
            self.conversion_progress.start()

            # Clear preview area first
            self.conversion_preview_text.delete(1.0, tk.END)

            # Run in thread
            thread = threading.Thread(
                target=self._get_sample_data_thread,
                args=(source_manager, selected_tables),
            )
            thread.daemon = True
            thread.start()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to get sample data:\n{str(e)}")
            self.conversion_status_label.config(
                text="Failed to get sample data", foreground="red"
            )
            self._end_conversion_operation()

    def _get_sample_data_thread(self, source_manager, selected_tables):
        """Thread for getting sample data"""
        total_tables = len(selected_tables)
        results_count = 0

        try:
            # Header
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"SAMPLE DATA (First Row from Each Table)\n",
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Checking {total_tables} table(s)\n",
            )
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n\n"
            )

            cursor = source_manager.conn.cursor()

            for idx, table_name in enumerate(selected_tables, 1):
                # Check if stop was requested
                if self.conversion_stop_event.is_set():
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"\nPreview stopped by user at table {idx} of {total_tables}\n",
                    )
                    break

                try:
                    # Update status
                    self.root.after(
                        0,
                        self.conversion_status_label.config,
                        {
                            "text": f"Getting sample data {idx} of {total_tables}: {table_name}",
                            "foreground": "blue",
                        },
                    )

                    # Get one row with column names
                    if source_manager.db_type == "Oracle":
                        cursor.execute(f"SELECT * FROM {table_name} WHERE ROWNUM <= 1")
                    elif source_manager.db_type == "PostgreSQL":
                        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                    else:  # MySQL/MariaDB
                        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")

                    row = cursor.fetchone()

                    # Display table header
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"[{idx}/{total_tables}] {table_name}\n",
                    )
                    self.root.after(
                        0, self.conversion_preview_text.insert, tk.END, f"{'-' * 80}\n"
                    )

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

                            self.root.after(
                                0,
                                self.conversion_preview_text.insert,
                                tk.END,
                                f"  {col_name}: {display_value}\n",
                            )

                        results_count += 1
                    else:
                        self.root.after(
                            0,
                            self.conversion_preview_text.insert,
                            tk.END,
                            f"  (No data in table)\n",
                        )

                    self.root.after(
                        0, self.conversion_preview_text.insert, tk.END, f"\n"
                    )

                except Exception as e:
                    error_msg = str(e)
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"[{idx}/{total_tables}] {table_name}\n",
                    )
                    self.root.after(
                        0,
                        self.conversion_preview_text.insert,
                        tk.END,
                        f"   ERROR: {error_msg}\n\n",
                    )

            cursor.close()

            # Summary
            self.root.after(
                0, self.conversion_preview_text.insert, tk.END, f"{'=' * 80}\n"
            )
            self.root.after(
                0,
                self.conversion_preview_text.insert,
                tk.END,
                f"Sample data retrieved from {results_count} table(s)\n",
            )

        except Exception as e:
            import traceback

            error_msg = (
                f"Failed to get sample data:\n{str(e)}\n\n{traceback.format_exc()}"
            )
            self.root.after(0, messagebox.showerror, "Error", error_msg)
        finally:
            self.root.after(0, self.conversion_progress.stop)
            status_text = (
                "Sample data stopped"
                if self.conversion_stop_event.is_set()
                else f"Sample data complete for {results_count} table(s)"
            )
            status_color = "orange" if self.conversion_stop_event.is_set() else "green"
            self.root.after(
                0,
                self.conversion_status_label.config,
                {"text": status_text, "foreground": status_color},
            )
            self.root.after(0, self._end_conversion_operation)

    def stop_conversion_operation(self):
        """Stop the currently running conversion operation"""
        if self.conversion_running:
            if messagebox.askyesno(
                "Confirm Stop",
                "Are you sure you want to stop the current operation?\n\nNote: The current batch will complete before stopping.",
            ):
                self.conversion_stop_event.set()
                self.conversion_status_label.config(
                    text="Stopping operation...", foreground="orange"
                )
                self.conversion_preview_text.insert(
                    tk.END, "\nPreview stopped by user. Completing current batch...\n"
                )
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
        if self.dump_schema_btn is not None:
            self.dump_schema_btn.config(state=tk.DISABLED)
        self.convert_schema_btn.config(state=tk.DISABLED)
        self.transfer_data_btn.config(state=tk.DISABLED)
        self.compare_data_btn.config(state=tk.DISABLED)
        self.stop_conversion_btn.pack(side=tk.LEFT, padx=5, after=self.compare_data_btn)
        self.stop_conversion_btn.config(state=tk.NORMAL)

    def _end_conversion_operation(self):
        """Called when conversion operation completes or is stopped"""
        self.conversion_running = False
        self.conversion_stop_event.clear()
        # Re-enable operation buttons
        self.preview_schema_btn.config(state=tk.NORMAL)
        self.row_counts_btn.config(state=tk.NORMAL)
        self.sample_data_btn.config(state=tk.NORMAL)
        if self.dump_schema_btn is not None:
            self.dump_schema_btn.config(state=tk.NORMAL)
        self.convert_schema_btn.config(state=tk.NORMAL)
        self.transfer_data_btn.config(state=tk.NORMAL)
        self.compare_data_btn.config(state=tk.NORMAL)
        self.stop_conversion_btn.pack_forget()

    def get_dashboard_snapshot(self) -> dict:
        """Runtime state for the operational dashboard."""
        status_text = ""
        if self.conversion_status_label is not None:
            try:
                status_text = self.conversion_status_label.cget("text")
            except tk.TclError:
                status_text = ""

        source = ""
        target = ""
        if self.source_conn_combo is not None:
            try:
                source = self.source_conn_combo.get() or ""
            except tk.TclError:
                pass
        if self.target_conn_combo is not None:
            try:
                target = self.target_conn_combo.get() or ""
            except tk.TclError:
                pass

        selected = 0
        try:
            selected = len(self.get_selected_tables())
        except Exception:
            pass

        if self.conversion_running:
            overview = status_text or "Conversion or transfer in progress…"
        elif selected:
            overview = f"{selected} table(s) selected — {source or '?'} → {target or '?'}"
        else:
            overview = "Idle — pick source/target connections and tables to convert"

        return {
            "installed": True,
            "running": self.conversion_running,
            "status_text": status_text or ("Running…" if self.conversion_running else "Idle"),
            "source_connection": source,
            "target_connection": target,
            "selected_tables": selected,
            "overview": overview,
        }


def launch_ui(**_context) -> None:
    """Canonical desktop UI entry for Data Migration (``--ui`` and direct script)."""
    from common.ui.tk.launcher import launch_desktop_ui

    launch_desktop_ui(feature_module="migrator")


if __name__ == "__main__":
    launch_ui()
