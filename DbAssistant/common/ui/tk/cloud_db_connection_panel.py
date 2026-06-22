"""
Embedded cloud DB connection form for the Connections tab.

Self-contained: cloud API auth + DB login + load/save/connect without using
the direct 'Add database connection' form.
"""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Any, Callable, Optional

from common.cloud import CLOUD_PROVIDER_SCHEMAS, CloudConnectionManager, validate_cloud_profile
from common.cloud.profiles import PURPOSE_CONNECTIONS, TARGET_CLOUD_DB
from common.cloud.schemas import CLOUD_DB_SQL_FIELDS as _CLOUD_DB_FIELDS
from common.cloud.sql_bridge import enrich_sql_connection, resolve_aws_rds_sql_endpoint, sync_cloud_db_to_saved_connections
from common.db_manager import DatabaseManager
from common.ui.tk import make_scrollable
from common.ui.tk.cloud_connection_dialog import CloudConnectionWizardAdapter


def _build_resource_field_keys() -> frozenset[str]:
    keys = {f[1] for f in _CLOUD_DB_FIELDS}
    for schema in CLOUD_PROVIDER_SCHEMAS.values():
        for field in schema.get("resource", []):
            keys.add(field[1])
    return frozenset(keys)


def _build_auth_field_keys() -> frozenset[str]:
    keys: set[str] = set()
    for schema in CLOUD_PROVIDER_SCHEMAS.values():
        for field in schema.get("keys_auth", []):
            keys.add(field[1])
        for field in schema.get("pwd_auth", []):
            keys.add(field[1])
        for field in schema.get("sso_auth", {}).get("fields", []):
            keys.add(field[1])
    return frozenset(keys)


_RESOURCE_FIELD_KEYS = _build_resource_field_keys()
_AUTH_FIELD_KEYS = _build_auth_field_keys()


class CloudDBConnectionPanel:
    """Inline cloud DB section — independent of direct DB connection form."""

    _LBL_W = 28
    # Match width of the regular "Add Database Connection" form
    # (master_shell.py uses width=35 for host/port/user/etc.).
    _FIELD_W = 35
    # Outer LabelFrame width — keeps the form snug around the inputs.
    # ≈ (LBL_W + FIELD_W) chars * ~8 px + Browse button + padding.
    _FRAME_W = 560

    def __init__(
        self,
        parent: tk.Widget,
        root: tk.Tk,
        ui_font: tuple,
        title_font: tuple,
        connection_manager: Any,
        update_status: Callable[..., Any],
        on_register_connection: Optional[Callable[[str, Any], None]] = None,
    ) -> None:
        self.parent = parent
        self.root = root
        self.ui_font = ui_font
        self.title_font = title_font
        self.connection_manager = connection_manager
        self.update_status = update_status
        self.on_register_connection = on_register_connection
        self.cloud_manager = CloudConnectionManager()
        self.cloud_databases = self.cloud_manager.load_cloud_databases()
        self._editing_name: str = ""
        self._api_adapter = CloudConnectionWizardAdapter(
            root,
            ui_font,
            update_status=update_status,
            cloud_manager=self.cloud_manager,
            cloud_databases=self.cloud_databases,
            connection_manager=connection_manager,
        )
        self.provider_var = tk.StringVar(value="AWS")
        self.entries: dict[str, Any] = {}
        self.auth_nb: Optional[ttk.Notebook] = None
        self._resource_frame: Optional[ttk.Frame] = None
        self._auth_body: Optional[ttk.Frame] = None
        self._status_var = tk.StringVar(value="")
        self._status_lbl: Optional[ttk.Label] = None
        self._editing_lbl: Optional[ttk.Label] = None

    def build(self, expanded: bool | None = None) -> None:
        from common.ui.tk.widgets import make_collapsible_section

        if expanded is None:
            try:
                from common.ui.shared import specs

                expanded = not specs.connection_section_collapsed("cloud")
            except Exception:
                expanded = False
        content = make_collapsible_section(
            self.parent,
            "Add or select cloud database connection",
            self.title_font,
            expanded=expanded,
        )
        shell = ttk.Frame(content)
        shell.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        hint = ttk.Label(
            shell,
            text=(
                "Register cloud API credentials, then enter database login in Cloud resource. "
                "Use Load Saved to edit an existing profile, or Connect to add it to active connections."
            ),
            foreground="gray",
            font=(self.ui_font[0], max(8, self.ui_font[1] - 1)),
            justify=tk.LEFT,
        )
        hint.pack(anchor=tk.W, pady=(0, 6))

        self._editing_lbl = ttk.Label(
            shell,
            text="",
            font=(self.ui_font[0], self.ui_font[1], "italic"),
            foreground="#1565c0",
        )
        self._editing_lbl.pack(anchor=tk.W, pady=(0, 4))

        # Master grouping frame — visually wraps every cloud-DB widget
        # (provider, resource, auth, status, buttons) so the user sees them
        # as one cohesive "cloud DB connection" unit.
        master = ttk.LabelFrame(shell, text="Cloud DB connection", padding=10)
        master.pack(anchor=tk.W, fill=tk.Y, expand=True)

        prov_row = ttk.Frame(master)
        prov_row.pack(anchor=tk.W, pady=(0, 6))
        ttk.Label(prov_row, text="Cloud provider:", width=self._LBL_W, anchor=tk.W).pack(
            side=tk.LEFT
        )
        prov_combo = ttk.Combobox(
            prov_row,
            textvariable=self.provider_var,
            values=sorted(CLOUD_PROVIDER_SCHEMAS.keys()),
            state="readonly",
            width=self._FIELD_W - 2,
        )
        prov_combo.pack(side=tk.LEFT)
        prov_combo.bind("<<ComboboxSelected>>", lambda _e: self._rebuild_auth_only())

        # Cloud resource + DB login — always visible (not inside scroll).
        self._resource_frame = ttk.LabelFrame(master, text="Cloud resource", padding=6)
        self._resource_frame.pack(anchor=tk.W, pady=(0, 8))

        # Cloud API authentication notebook (scrollable).
        auth_outer = ttk.LabelFrame(master, text="Cloud API authentication", padding=4)
        auth_outer.pack(anchor=tk.W, fill=tk.Y, expand=True, pady=(0, 8))

        auth_scroll_wrap = ttk.Frame(auth_outer)
        auth_scroll_wrap.pack(fill=tk.BOTH, expand=True)

        self._auth_body = make_scrollable(auth_scroll_wrap)

        self._build_auth_fields()

        # Status line + action buttons — also inside master so the whole
        # cloud-DB section reads as a single bordered unit.  Note: we no
        # longer pre-fill sql_port with 3306; the port is auto-filled from
        # the engine default when the user picks a DB type.
        self._status_lbl = ttk.Label(
            master,
            textvariable=self._status_var,
            foreground="gray",
            font=(self.ui_font[0], max(8, self.ui_font[1] - 1)),
            wraplength=self._FRAME_W - 40,
            justify=tk.LEFT,
        )
        self._status_lbl.pack(anchor=tk.W, pady=(0, 6))

        btn_row1 = ttk.Frame(master)
        btn_row1.pack(anchor=tk.W, pady=(0, 4))
        btn_row2 = ttk.Frame(master)
        btn_row2.pack(anchor=tk.W)

        ttk.Button(
            btn_row1, text="Load Saved", command=self._load_saved_dialog, width=14
        ).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(
            btn_row1, text="Save", command=self._save_connection, width=10
        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(
            btn_row1, text="Test Cloud Login", command=self._test_cloud_login, width=16
        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(
            btn_row1, text="Test DB Connection", command=self._test_db_connection, width=16
        ).pack(side=tk.LEFT, padx=6)

        ttk.Button(
            btn_row2,
            text="Connect",
            command=self._connect_cloud_db,
            style="Primary.TButton",
            width=12,
        ).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(
            btn_row2, text="Resolve SQL Endpoint", command=self._resolve_sql_endpoint, width=18
        ).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_row2, text="Clear", command=self._clear_form, width=10).pack(
            side=tk.LEFT, padx=6
        )

        self._build_resource_fields()

    def _field_tuple(self, field: tuple) -> tuple:
        """Normalize field definitions that may include optional choices."""
        if len(field) > 4:
            return (*field[:4], field[4])
        return field

    def _snapshot_form(self) -> dict[str, str]:
        """Read current form values, skipping destroyed widgets."""
        out: dict[str, str] = {}
        for key, widget in list(self.entries.items()):
            try:
                if not widget.winfo_exists():
                    continue
                out[key] = self._get_entry(key)
            except tk.TclError:
                continue
        return out

    def _drop_entry_keys(self, keys: frozenset[str]) -> None:
        for key in keys:
            self.entries.pop(key, None)

    def _restore_form(self, preserved: dict[str, str]) -> None:
        for key, val in preserved.items():
            if val and key in self.entries:
                self._set_entry(key, val)

    def _build_resource_fields(self, preserved: Optional[dict[str, str]] = None) -> None:
        if self._resource_frame is None:
            return
        if preserved is None:
            preserved = self._snapshot_form()
        for child in self._resource_frame.winfo_children():
            child.destroy()
        self._drop_entry_keys(_RESOURCE_FIELD_KEYS)

        provider = self.provider_var.get()
        schema = CLOUD_PROVIDER_SCHEMAS.get(provider, {})
        for field in schema.get("resource", []):
            self._add_field(self._resource_frame, self._field_tuple(field))
        for field in _CLOUD_DB_FIELDS:
            self._add_field(self._resource_frame, self._field_tuple(field))

        # Auto-fill SQL port from the engine default when DB type changes,
        # but only if the port field is currently empty (don't clobber input).
        db_type_widget = self.entries.get("sql_db_type")
        if isinstance(db_type_widget, ttk.Combobox):
            db_type_widget.bind(
                "<<ComboboxSelected>>",
                lambda _e: self._sync_port_to_db_type(),
            )

        self._restore_form(preserved)

    def _sync_port_to_db_type(self) -> None:
        """Fill SQL port with the engine default when empty."""
        from common.ui.tk.master_shell import DatabaseConfig

        if self._get_entry("sql_port"):
            return
        db_type = self._get_entry("sql_db_type")
        if not db_type:
            return
        port = DatabaseConfig.get_default_port(db_type)
        if port:
            self._set_entry("sql_port", port)

    def _rebuild_auth_only(self) -> None:
        preserved = self._snapshot_form()
        self._build_resource_fields(preserved)
        self._build_auth_fields(preserved)

    def _build_auth_fields(self, preserved: Optional[dict[str, str]] = None) -> None:
        if self._auth_body is None:
            return
        if preserved is None:
            preserved = self._snapshot_form()
        for child in self._auth_body.winfo_children():
            child.destroy()
        self.auth_nb = None
        self._drop_entry_keys(_AUTH_FIELD_KEYS)

        provider = self.provider_var.get()
        schema = CLOUD_PROVIDER_SCHEMAS.get(provider)
        if not schema:
            ttk.Label(self._auth_body, text="Unknown provider.").pack()
            return

        self.auth_nb = ttk.Notebook(self._auth_body)
        self.auth_nb.pack(fill=tk.BOTH, expand=True)

        keys_tab = ttk.Frame(self.auth_nb, padding=4)
        pwd_tab = ttk.Frame(self.auth_nb, padding=4)
        sso_tab = ttk.Frame(self.auth_nb, padding=4)
        sso_schema = schema.get("sso_auth", {})
        sso_label = sso_schema.get("tab_label", "SSO / OIDC")
        sso_fields = sso_schema.get("fields", [])

        self.auth_nb.add(keys_tab, text=" Access keys / tokens ")
        self.auth_nb.add(pwd_tab, text=" Username / password ")
        self.auth_nb.add(sso_tab, text=f" {sso_label} ")

        for field in schema["keys_auth"]:
            self._add_field(keys_tab, field)
        for field in schema["pwd_auth"]:
            self._add_field(pwd_tab, field)

        if provider == "AWS":
            sso_info = (
                "Access keys, IAM Identity Center (Start URL + SSO Region), or "
                "leave Start URL blank and use Test Cloud Login for `aws login`."
            )
        elif provider == "Azure":
            sso_info = "Test Cloud Login runs `az login` in the browser."
        elif provider == "GCP":
            sso_info = "Service account key or gcloud ADC / workforce identity."
        else:
            sso_info = "SSO / OIDC provider fields."

        ttk.Label(
            sso_tab,
            text=sso_info,
            foreground="gray",
            font=("Arial", 8),
            wraplength=self._FRAME_W - 60,
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(2, 6), padx=2)
        for field in sso_fields:
            self._add_field(sso_tab, field)

        self._restore_form(preserved)

        saved_mode = preserved.get("_auth_mode_hint", "")
        if self.auth_nb and saved_mode == "sso":
            self.auth_nb.select(2)
        elif self.auth_nb and saved_mode == "pwd":
            self.auth_nb.select(1)
        elif self.auth_nb:
            self.auth_nb.select(0)

    def _add_field(self, parent: ttk.Frame, field_tuple: tuple) -> None:
        lbl_text, key, show, *rest = field_tuple
        help_text = rest[0] if rest else ""
        choices = rest[1] if len(rest) > 1 else None

        grp = ttk.Frame(parent)
        grp.pack(fill=tk.X, pady=(2, 0), padx=2)

        if show == "multi" and not choices:
            ttk.Label(grp, text=lbl_text, anchor=tk.W).pack(fill=tk.X, padx=2, pady=(2, 2))
            widget = tk.Text(grp, height=5, wrap="word", font=self.ui_font)
            widget.pack(fill=tk.X, expand=True, padx=2)
            self.entries[key] = widget
            return

        row = ttk.Frame(grp)
        row.pack(fill=tk.X)
        ttk.Label(row, text=lbl_text, width=self._LBL_W, anchor=tk.W).pack(side=tk.LEFT)

        if choices:
            var = tk.StringVar(value=choices[0])
            widget = ttk.Combobox(
                row,
                textvariable=var,
                values=choices,
                state="readonly",
                width=self._FIELD_W - 2,
            )
            widget.pack(side=tk.LEFT)
            self.entries[key] = widget
        else:
            kw: dict = {"width": self._FIELD_W}
            if show == "*":
                kw["show"] = "*"
            widget = ttk.Entry(row, **kw)
            widget.pack(side=tk.LEFT)
            self.entries[key] = widget

        if help_text:
            ttk.Label(
                grp,
                text=help_text,
                foreground="#888888",
                font=("Arial", 7),
                wraplength=self._FRAME_W - 40,
                justify=tk.LEFT,
            ).pack(anchor=tk.W, padx=(self._LBL_W * 7, 0), pady=(0, 2))

    def _get_entry(self, key: str) -> str:
        w = self.entries.get(key)
        if w is None:
            return ""
        try:
            if not w.winfo_exists():
                return ""
        except tk.TclError:
            return ""
        if isinstance(w, tk.Text):
            return w.get("1.0", tk.END).strip()
        return w.get().strip()

    def _set_entry(self, key: str, value: str) -> None:
        w = self.entries.get(key)
        if w is None:
            return
        if isinstance(w, ttk.Combobox):
            w.set(value or w.cget("values")[0] if w.cget("values") else value)
        elif isinstance(w, tk.Text):
            w.delete("1.0", tk.END)
            if value:
                w.insert("1.0", value)
        else:
            w.delete(0, tk.END)
            if value:
                w.insert(0, value)

    def _collect(self) -> dict:
        provider = self.provider_var.get()
        schema = CLOUD_PROVIDER_SCHEMAS.get(provider, {})
        auth_mode = "keys"
        if self.auth_nb is not None:
            tab = self.auth_nb.index(self.auth_nb.select())
            auth_mode = "keys" if tab == 0 else "sso" if tab == 2 else "pwd"

        flat = {key: self._get_entry(key) for key in self.entries}

        data: dict = {
            "provider": provider,
            "auth_mode": auth_mode,
            "mfa_enabled": False,
            "mfa_type": "",
            "monitoring": False,
            "purpose": PURPOSE_CONNECTIONS,
            "target_kind": TARGET_CLOUD_DB,
        }
        for field_list in (
            schema.get("resource", []),
            schema.get("keys_auth", []),
            schema.get("pwd_auth", []),
            schema.get("sso_auth", {}).get("fields", []),
        ):
            for f in field_list:
                data[f[1]] = flat.get(f[1], "")

        data["sql_connection"] = {
            "db_type": flat.get("sql_db_type", ""),
            "host": flat.get("sql_host", ""),
            "port": flat.get("sql_port", ""),
            "service_or_db": flat.get("sql_database", ""),
            "username": flat.get("sql_username", ""),
            "password": flat.get("sql_password", ""),
        }
        return data

    def _profile_to_form(self, profile: dict) -> None:
        provider = profile.get("provider", "AWS")
        if provider in CLOUD_PROVIDER_SCHEMAS:
            self.provider_var.set(provider)

        # Loading a saved profile must not preserve the previous form snapshot.
        # Provider schemas share some field names and differ on others; carrying
        # the current UI values across here can leave stale AWS/Azure/GCP values
        # in fields that the selected saved profile does not define.
        mode = profile.get("auth_mode", "keys")
        self._build_resource_fields({})
        self._build_auth_fields({"_auth_mode_hint": mode})

        for key, val in profile.items():
            if key in self.entries and val is not None:
                self._set_entry(key, str(val))

        sql = profile.get("sql_connection") or {}
        mapping = {
            "sql_db_type": sql.get("db_type", ""),
            "sql_host": sql.get("host", ""),
            "sql_port": str(sql.get("port", "") or ""),
            "sql_database": sql.get("service_or_db", "") or sql.get("database", ""),
            "sql_username": sql.get("username", ""),
            "sql_password": sql.get("password", ""),
        }
        for key, val in mapping.items():
            self._set_entry(key, val)

        if self.auth_nb:
            self.auth_nb.select(2 if mode == "sso" else 1 if mode == "pwd" else 0)

        self._editing_name = profile.get("display_name", "")
        if self._editing_lbl:
            if self._editing_name:
                self._editing_lbl.config(
                    text=f"Editing saved profile: {self._editing_name}"
                )
            else:
                self._editing_lbl.config(text="")

    def _set_status(self, message: str, kind: str = "info") -> None:
        colours = {
            "info": "gray",
            "ok": "#2e7d32",
            "warn": "#e65100",
            "error": "#c62828",
        }
        self._status_var.set(message)
        if self._status_lbl is not None:
            self._status_lbl.config(foreground=colours.get(kind, "gray"))

    def _validate(self, data: dict) -> Optional[str]:
        provider = data.get("provider", "")
        schema = CLOUD_PROVIDER_SCHEMAS.get(provider)
        if not schema:
            return "Select a cloud provider."
        err = validate_cloud_profile(
            data,
            provider,
            schema,
            require_db_identifier=True,
            target_kind=TARGET_CLOUD_DB,
        )
        if err:
            return err
        sql = data.get("sql_connection") or {}
        if not (sql.get("username") or "").strip():
            return "DB username is required (Cloud resource section)."
        if not (sql.get("password") or "").strip():
            return "DB password is required (Cloud resource section)."
        if not (sql.get("host") or "").strip():
            return "SQL host is required — use Resolve SQL Endpoint for AWS RDS."
        if not (sql.get("db_type") or "").strip():
            return "DB type is required."
        if not (sql.get("port") or "").strip():
            return "SQL port is required (pick a DB type to auto-fill the engine default)."
        return None

    def _sql_params(self, data: dict) -> tuple[str, str, str, str, str, str]:
        """Return engine-aware connect parameters.

        Port is taken as-is from the form (now validated by ``_validate``);
        no hidden 3306 fallback — engine defaults are filled into the form
        when the user picks a DB type via the combobox.
        """
        from common.ui.tk.master_shell import DatabaseConfig

        data = enrich_sql_connection(data)
        sql = data.get("sql_connection") or {}
        db_type = (sql.get("db_type") or "").strip()
        port = (sql.get("port") or "").strip()
        if not port and db_type:
            port = DatabaseConfig.get_default_port(db_type)
        return (
            db_type,
            (sql.get("host") or "").strip(),
            port,
            (sql.get("username") or "").strip(),
            sql.get("password") or "",
            (sql.get("service_or_db") or "").strip(),
        )

    def _load_saved_dialog(self) -> None:
        self.cloud_databases = self.cloud_manager.load_cloud_databases()
        if not self.cloud_databases:
            messagebox.showinfo(
                "Load saved",
                "No saved cloud connections yet.\nFill the form and click Save.",
                parent=self.root,
            )
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Saved cloud connections")
        dialog.geometry("720x420")
        dialog.transient(self.root)

        tree_frame = ttk.Frame(dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        tree = ttk.Treeview(
            tree_frame,
            columns=("Provider", "Region", "Resource", "DB user", "SQL host"),
            yscrollcommand=vsb.set,
        )
        vsb.config(command=tree.yview)
        tree.heading("#0", text="Name")
        for col in ("Provider", "Region", "Resource", "DB user", "SQL host"):
            tree.heading(col, text=col)
            tree.column(col, width=100)
        tree.column("#0", width=140)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(fill=tk.BOTH, expand=True)

        for name, prof in sorted(self.cloud_databases.items()):
            sql = prof.get("sql_connection") or {}
            tree.insert(
                "",
                tk.END,
                text=name,
                values=(
                    prof.get("provider", ""),
                    prof.get("region", ""),
                    prof.get("resource_name", ""),
                    sql.get("username", ""),
                    sql.get("host", ""),
                ),
            )

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        def _selected_name() -> str:
            sel = tree.selection()
            if not sel:
                return ""
            return tree.item(sel[0])["text"]

        def _load():
            name = _selected_name()
            if not name:
                messagebox.showwarning("Load", "Select a connection.", parent=dialog)
                return
            prof = self.cloud_databases.get(name)
            if not prof:
                return
            self._profile_to_form(prof)
            self._set_status(f"Loaded cloud profile '{name}'.", "ok")
            dialog.destroy()

        def _delete():
            name = _selected_name()
            if not name:
                messagebox.showwarning("Delete", "Select a connection.", parent=dialog)
                return
            if not messagebox.askyesno(
                "Delete", f"Delete cloud profile '{name}'?", parent=dialog
            ):
                return
            self.cloud_databases.pop(name, None)
            self.cloud_manager.save_cloud_databases(self.cloud_databases)
            if self.connection_manager.connection_exists(name):
                self.connection_manager.delete_connection(name)
            tree.delete(tree.selection()[0])
            self._api_adapter.cloud_databases = self.cloud_databases
            if self._editing_name == name:
                self._clear_form()
            self.update_status(f"Deleted cloud profile '{name}'.", "info")

        ttk.Button(btn_frame, text="Load into form", command=_load, width=16).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(btn_frame, text="Delete", command=_delete, width=10).pack(
            side=tk.LEFT, padx=5
        )
        ttk.Button(btn_frame, text="Close", command=dialog.destroy, width=10).pack(
            side=tk.LEFT, padx=5
        )

    def _connect_cloud_db(self) -> None:
        data = self._collect()
        err = self._validate(data)
        if err:
            messagebox.showwarning("Connect", err, parent=self.root)
            return

        conn_name = (data.get("display_name") or "").strip()
        db_type, host, port, user, password, database = self._sql_params(data)

        self._set_status(f"Connecting to {conn_name} ({host})…", "info")
        self.root.update_idletasks()

        def _run():
            try:
                mgr = DatabaseManager(db_type)
                conn = mgr.connect(
                    host=host,
                    port=int(port),
                    username=user,
                    password=password,
                    database=database,
                    service=database,
                )
                if conn is None:
                    raise RuntimeError(f"Could not connect to {db_type} at {host}:{port}")

                def _done():
                    if self.on_register_connection:
                        self.on_register_connection(conn_name, mgr)
                    self._set_status(
                        f"Connected '{conn_name}' — added to active connections.",
                        "ok",
                    )
                    self.update_status(
                        f"Cloud DB '{conn_name}' connected.", "success"
                    )

                self.root.after(0, _done)
            except Exception as exc:
                err_msg = str(exc)

                def _fail(message: str = err_msg):
                    self._set_status(message, "error")
                    messagebox.showerror("Connect failed", message, parent=self.root)

                self.root.after(0, _fail)

        threading.Thread(target=_run, daemon=True).start()

    def _test_cloud_login(self) -> None:
        data = self._collect()
        err = validate_cloud_profile(
            data,
            data.get("provider", ""),
            CLOUD_PROVIDER_SCHEMAS.get(data.get("provider", ""), {}),
            require_db_identifier=True,
            target_kind=TARGET_CLOUD_DB,
        )
        if err:
            messagebox.showwarning("Cloud login", err, parent=self.root)
            return

        self._set_status("Testing cloud login…", "info")
        self.root.update_idletasks()

        def _run():
            from common.ui.tk.monitor.server_monitor.server_monitor_ui import ServerMonitorUI

            msg, status = ServerMonitorUI._run_cloud_api_test(self._api_adapter, data)
            colour = {"ok": "ok", "auth": "warn", "sso": "info", "error": "error"}.get(
                status, "info"
            )

            def _done():
                self._set_status(msg, colour)
                self.update_status(msg, "success" if status == "ok" else "info")

            self.root.after(0, _done)

        threading.Thread(target=_run, daemon=True).start()

    def _resolve_sql_endpoint(self) -> None:
        data = self._collect()
        if (data.get("provider") or "").upper() != "AWS":
            messagebox.showinfo(
                "Resolve endpoint",
                "Auto-resolve is supported for AWS RDS only.",
                parent=self.root,
            )
            return
        resolved = resolve_aws_rds_sql_endpoint(data)
        if not resolved:
            self._set_status("Could not resolve RDS endpoint.", "error")
            return
        self._set_entry("sql_host", resolved.get("host", ""))
        self._set_entry("sql_port", resolved.get("port", ""))
        self._set_entry("sql_db_type", resolved.get("db_type", ""))
        self._set_status(
            f"Resolved: {resolved.get('host')}:{resolved.get('port')}", "ok"
        )

    def _test_db_connection(self) -> None:
        data = self._collect()
        sql = data.get("sql_connection") or {}
        if not (sql.get("username") or "").strip():
            messagebox.showwarning(
                "DB test", "Enter DB username in Cloud resource.", parent=self.root
            )
            return
        if not (sql.get("password") or "").strip():
            messagebox.showwarning(
                "DB test", "Enter DB password in Cloud resource.", parent=self.root
            )
            return

        db_type, host, port, user, password, database = self._sql_params(data)
        if not host:
            messagebox.showwarning(
                "DB test", "Enter SQL host or use Resolve.", parent=self.root
            )
            return

        self._set_status(f"Testing SQL to {host}:{port}…", "info")

        def _run():
            try:
                mgr = DatabaseManager(db_type)
                conn = mgr.connect(
                    host=host,
                    port=int(port),
                    username=user,
                    password=password,
                    database=database,
                    service=database,
                )
                ok = conn is not None
                if ok:
                    mgr.disconnect()
                msg = f"✓ DB login OK at {host}:{port}" if ok else f"✗ DB login failed"
                kind = "ok" if ok else "error"
            except Exception as exc:
                msg = f"✗ {exc}"
                kind = "error"

            def _done():
                self._set_status(msg, kind)

            self.root.after(0, _done)

        threading.Thread(target=_run, daemon=True).start()

    def _save_connection(self) -> None:
        data = self._collect()
        err = self._validate(data)
        if err:
            messagebox.showwarning("Save", err, parent=self.root)
            return

        display_name = data.get("display_name", "").strip()
        if (
            display_name in self.cloud_databases
            and display_name != self._editing_name
            and not messagebox.askyesno(
                "Overwrite?", f"'{display_name}' exists. Overwrite?", parent=self.root
            )
        ):
            return

        if self._editing_name and self._editing_name != display_name:
            self.cloud_databases.pop(self._editing_name, None)

        data = enrich_sql_connection(data)
        self.cloud_databases[display_name] = data
        if not self.cloud_manager.save_cloud_databases(self.cloud_databases):
            messagebox.showerror("Save failed", "Could not write cloud file.", parent=self.root)
            return

        sync_cloud_db_to_saved_connections(data, self.connection_manager)
        self.connection_manager.connections = self.connection_manager.load_connections()
        self._api_adapter.cloud_databases = self.cloud_databases
        self._editing_name = display_name
        if self._editing_lbl:
            self._editing_lbl.config(text=f"Editing saved profile: {display_name}")

        msg = f"Saved cloud profile '{display_name}'."
        self._set_status(msg, "ok")
        self.update_status(msg, "success")
        messagebox.showinfo("Saved", msg + "\nUse Connect to add to active connections.", parent=self.root)

    def _clear_form(self) -> None:
        self._editing_name = ""
        if self._editing_lbl:
            self._editing_lbl.config(text="")
        self.provider_var.set("AWS")
        self._build_resource_fields({})
        self._build_auth_fields({})
        self._set_status("", "info")
