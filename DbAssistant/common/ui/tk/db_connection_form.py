"""Reusable "Add database connection" dialog.

A single, self-contained Toplevel form that builds the standard database
connection fields (type / host / port / service-or-db / user / password +
capability-driven SSL/TLS) and **saves into whichever**
:class:`~common.connection_manager.ConnectionManager` it is handed.

This is the shared piece that lets the Monitor tab offer an "Add Database"
flow identical to the Connections tab without duplicating connection logic:
the Connections tab points it at the core store, the Monitor tab points it at
its isolated ``monitor_db.json`` store. All persistence, encryption and the
data model stay in ``ConnectionManager``; this module only renders the form
and calls ``manager.add_connection(...)`` / ``DatabaseManager.connect(...)``.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Callable, Optional

from common.database_registry import DatabaseRegistry
from common.ui.tk.theme import ColorTheme, default_ui_font
from common.ui.tk.widgets import make_scrollable

_LOC_LOCAL = "Localhost / direct"
_LOC_REMOTE = "Remote host (SSH tunnel)"


def _service_field_label(db_type: str) -> str:
    return "Service name" if db_type == "Oracle" else "Database name"


class DBConnectionFormDialog:
    """Modal dialog that creates/saves a DB connection into ``manager``."""

    def __init__(
        self,
        parent,
        manager,
        *,
        title: str = "Add database connection",
        theme=None,
        on_saved: Optional[Callable[[str], None]] = None,
        remote: bool = False,
    ):
        self.parent = parent
        self.manager = manager
        self.theme = theme or ColorTheme
        self.on_saved = on_saved
        self.ui_font = default_ui_font()
        self.saved_name: Optional[str] = None
        self._remote = bool(remote)

        self.win = tk.Toplevel(parent)
        self.win.title(title)
        self.win.transient(parent)
        self.win.geometry("520x680")
        try:
            self.win.configure(bg=self.theme.BG_MAIN)
        except Exception:
            pass

        self._build()
        self._on_db_type_changed()
        self._on_location_changed()
        self._center()
        self.win.grab_set()

    # ------------------------------------------------------------------ #
    def _build(self) -> None:
        # Fixed action bar pinned to the bottom so Test/Save/Cancel stay visible
        # no matter how tall the (scrollable) form grows in remote/SSH mode.
        btns = ttk.Frame(self.win)
        btns.pack(side=tk.BOTTOM, fill=tk.X, padx=14, pady=12)
        ttk.Button(btns, text="Test Connection", command=self._test, width=16).pack(
            side=tk.LEFT, padx=(0, 6))
        ttk.Button(btns, text="Save", command=self._save, width=12,
                   style="Success.TButton").pack(side=tk.LEFT, padx=6)
        ttk.Button(btns, text="Cancel", command=self.win.destroy, width=12).pack(
            side=tk.LEFT, padx=6)

        body = make_scrollable(self.win, bg=self.theme.BG_MAIN)

        ttk.Label(
            body, text="Add database connection",
            font=(self.ui_font[0], self.ui_font[1] + 2, "bold"),
        ).pack(anchor=tk.W, padx=14, pady=(12, 4))

        # Connection location: localhost/direct vs remote host over an SSH tunnel.
        loc_row = ttk.Frame(body)
        loc_row.pack(anchor=tk.W, fill=tk.X, padx=14, pady=(0, 2))
        ttk.Label(
            loc_row, text="Connection location:",
            font=(self.ui_font[0], self.ui_font[1], "bold"),
        ).pack(side=tk.LEFT)
        self.location_combo = ttk.Combobox(
            loc_row, width=26, state="readonly", values=[_LOC_LOCAL, _LOC_REMOTE],
        )
        self.location_combo.set(_LOC_REMOTE if self._remote else _LOC_LOCAL)
        self.location_combo.pack(side=tk.LEFT, padx=(6, 0))
        self.location_combo.bind(
            "<<ComboboxSelected>>", lambda e: self._on_location_changed())

        form = ttk.Frame(body)
        form.pack(fill=tk.BOTH, expand=True, padx=14, pady=6)
        self._form = form

        # Connection name
        ttk.Label(form, text="Connection name:",
                  font=(self.ui_font[0], self.ui_font[1], "bold")).grid(
            row=0, column=0, sticky=tk.W, padx=5, pady=(0, 5))
        self.name_entry = ttk.Entry(form, width=35)
        self.name_entry.grid(row=0, column=1, sticky=tk.W, padx=5, pady=(0, 5))

        # Database type
        ttk.Label(form, text="Database Type:",
                  font=(self.ui_font[0], self.ui_font[1], "bold")).grid(
            row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.db_type_combo = ttk.Combobox(form, width=33, state="readonly")
        self.db_type_combo["values"] = DatabaseRegistry.get_all_types()
        if self.db_type_combo["values"]:
            self.db_type_combo.current(0)
        self.db_type_combo.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        self.db_type_combo.bind(
            "<<ComboboxSelected>>", lambda e: self._on_db_type_changed())

        # Host
        ttk.Label(form, text="Host:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.host_entry = ttk.Entry(form, width=35)
        self.host_entry.insert(0, "localhost")
        self.host_entry.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)

        # Port
        ttk.Label(form, text="Port:").grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        self.port_entry = ttk.Entry(form, width=35)
        self.port_entry.grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)

        # Service / database (dynamic label)
        self.service_label = ttk.Label(form, text="Database name:")
        self.service_label.grid(row=4, column=0, sticky=tk.W, padx=5, pady=5)
        self.service_entry = ttk.Entry(form, width=35)
        self.service_entry.grid(row=4, column=1, sticky=tk.W, padx=5, pady=5)

        # Username
        ttk.Label(form, text="Username:").grid(row=5, column=0, sticky=tk.W, padx=5, pady=5)
        self.user_entry = ttk.Entry(form, width=35)
        self.user_entry.grid(row=5, column=1, sticky=tk.W, padx=5, pady=5)

        # Password
        ttk.Label(form, text="Password:").grid(row=6, column=0, sticky=tk.W, padx=5, pady=5)
        self.password_entry = ttk.Entry(form, width=35, show="*")
        self.password_entry.grid(row=6, column=1, sticky=tk.W, padx=5, pady=5)

        # Save password
        self.save_password_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            form, text="Save password (encrypted)", variable=self.save_password_var,
        ).grid(row=7, column=1, sticky=tk.W, padx=5, pady=(0, 4))

        # --- capability-driven SSL / TLS widgets (created once, gridded on demand)
        self.ssl_mode_label = ttk.Label(form, text="SSL mode:")
        self.ssl_mode_combo = ttk.Combobox(form, width=33, state="readonly")
        self.ssl_mode_combo.set("disable")
        self.ssl_ca_label = ttk.Label(form, text="SSL CA file:")
        self.ssl_ca_entry = ttk.Entry(form, width=35)
        self.ssl_cert_label = ttk.Label(form, text="SSL client cert:")
        self.ssl_cert_entry = ttk.Entry(form, width=35)
        self.ssl_key_label = ttk.Label(form, text="SSL client key:")
        self.ssl_key_entry = ttk.Entry(form, width=35)
        self.wallet_label = ttk.Label(form, text="Oracle wallet dir:")
        self.wallet_entry = ttk.Entry(form, width=35)
        self.mongo_tls_var = tk.BooleanVar(value=False)
        self.mongo_tls_cb = ttk.Checkbutton(
            form, text="Use TLS (MongoDB / DocumentDB)", variable=self.mongo_tls_var)
        self.mongo_tls_ca_label = ttk.Label(form, text="TLS CA file:")
        self.mongo_tls_ca_entry = ttk.Entry(form, width=35)

        # --- SSH tunnel section (shown only for "Remote host") ---
        self.ssh_frame = ttk.LabelFrame(
            body, text="SSH tunnel (reach DB via a bastion / jump host)", padding=8)
        ttk.Label(
            self.ssh_frame,
            text=("Host/Port above are the DB endpoint as seen FROM the SSH host "
                  "(often localhost)."),
            foreground="gray", font=(self.ui_font[0], max(8, self.ui_font[1] - 1)),
            wraplength=460, justify=tk.LEFT,
        ).grid(row=0, column=0, columnspan=3, sticky=tk.W, pady=(0, 4))

        ttk.Label(self.ssh_frame, text="SSH host:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=3)
        self.ssh_host_entry = ttk.Entry(self.ssh_frame, width=33)
        self.ssh_host_entry.grid(row=1, column=1, sticky=tk.W, padx=5, pady=3)

        ttk.Label(self.ssh_frame, text="SSH port:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=3)
        self.ssh_port_entry = ttk.Entry(self.ssh_frame, width=33)
        self.ssh_port_entry.insert(0, "22")
        self.ssh_port_entry.grid(row=2, column=1, sticky=tk.W, padx=5, pady=3)

        ttk.Label(self.ssh_frame, text="SSH username:").grid(row=3, column=0, sticky=tk.W, padx=5, pady=3)
        self.ssh_user_entry = ttk.Entry(self.ssh_frame, width=33)
        self.ssh_user_entry.grid(row=3, column=1, sticky=tk.W, padx=5, pady=3)

        ttk.Label(self.ssh_frame, text="SSH auth:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=3)
        auth_row = ttk.Frame(self.ssh_frame)
        auth_row.grid(row=4, column=1, sticky=tk.W, padx=5, pady=3)
        self.ssh_auth_var = tk.StringVar(value="password")
        ttk.Radiobutton(auth_row, text="Password", value="password",
                        variable=self.ssh_auth_var,
                        command=self._update_ssh_auth_visibility).pack(side=tk.LEFT)
        ttk.Radiobutton(auth_row, text="Key file", value="key",
                        variable=self.ssh_auth_var,
                        command=self._update_ssh_auth_visibility).pack(side=tk.LEFT, padx=(8, 0))

        self.ssh_password_label = ttk.Label(self.ssh_frame, text="SSH password:")
        self.ssh_password_entry = ttk.Entry(self.ssh_frame, width=33, show="*")
        self.ssh_key_label = ttk.Label(self.ssh_frame, text="SSH key file:")
        self.ssh_key_entry = ttk.Entry(self.ssh_frame, width=24)
        self.ssh_key_browse = ttk.Button(
            self.ssh_frame, text="Browse", width=8,
            command=lambda: self._browse_into(self.ssh_key_entry))

        # Status line (inside the scrollable body, above the fixed button bar)
        self.status_var = tk.StringVar(value="")
        self.status_label = ttk.Label(body, textvariable=self.status_var)
        self.status_label.pack(anchor=tk.W, padx=14, pady=(2, 0))

    # ------------------------------------------------------------------ #
    def _is_remote(self) -> bool:
        return self.location_combo.get() == _LOC_REMOTE

    def _on_location_changed(self) -> None:
        if self._is_remote():
            self.ssh_frame.pack(
                fill=tk.X, padx=14, pady=(4, 0), before=self.status_label)
            self._update_ssh_auth_visibility()
        else:
            self.ssh_frame.pack_forget()

    def _update_ssh_auth_visibility(self) -> None:
        for w in (self.ssh_password_label, self.ssh_password_entry,
                  self.ssh_key_label, self.ssh_key_entry, self.ssh_key_browse):
            w.grid_remove()
        if self.ssh_auth_var.get() == "key":
            self.ssh_key_label.grid(row=5, column=0, sticky=tk.W, padx=5, pady=3)
            self.ssh_key_entry.grid(row=5, column=1, sticky=tk.W, padx=5, pady=3)
            self.ssh_key_browse.grid(row=5, column=2, sticky=tk.W, padx=(2, 5), pady=3)
        else:
            self.ssh_password_label.grid(row=5, column=0, sticky=tk.W, padx=5, pady=3)
            self.ssh_password_entry.grid(row=5, column=1, sticky=tk.W, padx=5, pady=3)

    def _browse_into(self, entry: ttk.Entry) -> None:
        path = filedialog.askopenfilename(
            title="Select SSH private key", parent=self.win)
        if path:
            entry.delete(0, tk.END)
            entry.insert(0, path)

    def _collect_ssh_tunnel(self) -> Optional[dict]:
        """Return a validated ssh_tunnel dict, or ``False`` on a validation error.

        Returns ``None`` when the form is in localhost mode (no tunnel).
        """
        if not self._is_remote():
            return None
        ssh_host = self.ssh_host_entry.get().strip()
        ssh_user = self.ssh_user_entry.get().strip()
        ssh_port = self.ssh_port_entry.get().strip() or "22"
        if not ssh_host:
            self._warn("SSH host is required for a remote connection.")
            return False
        if not ssh_user:
            self._warn("SSH username is required for a remote connection.")
            return False
        try:
            ssh_port_int = int(ssh_port)
        except ValueError:
            self._warn("SSH port must be a number.")
            return False
        use_key = self.ssh_auth_var.get() == "key"
        ssh_password = "" if use_key else self.ssh_password_entry.get()
        ssh_key_file = self.ssh_key_entry.get().strip() if use_key else ""
        if use_key and not ssh_key_file:
            self._warn("SSH key file is required (or switch to Password auth).")
            return False
        if not use_key and not ssh_password:
            self._warn("SSH password is required (or switch to Key file auth).")
            return False
        return {
            "ssh_host": ssh_host,
            "ssh_user": ssh_user,
            "ssh_port": ssh_port_int,
            "ssh_password": ssh_password,
            "ssh_key_file": ssh_key_file,
        }

    def _on_db_type_changed(self) -> None:
        db_type = self.db_type_combo.get() or ""
        # Default port
        try:
            port = DatabaseRegistry.get_default_port(db_type)
        except Exception:
            port = None
        self.port_entry.delete(0, tk.END)
        if port:
            self.port_entry.insert(0, str(port))
        # Service vs database label
        self.service_label.config(text=_service_field_label(db_type))
        self._update_security_fields_visibility(db_type)

    def _update_security_fields_visibility(self, db_type: str) -> None:
        sql_widgets = (
            self.ssl_mode_label, self.ssl_mode_combo,
            self.ssl_ca_label, self.ssl_ca_entry,
            self.ssl_cert_label, self.ssl_cert_entry,
            self.ssl_key_label, self.ssl_key_entry,
            self.wallet_label, self.wallet_entry,
        )
        mongo_widgets = (self.mongo_tls_cb, self.mongo_tls_ca_label, self.mongo_tls_ca_entry)
        for w in sql_widgets + mongo_widgets:
            w.grid_remove()

        if db_type in ("MongoDB", "DocumentDB"):
            self.mongo_tls_cb.grid(row=8, column=0, columnspan=2, sticky=tk.W, padx=5, pady=2)
            self.mongo_tls_ca_label.grid(row=9, column=0, sticky=tk.W, padx=5, pady=2)
            self.mongo_tls_ca_entry.grid(row=9, column=1, sticky=tk.W, padx=5, pady=2)
            if db_type == "DocumentDB":
                self.mongo_tls_var.set(True)
            return

        try:
            caps = DatabaseRegistry.get_capabilities(db_type)
        except Exception:
            return
        if not getattr(caps, "supports_ssl", False):
            return

        self.ssl_mode_label.grid(row=8, column=0, sticky=tk.W, padx=5, pady=2)
        self.ssl_mode_combo.grid(row=8, column=1, sticky=tk.W, padx=5, pady=2)
        modes = list(caps.ssl_mode_options or ("disable",))
        self.ssl_mode_combo["values"] = modes
        if self.ssl_mode_combo.get() not in modes:
            self.ssl_mode_combo.set(modes[0])

        fields = set(caps.ssl_fields or ())
        row = 9
        for fname, lbl, ent in (
            ("ca", self.ssl_ca_label, self.ssl_ca_entry),
            ("cert", self.ssl_cert_label, self.ssl_cert_entry),
            ("key", self.ssl_key_label, self.ssl_key_entry),
            ("wallet", self.wallet_label, self.wallet_entry),
        ):
            if fname in fields:
                lbl.grid(row=row, column=0, sticky=tk.W, padx=5, pady=2)
                ent.grid(row=row, column=1, sticky=tk.W, padx=5, pady=2)
                row += 1

    # ------------------------------------------------------------------ #
    def _collect(self) -> Optional[dict]:
        """Read + validate the form, returning a kwargs dict or None."""
        name = self.name_entry.get().strip()
        db_type = self.db_type_combo.get().strip()
        host = self.host_entry.get().strip()
        port = self.port_entry.get().strip()
        service_or_db = self.service_entry.get().strip()
        username = self.user_entry.get().strip()
        password = self.password_entry.get()

        if not name:
            self._warn("Connection name is required.")
            return None
        if not db_type:
            self._warn("Please select a database type.")
            return None
        if not host:
            self._warn("Host is required.")
            return None

        params = {
            "name": name,
            "db_type": db_type,
            "host": host,
            "port": port,
            "service_or_db": service_or_db,
            "username": username,
            "password": password,
            "save_password": bool(self.save_password_var.get()),
        }
        params.update(self._collect_security(db_type))

        ssh_tunnel = self._collect_ssh_tunnel()
        if ssh_tunnel is False:  # validation failed (distinct from None = local)
            return None
        if ssh_tunnel:
            params["ssh_tunnel"] = ssh_tunnel
        return params

    def _collect_security(self, db_type: str) -> dict:
        params: dict = {}
        if db_type in ("MongoDB", "DocumentDB"):
            params["tls"] = bool(self.mongo_tls_var.get())
            ca = self.mongo_tls_ca_entry.get().strip()
            if ca:
                params["tls_ca_file"] = ca
            return params
        try:
            caps = DatabaseRegistry.get_capabilities(db_type)
        except Exception:
            return params
        if not getattr(caps, "supports_ssl", False):
            return params
        mode = self.ssl_mode_combo.get().strip()
        if mode:
            params["ssl_mode"] = mode
        for key, entry in (
            ("ssl_ca", self.ssl_ca_entry),
            ("ssl_cert", self.ssl_cert_entry),
            ("ssl_key", self.ssl_key_entry),
            ("wallet_location", self.wallet_entry),
        ):
            val = entry.get().strip()
            if val:
                params[key] = val
        return params

    def _connect_params(self, p: dict) -> dict:
        """Map a stored-profile dict to DatabaseManager.connect kwargs."""
        cp = {
            "host": p["host"],
            "port": p["port"],
            "username": p["username"],
            "password": p["password"],
        }
        if p["db_type"] == "Oracle":
            cp["service"] = p["service_or_db"]
        else:
            cp["database"] = p["service_or_db"]
        for k in ("ssl_mode", "ssl_ca", "ssl_cert", "ssl_key", "wallet_location",
                  "tls", "tls_ca_file"):
            if k in p and p[k] not in (None, ""):
                cp[k] = p[k]
        if p.get("ssh_tunnel"):
            cp["ssh_tunnel"] = p["ssh_tunnel"]
        return cp

    # ------------------------------------------------------------------ #
    def _test(self) -> None:
        p = self._collect()
        if p is None:
            return
        self._status("Testing connection...", "blue")
        self.win.update_idletasks()
        try:
            from common.db_manager import DatabaseManager

            dbm = DatabaseManager(p["db_type"])
            conn = dbm.connect(**self._connect_params(p))
            if conn:
                self._status("Connection successful.", "green")
                try:
                    dbm.disconnect()
                except Exception:
                    pass
            else:
                self._status("Connection failed.", "red")
        except Exception as exc:
            self._status(f"Connection failed: {exc}", "red")

    def _save(self) -> None:
        from common.connection_params import ConnectionParams

        p = self._collect()
        if p is None:
            return
        if self.manager.connection_exists(p["name"]):
            self._warn(f"A connection named '{p['name']}' already exists.")
            return
        ok, msg = self.manager.add_connection(ConnectionParams.from_mapping(p))
        if not ok:
            self._warn(msg)
            return
        self.saved_name = p["name"]
        if self.on_saved:
            try:
                self.on_saved(p["name"])
            except Exception:
                pass
        messagebox.showinfo("Saved", msg, parent=self.win)
        self.win.destroy()

    # ------------------------------------------------------------------ #
    def _warn(self, msg: str) -> None:
        messagebox.showwarning("Add database connection", msg, parent=self.win)

    def _status(self, text: str, color: str = "blue") -> None:
        self.status_var.set(text)
        try:
            self.status_label.config(foreground=color)
        except Exception:
            pass

    def _center(self) -> None:
        try:
            self.win.update_idletasks()
            x = (self.win.winfo_screenwidth() // 2) - (self.win.winfo_width() // 2)
            y = (self.win.winfo_screenheight() // 2) - (self.win.winfo_height() // 2)
            self.win.geometry(f"+{x}+{y}")
        except Exception:
            pass


def open_db_connection_form(
    parent,
    manager,
    *,
    title: str = "Add database connection",
    theme=None,
    on_saved: Optional[Callable[[str], None]] = None,
    remote: bool = False,
) -> DBConnectionFormDialog:
    """Open the reusable Add-database-connection dialog bound to ``manager``.

    Set ``remote=True`` to preselect the "Remote host (SSH tunnel)" location.
    """
    return DBConnectionFormDialog(
        parent, manager, title=title, theme=theme, on_saved=on_saved, remote=remote)
