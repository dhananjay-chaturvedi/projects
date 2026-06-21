"""
Embedded remote DB connection form for the Connections tab.

Lets the user reach a database that is only accessible through a bastion / jump
host by tunnelling the connection over SSH (local port forwarding). The
database host/port entered here are the endpoint *as seen from the SSH host*
(often ``localhost``); the tool opens an SSH tunnel and points the driver at the
local end.

Self-contained: collects DB login + SSH tunnel details and supports
load/save/test/connect without using the direct 'Add database connection' form.
"""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any, Callable, Optional

from common.db_manager import DatabaseManager

# Engines that make sense to reach over a TCP SSH tunnel (SQLite is file-based).
_REMOTE_DB_TYPES = ("MySQL", "MariaDB", "PostgreSQL", "Oracle", "SQLServer", "MongoDB", "DocumentDB")
_DB_NAME_OPTIONAL = ("MongoDB", "DocumentDB", "SQLServer")


class RemoteDBConnectionPanel:
    """Inline remote (SSH-tunnel) DB section — independent of the direct form."""

    _LBL_W = 22
    _FIELD_W = 35

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

        self.entries: dict[str, Any] = {}
        self.db_type_var = tk.StringVar(value=_REMOTE_DB_TYPES[0])
        self.auth_var = tk.StringVar(value="password")
        self.save_pw_var = tk.BooleanVar(value=True)
        self._editing_name = ""
        self._status_var = tk.StringVar(value="")
        self._status_lbl: Optional[ttk.Label] = None
        self._editing_lbl: Optional[ttk.Label] = None

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self, expanded: bool | None = None) -> None:
        from common.ui.tk.widgets import make_collapsible_section

        if expanded is None:
            try:
                from common.ui.shared import specs

                expanded = not specs.connection_section_collapsed("remote")
            except Exception:
                expanded = False
        content = make_collapsible_section(
            self.parent,
            "Add or select remote database connection",
            self.title_font,
            expanded=expanded,
        )
        shell = ttk.Frame(content)
        shell.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(
            shell,
            text=(
                "Connect to a database that is only reachable through a bastion / jump "
                "host. The tool opens an SSH tunnel, then connects locally.\n"
                "DB host/port below are the database endpoint as seen FROM the SSH host "
                "(commonly localhost)."
            ),
            foreground="gray",
            font=(self.ui_font[0], max(8, self.ui_font[1] - 1)),
            justify=tk.LEFT,
        ).pack(anchor=tk.W, pady=(0, 6))

        self._editing_lbl = ttk.Label(
            shell, text="",
            font=(self.ui_font[0], self.ui_font[1], "italic"),
            foreground="#1565c0",
        )
        self._editing_lbl.pack(anchor=tk.W, pady=(0, 4))

        master = ttk.LabelFrame(shell, text="Remote DB connection", padding=10)
        master.pack(anchor=tk.W, fill=tk.X, expand=True)

        # --- Connection name + DB type ---
        self._add_entry(master, "Connection name *", "name")
        type_row = ttk.Frame(master)
        type_row.pack(fill=tk.X, pady=2)
        ttk.Label(type_row, text="Database type *", width=self._LBL_W, anchor=tk.W).pack(side=tk.LEFT)
        type_combo = ttk.Combobox(
            type_row, textvariable=self.db_type_var, values=_REMOTE_DB_TYPES,
            state="readonly", width=self._FIELD_W - 2,
        )
        type_combo.pack(side=tk.LEFT)
        type_combo.bind("<<ComboboxSelected>>", lambda _e: self._sync_port_to_db_type())

        # --- Database endpoint (as seen from SSH host) ---
        db_frame = ttk.LabelFrame(master, text="Database (from SSH host's view)", padding=6)
        db_frame.pack(fill=tk.X, pady=(8, 4))
        self._add_entry(db_frame, "DB host *", "host", default="localhost")
        self._add_entry(db_frame, "DB port *", "port")
        self._add_entry(db_frame, "Database / Service", "service_or_db")
        self._add_entry(db_frame, "DB username *", "username")
        self._add_entry(db_frame, "DB password *", "password", show="*")

        # --- SSH tunnel ---
        ssh_frame = ttk.LabelFrame(master, text="SSH tunnel", padding=6)
        ssh_frame.pack(fill=tk.X, pady=(8, 4))
        self._add_entry(ssh_frame, "SSH host *", "ssh_host")
        self._add_entry(ssh_frame, "SSH port", "ssh_port", default="22")
        self._add_entry(ssh_frame, "SSH username *", "ssh_user")

        auth_row = ttk.Frame(ssh_frame)
        auth_row.pack(fill=tk.X, pady=(4, 2))
        ttk.Label(auth_row, text="SSH auth", width=self._LBL_W, anchor=tk.W).pack(side=tk.LEFT)
        ttk.Radiobutton(
            auth_row, text="Password", value="password",
            variable=self.auth_var, command=self._update_auth_visibility,
        ).pack(side=tk.LEFT)
        ttk.Radiobutton(
            auth_row, text="Key file", value="key",
            variable=self.auth_var, command=self._update_auth_visibility,
        ).pack(side=tk.LEFT, padx=(8, 0))

        self._add_entry(ssh_frame, "SSH password", "ssh_password", show="*")
        self._ssh_key_row = self._add_entry(
            ssh_frame, "SSH key file", "ssh_key_file", with_browse=True
        )
        self._update_auth_visibility()

        ttk.Checkbutton(
            master, text="Save passwords (encrypted)", variable=self.save_pw_var
        ).pack(anchor=tk.W, pady=(6, 4))

        self._status_lbl = ttk.Label(
            master, textvariable=self._status_var, foreground="gray",
            font=(self.ui_font[0], max(8, self.ui_font[1] - 1)),
            wraplength=520, justify=tk.LEFT,
        )
        self._status_lbl.pack(anchor=tk.W, pady=(0, 6))

        btn_row = ttk.Frame(master)
        btn_row.pack(anchor=tk.W, pady=(2, 0))
        ttk.Button(btn_row, text="Connect", command=self._connect, style="Primary.TButton", width=12).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btn_row, text="Test Connection", command=self._test, width=15).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_row, text="Load Saved", command=self._load_saved_dialog, width=12).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_row, text="Save", command=self._save, width=10).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_row, text="Clear", command=self._clear_form, width=10).pack(side=tk.LEFT, padx=6)

        self._sync_port_to_db_type()

    # ------------------------------------------------------------------
    # Field helpers
    # ------------------------------------------------------------------

    def _add_entry(self, parent, label, key, *, default="", show="", with_browse=False):
        row = ttk.Frame(parent)
        row.pack(fill=tk.X, pady=2)
        ttk.Label(row, text=label, width=self._LBL_W, anchor=tk.W).pack(side=tk.LEFT)
        kw: dict = {"width": self._FIELD_W if not with_browse else self._FIELD_W - 10}
        if show:
            kw["show"] = show
        entry = ttk.Entry(row, **kw)
        entry.pack(side=tk.LEFT)
        if default:
            entry.insert(0, default)
        self.entries[key] = entry
        if with_browse:
            ttk.Button(
                row, text="Browse",
                command=lambda: self._browse_into(key), width=8,
            ).pack(side=tk.LEFT, padx=(4, 0))
        return row

    def _browse_into(self, key: str) -> None:
        path = filedialog.askopenfilename(title="Select SSH private key", parent=self.root)
        if path:
            self._set(key, path)

    def _get(self, key: str) -> str:
        w = self.entries.get(key)
        if w is None:
            return ""
        try:
            return w.get().strip()
        except tk.TclError:
            return ""

    def _set(self, key: str, value: str) -> None:
        w = self.entries.get(key)
        if w is None:
            return
        w.delete(0, tk.END)
        if value:
            w.insert(0, value)

    def _update_auth_visibility(self) -> None:
        if self.auth_var.get() == "key":
            self.entries["ssh_password"].master.pack_forget()
            self._ssh_key_row.pack(fill=tk.X, pady=2)
        else:
            self._ssh_key_row.pack_forget()
            self.entries["ssh_password"].master.pack(fill=tk.X, pady=2)

    def _sync_port_to_db_type(self) -> None:
        if self._get("port"):
            return
        from common.ui.tk.master_shell import DatabaseConfig

        port = DatabaseConfig.get_default_port(self.db_type_var.get())
        if port:
            self._set("port", str(port))

    def _set_status(self, message: str, kind: str = "info") -> None:
        colours = {"info": "gray", "ok": "#2e7d32", "warn": "#e65100", "error": "#c62828"}
        self._status_var.set(message)
        if self._status_lbl is not None:
            self._status_lbl.config(foreground=colours.get(kind, "gray"))

    # ------------------------------------------------------------------
    # Collect + validate
    # ------------------------------------------------------------------

    def _ssh_tunnel_dict(self) -> dict:
        return {
            "ssh_host": self._get("ssh_host"),
            "ssh_user": self._get("ssh_user"),
            "ssh_port": int(self._get("ssh_port") or 22),
            "ssh_password": self._get("ssh_password") if self.auth_var.get() == "password" else "",
            "ssh_key_file": self._get("ssh_key_file") if self.auth_var.get() == "key" else "",
        }

    def _validate(self) -> Optional[str]:
        db_type = self.db_type_var.get()
        if not self._get("name"):
            return "Connection name is required."
        if not self._get("host"):
            return "DB host is required."
        if not self._get("port"):
            return "DB port is required."
        if not self._get("username"):
            return "DB username is required."
        if not self._get("password"):
            return "DB password is required."
        if db_type not in _DB_NAME_OPTIONAL and not self._get("service_or_db"):
            return f"Database/Service is required for {db_type}."
        if not self._get("ssh_host"):
            return "SSH host is required."
        if not self._get("ssh_user"):
            return "SSH username is required."
        try:
            int(self._get("port"))
            int(self._get("ssh_port") or 22)
        except ValueError:
            return "Port and SSH port must be numbers."
        if self.auth_var.get() == "password" and not self._get("ssh_password"):
            return "SSH password is required (or switch to Key file auth)."
        if self.auth_var.get() == "key" and not self._get("ssh_key_file"):
            return "SSH key file is required (or switch to Password auth)."
        return None

    def _connect_params(self) -> dict:
        db_type = self.db_type_var.get()
        service_or_db = self._get("service_or_db")
        params: dict = {
            "host": self._get("host"),
            "port": int(self._get("port")),
            "username": self._get("username"),
            "password": self._get("password"),
            "ssh_tunnel": self._ssh_tunnel_dict(),
        }
        if db_type == "Oracle":
            params["service"] = service_or_db
        else:
            params["database"] = service_or_db
        return params

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        err = self._validate()
        if err:
            messagebox.showwarning("Connect", err, parent=self.root)
            return
        conn_name = self._get("name")
        if self.on_register_connection is None:
            messagebox.showerror("Connect", "Active connections are unavailable.", parent=self.root)
            return
        db_type = self.db_type_var.get()
        params = self._connect_params()
        self._set_status(f"Opening SSH tunnel and connecting '{conn_name}'…", "info")
        self.root.update_idletasks()

        def _run():
            try:
                mgr = DatabaseManager(db_type)
                conn = mgr.connect(**params)
                if conn is None:
                    raise RuntimeError(f"Could not connect to {db_type}.")

                def _done():
                    self.on_register_connection(conn_name, mgr)
                    self._set_status(f"Connected '{conn_name}' — added to active connections.", "ok")
                    self.update_status(f"Remote DB '{conn_name}' connected.", "success")

                self.root.after(0, _done)
            except Exception as exc:
                msg = str(exc)

                def _fail(message: str = msg):
                    self._set_status(message, "error")
                    messagebox.showerror("Connect failed", message, parent=self.root)

                self.root.after(0, _fail)

        threading.Thread(target=_run, daemon=True).start()

    def _test(self) -> None:
        err = self._validate()
        if err:
            messagebox.showwarning("Test", err, parent=self.root)
            return
        db_type = self.db_type_var.get()
        params = self._connect_params()
        self._set_status("Opening SSH tunnel and testing…", "info")
        self.root.update_idletasks()

        def _run():
            mgr = DatabaseManager(db_type)
            try:
                conn = mgr.connect(**params)
                ok = conn is not None
                msg = "✓ Remote DB connection OK." if ok else "✗ Connection failed."
                kind = "ok" if ok else "error"
            except Exception as exc:
                msg, kind = f"✗ {exc}", "error"
            finally:
                try:
                    mgr.disconnect()
                except Exception:
                    pass

            self.root.after(0, lambda: self._set_status(msg, kind))

        threading.Thread(target=_run, daemon=True).start()

    def _save(self) -> None:
        from common.connection_params import ConnectionParams

        err = self._validate()
        if err:
            messagebox.showwarning("Save", err, parent=self.root)
            return
        name = self._get("name")
        db_type = self.db_type_var.get()
        save_pw = self.save_pw_var.get()
        exists = self.connection_manager.connection_exists(name)
        if exists and name != self._editing_name and not messagebox.askyesno(
            "Overwrite?", f"'{name}' exists. Overwrite?", parent=self.root
        ):
            return

        kwargs = dict(
            name=name, db_type=db_type, host=self._get("host"),
            port=self._get("port"), service_or_db=self._get("service_or_db"),
            username=self._get("username"), password=self._get("password"),
            save_password=save_pw, ssh_tunnel=self._ssh_tunnel_dict(),
        )
        params = ConnectionParams.from_mapping(kwargs)
        if exists:
            ok, message = self.connection_manager.update_connection(name, params)
        else:
            ok, message = self.connection_manager.add_connection(params)
        if ok:
            self._editing_name = name
            if self._editing_lbl:
                self._editing_lbl.config(text=f"Editing saved connection: {name}")
            self._set_status(message, "ok")
            self.update_status(f"Saved remote connection '{name}'.", "success")
        else:
            self._set_status(message, "error")
            messagebox.showerror("Save failed", message, parent=self.root)

    def _load_saved_dialog(self) -> None:
        remote = [
            c for c in self.connection_manager.get_all_connections()
            if c.get("ssh_tunnel")
        ]
        if not remote:
            messagebox.showinfo(
                "Load saved",
                "No saved remote connections yet.\nFill the form and click Save.",
                parent=self.root,
            )
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Saved remote connections")
        dialog.geometry("680x380")
        dialog.transient(self.root)

        tree_frame = ttk.Frame(dialog)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        vsb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL)
        tree = ttk.Treeview(
            tree_frame, columns=("Type", "DB host", "SSH host", "User"),
            yscrollcommand=vsb.set,
        )
        vsb.config(command=tree.yview)
        tree.heading("#0", text="Name")
        for col in ("Type", "DB host", "SSH host", "User"):
            tree.heading(col, text=col)
            tree.column(col, width=110)
        tree.column("#0", width=160)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        tree.pack(fill=tk.BOTH, expand=True)

        for prof in sorted(remote, key=lambda c: c.get("name", "")):
            tun = prof.get("ssh_tunnel") or {}
            tree.insert(
                "", tk.END, text=prof.get("name", ""),
                values=(
                    prof.get("db_type", ""), prof.get("host", ""),
                    tun.get("ssh_host", ""), prof.get("username", ""),
                ),
            )

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        def _selected_name() -> str:
            sel = tree.selection()
            return tree.item(sel[0])["text"] if sel else ""

        def _load():
            name = _selected_name()
            if not name:
                messagebox.showwarning("Load", "Select a connection.", parent=dialog)
                return
            prof = self.connection_manager.get_connection(name)
            if prof:
                self._profile_to_form(prof)
                self._set_status(f"Loaded remote connection '{name}'.", "ok")
            dialog.destroy()

        def _delete():
            name = _selected_name()
            if not name:
                messagebox.showwarning("Delete", "Select a connection.", parent=dialog)
                return
            if not messagebox.askyesno("Delete", f"Delete remote connection '{name}'?", parent=dialog):
                return
            self.connection_manager.delete_connection(name)
            tree.delete(tree.selection()[0])
            if self._editing_name == name:
                self._clear_form()
            self.update_status(f"Deleted remote connection '{name}'.", "info")

        ttk.Button(btn_frame, text="Load into form", command=_load, width=16).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Delete", command=_delete, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Close", command=dialog.destroy, width=10).pack(side=tk.LEFT, padx=5)

    def _profile_to_form(self, profile: dict) -> None:
        db_type = profile.get("db_type", _REMOTE_DB_TYPES[0])
        if db_type in _REMOTE_DB_TYPES:
            self.db_type_var.set(db_type)
        self._set("name", profile.get("name", ""))
        self._set("host", profile.get("host", ""))
        self._set("port", str(profile.get("port", "") or ""))
        self._set("service_or_db", profile.get("service_or_db", ""))
        self._set("username", profile.get("username", ""))
        self._set("password", profile.get("password", ""))

        tun = profile.get("ssh_tunnel") or {}
        self._set("ssh_host", tun.get("ssh_host", ""))
        self._set("ssh_port", str(tun.get("ssh_port", 22) or 22))
        self._set("ssh_user", tun.get("ssh_user", ""))
        if tun.get("ssh_key_file"):
            self.auth_var.set("key")
            self._set("ssh_key_file", tun.get("ssh_key_file", ""))
            self._set("ssh_password", "")
        else:
            self.auth_var.set("password")
            self._set("ssh_password", tun.get("ssh_password", ""))
            self._set("ssh_key_file", "")
        self._update_auth_visibility()

        self._editing_name = profile.get("name", "")
        if self._editing_lbl and self._editing_name:
            self._editing_lbl.config(text=f"Editing saved connection: {self._editing_name}")

    def _clear_form(self) -> None:
        self._editing_name = ""
        if self._editing_lbl:
            self._editing_lbl.config(text="")
        for key in self.entries:
            self._set(key, "")
        self.db_type_var.set(_REMOTE_DB_TYPES[0])
        self.auth_var.set("password")
        self._set("host", "localhost")
        self._set("ssh_port", "22")
        self._update_auth_visibility()
        self._sync_port_to_db_type()
        self._set_status("", "info")
