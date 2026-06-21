"""Connections management screen.

Functional parity with the desktop Connections tab and the Web UI. The tab has
four sections, each rendered with the SAME options/buttons defined in
``common.ui.shared.specs``:

* Active connections — Refresh / Disconnect Selected / Disconnect All.
* Add or select database connection (direct / localhost) — capability-driven
  SSL/TLS fields; buttons Connect / Test Connection / Load Saved /
  Save Connection / Clear.
* Add or select remote database connection (SSH tunnel) — its own DB + SSH
  fields; buttons Connect / Test Connection / Load Saved / Save / Clear.
* Cloud connection (next increment).

Terminal-native look, but every option, field and action mirrors the Tk tab and
the Web UI. Navigation is keyboard-first: Tab/Shift+Tab move between controls,
arrow keys move within tables and selects, Enter activates the focused button.
"""

from __future__ import annotations

from typing import Any

from textual.containers import Horizontal, Vertical
from textual.widgets import (
    Button,
    Checkbox,
    Collapsible,
    DataTable,
    Input,
    Label,
    Select,
    Static,
)

from common.ui.shared import specs
from common.ui.textual.screens.base import BaseScreen
from common.ui.textual.screens.form_modal import SelectModal


class ConnectionsScreen(BaseScreen):
    """List, add, connect, test, and remove database connections."""

    NAV_ID = "connections"

    # The SSL/TLS/SSH/cloud blocks are plain ``Vertical`` containers, which
    # default to ``height: 1fr`` and clip their overflow. Inside a Collapsible
    # that hides every field below the first row (e.g. under "SSL mode") until
    # the terminal is enlarged. Sizing them to their content lets the scrollable
    # ``#body`` show everything via its scrollbar, mirroring the Tk layout.
    DEFAULT_CSS = """
    ConnectionsScreen #ssl-group,
    ConnectionsScreen #tls-group,
    ConnectionsScreen #r-ssh-group,
    ConnectionsScreen #cloud-resource-fields,
    ConnectionsScreen #cloud-auth-fields,
    ConnectionsScreen #cloud-sql-fields {
        height: auto;
    }
    """

    BINDINGS = BaseScreen.BINDINGS + [
        ("ctrl+r", "refresh", "Refresh"),
    ]

    def __init__(self, svc: Any, **kwargs) -> None:
        super().__init__(svc, **kwargs)
        self._meta = self._load_metadata()
        self._saved: list[dict] = []
        self._direct_edit: str | None = None
        self._remote_edit: str | None = None
        self._cloud = self._load_cloud_schema()
        self._cloud_edit: str | None = None

    def screen_title(self) -> str:
        return "Connections"

    # ------------------------------------------------------------------ #
    def _load_metadata(self) -> dict:
        try:
            return self.svc.connection_metadata()
        except Exception:
            return {"db_types": [], "engines": {}}

    def _engine(self, db_type: str) -> dict:
        return (self._meta.get("engines") or {}).get(db_type, {})

    def _load_cloud_schema(self) -> dict:
        if not hasattr(self.svc, "cloud_db_provider_schemas"):
            return {}
        try:
            return self.svc.cloud_db_provider_schemas()
        except Exception:
            return {}

    # ------------------------------------------------------------------ #
    def compose_body(self):
        db_types = self._meta.get("db_types") or ["PostgreSQL"]
        first = db_types[0]

        # Section ORDER, titles and COLLAPSED-by-default state all come from the
        # shared spec (common.ui.shared.specs.CONNECTION_SECTIONS). Each UI only
        # supplies its own native body builders below, so a layout change is made
        # once in the shared spec and propagates to Tk, Textual and Web alike.
        builders = {
            "active": self._body_active,
            "saved": self._body_saved,
            "direct": self._body_direct,
            "remote": self._body_remote,
            "cloud": self._body_cloud,
        }
        for section in specs.CONNECTION_SECTIONS:
            build = builders.get(section["id"])
            if build is None:
                continue
            with Collapsible(title=section["title"],
                             collapsed=bool(section.get("collapsed", True))):
                yield from build(db_types, first)

    # -- Per-section body builders (native widgets; order/collapse from spec) --
    def _body_active(self, db_types, first):
        yield DataTable(id="active-table", zebra_stripes=True)
        with Horizontal(classes="actions-row"):
            yield Button("Refresh", id="active-refresh", variant="primary")
            yield Button("Disconnect Selected", id="active-disc", variant="warning")
            yield Button("Disconnect All", id="active-disc-all", variant="error")
        yield Static("", id="active-status", classes="status")

    def _body_saved(self, db_types, first):
        yield Static("Select a row, then Connect / Test / Remove:", classes="hint")
        yield DataTable(id="conn-table", zebra_stripes=True)
        with Horizontal(classes="actions-row"):
            yield Button("Refresh", id="conn-refresh", variant="primary")
            yield Button("Connect", id="conn-connect", variant="success")
            yield Button("Test", id="conn-test")
            yield Button("Remove", id="conn-remove", variant="error")
        yield Static("", id="saved-status", classes="status")

    def _body_direct(self, db_types, first):
        yield Label("Connection name")
        yield Input(placeholder="my_db", id="add-name")
        yield Label("Database Type")
        yield Select(
            [(t, t) for t in db_types],
            id="add-type", value=first, allow_blank=False,
        )
        yield Label("Host")
        yield Input(value="localhost", id="add-host")
        yield Label("Port")
        yield Input(id="add-port")
        yield Label("Database name", id="add-service-label")
        yield Input(id="add-service")
        yield Label("Username")
        yield Input(id="add-user")
        yield Label("Password")
        yield Input(password=True, id="add-password")
        yield Checkbox("Save password (encrypted)", value=True, id="add-save-pw")

        with Vertical(id="ssl-group"):
            yield Static("[b]SSL / TLS[/]", classes="section")
            yield Label("SSL mode")
            yield Select([], id="add-ssl-mode", allow_blank=True)
            yield Label("SSL CA file", id="ssl-ca-label")
            yield Input(id="add-ssl-ca")
            yield Label("SSL client cert", id="ssl-cert-label")
            yield Input(id="add-ssl-cert")
            yield Label("SSL client key", id="ssl-key-label")
            yield Input(id="add-ssl-key")
            yield Label("Oracle wallet dir", id="ssl-wallet-label")
            yield Input(id="add-wallet")
        with Vertical(id="tls-group"):
            yield Static("[b]TLS (MongoDB / DocumentDB)[/]", classes="section")
            yield Checkbox("Use TLS", id="add-tls")
            yield Label("TLS CA file")
            yield Input(id="add-tls-ca")

        with Horizontal(classes="actions-row"):
            yield Button("Connect", id="conn-connect-form", variant="primary")
            yield Button("Test Connection", id="conn-test-form")
            yield Button("Load Saved", id="conn-load-saved")
            yield Button("Save Connection", id="conn-add", variant="success")
            yield Button("Clear", id="conn-clear")
        yield Static("", id="conn-status", classes="status")

    def _body_remote(self, db_types, first):
        yield Static(
            "Connect to a database reachable only through a bastion / jump "
            "host. DB host/port are the endpoint as seen FROM the SSH host "
            "(often localhost).", classes="hint")
        yield Label("Connection name")
        yield Input(placeholder="my_remote_db", id="r-name")
        yield Label("Database type")
        yield Select(
            [(t, t) for t in db_types],
            id="r-type", value=first, allow_blank=False,
        )
        yield Label("DB host")
        yield Input(value="localhost", id="r-host")
        yield Label("DB port")
        yield Input(id="r-port")
        yield Label("Database / Service", id="r-service-label")
        yield Input(id="r-service")
        yield Label("DB username")
        yield Input(id="r-user")
        yield Label("DB password")
        yield Input(password=True, id="r-password")
        yield Checkbox("Save passwords (encrypted)", value=True, id="r-save-pw")

        with Vertical(id="r-ssh-group"):
            yield Static("[b]SSH tunnel[/]", classes="section")
            yield Label("SSH host")
            yield Input(id="r-ssh-host")
            yield Label("SSH port")
            yield Input(value="22", id="r-ssh-port")
            yield Label("SSH username")
            yield Input(id="r-ssh-user")
            yield Label("SSH auth")
            yield Select([("Password", "password"), ("Key file", "key")],
                         id="r-ssh-auth", value="password", allow_blank=False)
            yield Label("SSH password", id="r-ssh-pw-label")
            yield Input(password=True, id="r-ssh-password")
            yield Label("SSH key file", id="r-ssh-key-label")
            yield Input(id="r-ssh-key")

        with Horizontal(classes="actions-row"):
            yield Button("Connect", id="r-connect", variant="primary")
            yield Button("Test Connection", id="r-test")
            yield Button("Load Saved", id="r-load-saved")
            yield Button("Save", id="r-save", variant="success")
            yield Button("Clear", id="r-clear")
        yield Static("", id="r-status", classes="status")

    def _body_cloud(self, db_types, first):
        if not self._cloud:
            yield Static(
                "Cloud connections are not available in this build.",
                classes="hint")
            return
        providers = self._cloud.get("providerOrder") or ["AWS"]
        yield Static(
            "Authenticate to a managed cloud database and mirror it into "
            "your saved connections. Fields adapt to provider + auth.",
            classes="hint")
        yield Label("Cloud provider")
        yield Select([(self._cloud["providers"][p].get("label", p), p)
                      for p in providers],
                     id="cloud-provider", value=providers[0],
                     allow_blank=False)
        yield Label("Authentication")
        yield Select([("Access Keys", "keys"),
                      ("Username / Password", "pwd"),
                      ("SSO / OIDC", "sso"),
                      ("Environment / Instance Role", "env")],
                     id="cloud-auth-mode", value="keys", allow_blank=False)
        yield Static("[b]Cloud resource[/]", classes="section")
        yield Vertical(id="cloud-resource-fields")
        yield Static("[b]Credentials[/]", classes="section")
        yield Static("", id="cloud-auth-help", classes="hint")
        yield Vertical(id="cloud-auth-fields")
        yield Static("[b]Database login[/]", classes="section")
        yield Vertical(id="cloud-sql-fields")
        with Horizontal(classes="actions-row"):
            yield Button("Connect Cloud DB", id="cloud-connect", variant="primary")
            yield Button("Test Cloud Login", id="cloud-test-login")
            yield Button("Test DB Connection", id="cloud-test-db")
            yield Button("Resolve SQL Endpoint", id="cloud-resolve")
            yield Button("Load Saved", id="cloud-load-saved")
            yield Button("Save", id="cloud-save", variant="success")
            yield Button("Clear", id="cloud-clear")
        yield Static("", id="cloud-status", classes="status")

    # ------------------------------------------------------------------ #
    async def on_mount(self) -> None:
        active = self.query_one("#active-table", DataTable)
        active.add_columns("name", "type", "host", "port", "database", "user", "state")
        saved = self.query_one("#conn-table", DataTable)
        saved.add_columns("name", "type", "host", "port", "database", "user", "ssh")
        self._apply_engine()
        self._apply_remote_engine()
        self._apply_ssh_auth()
        self._refresh_saved()
        self._refresh_active()
        self._cloud_built = False
        if self._cloud:
            self._cloud_values: dict[str, str] = {}
            await self._refresh_cloud_fields()
            self._cloud_built = True

    # ------------------------------------------------------------------ #
    # Dynamic form behaviour (mirrors the desktop form)
    # ------------------------------------------------------------------ #
    def _current_type(self) -> str:
        val = self.query_one("#add-type", Select).value
        return str(val) if val is not None else ""

    def _remote_type(self) -> str:
        val = self.query_one("#r-type", Select).value
        return str(val) if val is not None else ""

    def _apply_engine(self) -> None:
        eng = self._engine(self._current_type())
        port = self.query_one("#add-port", Input)
        if not port.value.strip():
            port.value = str(eng.get("default_port") or "")
        self.query_one("#add-service-label", Label).update(
            eng.get("service_label") or "Database name")

        is_doc = bool(eng.get("is_document"))
        supports_ssl = bool(eng.get("supports_ssl"))
        ssl_group = self.query_one("#ssl-group", Vertical)
        tls_group = self.query_one("#tls-group", Vertical)
        ssl_group.display = (not is_doc) and supports_ssl
        tls_group.display = is_doc

        if is_doc:
            self.query_one("#add-tls", Checkbox).value = bool(eng.get("tls_default"))
        elif supports_ssl:
            modes = eng.get("ssl_mode_options") or []
            sel = self.query_one("#add-ssl-mode", Select)
            sel.set_options([(m, m) for m in modes])
            if modes:
                sel.value = modes[0]
            fields = set(eng.get("ssl_fields") or ())
            self._toggle("ssl-ca-label", "add-ssl-ca", "ca" in fields)
            self._toggle("ssl-cert-label", "add-ssl-cert", "cert" in fields)
            self._toggle("ssl-key-label", "add-ssl-key", "key" in fields)
            self._toggle("ssl-wallet-label", "add-wallet", "wallet" in fields)

    def _apply_remote_engine(self) -> None:
        eng = self._engine(self._remote_type())
        port = self.query_one("#r-port", Input)
        if not port.value.strip():
            port.value = str(eng.get("default_port") or "")
        self.query_one("#r-service-label", Label).update(
            eng.get("service_label") or "Database / Service")

    def _toggle(self, label_id: str, input_id: str, show: bool) -> None:
        self.query_one("#" + label_id).display = show
        self.query_one("#" + input_id).display = show

    def _apply_ssh_auth(self) -> None:
        use_key = str(self.query_one("#r-ssh-auth", Select).value) == "key"
        self.query_one("#r-ssh-key-label").display = use_key
        self.query_one("#r-ssh-key").display = use_key
        self.query_one("#r-ssh-pw-label").display = not use_key
        self.query_one("#r-ssh-password").display = not use_key

    async def on_select_changed(self, event: Select.Changed) -> None:
        sid = event.select.id or ""
        if sid == "add-type":
            self.query_one("#add-port", Input).value = ""
            self._apply_engine()
        elif sid == "r-type":
            self.query_one("#r-port", Input).value = ""
            self._apply_remote_engine()
        elif sid == "r-ssh-auth":
            self._apply_ssh_auth()
        elif sid in ("cloud-provider", "cloud-auth-mode"):
            if getattr(self, "_cloud_built", False):
                self._capture_cloud_values()
                await self._refresh_cloud_fields()

    # ------------------------------------------------------------------ #
    # Tables
    # ------------------------------------------------------------------ #
    def _refresh_saved(self) -> None:
        table = self.query_one("#conn-table", DataTable)
        table.clear()
        self._saved = list(self.svc.list_connections())
        for row in self._saved:
            table.add_row(
                row.get("name", ""),
                row.get("db_type", row.get("type", "")),
                row.get("host", ""),
                str(row.get("port", "")),
                row.get("service_or_db", row.get("database", "")),
                row.get("username", row.get("user", "")),
                "yes" if row.get("ssh_tunnel") else "",
            )

    def _refresh_active(self) -> None:
        table = self.query_one("#active-table", DataTable)
        table.clear()
        if not hasattr(self.svc, "list_active_connections"):
            return
        for row in self.svc.list_active_connections():
            table.add_row(
                row.get("name", ""),
                row.get("db_type", ""),
                row.get("host", ""),
                str(row.get("port", "")),
                row.get("service_or_db", ""),
                row.get("username", ""),
                "connected" if row.get("connected") else "idle",
            )

    def _selected(self, table_id: str) -> str | None:
        table = self.query_one("#" + table_id, DataTable)
        if table.cursor_row is None or table.row_count == 0:
            return None
        try:
            row = table.get_row_at(table.cursor_row)
        except Exception:
            return None
        return str(row[0]) if row else None

    def _selected_saved(self) -> dict | None:
        table = self.query_one("#conn-table", DataTable)
        if table.cursor_row is None or table.row_count == 0:
            return None
        idx = table.cursor_row
        if 0 <= idx < len(self._saved):
            return self._saved[idx]
        return None

    def _status(self, wid: str, msg: str) -> None:
        self.query_one("#" + wid, Static).update(msg)

    def action_refresh(self) -> None:
        self._refresh_saved()
        self._refresh_active()

    # ------------------------------------------------------------------ #
    # Build + save (direct)
    # ------------------------------------------------------------------ #
    def _build_kwargs(self) -> dict | None:
        name = self.query_one("#add-name", Input).value.strip()
        if not name:
            self._status("conn-status", "Connection name is required.")
            return None
        db_type = self._current_type()
        host = self.query_one("#add-host", Input).value.strip()
        if not host:
            self._status("conn-status", "Host is required.")
            return None
        eng = self._engine(db_type)
        kwargs: dict = {
            "name": name,
            "db_type": db_type,
            "host": host,
            "port": self.query_one("#add-port", Input).value.strip(),
            "user": self.query_one("#add-user", Input).value.strip(),
            "password": self.query_one("#add-password", Input).value,
            "service": self.query_one("#add-service", Input).value.strip(),
            "save_password": self.query_one("#add-save-pw", Checkbox).value,
        }
        if eng.get("is_document"):
            kwargs["tls"] = self.query_one("#add-tls", Checkbox).value
            ca = self.query_one("#add-tls-ca", Input).value.strip()
            if ca:
                kwargs["tls_ca_file"] = ca
        elif eng.get("supports_ssl"):
            mode = self.query_one("#add-ssl-mode", Select).value
            if mode is not None:
                kwargs["ssl_mode"] = str(mode)
            for fld, wid in (("ssl_ca", "add-ssl-ca"), ("ssl_cert", "add-ssl-cert"),
                             ("ssl_key", "add-ssl-key"), ("wallet_location", "add-wallet")):
                val = self.query_one("#" + wid, Input).value.strip()
                if val:
                    kwargs[fld] = val
        return kwargs

    def _build_remote_kwargs(self) -> dict | None:
        name = self.query_one("#r-name", Input).value.strip()
        if not name:
            self._status("r-status", "Connection name is required.")
            return None
        host = self.query_one("#r-host", Input).value.strip()
        if not host:
            self._status("r-status", "DB host is required.")
            return None
        ssh_host = self.query_one("#r-ssh-host", Input).value.strip()
        if not ssh_host:
            self._status("r-status", "SSH host is required for a remote connection.")
            return None
        use_key = str(self.query_one("#r-ssh-auth", Select).value) == "key"
        try:
            ssh_port = int(self.query_one("#r-ssh-port", Input).value.strip() or "22")
        except ValueError:
            self._status("r-status", "SSH port must be a number.")
            return None
        return {
            "name": name,
            "db_type": self._remote_type(),
            "host": host,
            "port": self.query_one("#r-port", Input).value.strip(),
            "user": self.query_one("#r-user", Input).value.strip(),
            "password": self.query_one("#r-password", Input).value,
            "service": self.query_one("#r-service", Input).value.strip(),
            "save_password": self.query_one("#r-save-pw", Checkbox).value,
            "ssh_tunnel": {
                "ssh_host": ssh_host,
                "ssh_user": self.query_one("#r-ssh-user", Input).value.strip(),
                "ssh_port": ssh_port,
                "ssh_password": "" if use_key else self.query_one("#r-ssh-password", Input).value,
                "ssh_key_file": self.query_one("#r-ssh-key", Input).value.strip() if use_key else "",
            },
        }

    @staticmethod
    def _test_kwargs(kwargs: dict) -> dict:
        """Return inline-test kwargs with persistence-only fields removed."""
        out = dict(kwargs)
        out.pop("save_password", None)
        return out

    @staticmethod
    def _format_test_result(r: dict) -> str:
        """Render a clear, parsed pass/fail line from a test result dict."""
        ok = bool(r.get("ok"))
        msg = r.get("message") or ("Connection succeeded." if ok else "Connection failed.")
        mark = "[green]✓[/]" if ok else "[red]✗[/]"
        parts = [f"{mark} {msg}"]
        version = r.get("version")
        if ok and version and str(version) not in msg:
            parts.append(f"Server: {version}.")
        latency = r.get("latency_ms")
        if latency is not None and "ms" not in msg:
            parts.append(f"{latency} ms.")
        return " ".join(parts)

    async def _run_inline_test(self, kwargs: dict, status_id: str) -> None:
        """Test the typed credentials in real time without blocking the UI.

        The DB driver's ``connect`` is a blocking call, so it runs in a thread
        worker; the form shows progress immediately and the parsed pass/fail
        result once the database actually answers.
        """
        name = kwargs.get("name", "")
        self._status(status_id, f"Testing '{name}'… contacting the database.")
        test_kwargs = self._test_kwargs(kwargs)
        from common.connection_params import ConnectionParams

        params = ConnectionParams.from_mapping(test_kwargs)
        worker = self.run_worker(
            lambda: self.svc.test_connection_inline(params),
            group="conn-test", exclusive=True, exit_on_error=False, thread=True,
        )
        try:
            result = await worker.wait()
        except Exception as exc:  # noqa: BLE001 - surface any driver error
            self._status(status_id, f"[red]✗[/] Connection failed: {exc}")
            return
        if not isinstance(result, dict):
            self._status(status_id, str(result))
            return
        self._status(status_id, self._format_test_result(result))

    def _upsert(self, kwargs: dict, edit_name: str | None, status_id: str) -> str | None:
        from common.connection_params import ConnectionParams

        existing = {c.get("name") for c in self._saved}
        target = edit_name or (kwargs["name"] if kwargs["name"] in existing else None)
        params = ConnectionParams.from_mapping(kwargs)
        if target and hasattr(self.svc, "update_connection"):
            r = self.svc.update_connection(target, params)
        else:
            r = self.svc.add_connection(params)
        self._status(status_id, r.get("message", str(r)))
        if r.get("ok"):
            self._refresh_saved()
            return kwargs["name"]
        return None

    def _save_direct(self) -> str | None:
        kwargs = self._build_kwargs()
        if kwargs is None:
            return None
        name = self._upsert(kwargs, self._direct_edit, "conn-status")
        if name:
            self._direct_edit = name
        return name

    def _save_remote(self) -> str | None:
        kwargs = self._build_remote_kwargs()
        if kwargs is None:
            return None
        name = self._upsert(kwargs, self._remote_edit, "r-status")
        if name:
            self._remote_edit = name
        return name

    # ------------------------------------------------------------------ #
    # Clear + Load Saved
    # ------------------------------------------------------------------ #
    def _clear_direct(self) -> None:
        for wid in ("add-name", "add-port", "add-service", "add-user",
                    "add-password", "add-ssl-ca", "add-ssl-cert", "add-ssl-key",
                    "add-wallet", "add-tls-ca"):
            self.query_one("#" + wid, Input).value = ""
        self.query_one("#add-host", Input).value = "localhost"
        self.query_one("#add-save-pw", Checkbox).value = True
        self._direct_edit = None
        self._apply_engine()
        self._status("conn-status", "Cleared.")

    def _clear_remote(self) -> None:
        for wid in ("r-name", "r-port", "r-service", "r-user", "r-password",
                    "r-ssh-host", "r-ssh-user", "r-ssh-password", "r-ssh-key"):
            self.query_one("#" + wid, Input).value = ""
        self.query_one("#r-host", Input).value = "localhost"
        self.query_one("#r-ssh-port", Input).value = "22"
        self.query_one("#r-save-pw", Checkbox).value = True
        self._remote_edit = None
        self._apply_remote_engine()
        self._status("r-status", "Cleared.")

    def _load_into_direct(self, c: dict) -> None:
        if c.get("ssh_tunnel"):
            self._status("conn-status",
                         f"'{c.get('name')}' is a remote (SSH) connection — "
                         "use Load Saved in the remote section.")
            return
        self.query_one("#add-name", Input).value = c.get("name", "")
        db_type = c.get("db_type", c.get("type", ""))
        if db_type:
            self.query_one("#add-type", Select).value = db_type
            self._apply_engine()
        self.query_one("#add-host", Input).value = c.get("host", "")
        self.query_one("#add-port", Input).value = str(c.get("port", "") or "")
        self.query_one("#add-service", Input).value = \
            c.get("service_or_db", c.get("database", "")) or ""
        self.query_one("#add-user", Input).value = \
            c.get("username", c.get("user", "")) or ""
        self.query_one("#add-password", Input).value = ""
        self.query_one("#add-save-pw", Checkbox).value = c.get("save_password", True) is not False
        self._direct_edit = c.get("name")
        self._status("conn-status", f"Loaded '{c.get('name')}' into the form.")

    def _load_into_remote(self, c: dict) -> None:
        if not c.get("ssh_tunnel"):
            self._status("r-status",
                         f"'{c.get('name')}' is a direct connection — "
                         "use Load Saved in the database section.")
            return
        self.query_one("#r-name", Input).value = c.get("name", "")
        db_type = c.get("db_type", c.get("type", ""))
        if db_type:
            self.query_one("#r-type", Select).value = db_type
            self._apply_remote_engine()
        self.query_one("#r-host", Input).value = c.get("host", "")
        self.query_one("#r-port", Input).value = str(c.get("port", "") or "")
        self.query_one("#r-service", Input).value = \
            c.get("service_or_db", c.get("database", "")) or ""
        self.query_one("#r-user", Input).value = \
            c.get("username", c.get("user", "")) or ""
        self.query_one("#r-password", Input).value = ""
        self.query_one("#r-save-pw", Checkbox).value = c.get("save_password", True) is not False
        ssh = c.get("ssh_tunnel") or {}
        self.query_one("#r-ssh-host", Input).value = ssh.get("ssh_host", "")
        self.query_one("#r-ssh-port", Input).value = str(ssh.get("ssh_port", 22) or 22)
        self.query_one("#r-ssh-user", Input).value = ssh.get("ssh_user", "")
        if ssh.get("ssh_key_file"):
            self.query_one("#r-ssh-auth", Select).value = "key"
            self.query_one("#r-ssh-key", Input).value = ssh.get("ssh_key_file", "")
        else:
            self.query_one("#r-ssh-auth", Select).value = "password"
        self._apply_ssh_auth()
        self._remote_edit = c.get("name")
        self._status("r-status", f"Loaded '{c.get('name')}' into the form.")

    # ------------------------------------------------------------------ #
    # Cloud connection (dynamic, schema-driven — mirrors Tk + Web)
    # ------------------------------------------------------------------ #
    def _cloud_provider(self) -> str:
        val = self.query_one("#cloud-provider", Select).value
        return str(val) if val not in (None, Select.BLANK) else ""

    def _cloud_auth_mode(self) -> str:
        val = self.query_one("#cloud-auth-mode", Select).value
        return str(val) if val not in (None, Select.BLANK) else "keys"

    def _cloud_schema(self) -> dict:
        return (self._cloud.get("providers") or {}).get(self._cloud_provider(), {})

    def _cloud_auth_fields(self) -> tuple[list, str]:
        schema = self._cloud_schema()
        mode = self._cloud_auth_mode()
        if mode == "keys":
            return schema.get("keysAuth", []), ""
        if mode == "pwd":
            return schema.get("pwdAuth", []), ""
        if mode == "sso":
            return (schema.get("ssoAuth") or {}).get("fields", []), ""
        env = schema.get("envAuth") or {}
        return env.get("fields", []), env.get("help", "")

    def _cloud_field_widgets(self, f: dict) -> list:
        wid = "cf-" + f["key"]
        widgets: list = [Label(f["label"])]
        choices = [c for c in (f.get("choices") or []) if c != ""]
        if choices:
            widgets.append(Select([(c, c) for c in choices], id=wid, allow_blank=True))
        else:
            widgets.append(Input(password=bool(f.get("secret")), id=wid))
        return widgets

    async def _mount_fields(self, container_id: str, fields: list) -> None:
        container = self.query_one("#" + container_id, Vertical)
        await container.remove_children()
        widgets: list = []
        for f in fields:
            widgets.extend(self._cloud_field_widgets(f))
        if widgets:
            await container.mount(*widgets)

    async def _refresh_cloud_fields(self) -> None:
        schema = self._cloud_schema()
        auth_fields, help_text = self._cloud_auth_fields()
        await self._mount_fields("cloud-resource-fields", schema.get("resource", []))
        await self._mount_fields("cloud-auth-fields", auth_fields)
        await self._mount_fields("cloud-sql-fields", self._cloud.get("sqlFields", []))
        self.query_one("#cloud-auth-help", Static).update(help_text)
        self._apply_cloud_values()

    def _all_cloud_keys(self) -> list[str]:
        schema = self._cloud_schema()
        keys = [f["key"] for f in schema.get("resource", [])]
        keys += [f["key"] for f in self._cloud_auth_fields()[0]]
        keys += [f["key"] for f in self._cloud.get("sqlFields", [])]
        return keys

    def _cf_get(self, key: str) -> str:
        try:
            w = self.query_one("#cf-" + key)
        except Exception:
            return ""
        if isinstance(w, Select):
            v = w.value
            # The blank sentinel is not a plain string; treat it as empty.
            return v if isinstance(v, str) else ""
        return w.value

    def _cf_set(self, key: str, value: str) -> None:
        try:
            w = self.query_one("#cf-" + key)
        except Exception:
            return
        if isinstance(w, Select):
            if value:
                w.value = value
            else:
                w.clear()
        else:
            w.value = value or ""

    def _capture_cloud_values(self) -> None:
        for key in self._all_cloud_keys():
            self._cloud_values[key] = self._cf_get(key)

    def _apply_cloud_values(self) -> None:
        for key in self._all_cloud_keys():
            if key in self._cloud_values:
                self._cf_set(key, self._cloud_values.get(key, ""))

    def _collect_cloud(self) -> dict:
        self._capture_cloud_values()
        schema = self._cloud_schema()
        profile: dict = {
            "provider": self._cloud_provider(),
            "auth_mode": self._cloud_auth_mode(),
        }
        for f in schema.get("resource", []):
            profile[f["key"]] = self._cf_get(f["key"])
        for f in self._cloud_auth_fields()[0]:
            profile[f["key"]] = self._cf_get(f["key"])
        profile["sql_connection"] = {
            "db_type": self._cf_get("sql_db_type"),
            "host": self._cf_get("sql_host"),
            "port": self._cf_get("sql_port"),
            "service_or_db": self._cf_get("sql_database"),
            "username": self._cf_get("sql_username"),
            "password": self._cf_get("sql_password"),
        }
        return profile

    def _flatten_cloud(self, profile: dict) -> dict:
        vals: dict[str, str] = {}
        for k, v in profile.items():
            if k != "sql_connection":
                vals[k] = "" if v is None else str(v)
        sql = profile.get("sql_connection") or {}
        vals["sql_db_type"] = sql.get("db_type", "")
        vals["sql_host"] = sql.get("host", "")
        vals["sql_port"] = str(sql.get("port", "") or "")
        vals["sql_database"] = sql.get("service_or_db", "") or sql.get("database", "")
        vals["sql_username"] = sql.get("username", "")
        vals["sql_password"] = ""
        return vals

    async def _load_cloud_into_form(self, profile: dict) -> None:
        prov = profile.get("provider") or self._cloud_provider()
        self._cloud_values = self._flatten_cloud(profile)
        self.query_one("#cloud-provider", Select).value = prov
        self.query_one("#cloud-auth-mode", Select).value = profile.get("auth_mode", "keys")
        await self._refresh_cloud_fields()
        self._cloud_edit = profile.get("display_name", "")
        self._status("cloud-status", f"Loaded '{self._cloud_edit}' into the form.")

    def _save_cloud(self) -> str | None:
        profile = self._collect_cloud()
        if not (profile.get("display_name") or "").strip():
            self._status("cloud-status", "Display Name is required.")
            return None
        r = self.svc.save_cloud_db_connection(profile, old_name=self._cloud_edit)
        self._status("cloud-status", r.get("message", str(r)))
        if r.get("ok"):
            self._cloud_edit = r.get("name")
            self._refresh_saved()
            return r.get("name")
        return None

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        # Active section
        if bid == "active-refresh":
            self._refresh_active()
            self._status("active-status", "Refreshed.")
        elif bid == "active-disc":
            name = self._selected("active-table")
            if not name:
                self._status("active-status", "Select an active connection first.")
                return
            self.svc.close_connection(name)
            self._refresh_active()
            self._status("active-status", f"Disconnected {name}.")
        elif bid == "active-disc-all":
            if hasattr(self.svc, "close_all_connections"):
                self.svc.close_all_connections()
            else:
                self.svc.disconnect_all()
            self._refresh_active()
            self._status("active-status", "Disconnected all.")
        # Direct form
        elif bid == "conn-add":
            self._save_direct()
        elif bid == "conn-connect-form":
            kwargs = self._build_kwargs()
            if kwargs is None:
                return
            name = self._upsert(kwargs, self._direct_edit, "conn-status")
            if name:
                r = self.svc.open_connection(name, form=kwargs)
                self._status("conn-status", r.get("message") or str(r))
                self._refresh_active()
        elif bid == "conn-test-form":
            kwargs = self._build_kwargs()
            if kwargs is None:
                return
            await self._run_inline_test(kwargs, "conn-status")
        elif bid == "conn-load-saved":
            self._open_load_picker(
                want_ssh=False,
                title="Load a saved database connection",
                status_id="conn-status",
                on_pick=self._load_into_direct,
                empty="No saved direct (non-SSH) connections yet.",
            )
        elif bid == "conn-clear":
            self._clear_direct()
        # Remote form
        elif bid == "r-save":
            self._save_remote()
        elif bid == "r-connect":
            kwargs = self._build_remote_kwargs()
            if kwargs is None:
                return
            name = self._upsert(kwargs, self._remote_edit, "r-status")
            if name:
                r = self.svc.open_connection(name, form=kwargs)
                self._status("r-status", r.get("message") or str(r))
                self._refresh_active()
        elif bid == "r-test":
            kwargs = self._build_remote_kwargs()
            if kwargs is None:
                return
            await self._run_inline_test(kwargs, "r-status")
        elif bid == "r-load-saved":
            self._open_load_picker(
                want_ssh=True,
                title="Load a saved remote (SSH) connection",
                status_id="r-status",
                on_pick=self._load_into_remote,
                empty="No saved remote (SSH tunnel) connections yet.",
            )
        elif bid == "r-clear":
            self._clear_remote()
        # Saved section
        elif bid == "conn-refresh":
            self._refresh_saved()
            self._status("saved-status", "Refreshed.")
        elif bid == "conn-connect":
            name = self._selected("conn-table")
            if not name:
                self._status("saved-status", "Select a connection first.")
                return
            r = self.svc.open_connection(name) if hasattr(self.svc, "open_connection") \
                else self.svc.test_connection(name)
            self._status("saved-status", r.get("message") or str(r))
            self._refresh_active()
        elif bid == "conn-test":
            name = self._selected("conn-table")
            if not name:
                self._status("saved-status", "Select a connection first.")
                return
            r = self.svc.test_connection(name)
            self._status("saved-status", r.get("message") or str(r))
            self._refresh_active()
        elif bid == "conn-remove":
            name = self._selected("conn-table")
            if not name:
                self._status("saved-status", "Select a connection first.")
                return
            r = self.svc.remove_connection(name)
            self._status("saved-status", r.get("message", str(r)))
            self._refresh_saved()
            self._refresh_active()
        # Cloud section
        elif bid == "cloud-save":
            self._save_cloud()
        elif bid == "cloud-connect":
            profile = self._collect_cloud()
            if not (profile.get("display_name") or "").strip():
                self._status("cloud-status", "Display Name is required.")
                return
            r = self.svc.connect_cloud_db(profile, old_name=self._cloud_edit)
            self._status("cloud-status", r.get("message", str(r)))
            if r.get("ok"):
                self._cloud_edit = profile.get("display_name")
                self._refresh_saved()
                self._refresh_active()
        elif bid == "cloud-test-login":
            r = self.svc.cloud_db_test_login(self._collect_cloud())
            self._status("cloud-status", r.get("message", str(r)))
        elif bid == "cloud-test-db":
            r = self.svc.test_cloud_db(self._collect_cloud())
            self._status("cloud-status", r.get("message", str(r)))
        elif bid == "cloud-resolve":
            r = self.svc.resolve_cloud_db_endpoint(self._collect_cloud())
            if r.get("ok"):
                self._cf_set("sql_host", r.get("host", ""))
                self._cf_set("sql_port", str(r.get("port", "") or ""))
                if r.get("db_type"):
                    self._cf_set("sql_db_type", r.get("db_type"))
            self._status("cloud-status", r.get("message", str(r)))
        elif bid == "cloud-load-saved":
            await self._cloud_load_saved()
        elif bid == "cloud-clear":
            self._cloud_values = {}
            self._cloud_edit = None
            providers = self._cloud.get("providerOrder") or ["AWS"]
            self.query_one("#cloud-provider", Select).value = providers[0]
            self.query_one("#cloud-auth-mode", Select).value = "keys"
            await self._refresh_cloud_fields()
            self._status("cloud-status", "Cleared.")

    async def _cloud_load_saved(self) -> None:
        rows = self.svc.list_cloud_db_connections()
        if not rows:
            self._status("cloud-status", "No saved cloud connections yet.")
            return
        options = [(self._cloud_label(r), r.get("name")) for r in rows]

        async def _on_pick(name: Any) -> None:
            if not name:
                self._status("cloud-status", "Load cancelled.")
                return
            full = self.svc.get_cloud_db_connection(name) or {}
            await self._load_cloud_into_form(full)

        self.app.push_screen(
            SelectModal("Load a saved cloud database connection", options,
                        empty_message="No saved cloud connections yet."),
            _on_pick,
        )

    @staticmethod
    def _cloud_label(row: dict) -> str:
        name = row.get("name", "")
        provider = row.get("provider", "")
        return f"{name}  ({provider})" if provider else str(name)

    # ------------------------------------------------------------------ #
    def _saved_label(self, c: dict) -> str:
        name = c.get("name", "")
        db_type = c.get("db_type", c.get("type", ""))
        host = c.get("host", "")
        port = c.get("port", "")
        loc = f"{host}:{port}" if port else host
        bits = [b for b in (db_type, loc) if b]
        return f"{name}  ({' @ '.join(bits)})" if bits else str(name)

    def _open_load_picker(self, *, want_ssh: bool, title: str, status_id: str,
                          on_pick, empty: str) -> None:
        self._refresh_saved()
        rows = [c for c in self._saved if bool(c.get("ssh_tunnel")) == want_ssh]
        options = [(self._saved_label(c), c.get("name")) for c in rows]
        by_name = {c.get("name"): c for c in rows}

        def _handle(name: Any) -> None:
            if not name:
                self._status(status_id, "Load cancelled.")
                return
            chosen = by_name.get(name)
            if chosen is not None:
                on_pick(chosen)

        self.app.push_screen(SelectModal(title, options, empty_message=empty), _handle)
