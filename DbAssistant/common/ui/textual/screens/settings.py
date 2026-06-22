"""Settings screen — view configuration, clear caches, show shortcuts."""

from __future__ import annotations

from textual.containers import Horizontal
from textual.widgets import Button, DataTable, Input, Static, TextArea

from common.ui.textual.screens.base import BaseScreen


class SettingsScreen(BaseScreen):
    """Read-only settings view plus app-level maintenance actions."""

    NAV_ID = "settings"

    def __init__(self, svc, **kwargs) -> None:
        super().__init__(svc, **kwargs)
        self._settings: list[dict] = []

    def screen_title(self) -> str:
        return "Settings"

    def compose_body(self):
        with Horizontal(classes="actions-row"):
            yield Button("Save changes", id="set-save", variant="primary")
            yield Button("Reload", id="set-reload", variant="primary")
            yield Button("Restore defaults…", id="set-restore-defaults")
            yield Button("Clear caches", id="set-clear-cache", variant="error")
            yield Button("Shortcuts", id="set-shortcuts")
            yield Button("API keys", id="set-apikey-list")
            yield Button("Create API key", id="set-apikey-create")
            yield Button("Revoke/regenerate key", id="set-apikey-edit")
        yield Input(id="set-filter", placeholder="Filter settings…")
        yield DataTable(id="set-grid", zebra_stripes=True)
        yield TextArea("", id="set-out", read_only=True)
        yield Static("", id="set-status", classes="status")

    def on_mount(self) -> None:
        self.query_one("#set-grid", DataTable).add_columns("Group", "Setting", "Value", "Description")
        self._load()

    def _status(self, msg: str) -> None:
        self.query_one("#set-status", Static).update(msg)

    def _load(self) -> None:
        try:
            from common.config import settings_service as S
            self._settings = S.describe_all(redact=True)
        except Exception as exc:
            self._status(str(exc))
            self._settings = []
        self._render_rows()
        self._status(f"{len(self._settings)} settings (select a row, then Save changes to edit).")

    def _visible_settings(self) -> list[dict]:
        flt = self.query_one("#set-filter", Input).value.strip().lower()
        out = []
        for s in self._settings:
            blob = f"{s.get('group','')} {s.get('label','')} {s.get('key','')} {s.get('description','')}".lower()
            if flt and flt not in blob:
                continue
            out.append(s)
        return out

    def _render_rows(self) -> None:
        grid = self.query_one("#set-grid", DataTable)
        grid.clear()
        for s in self._visible_settings():
            grid.add_row(
                str(s.get("group", s.get("section", ""))),
                str(s.get("label", s.get("key", s.get("id", "")))),
                str(s.get("value", s.get("current", ""))),
                str(s.get("description", ""))[:80])

    def on_input_changed(self, event: Input.Changed) -> None:
        if (event.input.id or "") == "set-filter":
            self._render_rows()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "set-save":
            self._edit_selected()
        elif bid == "set-reload":
            self._load()
        elif bid == "set-restore-defaults":
            try:
                from common.config import settings_service as S
                r = S.restore_defaults("all")
                import json
                self.query_one("#set-out", TextArea).text = json.dumps(r, indent=2, default=str)
                self._status(r.get("message", "Defaults restored."))
                self._load()
            except Exception as exc:  # noqa: BLE001
                self._status(str(exc))
        elif bid == "set-clear-cache":
            try:
                from common.headless import app_service as appsvc
                r = appsvc.clear_all_caches(self.svc)
                import json
                self.query_one("#set-out", TextArea).text = json.dumps(r, indent=2, default=str)
                self._status(r.get("summary", "Caches cleared."))
            except Exception as exc:
                self._status(str(exc))
        elif bid == "set-shortcuts":
            try:
                from common.headless import app_service as appsvc
                r = appsvc.list_shortcuts()
                lines = [f"[{s.get('section')}] {s.get('shortcut')} — {s.get('action')}"
                         for s in (r.get("shortcuts") or [])]
                self.query_one("#set-out", TextArea).text = "\n".join(lines) or "No shortcuts available."
            except Exception as exc:
                self._status(str(exc))
        elif bid == "set-apikey-list":
            self._apikey_list()
        elif bid == "set-apikey-create":
            self._apikey_create()
        elif bid == "set-apikey-edit":
            self._apikey_edit()

    def _apikey_list(self) -> None:
        import json
        from common.security import api_keys

        keys = api_keys.list_keys()
        self.query_one("#set-out", TextArea).text = json.dumps(keys, indent=2, default=str)
        self._status(f"{len(keys)} API key(s). Secrets are never shown.")

    def _apikey_create(self) -> None:
        from common.ui.textual.screens.form_modal import FormModal

        def _done(v: dict | None) -> None:
            if not v:
                return
            from common.security import api_keys
            r = api_keys.create_key(str(v.get("name") or ""))
            self.query_one("#set-out", TextArea).text = (
                "Save this token now. It will not be shown again.\n\n" + r["token"]
            )
            self._status("API key created.")

        self.app.push_screen(
            FormModal("Create API key", [{"name": "name", "label": "Friendly name"}],
                      submit_label="Create"),
            _done,
        )

    def _apikey_edit(self) -> None:
        from common.security import api_keys
        from common.ui.textual.screens.form_modal import FormModal

        keys = api_keys.list_keys()
        if not keys:
            self._status("No API keys exist yet.")
            return

        def _done(v: dict | None) -> None:
            if not v:
                return
            key_id = str(v.get("key_id") or "")
            action = str(v.get("action") or "revoke")
            if action == "regenerate":
                r = api_keys.regenerate_key(key_id)
                if r.get("ok"):
                    self.query_one("#set-out", TextArea).text = (
                        "Save this regenerated token now. It will not be shown again.\n\n"
                        + r["token"]
                    )
                else:
                    self.query_one("#set-out", TextArea).text = str(r.get("error"))
            else:
                r = api_keys.revoke_key(key_id)
                import json
                self.query_one("#set-out", TextArea).text = json.dumps(r, indent=2, default=str)
            self._status(f"API key {action} complete.")

        self.app.push_screen(
            FormModal(
                "Revoke/regenerate API key",
                [
                    {"name": "key_id", "label": "Key", "type": "select",
                     "options": [(k["key_id"], f"{k['key_id']} — {k.get('name','')}")
                                 for k in keys],
                     "value": keys[0]["key_id"]},
                    {"name": "action", "label": "Action", "type": "select",
                     "options": [("revoke", "Revoke"), ("regenerate", "Regenerate")],
                     "value": "revoke"},
                ],
                submit_label="Run",
            ),
            _done,
        )

    def _edit_selected(self) -> None:
        grid = self.query_one("#set-grid", DataTable)
        visible = self._visible_settings()
        row = grid.cursor_row
        if row is None or row < 0 or row >= len(visible):
            self._status("Select a setting row first, then Save changes.")
            return
        s = visible[row]
        sid = s.get("id") or s.get("key") or ""
        from common.ui.textual.screens.form_modal import FormModal

        stype = str(s.get("type", "str"))
        options = list(s.get("options") or [])
        sensitive = bool(s.get("sensitive"))
        label = s.get("label", sid)
        # Build a type-aware field (matches Tk's per-type widgets): checkbox for
        # bool, select for enum/options, masked input for secrets (write-only —
        # blank leaves it unchanged), plain input otherwise.
        if sensitive:
            field = {"name": "value", "label": f"{label} (blank = keep current)",
                     "type": "password", "value": ""}
        elif stype == "bool":
            cur = str(s.get("value", "")).strip().lower() in ("1", "true", "yes", "on")
            field = {"name": "value", "label": label, "type": "checkbox", "value": cur}
        elif options:
            field = {"name": "value", "label": label, "type": "select",
                     "options": [(o, o) for o in options],
                     "value": str(s.get("value", "")) or options[0]}
        else:
            unit = f" ({s.get('unit')})" if s.get("unit") else ""
            field = {"name": "value", "label": f"{label}{unit}",
                     "value": str(s.get("value", s.get("current", "")))}

        def _done(v: dict | None) -> None:
            if not v:
                return
            raw = v.get("value", "")
            if sensitive and not str(raw).strip():
                self._status(f"'{label}' left unchanged.")
                return
            if stype == "bool":
                value = "true" if raw else "false"
            else:
                value = str(raw)
            from common.config import settings_service as S
            r = S.set_value(sid, value)
            import json
            self.query_one("#set-out", TextArea).text = json.dumps(r, indent=2, default=str)
            self._status(r.get("message", "Saved." if r.get("ok") else "Save failed."))
            self._load()

        self.app.push_screen(FormModal(f"Edit {label}", [field], submit_label="Save"), _done)
