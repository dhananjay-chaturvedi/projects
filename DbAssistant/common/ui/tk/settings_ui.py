"""Settings tab — a friendly editor for config.ini / properties.ini.

Renders entirely from :mod:`common.config.settings_schema` (the same schema the
``config`` CLI and the read-only config API use), so the surfaces never drift.

Features
--------
* Grouped, scrollable form with a one-line description under every field.
* The right widget per type: dropdowns for enums/booleans, entries for
  text/numbers, masked entries for secrets (Teams webhook, SMTP password).
* Inline per-field detail (range / unit / default / "restart required").
* "Save changes" with a confirmation dialog listing exactly what will change.
* "Restore defaults" (from the shipped ``*.ini.example`` files) with a
  confirmation dialog.
* Notification secrets are write-only: the field shows "configured" status but
  never the stored value.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

from common.config import settings_service as S
from common.ui.tk.theme import ColorTheme, default_ui_font
from common.ui.tk.widgets import disable_combobox_mousewheel, make_scrollable

_GROUP_HELP = {
    "Database": "How the tool connects to and talks to databases.",
    "Performance": "Batch sizes and limits for transfers and comparisons.",
    "Monitoring": "Polling intervals and refresh behaviour for the Monitor tab.",
    "SSH": "Timeouts for remote OS monitoring over SSH.",
    "AI": "AI Query Assistant backend and behaviour.",
    "Interface": "Window sizes and result/grid limits.",
    "General": "Logging, debug, timezone and application identity.",
    "Notifications": "Where threshold alerts are delivered (Teams / email). "
                     "Secrets are stored encrypted, never in plain text.",
}


class SettingsUI:
    def __init__(
        self,
        parent_frame,
        root,
        update_status_callback=None,
        theme=None,
        on_settings_saved=None,
    ):
        self.parent = parent_frame
        self.root = root
        self.update_status = update_status_callback or (lambda *a, **k: None)
        self.theme = theme or ColorTheme
        self.on_settings_saved = on_settings_saved
        self.ui_font = default_ui_font()
        # spec_id -> {"spec": SettingSpec, "var": tk.Variable, "widget": w,
        #             "original": str}
        self._fields: dict = {}
        self._status_var = None

    # ------------------------------------------------------------------ #
    def create_ui(self):
        disable_combobox_mousewheel(self.root)

        outer = ttk.Frame(self.parent)
        outer.pack(fill=tk.BOTH, expand=True)

        # ---- Header -----------------------------------------------------
        header = tk.Frame(outer, bg=self.theme.BG_SECONDARY)
        header.pack(fill=tk.X)
        tk.Label(
            header, text="Settings", font=(self.ui_font[0], 15, "bold"),
            bg=self.theme.BG_SECONDARY, fg=self.theme.TEXT_PRIMARY,
        ).pack(side=tk.LEFT, padx=12, pady=8)
        tk.Label(
            header,
            text="Edit configuration & properties. Changes are saved to your "
                 "config.ini / properties.ini.",
            font=(self.ui_font[0], 9), bg=self.theme.BG_SECONDARY,
            fg=self.theme.TEXT_SECONDARY,
        ).pack(side=tk.LEFT, padx=4)

        # ---- Action bar (top) ------------------------------------------
        actions = ttk.Frame(outer)
        actions.pack(fill=tk.X, padx=10, pady=(8, 0))
        ttk.Button(actions, text="Save changes", style="Primary.TButton",
                   command=self._on_save).pack(side=tk.LEFT)
        ttk.Button(actions, text="Reload", command=self._reload_values).pack(
            side=tk.LEFT, padx=6)
        ttk.Button(actions, text="Restore defaults…",
                   command=self._on_restore).pack(side=tk.LEFT)
        self._status_var = tk.StringVar(value="")
        ttk.Label(actions, textvariable=self._status_var,
                  foreground=self.theme.TEXT_SECONDARY).pack(side=tk.LEFT, padx=12)

        self._build_access_keys(outer)

        # ---- Scrollable body -------------------------------------------
        body = ttk.Frame(outer)
        body.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        inner = make_scrollable(body, bg=self.theme.BG_MAIN)

        self._build_groups(inner)
        self.update_status("Settings loaded.")

    def _build_access_keys(self, parent):
        box = ttk.LabelFrame(parent, text="  Access Keys  ")
        box.pack(fill=tk.X, expand=False, padx=10, pady=(8, 0))
        top = ttk.Frame(box)
        top.pack(fill=tk.X, padx=8, pady=6)
        ttk.Button(top, text="Create key", command=self._apikey_create).pack(side=tk.LEFT)
        ttk.Button(top, text="Refresh", command=self._apikey_refresh).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="Regenerate selected",
                   command=self._apikey_regenerate).pack(side=tk.LEFT, padx=6)
        ttk.Button(top, text="Revoke selected",
                   command=self._apikey_revoke).pack(side=tk.LEFT, padx=6)
        self._apikey_tree = ttk.Treeview(
            box,
            columns=("key_id", "name", "created", "last_used", "revoked"),
            show="headings",
            height=4,
        )
        for col, label, width in (
            ("key_id", "Key ID", 190),
            ("name", "Name", 160),
            ("created", "Created", 180),
            ("last_used", "Last used", 180),
            ("revoked", "Revoked", 180),
        ):
            self._apikey_tree.heading(col, text=label)
            self._apikey_tree.column(col, width=width, stretch=True)
        self._apikey_tree.pack(fill=tk.X, padx=8, pady=(0, 8))
        self._apikey_refresh()

    def _apikey_service(self):
        from common.security import api_keys
        return api_keys

    def _apikey_selected(self) -> str:
        sel = self._apikey_tree.selection()
        if not sel:
            messagebox.showinfo("Access Keys", "Select an API key first.")
            return ""
        return str(self._apikey_tree.item(sel[0], "values")[0])

    def _apikey_refresh(self):
        svc = self._apikey_service()
        tree = self._apikey_tree
        for item in tree.get_children():
            tree.delete(item)
        for r in svc.list_keys():
            tree.insert("", tk.END, values=(
                r.get("key_id", ""),
                r.get("name", ""),
                r.get("created_at", ""),
                r.get("last_used_at", "") or "-",
                r.get("revoked_at", "") or "-",
            ))
        if self._status_var is not None:
            self._status_var.set("Access keys loaded.")

    def _show_token_once(self, title: str, token: str):
        messagebox.showinfo(
            title,
            "Save this token now. It will not be shown again:\n\n" + token,
        )

    def _apikey_create(self):
        name = simpledialog.askstring("Create API key", "Friendly name:", parent=self.root)
        if name is None:
            return
        r = self._apikey_service().create_key(name)
        self._apikey_refresh()
        self._show_token_once("API key created", r["token"])

    def _apikey_regenerate(self):
        key_id = self._apikey_selected()
        if not key_id:
            return
        r = self._apikey_service().regenerate_key(key_id)
        if not r.get("ok"):
            messagebox.showerror("Regenerate API key", r.get("error", "Failed."))
            return
        self._apikey_refresh()
        self._show_token_once("API key regenerated", r["token"])

    def _apikey_revoke(self):
        key_id = self._apikey_selected()
        if not key_id:
            return
        if not messagebox.askyesno("Revoke API key", f"Revoke {key_id}?"):
            return
        r = self._apikey_service().revoke_key(key_id)
        if not r.get("ok"):
            messagebox.showerror("Revoke API key", r.get("error", "Failed."))
        self._apikey_refresh()

    # ------------------------------------------------------------------ #
    def _build_groups(self, parent):
        grouped = S.grouped(redact=True)
        for group, specs in grouped.items():
            box = ttk.LabelFrame(parent, text=f"  {group}  ")
            box.pack(fill=tk.X, expand=True, padx=8, pady=6)
            help_text = _GROUP_HELP.get(group, "")
            if help_text:
                tk.Label(
                    box, text=help_text, font=(self.ui_font[0], 8, "italic"),
                    fg=self.theme.TEXT_SECONDARY, bg=self.theme.BG_MAIN,
                    anchor=tk.W, justify=tk.LEFT, wraplength=820,
                ).pack(fill=tk.X, padx=10, pady=(4, 6))
            for d in specs:
                self._build_field(box, d)

    def _build_field(self, parent, d: dict):
        spec = S.find(d["id"])
        row = ttk.Frame(parent)
        row.pack(fill=tk.X, padx=10, pady=(2, 8))

        top = ttk.Frame(row)
        top.pack(fill=tk.X)
        label = d["label"] + (" *" if d["sensitive"] else "")
        ttk.Label(top, text=label, width=26, anchor=tk.W,
                  font=(self.ui_font[0], 10, "bold")).pack(side=tk.LEFT)

        var = tk.StringVar(value="" if d["type"] == "secret" else str(d["value"]))
        widget = self._make_widget(top, d, var)
        widget.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(6, 0))

        # detail line
        bits = []
        if d["type"] == "secret":
            bits.append("configured" if d["value"] == "***set***" else "not set")
            bits.append("write-only — leave blank to keep current")
        else:
            if d["options"]:
                bits.append("options: " + ", ".join(d["options"]))
            if d["minimum"] is not None or d["maximum"] is not None:
                bits.append(f"range {d['minimum']}–{d['maximum']}")
            if d["unit"]:
                bits.append(f"unit: {d['unit']}")
            bits.append(f"default: {d['default'] or '(blank)'}")
        if d["requires_restart"]:
            bits.append("takes effect after restart")

        desc = d["description"]
        if bits:
            desc += "    [" + " | ".join(bits) + "]"
        tk.Label(row, text=desc, font=(self.ui_font[0], 8),
                 fg=self.theme.TEXT_SECONDARY, bg=self.theme.BG_MAIN,
                 anchor=tk.W, justify=tk.LEFT, wraplength=820).pack(
            fill=tk.X, padx=(0, 0), pady=(1, 0))

        self._fields[d["id"]] = {
            "spec": spec, "var": var, "widget": widget,
            "original": var.get(), "type": d["type"],
        }

    def _make_widget(self, parent, d: dict, var: tk.StringVar):
        t = d["type"]
        if t == "bool":
            return ttk.Combobox(parent, textvariable=var, values=["true", "false"],
                                state="readonly", width=18)
        if t == "enum":
            return ttk.Combobox(parent, textvariable=var, values=list(d["options"]),
                                state="readonly", width=28)
        if t == "secret":
            return ttk.Entry(parent, textvariable=var, show="*", width=42)
        return ttk.Entry(parent, textvariable=var, width=42)

    # ------------------------------------------------------------------ #
    def _reload_values(self):
        for spec_id, f in self._fields.items():
            d = S.describe(f["spec"], redact=True)
            if f["type"] == "secret":
                f["var"].set("")
                f["original"] = ""
            else:
                f["var"].set(str(d["value"]))
                f["original"] = str(d["value"])
        self._set_status("Reloaded from disk.")

    def _collect_changes(self) -> dict:
        """Return {spec_id: new_value} for fields that changed."""
        changes = {}
        for spec_id, f in self._fields.items():
            current = f["var"].get()
            if f["type"] == "secret":
                if current:  # only when user typed something
                    changes[spec_id] = current
            elif current != f["original"]:
                changes[spec_id] = current
        return changes

    def _on_save(self):
        changes = self._collect_changes()
        if not changes:
            messagebox.showinfo("No changes", "Nothing to save.", parent=self.root)
            return

        lines = []
        for spec_id, val in changes.items():
            f = self._fields[spec_id]
            label = f["spec"].label
            if f["type"] == "secret":
                lines.append(f"  • {label}: (new secret value)")
            else:
                lines.append(f"  • {label}: {f['original'] or '(blank)'} → {val or '(blank)'}")
        msg = "Save the following change(s)?\n\n" + "\n".join(lines)
        if not messagebox.askyesno("Confirm save", msg, parent=self.root):
            return

        result = S.set_many(changes)
        if not result["ok"]:
            errs = "\n".join(f"  • {k}: {v}" for k, v in result.get("errors", {}).items())
            messagebox.showerror(
                "Save failed",
                "Some settings could not be saved:\n\n" + errs, parent=self.root,
            )
            # still reflect any that did save
        # Refresh originals for saved ids
        self._reload_values()
        saved = result.get("saved", [])
        note = f"Saved {len(saved)} setting(s)."
        if result.get("requires_restart"):
            note += " Some changes take effect after restart."
        self._set_status(note)
        self.update_status(note)
        if self.on_settings_saved:
            self.on_settings_saved(saved)
        if result["ok"]:
            messagebox.showinfo("Saved", note, parent=self.root)

    def _on_restore(self):
        if not messagebox.askyesno(
            "Restore defaults",
            "Restore ALL settings to the shipped defaults?\n\n"
            "This overwrites your edits in config.ini and properties.ini with "
            "the values from the *.ini.example defaults. Notification secrets "
            "are not affected.\n\nContinue?",
            icon="warning", parent=self.root,
        ):
            return
        result = S.restore_defaults("all")
        self._reload_values()
        if self.on_settings_saved:
            self.on_settings_saved(["config.database.connection.default_autocommit"])
        self._set_status(result["message"])
        (messagebox.showinfo if result["ok"] else messagebox.showerror)(
            "Restore defaults", result["message"], parent=self.root,
        )

    def _set_status(self, text: str):
        if self._status_var is not None:
            self._status_var.set(text)
