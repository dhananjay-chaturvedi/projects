"""Generic module-owned INI settings dialog (scrollable form + save/restore)."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable, Optional, Sequence

from common.ui.tk.widgets import make_scrollable

# (section, key, label, type, options_tuple_or_empty)
FieldSpec = tuple[str, str, str, str, tuple[str, ...]]


def open_module_config_dialog(
    root,
    *,
    title: str,
    config_module,
    fields: Sequence[FieldSpec],
    on_saved: Optional[Callable[[], None]] = None,
    secret_fields: Sequence[tuple[str, str]] = (),
):
    """Open a modal editor for module INI settings.

    ``config_module`` must expose ``get``, ``get_bool``, ``set_value``,
    ``restore_defaults``. ``secret_fields`` are (key, label) pairs written via
    :class:`common.notifications.NotificationSecretStore` when provided.
    """
    dlg = tk.Toplevel(root)
    dlg.title(title)
    dlg.geometry("720x520")
    dlg.minsize(520, 360)
    dlg.transient(root)
    dlg.grab_set()

    # Layout, top to bottom: action bar -> separator -> scrollable form.
    # Each region is packed in order with side=TOP so the bar always renders as
    # a full-width strip directly above the form (never beside it).
    btn = ttk.Frame(dlg, padding=(10, 8))
    btn.pack(side=tk.TOP, fill=tk.X)
    ttk.Separator(dlg, orient=tk.HORIZONTAL).pack(side=tk.TOP, fill=tk.X)

    body = ttk.Frame(dlg)
    body.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    inner = make_scrollable(body)
    inner.configure(padding=12)
    inner.columnconfigure(1, weight=1)

    widgets: dict[tuple[str, str], tk.Variable] = {}

    def _current(section: str, key: str, ftype: str) -> str:
        if ftype == "bool":
            return "true" if config_module.get_bool(section, key, default=False) else "false"
        return str(config_module.get(section, key, default="") or "")

    row = 0
    last_sec = ""
    for section, key, label, ftype, options in fields:
        if section != last_sec:
            ttk.Label(inner, text=f"[{section}]", font=("", 11, "bold")).grid(
                row=row, column=0, columnspan=2, sticky=tk.W, pady=(12, 4)
            )
            row += 1
            last_sec = section
        ttk.Label(inner, text=label, wraplength=280).grid(
            row=row, column=0, sticky=tk.W, padx=(4, 12), pady=2
        )
        cur = _current(section, key, ftype)
        if ftype == "bool":
            var = tk.BooleanVar(value=cur.lower() in ("true", "1", "yes", "on"))
            ttk.Checkbutton(inner, variable=var).grid(row=row, column=1, sticky=tk.W, pady=2)
        elif ftype == "enum" and options:
            var = tk.StringVar(value=cur)
            ttk.Combobox(inner, textvariable=var, values=list(options)).grid(
                row=row, column=1, sticky=tk.EW, pady=2
            )
        else:
            var = tk.StringVar(value=cur)
            ttk.Entry(inner, textvariable=var).grid(row=row, column=1, sticky=tk.EW, pady=2)
        widgets[(section, key)] = var
        row += 1

    if secret_fields:
        ttk.Label(inner, text="[secrets]", font=("", 11, "bold")).grid(
            row=row, column=0, columnspan=2, sticky=tk.W, pady=(12, 4)
        )
        row += 1
        from common.notifications import NotificationSecretStore

        store = NotificationSecretStore()
        for skey, slabel in secret_fields:
            ttk.Label(inner, text=slabel).grid(row=row, column=0, sticky=tk.W, padx=(4, 12), pady=2)
            hint = "configured" if store.has(skey) else "not set"
            var = tk.StringVar()
            ttk.Label(inner, text=f"({hint}; enter new value to change)").grid(
                row=row, column=1, sticky=tk.W, pady=2
            )
            ent = ttk.Entry(inner, textvariable=var, show="*")
            ent.grid(row=row + 1, column=1, sticky=tk.EW, pady=2)
            widgets[("__secret__", skey)] = var
            row += 2

    def _save():
        try:
            for (section, key), var in widgets.items():
                if section == "__secret__":
                    val = var.get().strip()
                    if val:
                        from common.notifications import NotificationSecretStore
                        NotificationSecretStore().set(key, val)
                    continue
                if isinstance(var, tk.BooleanVar):
                    config_module.set_value(section, key, "true" if var.get() else "false")
                else:
                    config_module.set_value(section, key, var.get().strip())
        except Exception as exc:
            messagebox.showerror(title, f"Save failed:\n{exc}", parent=dlg)
            return
        if on_saved:
            on_saved()
        messagebox.showinfo(title, "Settings saved.", parent=dlg)
        dlg.destroy()

    def _restore():
        if not messagebox.askyesno(
            title, "Restore all module settings to shipped defaults?", parent=dlg
        ):
            return
        try:
            config_module.restore_defaults()
        except Exception as exc:
            messagebox.showerror(title, str(exc), parent=dlg)
            return
        dlg.destroy()
        open_module_config_dialog(
            root, title=title, config_module=config_module, fields=fields,
            on_saved=on_saved, secret_fields=secret_fields,
        )

    ttk.Button(btn, text="Save", command=_save).pack(side=tk.RIGHT, padx=4)
    ttk.Button(btn, text="Restore defaults", command=_restore).pack(side=tk.RIGHT, padx=4)
    ttk.Button(btn, text="Cancel", command=dlg.destroy).pack(side=tk.RIGHT)
