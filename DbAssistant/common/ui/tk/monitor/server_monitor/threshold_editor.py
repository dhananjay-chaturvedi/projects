"""Threshold rules editor dialog (monitor_thresholds.ini).

Opened from the Monitor tab's top-right "Alert Settings" button. Lists every
threshold rule and lets the user edit the critical/warning/info levels, the
comparison operator, the sustained-breach window, and enable/disable a rule.

All writes go through :meth:`ThresholdChecker.update_rule` /
:meth:`ThresholdChecker.set_enabled`, which perform comment-preserving,
validated, surgical edits of ``monitor_thresholds.ini``.
"""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk


def open_threshold_editor(root, checker, *, on_change=None):
    """Open the threshold editor as a modal dialog.

    Parameters
    ----------
    root: the Tk root / parent window.
    checker: a ``ThresholdChecker`` instance (must expose ``all_rules``,
        ``update_rule`` and ``set_enabled``).
    on_change: optional callback invoked after any successful write so the
        caller can reload its in-memory checker.
    """
    if checker is None:
        messagebox.showerror(
            "Alert Settings",
            "Threshold checker is unavailable (monitor_thresholds.ini not found).",
            parent=root,
        )
        return

    dlg = tk.Toplevel(root)
    dlg.title("Alert Thresholds — monitor_thresholds.ini")
    dlg.geometry("960x560")
    dlg.transient(root)
    dlg.grab_set()

    # ---- Filter bar ----------------------------------------------------
    bar = ttk.Frame(dlg, padding=8)
    bar.pack(fill=tk.X)
    ttk.Label(bar, text="Source:").pack(side=tk.LEFT)
    source_var = tk.StringVar(value="all")
    src_combo = ttk.Combobox(
        bar, textvariable=source_var, state="readonly", width=12,
        values=["all", "db", "os", "aws", "azure", "gcp"],
    )
    src_combo.pack(side=tk.LEFT, padx=(4, 12))
    ttk.Label(bar, text="Search:").pack(side=tk.LEFT)
    search_var = tk.StringVar()
    search_entry = ttk.Entry(bar, textvariable=search_var, width=24)
    search_entry.pack(side=tk.LEFT, padx=4)
    ttk.Label(
        bar, text="Double-click a rule to edit. Blank a level to disable it.",
        foreground="#757575",
    ).pack(side=tk.RIGHT)

    # ---- Tree ----------------------------------------------------------
    cols = ("source", "path", "metric", "operator",
            "critical", "warning", "info", "window", "enabled")
    tree = ttk.Frame(dlg)
    tree.pack(fill=tk.BOTH, expand=True, padx=8)
    tv = ttk.Treeview(tree, columns=cols, show="headings", selectmode="browse")
    widths = {"source": 60, "path": 200, "metric": 180, "operator": 70,
              "critical": 80, "warning": 80, "info": 70, "window": 60,
              "enabled": 70}
    for c in cols:
        tv.heading(c, text=c)
        tv.column(c, width=widths[c], anchor=tk.W)
    vs = ttk.Scrollbar(tree, orient=tk.VERTICAL, command=tv.yview)
    tv.configure(yscrollcommand=vs.set)
    tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    vs.pack(side=tk.RIGHT, fill=tk.Y)

    # rule lookup keyed by tree item id
    item_rules: dict[str, object] = {}

    def _fmt(v):
        return "" if v is None else v

    def _refresh():
        tv.delete(*tv.get_children())
        item_rules.clear()
        src = source_var.get()
        needle = search_var.get().strip().lower()
        for r in sorted(checker.all_rules(),
                        key=lambda x: (x.source, x.path_str, x.metric)):
            if src != "all" and r.source != src:
                continue
            hay = f"{r.source} {r.path_str} {r.metric} {r.metric_name}".lower()
            if needle and needle not in hay:
                continue
            iid = tv.insert("", tk.END, values=(
                r.source, r.path_str, r.metric, r.operator,
                _fmt(r.critical), _fmt(r.warning), _fmt(r.info),
                r.window, "yes" if r.enabled else "no",
            ))
            item_rules[iid] = r

    def _selected_rule():
        sel = tv.selection()
        if not sel:
            return None
        return item_rules.get(sel[0])

    def _persist(rule, changes):
        path = list(rule.path) if rule.path else None
        res = checker.update_rule(rule.source, rule.metric, changes, path=path)
        if not res.get("ok"):
            messagebox.showerror("Save failed", res.get("message", "Unknown error"),
                                 parent=dlg)
            return False
        if on_change:
            try:
                on_change()
            except Exception:
                pass
        _refresh()
        return True

    def _edit(_event=None):
        rule = _selected_rule()
        if rule is None:
            return
        _open_edit_dialog(dlg, rule, _persist)

    def _toggle(enable: bool):
        rule = _selected_rule()
        if rule is None:
            messagebox.showinfo("Alert Settings", "Select a rule first.", parent=dlg)
            return
        path = list(rule.path) if rule.path else None
        res = checker.set_enabled(rule.source, rule.metric, enable, path=path)
        if not res.get("ok"):
            messagebox.showerror("Failed", res.get("message", ""), parent=dlg)
            return
        if on_change:
            try:
                on_change()
            except Exception:
                pass
        _refresh()

    def _add_rule():
        d = tk.Toplevel(dlg)
        d.title("Add threshold rule")
        d.geometry("480x360")
        d.transient(dlg)
        d.grab_set()
        frm = ttk.Frame(d, padding=12)
        frm.pack(fill=tk.BOTH, expand=True)
        vars_: dict[str, tk.StringVar] = {}

        def _row(label, key, default=""):
            row = ttk.Frame(frm)
            row.pack(fill=tk.X, pady=3)
            ttk.Label(row, text=label, width=18, anchor=tk.W).pack(side=tk.LEFT)
            v = tk.StringVar(value=default)
            ttk.Entry(row, textvariable=v, width=28).pack(side=tk.LEFT)
            vars_[key] = v

        _row("Source (db/os/aws…)", "source", "db")
        _row("Rule id / metric key", "metric")
        _row("Path (dot-separated)", "path", "")
        _row("Operator", "operator", ">")
        _row("Critical", "critical")
        _row("Warning", "warning")
        _row("Info", "info")
        _row("Window", "window", "3")
        _row("Description", "description")

        def _do_add():
            path_raw = vars_["path"].get().strip()
            path_parts = tuple(p for p in path_raw.split(".") if p) or None
            fields = {k: v.get() for k, v in vars_.items() if k not in ("source", "metric", "path")}
            res = checker.add_rule(
                vars_["source"].get().strip(),
                vars_["metric"].get().strip(),
                fields,
                path=path_parts,
            )
            if not res.get("ok"):
                messagebox.showerror("Add rule", res.get("message", ""), parent=d)
                return
            if on_change:
                try:
                    on_change()
                except Exception:
                    pass
            _refresh()
            d.destroy()

        bar = ttk.Frame(frm)
        bar.pack(fill=tk.X, pady=(10, 0))
        ttk.Button(bar, text="Add", command=_do_add).pack(side=tk.RIGHT)
        ttk.Button(bar, text="Cancel", command=d.destroy).pack(side=tk.RIGHT, padx=6)

    tv.bind("<Double-1>", _edit)
    src_combo.bind("<<ComboboxSelected>>", lambda e: _refresh())
    search_var.trace_add("write", lambda *a: _refresh())

    # ---- Buttons -------------------------------------------------------
    btns = ttk.Frame(dlg, padding=8)
    btns.pack(fill=tk.X)
    ttk.Button(btns, text="Add rule…", command=_add_rule).pack(side=tk.LEFT)
    ttk.Button(btns, text="Edit selected…", command=_edit).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Enable", command=lambda: _toggle(True)).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Disable", command=lambda: _toggle(False)).pack(side=tk.LEFT)
    ttk.Button(btns, text="Close", command=dlg.destroy).pack(side=tk.RIGHT)

    _refresh()


def _open_edit_dialog(parent, rule, persist):
    d = tk.Toplevel(parent)
    d.title(f"Edit rule — {rule.section_id}")
    d.geometry("480x420")
    d.transient(parent)
    d.grab_set()

    frm = ttk.Frame(d, padding=12)
    frm.pack(fill=tk.BOTH, expand=True)

    ttk.Label(frm, text=rule.section_id, font=("TkDefaultFont", 10, "bold")).pack(
        anchor=tk.W)
    if rule.description:
        ttk.Label(frm, text=rule.description, foreground="#757575",
                  wraplength=440, justify=tk.LEFT).pack(anchor=tk.W, pady=(0, 8))

    vars_: dict[str, tk.StringVar] = {}

    def _field(label, key, value, *, combo=None, hint=""):
        row = ttk.Frame(frm)
        row.pack(fill=tk.X, pady=4)
        ttk.Label(row, text=label, width=16, anchor=tk.W).pack(side=tk.LEFT)
        var = tk.StringVar(value="" if value is None else str(value))
        if combo:
            w = ttk.Combobox(row, textvariable=var, values=combo,
                             state="readonly", width=22)
        else:
            w = ttk.Entry(row, textvariable=var, width=24)
        w.pack(side=tk.LEFT)
        if hint:
            ttk.Label(row, text=hint, foreground="#9e9e9e").pack(side=tk.LEFT, padx=6)
        vars_[key] = var

    _field("Critical", "critical", rule.critical, hint="blank = off")
    _field("Warning", "warning", rule.warning, hint="blank = off")
    _field("Info", "info", rule.info, hint="blank = off")
    _field("Operator", "operator", rule.operator,
           combo=[">", ">=", "<", "<=", "==", "!="])
    _field("Window", "window", rule.window, hint="consecutive breaches")
    _field("Enabled", "enabled", "true" if rule.enabled else "false",
           combo=["true", "false"])
    _field("Description", "description", rule.description)

    def _save():
        changes = {k: v.get() for k, v in vars_.items()}
        if persist(rule, changes):
            d.destroy()

    bar = ttk.Frame(frm)
    bar.pack(fill=tk.X, pady=(12, 0))
    ttk.Button(bar, text="Save", style="Primary.TButton", command=_save).pack(
        side=tk.RIGHT)
    ttk.Button(bar, text="Cancel", command=d.destroy).pack(side=tk.RIGHT, padx=6)
