"""Tk dialog + helpers for the RAG subsystem (retrieval-augmented Generate SQL).

Wired to the real shared service (:class:`ai_assistant.rag.service.RagService`)
so the UI, CLI and API stay in parity. Long-running calls (indexing) run on a
worker thread and update the widgets via ``after``.

The *owner* is the AI Query UI object; we read the active connection + live
``DatabaseManager`` objects from it so RAG indexes exactly what the user is
connected to.
"""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Any

from common.ui.tk import make_scrollable


class _UiCore:
    """Minimal core shim so RagService can resolve live UI connections."""

    def __init__(self, owner: Any):
        self._owner = owner

    def get_manager(self, name: str):
        mgr = (getattr(self._owner, "active_connections", {}) or {}).get(name)
        if mgr is None:
            raise ValueError(f"Not connected to '{name}'. Connect first.")
        return mgr

    def get_connection_profile(self, name: str):
        mgr = (getattr(self._owner, "active_connections", {}) or {}).get(name)
        db_type = getattr(mgr, "db_type", "SQL") if mgr is not None else "SQL"
        return {"name": name, "db_type": db_type or "SQL"}


def _make_service(owner: Any):
    from ai_assistant.rag.service import RagService

    return RagService(_UiCore(owner), getattr(owner, "ai_agent", None))


def _current_conn(owner: Any) -> str:
    combo = getattr(owner, "ai_conn_combo", None)
    return combo.get() if combo is not None else ""


def _run_bg(owner: Any, work, done) -> None:
    """Run *work()* on a thread, then call ``done(result, error)`` on the UI loop."""
    root = getattr(owner, "root", None)

    def _worker():
        try:
            res, err = work(), None
        except Exception as exc:  # noqa: BLE001
            res, err = None, str(exc)
        if root is not None:
            root.after(0, lambda: done(res, err))
        else:
            done(res, err)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def rag_index_current(owner: Any, *, rebuild: bool = False) -> None:
    """Build/refresh the RAG index for the connection selected in the AI UI."""
    conn = _current_conn(owner)
    if not conn:
        messagebox.showwarning("RAG", "Select a database connection first.")
        return
    if conn not in (getattr(owner, "active_connections", {}) or {}):
        messagebox.showerror("RAG", f"Not connected to '{conn}'. Connect first.")
        return
    if hasattr(owner, "update_status"):
        owner.update_status(f"Building RAG index for '{conn}'…")

    def done(res, err):
        if err or not (res or {}).get("ok"):
            msg = err or (res or {}).get("error") or "Indexing failed."
            messagebox.showerror("RAG indexing failed", msg)
            if hasattr(owner, "update_status"):
                owner.update_status("RAG indexing failed.")
            return
        n = res.get("indexed", 0)
        if hasattr(owner, "update_status"):
            owner.update_status(
                f"RAG index ready for '{conn}': {n} docs "
                f"(provider={res.get('provider')}, dim={res.get('dim')})."
            )
        messagebox.showinfo(
            "RAG", f"Indexed {n} schema docs for '{conn}'.\n"
            "Tick 'Use RAG' to ground Generate SQL on it.")

    _run_bg(owner, lambda: _make_service(owner).index(conn, rebuild=rebuild), done)


def _active_connection_names(owner: Any) -> list[str]:
    conns = getattr(owner, "active_connections", {}) or {}
    return sorted(conns.keys())


def open_rag_panel(owner: Any) -> tk.Toplevel:
    """Open the RAG management dialog."""
    parent = getattr(owner, "root", None) or getattr(owner, "parent", None)
    dialog = tk.Toplevel(parent)
    dialog.title("RAG Manager — retrieval-augmented Generate SQL")
    dialog.geometry("820x680")
    main = make_scrollable(dialog)
    main.configure(padding=10)

    ttk.Label(main, text="RAG Manager", font=("Arial", 14, "bold")).pack(anchor=tk.W)
    ttk.Label(
        main,
        text="Index live database schema, upload documents, glossary terms, NL→SQL "
        "examples, analytical patterns, or an entire codebase folder. Enable "
        "'Use RAG' in Generate SQL to ground answers on the selected scope.",
        foreground="gray", wraplength=780, justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(0, 8))

    # ── Scope selector ─────────────────────────────────────────────────────
    scope_row = ttk.Frame(main)
    scope_row.pack(fill=tk.X, pady=(0, 6))
    standalone_var = tk.BooleanVar(value=False)
    conn_var = tk.StringVar(value="")

    def _refresh_conn_combo():
        names = _active_connection_names(owner)
        combo["values"] = names
        if names and not conn_var.get():
            conn_var.set(names[0])
        elif conn_var.get() and conn_var.get() not in names and not standalone_var.get():
            if names:
                conn_var.set(names[0])

    ttk.Label(scope_row, text="Database:").pack(side=tk.LEFT, padx=(0, 6))
    combo = ttk.Combobox(
        scope_row, textvariable=conn_var, width=28, state="readonly")
    combo.pack(side=tk.LEFT, padx=(0, 6))
    ttk.Button(scope_row, text="Refresh", command=_refresh_conn_combo).pack(side=tk.LEFT, padx=(0, 6))
    ttk.Checkbutton(
        scope_row, text="Standalone collection",
        variable=standalone_var,
        command=lambda: coll_entry.config(
            state=tk.NORMAL if standalone_var.get() else tk.DISABLED),
    ).pack(side=tk.LEFT, padx=(6, 0))

    coll_row = ttk.Frame(main)
    coll_row.pack(fill=tk.X, pady=(0, 6))
    ttk.Label(coll_row, text="Collection name:").pack(side=tk.LEFT, padx=(0, 6))
    coll_var = tk.StringVar(value="docs")
    coll_entry = ttk.Entry(coll_row, textvariable=coll_var, width=36, state=tk.DISABLED)
    coll_entry.pack(side=tk.LEFT, padx=(0, 6))

    idx_row = ttk.Frame(main)
    idx_row.pack(fill=tk.X, pady=(0, 6))

    status_var = tk.StringVar(value="")
    ttk.Label(main, textvariable=status_var, foreground="#1a7f37",
              wraplength=780, justify=tk.LEFT).pack(anchor=tk.W, pady=(0, 6))

    results = scrolledtext.ScrolledText(main, height=14, wrap=tk.WORD)
    results.pack(fill=tk.BOTH, expand=True, pady=(4, 6))

    def _set_status(msg: str, *, warn: bool = False):
        status_var.set(msg)
        if warn:
            results.tag_configure("warn", foreground="#b45309")

    def _show(text: str):
        results.delete("1.0", tk.END)
        results.insert("1.0", text)

    def _scope() -> str:
        if standalone_var.get():
            return coll_var.get().strip()
        return conn_var.get().strip()

    def _need_scope() -> bool:
        sc = _scope()
        if not sc:
            if standalone_var.get():
                messagebox.showwarning("RAG", "Enter a collection name.")
            else:
                messagebox.showwarning(
                    "RAG", "Select an active database connection or use Standalone.")
            return False
        if not standalone_var.get() and sc not in (getattr(owner, "active_connections", {}) or {}):
            messagebox.showerror("RAG", f"Not connected to '{sc}'. Connect first.")
            return False
        return True

    _refresh_conn_combo()

    def do_index(rebuild: bool = False):
        if standalone_var.get():
            messagebox.showinfo(
                "RAG", "Schema indexing applies to active database connections only.")
            return
        sc = _scope()
        if not sc:
            messagebox.showwarning("RAG", "Select an active database connection.")
            return
        if sc not in (getattr(owner, "active_connections", {}) or {}):
            messagebox.showerror("RAG", f"Not connected to '{sc}'. Connect first.")
            return
        _set_status(f"{'Re-indexing' if rebuild else 'Indexing'} schema for '{sc}'…")
        def done(r, e):
            if e or not (r or {}).get("ok"):
                _set_status(e or (r or {}).get("error") or "Indexing failed.", warn=True)
                return
            mm = ""
            st = _make_service(owner).status(sc)
            if (st.get("embedder_mismatch") or {}).get("mismatch"):
                mm = " " + (st["embedder_mismatch"].get("message") or "")
            _set_status(
                f"Indexed {r.get('indexed', 0)} schema docs for '{sc}' "
                f"(total={r.get('doc_count')}, provider={r.get('provider')}).{mm}")
        _run_bg(owner, lambda: _make_service(owner).index(sc, rebuild=rebuild), done)

    def do_overview():
        if not _need_scope():
            return
        sc = _scope()
        r = _make_service(owner).scope_overview(sc)
        if not r.get("ok"):
            _show(r.get("error") or "Overview failed.")
            return
        st = r.get("status") or {}
        br = r.get("breakdown") or {}
        mm = st.get("embedder_mismatch") or br.get("embedder_mismatch") or {}
        lines = [
            f"scope      : {sc}",
            f"indexed    : {st.get('indexed')}",
            f"doc_count  : {st.get('doc_count')}",
            f"provider   : {(st.get('meta') or {}).get('provider', '')}",
            f"dim        : {(st.get('meta') or {}).get('dim', '')}",
            f"indexed_at : {(st.get('meta') or {}).get('indexed_at', '')}",
        ]
        if mm.get("mismatch"):
            lines.append(f"WARNING    : {mm.get('message')}")
        lines.append("\nbreakdown:")
        for k, v in sorted((br.get("counts") or {}).items()):
            lines.append(f"  {k:<12} {v}")
        _show("\n".join(lines))
        if mm.get("mismatch"):
            _set_status(mm.get("message") or "Re-index recommended.", warn=True)

    def do_add_codebase():
        if not _need_scope():
            return
        path = filedialog.askdirectory(parent=dialog, title="Choose codebase root folder")
        if not path:
            return
        sc = _scope()
        _set_status(f"Indexing codebase '{path}' into '{sc}'…")
        def done(r, e):
            if e or not (r or {}).get("ok"):
                _set_status(e or (r or {}).get("error") or "Codebase indexing failed.", warn=True)
                return
            _set_status(
                f"Indexed {r.get('chunks', 0)} code chunk(s) from "
                f"{r.get('files_scanned', 0)} file(s) into '{sc}'.")
        _run_bg(
            owner,
            lambda: _make_service(owner).add_codebase(
                path, sc, standalone=standalone_var.get()),
            done,
        )

    def do_paste_document():
        if not _need_scope():
            return
        Df = _SimpleForm(
            dialog, "Paste document",
            [("Title", False), ("Source id (optional)", False), ("Content", True)],
        )
        if not Df.ok:
            return
        title, source, content = Df.values
        source = source or title or "pasted-document"
        _set_status(f"Indexing pasted document into '{_scope()}'…")
        _run_bg(
            owner,
            lambda: _make_service(owner).add_document(
                _scope(), text=content, title=title or source, source=source,
                standalone=standalone_var.get()),
            lambda r, e: _set_status(
                e or (r or {}).get("error")
                or f"Indexed '{(r or {}).get('source')}' as {(r or {}).get('chunks')} chunk(s)."
            ),
        )

    def do_status():
        if not _need_scope():
            return
        r = _make_service(owner).status(_scope())
        br = _make_service(owner).breakdown(_scope())
        if not r.get("ok"):
            _show(r.get("error") or "Status failed.")
            return
        meta = r.get("meta") or {}
        counts = br.get("counts") or {}
        _show(
            f"scope      : {_scope()}\n"
            f"indexed    : {r.get('indexed')}\n"
            f"doc_count  : {r.get('doc_count')}\n"
            f"provider   : {meta.get('provider', '')}\n"
            f"dim        : {meta.get('dim', '')}\n"
            f"indexed_at : {meta.get('indexed_at', '')}\n\n"
            "breakdown:\n"
            + "\n".join(f"  {k:<12} {v}" for k, v in sorted(counts.items()))
        )

    search_var = tk.StringVar()
    extra_scopes_var = tk.StringVar()

    def do_search():
        if not _need_scope():
            return
        q = search_var.get().strip()
        if not q:
            messagebox.showinfo("RAG", "Enter a question to search.")
            return
        extra = [s.strip() for s in extra_scopes_var.get().split(",") if s.strip()]
        if extra:
            scopes = [_scope()] + extra
            r = _make_service(owner).preview_multi(scopes, q, k=8)
        else:
            r = _make_service(owner).preview(_scope(), q, k=8)
        if not r.get("ok"):
            _show(r.get("error") or "Search failed.")
            return
        parts = [r.get("preview") or "", "", "Context block:", r.get("context") or ""]
        _show("\n".join(parts))

    def do_add_example():
        if not _need_scope():
            return
        Ex = _SimpleForm(dialog, "Add NL→SQL example",
                         [("Question", False), ("SQL", True), ("Note (optional)", False)])
        if not Ex.ok:
            return
        q, sql, note = Ex.values
        r = _make_service(owner).add_example(_scope(), q, sql, note)
        _set_status(r.get("doc_id") and f"Added example: {r['doc_id']}"
                    or (r.get("error") or "Add example failed."))

    def do_add_examples_file():
        if not _need_scope():
            return
        path = filedialog.askopenfilename(
            parent=dialog,
            title="Choose NL→SQL examples file (JSONL/JSON/CSV/TSV/Q:SQL: text)",
            filetypes=[
                ("Example files", "*.jsonl *.json *.csv *.tsv *.txt *.md"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return
        sc = _scope()
        _set_status(f"Importing examples from '{path}' into '{sc}'…")

        def done(r, e):
            if e or not (r or {}).get("ok"):
                _set_status(e or (r or {}).get("error")
                            or "Example import failed.", warn=True)
                return
            msg = (f"Imported {r.get('added', 0)} example(s) "
                   f"(parsed={r.get('parsed', 0)}, skipped={r.get('skipped', 0)}).")
            reasons = r.get("reasons") or {}
            if reasons:
                msg += " Skips: " + ", ".join(f"{k}={v}" for k, v in reasons.items())
            _set_status(msg)
        _run_bg(
            owner,
            lambda: _make_service(owner).add_examples_from_file(
                sc, path, standalone=standalone_var.get()),
            done,
        )

    def do_add_glossary():
        if not _need_scope():
            return
        Gf = _SimpleForm(dialog, "Add glossary term",
                         [("Term", False), ("Definition", True)])
        if not Gf.ok:
            return
        term, definition = Gf.values
        r = _make_service(owner).add_glossary(_scope(), term, definition)
        _set_status(r.get("doc_id") and f"Added glossary: {r['doc_id']}"
                    or (r.get("error") or "Add glossary failed."))

    def do_add_document():
        if not _need_scope():
            return
        path = filedialog.askopenfilename(
            parent=dialog,
            title="Choose document to index",
            filetypes=[
                ("Supported documents", "*.txt *.md *.markdown *.rst *.sql *.csv *.tsv *.json *.log *.yaml *.yml *.ini *.cfg *.html *.htm *.xml *.pdf *.docx"),
                ("All files", "*.*"),
            ],
        )
        if not path:
            return
        _set_status(f"Indexing document '{path}' into '{_scope()}'...")
        _run_bg(
            owner,
            lambda: _make_service(owner).add_document(
                _scope(), file_path=path, standalone=standalone_var.get()),
            lambda r, e: _set_status(
                e or (r or {}).get("error")
                or f"Indexed document '{(r or {}).get('source')}' as {(r or {}).get('chunks')} chunk(s)."
            ),
        )

    def do_list_documents():
        if not _need_scope():
            return
        r = _make_service(owner).documents(_scope())
        if not r.get("ok"):
            _show(r.get("error") or "List documents failed.")
            return
        docs = r.get("documents") or []
        lines = [f"Documents in scope: {_scope()}\n"]
        if not docs:
            lines.append("  (no uploaded documents)")
        for d in docs:
            lines.append(f"  {d.get('source')}  title={d.get('title')} chunks={d.get('chunks')}")
        _show("\n".join(lines))

    def do_seed_analytics():
        if not _need_scope():
            return
        r = _make_service(owner).seed_analytics(
            _scope(), standalone=standalone_var.get())
        _set_status(
            f"Seeded {r.get('seeded', 0)} analytical patterns into '{_scope()}'."
            if r.get("ok") else (r.get("error") or "Seed analytics failed.")
        )

    def do_analytics_library():
        r = _make_service(owner).analytics_library()
        if not r.get("ok"):
            _show(r.get("error") or "Analytics library failed.")
            return
        lines = ["Generic analytical query patterns (seed these into a scope):\n"]
        for q in r.get("queries") or []:
            lines.append(f"[{q.get('category')}] {q.get('question')}\n{q.get('sql')}\n")
        _show("\n".join(lines))

    def do_breakdown():
        if not _need_scope():
            return
        r = _make_service(owner).breakdown(_scope())
        if not r.get("ok"):
            _show(r.get("error") or "Breakdown failed.")
            return
        lines = [f"RAG breakdown for {_scope()} (total={r.get('total', 0)}):\n"]
        for k, v in sorted((r.get("counts") or {}).items()):
            lines.append(f"  {k:<12} {v}")
        _show("\n".join(lines))

    def do_help():
        _show(
            "How to use RAG\n\n"
            "1. Select an active database connection (or Standalone collection).\n"
            "2. Click Index Schema to build/refresh schema + relationship docs.\n"
            "3. Add documents, glossary terms, NL→SQL examples, or a codebase folder.\n"
            "4. Seed analytical patterns for generic query shapes.\n"
            "5. Search to preview ranked hits and the context block sent to the AI.\n"
            "6. Enable 'Use RAG' in Generate SQL to ground answers on this scope.\n"
            "7. Re-index if the embedder provider/dim warning appears after config changes.\n"
        )

    def do_clear():
        if not _need_scope():
            return
        if not messagebox.askyesno("RAG", f"Delete the RAG index for '{_scope()}'?"):
            return
        r = _make_service(owner).clear(_scope())
        _set_status(f"Removed {r.get('removed', 0)} docs." if r.get("ok")
                    else (r.get("error") or "Clear failed."))

    def do_eval():
        if not _need_scope():
            return
        r = _make_service(owner).evaluate(_scope(), k=8, per_case=True)
        if not r.get("ok"):
            _show(r.get("error") or "Eval failed.")
            return
        m = r.get("metrics") or {}
        lines = [
            f"Retrieval eval for '{_scope()}' (k={r.get('k')}, "
            f"seeded_from_examples={r.get('seeded_from_examples')})",
            f"  cases             : {m.get('cases', 0)}",
            f"  recall@k          : {m.get('recall_at_k', 0.0):.4f}",
            f"  MRR               : {m.get('mrr', 0.0):.4f}",
            f"  context precision : {m.get('context_precision', 0.0):.4f}",
            "",
        ]
        for c in (r.get("cases_detail") or [])[:30]:
            lines.append(
                f"  r@k={c['recall_at_k']:.2f} rr={c['reciprocal_rank']:.2f} "
                f"cp={c['context_precision']:.2f}  {c.get('question', '')[:60]}"
            )
        _show("\n".join(lines))

    def do_drift():
        if not _need_scope():
            return
        r = _make_service(owner).drift(_scope())
        _show(r.get("message") or r.get("error") or "Drift check failed.")

    def do_reindex_stale():
        if not _need_scope():
            return
        r = _make_service(owner).reindex_stale([_scope()])
        if not r.get("ok"):
            _set_status(r.get("error") or "Reindex failed.")
            return
        _set_status(f"Re-indexed {r.get('reindexed', 0)} connection(s).")

    def _show_schedule(r: dict):
        if not r.get("ok"):
            _set_status(r.get("error") or "Scheduler action failed.")
            return
        conns = ", ".join(r.get("connections") or []) or "(all indexed)"
        _show(
            "Scheduled re-index (incremental):\n"
            f"  enabled        : {r.get('enabled')}\n"
            f"  running        : {r.get('running')}\n"
            f"  start_time     : {r.get('start_time')}\n"
            f"  duration_hours : {r.get('duration_hours')}\n"
            f"  connections    : {conns}\n"
            f"  force          : {r.get('force')}\n"
            f"  next_run       : {r.get('next_run', '')}\n"
            f"  last_run_date  : {r.get('last_run_date', '')}\n"
            f"  last_result    : {r.get('last_result', {})}"
        )
        _set_status(f"Scheduler {'running' if r.get('running') else 'stopped'}.")

    def do_schedule_status():
        _show_schedule(_make_service(owner).reindex_schedule_status())

    def do_schedule_start():
        _show_schedule(_make_service(owner).reindex_schedule_start())

    def do_schedule_stop():
        _show_schedule(_make_service(owner).reindex_schedule_stop())

    # Search row
    srow = ttk.Frame(main)
    srow.pack(fill=tk.X, pady=(0, 6))
    ttk.Label(srow, text="Search:").pack(side=tk.LEFT, padx=(0, 6))
    se = ttk.Entry(srow, textvariable=search_var)
    se.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 6))
    ttk.Button(srow, text="Preview", command=do_search).pack(side=tk.LEFT)

    msrow = ttk.Frame(main)
    msrow.pack(fill=tk.X, pady=(0, 6))
    ttk.Label(msrow, text="Also search scopes (comma-sep):").pack(side=tk.LEFT, padx=(0, 6))
    ttk.Entry(msrow, textvariable=extra_scopes_var).pack(
        side=tk.LEFT, fill=tk.X, expand=True)

    ttk.Button(idx_row, text="Index Schema", command=lambda: do_index(False)).pack(side=tk.LEFT)
    ttk.Button(idx_row, text="Re-index", command=lambda: do_index(True)).pack(side=tk.LEFT, padx=4)
    ttk.Button(idx_row, text="Overview", command=do_overview).pack(side=tk.LEFT, padx=4)
    ttk.Button(idx_row, text="Add Codebase", command=do_add_codebase).pack(side=tk.LEFT, padx=4)
    ttk.Button(idx_row, text="Evaluate", command=do_eval).pack(side=tk.LEFT, padx=4)
    ttk.Button(idx_row, text="Check Drift", command=do_drift).pack(side=tk.LEFT, padx=4)
    ttk.Button(idx_row, text="Re-index if Stale", command=do_reindex_stale).pack(side=tk.LEFT, padx=4)
    ttk.Button(idx_row, text="Schedule Status", command=do_schedule_status).pack(side=tk.LEFT, padx=4)
    ttk.Button(idx_row, text="Schedule Start", command=do_schedule_start).pack(side=tk.LEFT, padx=4)
    ttk.Button(idx_row, text="Schedule Stop", command=do_schedule_stop).pack(side=tk.LEFT, padx=4)

    # Button row
    btns = ttk.Frame(main)
    btns.pack(fill=tk.X, pady=(4, 0))
    ttk.Button(btns, text="Add Document", command=do_add_document).pack(side=tk.LEFT)
    ttk.Button(btns, text="Paste Content", command=do_paste_document).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="List Docs", command=do_list_documents).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Breakdown", command=do_breakdown).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Add Example", command=do_add_example).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Import Examples File", command=do_add_examples_file).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Add Glossary", command=do_add_glossary).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Seed Analytics", command=do_seed_analytics).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Analytics", command=do_analytics_library).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="How to Use", command=do_help).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Clear Index", command=do_clear).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Close", command=dialog.destroy).pack(side=tk.RIGHT)

    if _active_connection_names(owner):
        do_overview()
    return dialog


class _SimpleForm:
    """Tiny modal form collecting a few labelled fields. ``.ok`` / ``.values``."""

    def __init__(self, parent, title: str, fields: list[tuple[str, bool]]):
        self.ok = False
        self.values: list[str] = []
        top = tk.Toplevel(parent)
        top.title(title)
        top.transient(parent)
        top.grab_set()
        frm = ttk.Frame(top, padding=10)
        frm.pack(fill=tk.BOTH, expand=True)
        widgets = []
        for label, multiline in fields:
            ttk.Label(frm, text=label + ":").pack(anchor=tk.W)
            if multiline:
                w = tk.Text(frm, height=4, width=60, wrap=tk.WORD)
            else:
                w = ttk.Entry(frm, width=60)
            w.pack(fill=tk.X, pady=(0, 6))
            widgets.append((w, multiline))

        def submit():
            vals = []
            for w, multiline in widgets:
                vals.append(
                    w.get("1.0", tk.END).strip() if multiline else w.get().strip())
            self.values = vals
            self.ok = all(v for v in vals[: max(1, len(vals) - 1)])  # last may be optional
            top.destroy()

        bar = ttk.Frame(frm)
        bar.pack(fill=tk.X)
        ttk.Button(bar, text="Save", command=submit).pack(side=tk.RIGHT)
        ttk.Button(bar, text="Cancel", command=top.destroy).pack(side=tk.RIGHT, padx=4)
        top.wait_window()
