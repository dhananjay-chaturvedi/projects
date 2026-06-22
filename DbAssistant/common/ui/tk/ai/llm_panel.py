"""Tk dialog for the local trainable NL->SQL LLM.

Wired to :class:`ai_assistant.llm.service.LlmService` (the same code path used by
``dbtool ai llm`` and the REST API). Training runs on a worker thread. The model
can optionally learn from the RAG examples you've saved for a connection
("train with RAG"), and a trained model powers the offline "Local LLM" backend.
"""

from __future__ import annotations

import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Any

from common.ui.tk import make_scrollable


def _service():
    from ai_assistant.llm.service import LlmService

    return LlmService()


def _training_service(owner: Any):
    from ai_assistant.llm.training_service import LlmTrainingService

    try:
        from common.ui.tk.ai.build_apps_dialogs import _ActiveConnectionCore

        return LlmTrainingService(_ActiveConnectionCore(owner))
    except Exception:
        try:
            from common.headless.db_service import CoreDBService

            return LlmTrainingService(CoreDBService())
        except Exception:
            return LlmTrainingService(None)


def _harvest_service(owner: Any):
    """Build an AIService for harvesting, reusing the owner's live agent (so the
    backend the user already selected in the UI drives generation)."""
    from ai_query.service import AIService

    try:
        from common.ui.tk.ai.build_apps_dialogs import _ActiveConnectionCore

        core = _ActiveConnectionCore(owner)
    except Exception:
        from common.headless.db_service import CoreDBService

        core = CoreDBService()
    svc = AIService(core)
    agent = getattr(owner, "ai_agent", None)
    if agent is not None:
        svc._ai = agent
    return svc


def _run_bg(owner: Any, work, done) -> None:
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

    threading.Thread(target=_worker, daemon=True).start()


def open_llm_panel(owner: Any) -> tk.Toplevel:
    """Open the local LLM workspace.

    Hosts a tabbed notebook so several training sessions can run side by side —
    e.g. train different models, or the same model against different database
    connections — each tab being a fully independent panel with its own state.
    """
    parent = getattr(owner, "root", None) or getattr(owner, "parent", None)

    dialog = tk.Toplevel(parent)
    dialog.title("Local LLM — train your own NL→SQL model")
    dialog.geometry("840x700")

    bar = ttk.Frame(dialog)
    bar.pack(fill=tk.X, padx=8, pady=(8, 0))
    notebook = ttk.Notebook(dialog)
    notebook.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

    tab_seq = {"n": 0}

    def _add_tab():
        tab_seq["n"] += 1
        ordinal = tab_seq["n"]
        frame = ttk.Frame(notebook)
        inner = make_scrollable(frame)
        inner.configure(padding=10)

        def _close_tab():
            # Closing the final tab closes the whole window.
            if len(notebook.tabs()) <= 1:
                dialog.destroy()
                return
            notebook.forget(frame)
            frame.destroy()

        def _set_title(text: str):
            try:
                notebook.tab(frame, text=text)
            except tk.TclError:
                pass

        _build_llm_tab(owner, inner, on_close=_close_tab, set_title=_set_title)
        notebook.add(frame, text=f"Training {ordinal}")
        notebook.select(frame)

    ttk.Button(bar, text="+ New training tab", command=_add_tab).pack(side=tk.LEFT)
    ttk.Label(
        bar,
        text="Open multiple tabs to train different models, or the same model "
             "against different connections.",
        foreground="gray",
    ).pack(side=tk.LEFT, padx=(8, 0))

    _add_tab()
    return dialog


def _build_llm_tab(owner: Any, main: ttk.Frame, *,
                   on_close=None, set_title=None) -> None:
    """Build one independent LLM train/status/generate panel into *main*.

    Each call wires its own service handles, Tk variables and worker threads, so
    multiple tabs can train concurrently without sharing state.
    """
    svc = _service()
    train_svc = _training_service(owner)

    ttk.Label(main, text="Local LLM (NL→SQL)", font=("Arial", 14, "bold")).pack(anchor=tk.W)
    ttk.Label(
        main,
        text="Train a small NL→SQL model entirely on your machine. The python engine "
        "needs no extra deps; numpy/pytorch are optional (requirements-llm.txt). "
        "Optionally fold in the examples you've saved via RAG for a connection.",
        foreground="gray", wraplength=740, justify=tk.LEFT,
    ).pack(anchor=tk.W, pady=(0, 8))

    # ── config row ──────────────────────────────────────────────────────────
    cfg = ttk.Frame(main)
    cfg.pack(fill=tk.X, pady=(0, 4))
    ttk.Label(cfg, text="Model:").grid(row=0, column=0, sticky=tk.W, padx=(0, 4))
    try:
        existing_models = [
            m.get("name", "") for m in (svc.list_models().get("models") or [])
            if m.get("name")
        ]
    except Exception:
        existing_models = []
    name_var = tk.StringVar(value=existing_models[0] if existing_models else "default")
    # Editable combo: pick an existing model or type a new name to train.
    ttk.Combobox(cfg, textvariable=name_var, values=existing_models, width=18).grid(
        row=0, column=1, sticky=tk.W)

    ttk.Label(cfg, text="Engine:").grid(row=0, column=2, sticky=tk.W, padx=(12, 4))
    eng_info = svc.engines().get("engines") or []
    eng_values = ["(config default)"] + [e["name"] for e in eng_info]
    engine_var = tk.StringVar(value="(config default)")
    ttk.Combobox(cfg, textvariable=engine_var, values=eng_values, width=14,
                 state="readonly").grid(row=0, column=3, sticky=tk.W)

    include_sample_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(cfg, text="Include sample data",
                    variable=include_sample_var).grid(
        row=0, column=4, sticky=tk.W, padx=(12, 0))

    rag_row = ttk.Frame(main)
    rag_row.pack(fill=tk.X, pady=(2, 6))
    use_rag_var = tk.BooleanVar(value=False)
    ttk.Checkbutton(rag_row, text="Train with RAG examples from connection:",
                    variable=use_rag_var).pack(side=tk.LEFT)
    conns = list((getattr(owner, "active_connections", {}) or {}).keys())
    cur = ""
    combo = getattr(owner, "ai_conn_combo", None)
    if combo is not None:
        cur = combo.get()
    rag_conn_var = tk.StringVar(value=cur or (conns[0] if conns else ""))
    ttk.Combobox(rag_row, textvariable=rag_conn_var, values=conns, width=28,
                 state="readonly").pack(side=tk.LEFT, padx=(6, 0))

    rich = ttk.LabelFrame(main, text="Rich DB training (from database)", padding=4)
    rich.pack(fill=tk.X, pady=(2, 6))
    mine_db_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(rich, text="Mine DB queries", variable=mine_db_var).pack(side=tk.LEFT)
    index_rag_var = tk.BooleanVar(value=False)
    ttk.Checkbutton(rich, text="Index RAG first", variable=index_rag_var).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Label(rich, text="Sample rows:").pack(side=tk.LEFT, padx=(8, 0))
    sample_var = tk.StringVar(value="5")
    ttk.Entry(rich, textvariable=sample_var, width=5).pack(side=tk.LEFT, padx=(4, 0))

    # ── Advanced-training connections (multi-select) ─────────────────────────
    # Advanced modes span EVERY selected connection (real objects from each DB);
    # each dialect is dry-run validated against a matching-type connection when
    # one is selected. Select none to fall back to the single RAG connection.
    multi = ttk.LabelFrame(
        main,
        text="Advanced-training connections (advanced mode spans all selected; "
             "per-dialect live validation)",
        padding=4)
    multi.pack(fill=tk.X, pady=(2, 6))
    ttk.Label(multi, text="Connections:").pack(side=tk.LEFT)
    multi_conn_list = tk.Listbox(
        multi, selectmode=tk.MULTIPLE, height=3, exportselection=False, width=30)
    for _c in conns:
        multi_conn_list.insert(tk.END, _c)
    multi_conn_list.pack(side=tk.LEFT, padx=(4, 0))

    # ── Auto-harvest (curated corpus + AI-generated question bank) ───────────
    harvest = ttk.LabelFrame(
        main, text="Auto-harvest & train (curated corpus + AI question bank)",
        padding=4)
    harvest.pack(fill=tk.X, pady=(2, 6))
    harvest_curated_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(harvest, text="Curated corpus",
                    variable=harvest_curated_var).pack(side=tk.LEFT)
    harvest_captures_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(harvest, text="Captures",
                    variable=harvest_captures_var).pack(side=tk.LEFT, padx=(8, 0))
    harvest_followups_var = tk.BooleanVar(value=True)
    ttk.Checkbutton(harvest, text="Follow-ups",
                    variable=harvest_followups_var).pack(side=tk.LEFT, padx=(8, 0))
    ttk.Label(harvest, text="AI questions:").pack(side=tk.LEFT, padx=(8, 0))
    harvest_qcount_var = tk.StringVar(value="40")
    ttk.Entry(harvest, textvariable=harvest_qcount_var, width=5).pack(
        side=tk.LEFT, padx=(4, 0))
    ttk.Label(harvest, text="Mode:").pack(side=tk.LEFT, padx=(8, 0))
    train_mode_var = tk.StringVar(value="advanced_incremental")
    ttk.Combobox(
        harvest, textvariable=train_mode_var,
        values=["advanced_incremental", "advanced_full", "incremental", "full"],
        width=18, state="readonly",
    ).pack(side=tk.LEFT, padx=(4, 0))
    ttk.Label(harvest, text="Depth:").pack(side=tk.LEFT, padx=(8, 0))
    train_depth_var = tk.StringVar(value="offline")
    ttk.Combobox(
        harvest, textvariable=train_depth_var,
        values=["offline", "online"], width=8, state="readonly",
    ).pack(side=tk.LEFT, padx=(4, 0))
    ttk.Label(harvest, text="Templates:").pack(side=tk.LEFT, padx=(8, 0))
    template_mode_var = tk.StringVar(value="both")
    ttk.Combobox(
        harvest, textvariable=template_mode_var,
        values=["both", "concrete", "placeholder"], width=11, state="readonly",
    ).pack(side=tk.LEFT, padx=(4, 0))
    questions_file_var = tk.StringVar(value="")
    qfile_row = ttk.Frame(main)
    qfile_row.pack(fill=tk.X, pady=(0, 4))
    ttk.Label(qfile_row, text="Questions file:").pack(side=tk.LEFT)
    ttk.Entry(qfile_row, textvariable=questions_file_var, width=48).pack(
        side=tk.LEFT, padx=(4, 4), fill=tk.X, expand=True)

    def _browse_questions_file():
        path = filedialog.askopenfilename(
            title="Select questions file",
            filetypes=[
                ("Text/CSV/JSON", "*.txt *.csv *.json *.jsonl *.md"),
                ("All files", "*.*"),
            ],
        )
        if path:
            questions_file_var.set(path)

    ttk.Button(qfile_row, text="Browse…", command=_browse_questions_file).pack(side=tk.LEFT)
    ttk.Label(harvest, text="Workers:").pack(side=tk.LEFT, padx=(8, 0))
    gen_workers_var = tk.StringVar(value="4")
    ttk.Entry(harvest, textvariable=gen_workers_var, width=4).pack(
        side=tk.LEFT, padx=(4, 0))
    ttk.Label(harvest, text="Timeout:").pack(side=tk.LEFT, padx=(8, 0))
    gen_timeout_var = tk.StringVar(value="120")
    ttk.Entry(harvest, textvariable=gen_timeout_var, width=5).pack(
        side=tk.LEFT, padx=(4, 0))

    def _harvest_mode_body() -> dict:
        mode = str(train_mode_var.get() or "advanced_full").strip().lower()
        if mode == "advanced_full":
            return {
                "train_mode": "full",
                "advanced_training": True,
                "multi_dialect": True,
                "multi_syntax": True,
            }
        if mode == "advanced_incremental":
            return {
                "train_mode": "incremental",
                "advanced_training": True,
                "multi_dialect": True,
                "multi_syntax": True,
            }
        return {
            "train_mode": mode if mode in ("full", "incremental") else "full",
            "advanced_training": False,
            "multi_dialect": False,
            "multi_syntax": False,
        }

    def _train_mode_arg() -> str:
        return str(_harvest_mode_body().get("train_mode") or "full")

    def _gen_int(var: tk.StringVar, default: int) -> int:
        try:
            return int(var.get().strip() or default)
        except ValueError:
            return default
    eng_lines = []
    for e in eng_info:
        mark = "OK" if e.get("available") else "no"
        eng_lines.append(
            f"  {e['name']:<8} stage={e.get('stage', '')} [{mark}] "
            f"{e.get('reason', '')}")
    ttk.Label(main, text="Engines:\n" + "\n".join(eng_lines),
              font=("Courier", 10), justify=tk.LEFT, foreground="gray").pack(
        anchor=tk.W, pady=(0, 6))

    status_var = tk.StringVar(value="")
    ttk.Label(main, textvariable=status_var, foreground="#1a7f37",
              wraplength=740, justify=tk.LEFT).pack(anchor=tk.W, pady=(0, 4))

    log = scrolledtext.ScrolledText(main, height=12, wrap=tk.WORD)
    log.pack(fill=tk.BOTH, expand=True, pady=(4, 6))

    def _ui(fn):
        """Run *fn* on the Tk main thread (progress callbacks fire on workers)."""
        root = getattr(owner, "root", None)
        if root is not None:
            root.after(0, fn)
        else:
            fn()

    def _log(text: str):
        log.delete("1.0", tk.END)
        log.insert("1.0", text)

    def _log_clear():
        _ui(lambda: log.delete("1.0", tk.END))

    def _log_line(text: str, *, stamp: bool = True):
        """Append a (timestamped) detail line and keep the newest visible.

        Auto-scrolls only when the view is already at the bottom so a user who
        scrolled up to read history is not yanked back down.
        """
        prefix = f"[{time.strftime('%H:%M:%S')}] " if stamp else ""

        def _do():
            try:
                at_end = log.yview()[1] >= 0.999
            except tk.TclError:
                at_end = True
            log.insert(tk.END, f"{prefix}{text}\n")
            if at_end:
                log.see(tk.END)

        _ui(_do)

    def _engine_arg():
        v = engine_var.get()
        return None if v == "(config default)" else v

    def _set_status(msg: str, *, detail: bool = False):
        """Update the heading; when *detail* also stream the line into the box."""
        _ui(lambda: status_var.set(msg))
        if detail:
            _log_line(msg)

    def _status_block_lines() -> list[str]:
        """Trained-model summary (name/engine/path/…) for the detail box."""
        r = svc.status(name_var.get().strip() or "default")
        if not r.get("ok"):
            return [r.get("error") or "Status unavailable."]
        if not r.get("trained"):
            return [f"Model '{r.get('name')}' is not trained yet."]
        meta = r.get("meta") or {}
        return [
            f"name       : {r.get('name')}",
            f"engine     : {r.get('engine')}",
            f"trained_at : {meta.get('trained_at', '')}",
            f"pairs      : {meta.get('num_pairs', '')}",
            f"final_loss : {meta.get('final_loss', '')}",
            f"path       : {r.get('path')}",
        ]

    def _training_progress_msg(ev: dict) -> str | None:
        etype = ev.get("type")
        if etype == "training_capture":
            status = ev.get("status")
            if status == "collecting":
                return "Collecting training data…"
            if status == "captured":
                return (
                    f"Collected {ev.get('pairs', 0)} pair(s) "
                    f"({ev.get('source', '')}); training…"
                )
        elif etype == "training_rag":
            rag_status = ev.get("status")
            if rag_status == "indexing_parallel":
                return f"Indexing RAG for '{ev.get('connection', '')}'…"
            if rag_status == "indexed":
                return "RAG indexing complete."
            if rag_status == "index_failed":
                return "RAG indexing failed."
        elif etype == "training_progress":
            return f"Training {ev.get('model', 'model')}…"
        elif etype == "training_epoch":
            return (
                f"Training {ev.get('model', 'model')}: "
                f"epoch {ev.get('epoch', '?')}, loss {ev.get('loss', '?')}"
            )
        elif etype == "training_done":
            if ev.get("ok"):
                return (
                    f"Training complete — {ev.get('pairs', 0)} pair(s) "
                    f"({ev.get('source', '')})"
                )
            return "Training failed."
        elif etype == "harvest_train_done":
            phase = str(ev.get("phase") or "training").replace("_", " ")
            if ev.get("ok"):
                return f"{phase.title()} training complete."
            return str(ev.get("reason") or "Training failed.")
        return None

    def do_train():
        rag_conn = rag_conn_var.get().strip() if use_rag_var.get() else ""
        try:
            sample_limit = int(sample_var.get().strip() or 5)
        except ValueError:
            sample_limit = 5
        if set_title is not None:
            set_title("Train: " + (name_var.get().strip() or "default")
                      + (f" @ {rag_conn}" if rag_conn else ""))
        _log_clear()
        _log_line(
            "Training "
            f"'{name_var.get().strip() or 'default'}' "
            f"(engine={engine_var.get()}"
            + (f", connection={rag_conn}" if rag_conn else "")
            + f", mode={_train_mode_arg()})", stamp=False)
        _set_status("Training… (this can take a moment)")

        def progress(ev: dict):
            msg = _training_progress_msg(ev)
            if msg:
                _set_status(msg, detail=True)

        def done(r, e):
            if e or not (r or {}).get("ok"):
                msg = e or (r or {}).get("error") or "Training failed."
                status_var.set(msg)
                _log_line(msg)
                return
            # Newly trained local model(s) should appear in the AI Query backend
            # dropdown without an app restart.
            if hasattr(owner, "refresh_backend_options"):
                try:
                    owner.refresh_backend_options()
                except Exception:
                    pass
            if "models" in r:
                status_var.set(
                    f"Trained {len(r.get('models') or [])} model(s) on "
                    f"{r.get('pairs')} pairs ({r.get('source', '')}); "
                    f"already={r.get('already_trained', 0)} new={r.get('new_pairs', 0)}")
                _log_line(r.get("reason") or str(r))
                models = r.get("models") or []
                if models:
                    ev = (models[0] or {}).get("eval")
                    if ev:
                        from ai_assistant.llm.eval import format_eval_summary
                        _log_line(format_eval_summary(ev), stamp=False)
                for line in _status_block_lines():
                    _log_line(line, stamp=False)
                return
            fb = ""
            if r.get("engine_fallback"):
                fb = (f"  (requested '{r.get('engine_requested')}' unavailable; "
                      f"used '{r.get('engine')}')")
            status_var.set(
                f"Trained '{r.get('name')}' engine={r.get('engine')} "
                f"pairs={r.get('num_pairs', '?')} loss={r.get('final_loss', '?')}"
                + (f" in {r.get('elapsed_sec')}s" if r.get('elapsed_sec') else "")
                + fb)
            if r.get("eval"):
                from ai_assistant.llm.eval import format_eval_summary
                _log_line(format_eval_summary(r.get("eval")), stamp=False)
            for line in _status_block_lines():
                _log_line(line, stamp=False)

        _run_bg(
            owner,
            lambda: train_svc.train_llm(
                {
                    "mode": "from_database",
                    "connections": [rag_conn] if rag_conn else [],
                    "train_new_name": name_var.get().strip() or "default",
                    "train_engine": _engine_arg() or "",
                    "include_sample": include_sample_var.get(),
                    "use_rag": use_rag_var.get(),
                    "index_rag": index_rag_var.get(),
                    "rag_strategy": "index_first",
                    "mine_db": mine_db_var.get(),
                    "train_sample_limit": sample_limit,
                    "train_mode": _train_mode_arg(),
                },
                on_progress=progress,
            ),
            done,
        )

    # Shared handle so the "Stop harvest" button can signal the running job.
    harvest_ctl: dict[str, Any] = {"stop": None}
    harvest_widgets: dict[str, Any] = {}

    def do_harvest():
        conn = rag_conn_var.get().strip()
        if not conn:
            messagebox.showinfo("LLM", "Select a connection first.")
            return
        try:
            qcount = int(harvest_qcount_var.get().strip() or 0)
        except ValueError:
            qcount = 0
        try:
            sample_limit = int(sample_var.get().strip() or 5)
        except ValueError:
            sample_limit = 5
        if set_title is not None:
            set_title(f"Harvest: {name_var.get().strip() or 'default'} @ {conn}")
        _log_clear()
        _log_line(
            f"Auto-harvest & train '{name_var.get().strip() or 'default'}' "
            f"(engine={engine_var.get()}, connection={conn}, "
            f"mode={_train_mode_arg()}, questions={qcount}, "
            f"workers={_gen_int(gen_workers_var, 4)}, "
            f"sample_limit={sample_limit})", stamp=False)
        status_var.set("Auto-harvesting & training… (backend generation can take a while)")

        stop_event = threading.Event()
        harvest_ctl["stop"] = stop_event
        if harvest_widgets.get("start"):
            harvest_widgets["start"].config(state=tk.DISABLED)
        if harvest_widgets.get("stop"):
            harvest_widgets["stop"].config(state=tk.NORMAL)

        def set_status(msg: str):
            _set_status(msg, detail=True)

        def progress(ev: dict):
            etype = ev.get("type")
            msg = _training_progress_msg(ev)
            if msg:
                set_status(msg)
                return
            if etype == "harvest_offline_collected":
                set_status(
                    f"Offline harvest collected {ev.get('pairs', 0)} validated pairs; "
                    "training local model…"
                )
            elif etype == "harvest_backend_start":
                set_status("Offline model trained; starting optional backend enrichment…")
            elif etype == "harvest_question_bank":
                if ev.get("status") == "generating":
                    set_status(
                        f"Asking AI to invent {ev.get('count', 0)} schema-grounded "
                        "questions… (this backend call can take a while)"
                    )
                elif ev.get("status") == "generated":
                    set_status(
                        f"AI proposed {ev.get('questions', 0)} questions; "
                        "preparing backend generation…"
                    )
            elif etype == "harvest_followup":
                q = (ev.get("question") or "").strip()
                tail = f": {q[:60]}" if q else ""
                set_status(
                    f"Backend follow-up thread {ev.get('done', 0)}/{ev.get('total', 0)} "
                    f"[{ev.get('category', '')}]{tail}…"
                )
            elif etype == "harvest_generate":
                done_n = ev.get("done", 0)
                total_n = ev.get("total", 0)
                kept_n = ev.get("kept", 0)
                if ev.get("status") == "planned":
                    set_status(
                        f"Prepared {total_n} backend question(s); generating SQL "
                        f"with {ev.get('workers', 1)} worker(s)…"
                    )
                else:
                    q = (ev.get("question") or "").strip()
                    tail = f" — {q[:60]}" if q else ""
                    set_status(
                        f"Generating SQL with backend {done_n}/{total_n} "
                        f"(kept {kept_n}){tail}…"
                    )
            elif etype == "harvest_collected":
                set_status(
                    f"Validated {ev.get('pairs', 0)} total pairs; finalizing training…"
                )
            elif etype == "harvest_stopped":
                set_status("Stopping gracefully — keeping the trained model…")

        def reset_buttons():
            harvest_ctl["stop"] = None
            if harvest_widgets.get("start"):
                harvest_widgets["start"].config(state=tk.NORMAL)
            if harvest_widgets.get("stop"):
                harvest_widgets["stop"].config(state=tk.DISABLED)

        def done(r, e):
            reset_buttons()
            if e or not (r or {}).get("ok"):
                msg = e or (r or {}).get("error") or "Harvest failed."
                status_var.set(msg)
                _log_line(msg)
                return
            if r.get("trained") and hasattr(owner, "refresh_backend_options"):
                try:
                    owner.refresh_backend_options()
                except Exception:
                    pass
            srcs = r.get("sources") or {}
            prefix = "Stopped — " if r.get("stopped") else ""
            summary = (
                prefix
                + f"Harvested {r.get('pairs', 0)} validated pairs "
                f"(offline {r.get('offline_pairs', 0)}, "
                f"backend {r.get('backend_pairs', 0)}, "
                f"skipped-known {r.get('skipped_known', 0)}, "
                f"already={r.get('already_trained', 0)} new={r.get('new_pairs', 0)}, "
                f"rejected {r.get('rejected', 0)}); "
                + ("trained " + ", ".join(
                    m.get("name", "") for m in (r.get("models") or []))
                   if r.get("trained") else "not trained"))
            status_var.set(summary)
            _log_line(summary)
            _log_line("Sources: " + ", ".join(f"{k}={v}" for k, v in srcs.items()),
                      stamp=False)
            if r.get("train_reason"):
                _log_line(r.get("train_reason"), stamp=False)
            for line in _status_block_lines():
                _log_line(line, stamp=False)

        # Advanced spans every selected connection; default to the single RAG
        # connection when nothing is selected in the multi-select.
        selected_conns = [multi_conn_list.get(i) for i in multi_conn_list.curselection()]
        harvest_conns = selected_conns or [conn]
        if conn not in harvest_conns:
            harvest_conns = [conn] + harvest_conns
        _run_bg(
            owner,
            lambda: _harvest_service(owner).llm_harvest({
                "connection": conn,
                "connections": harvest_conns,
                "training_depth": train_depth_var.get().strip() or "offline",
                "template_mode": template_mode_var.get().strip() or "both",
                "train_new_name": name_var.get().strip() or "default",
                "train_engine": _engine_arg() or "",
                "use_curated": harvest_curated_var.get(),
                "use_captures": harvest_captures_var.get(),
                "followups": harvest_followups_var.get(),
                "generated_questions": qcount,
                "questions_file": questions_file_var.get().strip(),
                "use_rag": use_rag_var.get(),
                "sample_limit": sample_limit,
                "do_train": True,
                "gen_workers": _gen_int(gen_workers_var, 4),
                "gen_timeout": _gen_int(gen_timeout_var, 120),
                **_harvest_mode_body(),
            }, progress=progress, should_stop=stop_event.is_set),
            done,
        )

    def do_stop_harvest():
        ev = harvest_ctl.get("stop")
        if ev is None:
            return
        ev.set()
        status_var.set("Stop requested — finishing the current step, then saving the model…")
        if harvest_widgets.get("stop"):
            harvest_widgets["stop"].config(state=tk.DISABLED)

    def do_preview_mined():
        conn = rag_conn_var.get().strip()
        if not conn:
            messagebox.showinfo("LLM", "Select a connection first.")
            return
        try:
            sample_limit = int(sample_var.get().strip() or 5)
        except ValueError:
            sample_limit = 5

        def done(r, e):
            if e or not (r or {}).get("ok"):
                _log(e or (r or {}).get("error") or "Mining failed.")
                return
            stats = r.get("stats") or {}
            lines = [
                f"Mined {stats.get('kept', 0)} validated pairs "
                f"({stats.get('validated', 0)}/{stats.get('candidates', 0)} passed)"
            ]
            for p in (r.get("pairs") or [])[:20]:
                lines.append(f"\nQ: {p.get('question')}\nSQL: {p.get('sql')}")
            _log("\n".join(lines))

        _run_bg(
            owner,
            lambda: train_svc.mine_training_pairs({
                "connections": [conn],
                "train_sample_limit": sample_limit,
            }),
            done,
        )

    def do_status():
        _log("\n".join(_status_block_lines()) + "\n")

    def do_versions():
        name = name_var.get().strip() or "default"
        r = _harvest_service(owner).llm_model_versions(name=name)
        if not r.get("ok"):
            _log(r.get("error") or "Could not list versions.")
            return
        versions = r.get("versions") or []
        if not versions:
            _log(f"No saved versions for model '{name}'.")
            return
        lines = [f"Saved versions for '{name}' ({len(versions)}):"]
        for v in versions:
            lines.append(
                f"  {v.get('version', '')}  [{v.get('reason', '')}]  "
                f"{v.get('created', '')}")
        _log("\n".join(lines))

    def do_restore():
        from tkinter import simpledialog

        name = name_var.get().strip() or "default"
        r = _harvest_service(owner).llm_model_versions(name=name)
        versions = (r or {}).get("versions") or []
        if not versions:
            messagebox.showinfo("LLM", f"No saved versions for model '{name}'.")
            return
        default_ver = versions[0].get("version", "")
        ver = simpledialog.askstring(
            "Restore model version",
            "Enter the version id to restore "
            f"(latest: {default_ver}):",
            initialvalue=default_ver, parent=owner)
        if not ver:
            return
        res = _harvest_service(owner).llm_model_restore(name=name, version=ver.strip())
        if not res.get("ok"):
            _log(res.get("error") or "Restore failed.")
            return
        status_var.set(f"Model '{name}' restored to version '{res.get('restored')}'.")
        _log(f"Model '{name}' restored to version '{res.get('restored')}'.")

    gen_var = tk.StringVar()

    alt_var = tk.BooleanVar(value=False)

    def do_generate():
        q = gen_var.get().strip()
        if not q:
            messagebox.showinfo("LLM", "Enter a question to generate SQL.")
            return
        r = svc.generate(q, name=name_var.get().strip() or "default",
                         engine=_engine_arg(), alternatives=alt_var.get())
        if not r.get("ok"):
            _log(r.get("error") or "Generation failed.")
            return
        out = f"Q: {q}\n\nSQL:\n{r.get('sql')}"
        alts = r.get("alternatives") or []
        if alts:
            out += f"\n\nAlternative SQL syntaxes ({len(alts)}):"
            for i, a in enumerate(alts, 1):
                tag = f" [{a.get('db_type')}]" if a.get("db_type") else ""
                out += f"\n  {i}.{tag} {a.get('sql')}"
        _log(out)

    def do_verify():
        name = name_var.get().strip() or "default"
        q = gen_var.get().strip()
        r = svc.dataset(name, query=q)
        if not r.get("ok"):
            _log(r.get("error") or "Dataset lookup failed.")
            return
        if not r.get("available"):
            status_var.set(r.get("reason") or "No saved training data for this model.")
            _log(r.get("reason") or "")
            return
        total = r.get("total", 0)
        pairs = r.get("pairs") or []
        if q:
            if r.get("matched"):
                status_var.set(
                    f"'{q}' IS in model '{name}': {len(pairs)} matching pair(s) "
                    f"of {total} trained.")
            else:
                status_var.set(
                    f"'{q}' is NOT in model '{name}' ({total} pairs trained).")
        else:
            status_var.set(f"Model '{name}' was trained on {total} pair(s).")
        lines = [f"Training pairs in model '{name}' (total {total}"
                 + (f", matching '{q}'" if q else "") + "):"]
        for p in pairs[:50]:
            lines.append(f"\nQ: {p.get('question')}\nSQL: {p.get('sql')}")
        _log("\n".join(lines))

    def do_export():
        path = filedialog.asksaveasfilename(
            title="Export NL→SQL dataset", defaultextension=".jsonl",
            filetypes=[("JSONL", "*.jsonl"), ("All files", "*.*")])
        if not path:
            return
        rag_conn = rag_conn_var.get().strip() if use_rag_var.get() else ""
        r = svc.export_dataset(path, include_sample=include_sample_var.get(),
                               rag_connection=rag_conn)
        status_var.set(f"Exported {r.get('count', 0)} pairs → {r.get('path')}"
                       if r.get("ok") else (r.get("error") or "Export failed."))

    # generate row
    grow = ttk.Frame(main)
    grow.pack(fill=tk.X, pady=(0, 6))
    ttk.Label(grow, text="Ask:").pack(side=tk.LEFT, padx=(0, 6))
    ttk.Entry(grow, textvariable=gen_var).pack(side=tk.LEFT, fill=tk.X, expand=True,
                                               padx=(0, 6))
    ttk.Button(grow, text="Generate SQL", command=do_generate).pack(side=tk.LEFT)
    ttk.Checkbutton(grow, text="Alternatives", variable=alt_var).pack(side=tk.LEFT, padx=(6, 0))
    ttk.Button(grow, text="Verify in model", command=do_verify).pack(side=tk.LEFT, padx=(6, 0))

    def do_enrich():
        """Enrich the reusable per-dialect template library via the AI backend."""
        conn = rag_conn_var.get().strip()
        selected = [multi_conn_list.get(i) for i in multi_conn_list.curselection()]
        conns = []
        for c in [conn, *selected]:
            if c and c not in conns:
                conns.append(c)
        backend = ""
        try:
            backend = owner.ai_agent.get_active_backend_name() or ""
        except Exception:
            backend = ""

        def progress(ev):
            if ev.get("type") != "enrich_template":
                return
            st = ev.get("status")
            if st in ("accepted", "rejected"):
                mark = "✓" if st == "accepted" else "✗"
                line = f"  {mark} [{ev.get('db_type')}] {ev.get('intent')}"
                if ev.get("reason"):
                    line += f" — {ev['reason']}"
                _log_line(line, stamp=False)
            elif st == "asking":
                _set_status(
                    f"Enriching [{ev.get('db_type')}] templates: {ev.get('intent')}…",
                    detail=False)

        def done(r, e):
            if e or not (r or {}).get("ok"):
                msg = e or (r or {}).get("error") or "No templates accepted."
                _set_status(f"Template enrichment: {msg}")
                _log_line(msg)
                return
            msg = (
                f"Enriched templates: accepted {r.get('accepted', 0)}, "
                f"rejected {r.get('rejected', 0)}. Store: {r.get('store') or {}}. "
                "They will be trained on the next harvest.")
            _set_status(msg)
            _log_line(msg)

        _set_status("Enriching reusable templates for all SQL dialects via the "
                    "AI backend — this can take a while…", detail=True)
        _run_bg(
            owner,
            lambda: _harvest_service(owner).llm_enrich_templates(
                {"connections": conns, "backend": backend}, progress=progress),
            done,
        )

    btns = ttk.Frame(main)
    btns.pack(fill=tk.X)
    ttk.Button(btns, text="Train", command=do_train).pack(side=tk.LEFT)
    ttk.Button(btns, text="Enrich templates", command=do_enrich).pack(
        side=tk.LEFT, padx=4)
    harvest_widgets["start"] = ttk.Button(
        btns, text="Auto-harvest & train", command=do_harvest)
    harvest_widgets["start"].pack(side=tk.LEFT, padx=4)
    harvest_widgets["stop"] = ttk.Button(
        btns, text="Stop harvest", command=do_stop_harvest, state=tk.DISABLED)
    harvest_widgets["stop"].pack(side=tk.LEFT, padx=(0, 4))

    def _fmt_schedule(r: dict) -> str:
        if not isinstance(r, dict):
            return str(r)
        return (
            f"Scheduled training: enabled={r.get('enabled')} "
            f"running={r.get('running')} start={r.get('start_time', '')} "
            f"duration={r.get('duration_hours', '')}h "
            f"next_run={r.get('next_run', '')} "
            f"last_run={r.get('last_run_date', '') or '—'}"
        )

    def do_schedule_start():
        r = _harvest_service(owner).llm_harvest_schedule_start()
        _log_line(_fmt_schedule(r), stamp=False)

    def do_schedule_stop():
        r = _harvest_service(owner).llm_harvest_schedule_stop()
        _log_line(_fmt_schedule(r), stamp=False)

    ttk.Button(btns, text="Schedule training", command=do_schedule_start).pack(
        side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Discard scheduled training", command=do_schedule_stop).pack(
        side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Status", command=do_status).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Versions", command=do_versions).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Restore version", command=do_restore).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Export dataset", command=do_export).pack(side=tk.LEFT, padx=4)
    ttk.Button(btns, text="Preview mined queries", command=do_preview_mined).pack(side=tk.LEFT, padx=4)
    if on_close is not None:
        ttk.Button(btns, text="Close tab", command=on_close).pack(side=tk.RIGHT)

    do_status()
