"""
CLI surface for the AI Query Assistant module.

Exposes the ``ai`` command (natural-language → SQL, plus backend listing).
Wired into the master CLI through the manifest; runnable standalone via
``python -m ai_query``.
"""

from __future__ import annotations

import json

from common.core import cliutil

_SQL_MODE_CHOICES = ("strict_summary", "summary", "open")


def _add_sql_mode_args(parser):
    parser.add_argument(
        "--sql-mode",
        choices=_SQL_MODE_CHOICES,
        default="",
        help="SQL mode: strict_summary | summary | open",
    )
    parser.add_argument(
        "--execution-rules",
        default="",
        metavar="TEXT",
        help="SQL execution rules (summary/open); or use --execution-rules-file",
    )
    parser.add_argument(
        "--execution-rules-file",
        default="",
        metavar="PATH",
        help="File containing SQL execution rules (one per line)",
    )


def _resolve_execution_rules(args) -> str | None:
    path = getattr(args, "execution_rules_file", "") or ""
    text = getattr(args, "execution_rules", "") or ""
    if path:
        from pathlib import Path
        text = Path(path).read_text(encoding="utf-8").strip()
    return text if text else None


def _resolve_sql_mode(args) -> str | None:
    mode = getattr(args, "sql_mode", "") or ""
    return mode or None


# Named ``ai`` subcommands. Anything else after ``ai`` is a one-shot question.
_AI_SUBCOMMANDS = frozenset(
    {"session", "ask", "explain", "optimize", "review", "configure",
     "fallback", "correct", "cache", "pii", "config", "rag", "llm"}
)
# Options that consume the following token as their value (so an argv scan does
# not mistake that value for a positional question/subcommand).
_AI_VALUE_OPTS = frozenset(
    {"--conn", "--backend", "--format", "--sql-mode",
     "--execution-rules", "--execution-rules-file"}
)


def inject_oneshot_ask(argv: list[str]) -> list[str]:
    """Route a free-text one-shot question through the ``ask`` subcommand.

    ``ai_query ai --conn X "how many tables?"`` must Just Work, but argparse
    cannot host both an ``ai`` subparsers action and a free positional on the
    same parser (the question gets matched against the subcommand choices and
    rejected). So when the first positional after ``ai`` is not a known
    subcommand, insert an explicit ``ask`` token; the dedicated ``ask``
    subparser then owns the one-shot flags + question with no ambiguity.
    """
    if "ai" not in argv:
        return argv
    i = argv.index("ai")
    rest = argv[i + 1:]
    if "--list-backends" in rest:
        return argv
    j = 0
    first_pos: str | None = None
    while j < len(rest):
        tok = rest[j]
        if tok in _AI_VALUE_OPTS:
            j += 2
            continue
        if tok.startswith("-"):
            j += 1
            continue
        first_pos = tok
        break
    if first_pos is None or first_pos in _AI_SUBCOMMANDS:
        return argv
    return argv[: i + 1] + ["ask"] + rest


def register_cli(subparsers) -> None:
    ai_p = subparsers.add_parser("ai", help="AI natural-language → SQL")
    ai_sub = ai_p.add_subparsers(dest="ai_subcommand", required=False)

    sp = ai_sub.add_parser("session", help="Manage AI query sessions")
    sp_sub = sp.add_subparsers(dest="session_cmd", required=True)

    def _add_session_ref(parser):
        parser.add_argument("--session", required=True, metavar="ID|tabN")

    p_list = sp_sub.add_parser("list", help="List sessions")
    p_list.add_argument("--format", choices=["table", "json", "csv"], default="table")

    p_show = sp_sub.add_parser("show", help="Show one session's details")
    _add_session_ref(p_show)
    p_show.add_argument("--format", choices=["table", "json"], default="table")

    p_new = sp_sub.add_parser("new", help="Create session")
    p_new.add_argument("--conn", default="", metavar="NAME")
    p_new.add_argument("--backend", default="", metavar="NAME")
    p_new.add_argument("--isolated", action="store_true")
    p_new.add_argument("--format", choices=["table", "json", "csv"], default="table")
    _add_sql_mode_args(p_new)

    p_ask = sp_sub.add_parser("ask", help="Ask in a session")
    _add_session_ref(p_ask)
    p_ask.add_argument("question", nargs="+")

    p_fu = sp_sub.add_parser("follow-up", help="Follow-up in a session")
    _add_session_ref(p_fu)
    p_fu.add_argument("message", nargs="+")

    p_exec = sp_sub.add_parser("execute-sql", help="Execute SQL with session execution rules")
    _add_session_ref(p_exec)
    p_exec.add_argument("--sql", required=True, help="SQL to execute")
    p_exec.add_argument("--format", choices=["table", "json", "csv"], default="table")

    p_mode = sp_sub.add_parser("set-mode", help="Update session SQL mode / execution rules")
    _add_session_ref(p_mode)
    _add_sql_mode_args(p_mode)

    p_cross = sp_sub.add_parser("cross", help="Cross-tab instruction")
    _add_session_ref(p_cross)
    p_cross.add_argument("instruction", nargs="+")

    p_close = sp_sub.add_parser("close", help="Close session")
    _add_session_ref(p_close)

    p_save = sp_sub.add_parser("save", help="Save sessions to disk")
    p_save.add_argument("--file", default="", metavar="PATH")

    p_load = sp_sub.add_parser("load", help="Load sessions from disk")
    p_load.add_argument("--file", default="", metavar="PATH")

    # One-shot stateless ask. Reached directly (``ai ask "…"``) or via
    # ``inject_oneshot_ask`` when the user writes ``ai --conn X "…"``.
    p_oneshot = ai_sub.add_parser(
        "ask", help="One-shot natural-language question (stateless)")
    p_oneshot.add_argument("--conn", default="", metavar="NAME",
                           help="Connection to use")
    p_oneshot.add_argument("--backend", default="", metavar="NAME",
                           help="AI backend to use (default: auto-select)")
    p_oneshot.add_argument("--format", choices=["table", "json", "csv"],
                           default="table")
    _add_sql_mode_args(p_oneshot)
    p_oneshot.add_argument("question", nargs="+",
                           help="Natural language question")

    # Phase 6 parity: explain / optimize / review / configure / cache / pii
    p_exp = ai_sub.add_parser("explain", help="Explain a SQL statement (UI's 'Explain query')")
    p_exp.add_argument("--sql", default="", help="SQL string (or use --sql-file)")
    p_exp.add_argument("--sql-file", default="", dest="sql_file",
                       help="Path to a file containing the SQL")
    p_exp.add_argument("--conn", default="",
                       help="Saved connection to infer db_type from")
    p_exp.add_argument("--db-type", default="", dest="db_type",
                       help="Override db_type (MySQL, PostgreSQL, ...)")

    p_opt = ai_sub.add_parser("optimize", help="Suggest optimizations (UI's 'Optimize')")
    p_opt.add_argument("--sql", default="")
    p_opt.add_argument("--sql-file", default="", dest="sql_file")
    p_opt.add_argument("--conn", default="")
    p_opt.add_argument("--db-type", default="", dest="db_type")

    p_rev = ai_sub.add_parser("review", help="Run AI SQL review (UI's 'Run Review')")
    p_rev.add_argument("--sql", default="")
    p_rev.add_argument("--sql-file", default="", dest="sql_file")
    p_rev.add_argument("--rules", default="", help="Inline review-rules text")
    p_rev.add_argument("--rules-file", default="", dest="rules_file",
                       help="Path to a file containing review rules")
    p_rev.add_argument("--conn", default="")
    p_rev.add_argument("--db-type", default="", dest="db_type")
    p_rev.add_argument("--timeout", type=int, default=None,
                       help="Review timeout (seconds); omit => config sql_review_timeout")

    p_cfg = ai_sub.add_parser("configure", help="Set the active AI backend")
    p_cfg.add_argument("--backend", required=True, help="Backend name")
    p_cfg.add_argument("--no-verify", action="store_true", dest="no_verify",
                       help="Skip backend availability check")

    p_fb = ai_sub.add_parser(
        "fallback", help="Set/clear/show the fallback AI backend (failover + corrector)")
    p_fb.add_argument("--backend", default="",
                      help="Fallback backend name (omit to show current)")
    p_fb.add_argument("--clear", action="store_true", help="Clear the fallback backend")
    p_fb.add_argument("--no-verify", action="store_true", dest="no_verify",
                      help="Skip backend availability check")

    p_corr = ai_sub.add_parser(
        "correct", help="Repair a wrong/failed SQL via the fallback backend")
    p_corr.add_argument("--question", required=True, help="The natural-language question")
    p_corr.add_argument("--sql", default="", help="The SQL to correct")
    p_corr.add_argument("--sql-file", default="", dest="sql_file",
                        help="Path to a file containing the SQL")
    p_corr.add_argument("--conn", default="",
                        help="Connection for schema/dialect context")
    p_corr.add_argument("--db-type", default="", dest="db_type",
                        help="Dialect override; blank => from connection")
    p_corr.add_argument("--error", default="", dest="error_text",
                        help="Execution error text (mode=syntax)")
    p_corr.add_argument("--mode", choices=["syntax", "interpretation"], default="syntax",
                        help="'syntax' fixes failures; 'interpretation' fixes intent")
    p_corr.add_argument("--backend", default="",
                        help="Override corrector backend; blank => fallback")

    p_cache = ai_sub.add_parser("cache", help="Inspect / clear the AI schema cache")
    p_cache_sub = p_cache.add_subparsers(dest="cache_action", required=True)
    p_cache_info = p_cache_sub.add_parser("info", help="Show cache statistics")
    p_cache_info.add_argument("--format", choices=["table", "json", "csv"], default="table")
    p_cache_clear = p_cache_sub.add_parser("clear", help="Clear cache (all or one)")
    p_cache_clear.add_argument("--conn", default="",
                               help="Connection to clear; omit to clear all")
    p_cache_show = p_cache_sub.add_parser("show", help="Dump cached schema info")
    p_cache_show.add_argument("--conn", default="",
                              help="Connection name; omit for last-sent context")
    p_cache_show.add_argument("--format", choices=["table", "json"], default="json")

    p_pii = ai_sub.add_parser("pii", help="Inspect / toggle PII masking")
    p_pii_sub = p_pii.add_subparsers(dest="pii_action", required=True)
    p_pii_sub.add_parser("status", help="Show whether PII masking is enabled")
    p_pii_on = p_pii_sub.add_parser("on", help="Enable PII masking")
    p_pii_off = p_pii_sub.add_parser("off", help="Disable PII masking")
    _ = (p_pii_on, p_pii_off)

    # RAG — retrieval-augmented Generate SQL over the connected DB schema.
    p_rag = ai_sub.add_parser("rag", help="Retrieval-augmented SQL (index/search/ask)")
    rag_sub = p_rag.add_subparsers(dest="rag_action", required=True)
    r_idx = rag_sub.add_parser("index", help="Build/refresh the RAG index")
    r_idx.add_argument("--conn", required=True, metavar="NAME")
    r_idx.add_argument("--rebuild", action="store_true", help="Drop and rebuild")
    r_st = rag_sub.add_parser("status", help="Show index status")
    r_st.add_argument("--conn", default="", metavar="NAME")
    r_st.add_argument("--format", choices=["table", "json"], default="table")
    r_se = rag_sub.add_parser("search", help="Show raw retrieval hits")
    r_se.add_argument("--conn", required=True, metavar="NAME")
    r_se.add_argument("-k", type=int, default=None, help="Top-K (omit => config top_k)")
    r_se.add_argument("query", nargs="+")
    r_ctx = rag_sub.add_parser("context", help="Show prompt-ready context block")
    r_ctx.add_argument("--conn", required=True, metavar="NAME")
    r_ctx.add_argument("-k", type=int, default=None, help="Top-K (omit => config top_k)")
    r_ctx.add_argument("query", nargs="+")
    r_ask = rag_sub.add_parser("ask", help="Generate RAG-augmented SQL")
    r_ask.add_argument("--conn", required=True, metavar="NAME")
    r_ask.add_argument("--backend", default="", metavar="NAME")
    r_ask.add_argument("-k", type=int, default=None, help="Top-K (omit => config top_k)")
    r_ask.add_argument("--no-auto-index", action="store_true", dest="no_auto_index",
                       help="Skip auto-indexing the connection before ask")
    r_ask.add_argument("question", nargs="+")
    r_ex = rag_sub.add_parser("add-example", help="Add a NL->SQL example")
    r_ex.add_argument("--conn", required=True, metavar="NAME")
    r_ex.add_argument("--question", required=True)
    r_ex.add_argument("--sql", required=True)
    r_ex.add_argument("--note", default="")
    r_exf = rag_sub.add_parser(
        "add-examples-file",
        help="Bulk-import NL->SQL examples from a file (JSONL/JSON/CSV/TSV/Q:SQL: text)")
    r_exf.add_argument("--conn", required=True, metavar="NAME")
    r_exf.add_argument("--file", required=True, dest="file",
                       help="Path to examples file")
    r_exf.add_argument("--format", default="auto", dest="format",
                       choices=["auto", "jsonl", "json", "csv", "tsv", "text"],
                       help="Input format (default: auto-detect)")
    r_exf.add_argument("--standalone", action="store_true",
                       help="Standalone collection (skip live-DB SQL validation)")
    r_gl = rag_sub.add_parser("add-glossary", help="Add a business glossary term")
    r_gl.add_argument("--conn", required=True, metavar="NAME")
    r_gl.add_argument("--term", required=True)
    r_gl.add_argument("--definition", required=True)
    r_cl = rag_sub.add_parser("clear", help="Delete the RAG index for a connection")
    r_cl.add_argument("--conn", required=True, metavar="NAME")

    # Documents (uploaded reference knowledge).
    r_doc = rag_sub.add_parser("add-document", help="Index a document (file or text)")
    r_doc.add_argument("--scope", required=True, metavar="NAME",
                       help="Connection name or a standalone collection label")
    r_doc.add_argument("--file", default="", help="Path to a document file")
    r_doc.add_argument("--text", default="", help="Raw text (instead of --file)")
    r_doc.add_argument("--title", default="")
    r_doc.add_argument("--source", default="")
    r_doc.add_argument("--standalone", action="store_true",
                       help="Treat scope as a standalone collection (no DB)")
    r_ld = rag_sub.add_parser("list-docs", help="List uploaded documents in a scope")
    r_ld.add_argument("--scope", required=True, metavar="NAME")
    r_ld.add_argument("--format", choices=["table", "json"], default="table")
    r_rd = rag_sub.add_parser("remove-doc", help="Remove one uploaded document")
    r_rd.add_argument("--scope", required=True, metavar="NAME")
    r_rd.add_argument("--source", required=True, help="The document's source/filename")
    # Analytical query library.
    r_an = rag_sub.add_parser("analytics", help="Show the built-in analytical query library")
    r_an.add_argument("--format", choices=["table", "json"], default="table")
    r_sa = rag_sub.add_parser("seed-analytics",
                              help="Seed generic analytical patterns into a scope")
    r_sa.add_argument("--scope", required=True, metavar="NAME")
    r_sa.add_argument("--categories", default="",
                      help="Comma-separated categories (default: all)")
    r_sa.add_argument("--standalone", action="store_true")
    r_bk = rag_sub.add_parser("breakdown", help="Show a per-kind document breakdown")
    r_bk.add_argument("--scope", required=True, metavar="NAME")
    r_bk.add_argument("--format", choices=["table", "json"], default="table")
    r_ov = rag_sub.add_parser("overview", help="Status + breakdown + embedder check")
    r_ov.add_argument("--scope", required=True, metavar="NAME")
    r_ov.add_argument("--format", choices=["table", "json"], default="table")
    r_pr = rag_sub.add_parser("preview", help="Search with ranked preview + context")
    r_pr.add_argument("--conn", required=True, metavar="NAME")
    r_pr.add_argument("-k", type=int, default=None, help="Top-K (omit => config top_k)")
    r_pr.add_argument("query", nargs="+")
    r_sm = rag_sub.add_parser(
        "search-multi",
        help="Search across multiple scopes (schema + codebase + docs) at once")
    r_sm.add_argument("--scopes", required=True,
                      help="Comma-separated scope/collection names")
    r_sm.add_argument("-k", type=int, default=None, help="Top-K (omit => config top_k)")
    r_sm.add_argument("query", nargs="+")
    r_cb = rag_sub.add_parser("add-codebase", help="Index a source folder (kind=code)")
    r_cb.add_argument("--folder", required=True, help="Application source root path")
    r_cb.add_argument("--scope", required=True, metavar="NAME",
                      help="RAG scope / collection name")
    r_cb.add_argument("--standalone", action="store_true",
                      help="Standalone collection (default)")
    r_cb.add_argument("--no-replace", action="store_true",
                      help="Keep existing code: chunks (append mode)")
    r_cb.add_argument("--max-files", type=int, default=0, dest="max_files",
                      help="Cap files scanned (0=config default)")
    r_ev = rag_sub.add_parser(
        "eval", help="Evaluate retrieval quality (recall@k / MRR / context precision)")
    r_ev.add_argument("connection")
    r_ev.add_argument("--gold", default="",
                      help="Gold-set file (JSONL/JSON: question + tables). "
                           "Omit to seed from indexed NL->SQL examples.")
    r_ev.add_argument("-k", type=int, default=None, help="Top-K (omit => config top_k)")
    r_ev.add_argument("--per-case", action="store_true", dest="per_case",
                      help="Include per-case metrics in the output")
    r_ev.add_argument("--format", choices=["table", "json"], default="table")
    r_dr = rag_sub.add_parser(
        "drift", help="Check whether the live schema changed since indexing")
    r_dr.add_argument("connection")
    r_dr.add_argument("--format", choices=["table", "json"], default="table")
    r_rs = rag_sub.add_parser(
        "reindex-stale",
        help="Incrementally re-index stale / schema-changed connections")
    r_rs.add_argument("connections", nargs="*",
                      help="Connections to check (default: all indexed)")
    r_rs.add_argument("--force", action="store_true",
                      help="Re-index regardless of staleness")
    r_rs.add_argument("--format", choices=["table", "json"], default="table")
    r_rsch = rag_sub.add_parser(
        "reindex-schedule",
        help="Start/stop the scheduled (daily) incremental re-index")
    r_rsch_sub = r_rsch.add_subparsers(dest="schedule_action", required=True)
    r_rsch_sub.add_parser("status", help="Show scheduler status")
    r_rsch_sub.add_parser("start", help="Start scheduler thread")
    r_rsch_sub.add_parser("stop", help="Stop scheduler thread")

    # LLM — local trainable NL->SQL model (python/numpy/pytorch/ollama).
    p_llm = ai_sub.add_parser("llm", help="Local trainable NL->SQL model")
    llm_sub = p_llm.add_subparsers(dest="llm_action", required=True)
    l_tr = llm_sub.add_parser("train", help="Train (or retrain) a local model")
    l_tr.add_argument("--name", default="default")
    l_tr.add_argument("--engine", default="", help="python|numpy|pytorch|ollama")
    l_tr.add_argument("--dataset", default="", help="Extra NL->SQL JSONL file")
    l_tr.add_argument("--rag-conn", default="", dest="rag_conn",
                      help="Fold this connection's saved RAG examples into training")
    l_tr.add_argument("--no-sample", action="store_true", dest="no_sample",
                      help="Exclude the built-in sample pairs")
    l_st = llm_sub.add_parser("status", help="Show a model's status")
    l_st.add_argument("--name", default="default")
    l_st.add_argument("--format", choices=["table", "json"], default="table")
    llm_sub.add_parser("list", help="List trained models")
    llm_sub.add_parser("engines", help="List available engines")
    l_gen = llm_sub.add_parser("generate", help="Generate SQL with a trained model")
    l_gen.add_argument("--name", default="default")
    l_gen.add_argument("--engine", default="")
    l_gen.add_argument("--connection", default="", help="Connection for EXPLAIN validation")
    l_gen.add_argument("--max-new", type=int, default=0, dest="max_new",
                       help="Max generated tokens (0 = config default)")
    l_gen.add_argument("--temperature", type=float, default=None,
                       help="Sampling temperature (omit => config temperature)")
    l_gen.add_argument("--alternatives", action="store_true",
                       help="Also list alternative SQL syntaxes saved for the question")
    l_gen.add_argument("question", nargs="+")
    l_vf = llm_sub.add_parser(
        "verify", help="Inspect/verify the pairs a trained model was built on")
    l_vf.add_argument("--name", default="default")
    l_vf.add_argument("--query", default="", help="Filter/verify by substring (Q/SQL/desc)")
    l_vf.add_argument("--limit", type=int, default=0)
    l_vf.add_argument("--format", choices=["table", "json"], default="table")
    l_ver = llm_sub.add_parser(
        "versions", help="List saved snapshots (versions) of a trained model")
    l_ver.add_argument("--name", default="default")
    l_ver.add_argument("--format", choices=["table", "json"], default="table")
    l_rest = llm_sub.add_parser(
        "restore", help="Roll a model back to a saved snapshot/version")
    l_rest.add_argument("--name", default="default")
    l_rest.add_argument("--version", required=True,
                        help="Version id from `llm versions`")
    l_eval = llm_sub.add_parser("eval", help="Run training-accuracy meters on a model")
    l_eval.add_argument("--name", default="default")
    l_eval.add_argument("--connection", default="")
    l_eval.add_argument("--depth", default="", choices=["", "lightweight", "full"])
    l_eval.add_argument("--rag-conn", default="")
    l_eval.add_argument("--no-sample", action="store_true")
    l_eval.add_argument("--format", default="text", choices=["text", "json"])
    l_exp = llm_sub.add_parser("export", help="Export NL->SQL dataset to JSONL")
    l_exp.add_argument("--out", required=True, metavar="PATH")
    l_exp.add_argument("--rag-conn", default="", dest="rag_conn")
    l_exp.add_argument("--no-sample", action="store_true", dest="no_sample")
    l_ds = llm_sub.add_parser("dataset", help="Print NL->SQL dataset JSONL to stdout")
    l_ds.add_argument("--rag-conn", default="", dest="rag_conn")
    l_ds.add_argument("--no-sample", action="store_true", dest="no_sample")
    l_rich = llm_sub.add_parser("train-llm", help="Rich DB/codebase/scratch training")
    l_rich.add_argument("--connection", default="")
    l_rich.add_argument("--mode", default="from_database",
                        choices=["from_scratch", "from_database", "from_codebase"])
    l_rich.add_argument("--name", action="append", default=[], dest="train_llm")
    l_rich.add_argument("--new-name", default="", dest="train_new_name")
    l_rich.add_argument("--engine", default="", dest="train_engine")
    l_rich.add_argument("--description", default="")
    l_rich.add_argument("--codebase-path", default="")
    l_rich.add_argument("--use-rag", action="store_true")
    l_rich.add_argument("--index-rag", action="store_true")
    l_rich.add_argument("--rag-strategy", default="index_first",
                        choices=["index_first", "parallel"])
    l_rich.add_argument("--no-mine", action="store_true")
    l_rich.add_argument("--sample-limit", type=int, default=5)
    l_rich.add_argument("--train-mode", default="", dest="train_mode",
                        choices=["", "full", "incremental"],
                        help="full retrain or incremental (union with ledger)")
    l_multi = llm_sub.add_parser(
        "train-multi",
        help="Train ONE model from several connections in parallel (per-connection "
             "shards merged under a lock) — safe concurrent same-model training")
    l_multi.add_argument("--connection", action="append", default=[], dest="connections",
                         required=True, help="DB connection to learn from (repeatable)")
    l_multi.add_argument("--name", action="append", default=[], dest="train_llm",
                         help="Existing model name (repeatable)")
    l_multi.add_argument("--new-name", default="", dest="train_new_name")
    l_multi.add_argument("--engine", default="", dest="train_engine")
    l_multi.add_argument("--gen-workers", type=int, default=1, dest="gen_workers",
                         help="Parallel connection-collection workers (1-8)")
    l_multi.add_argument("--sample-limit", type=int, default=5)
    l_multi.add_argument("--format", choices=["table", "json"], default="table")

    l_mine = llm_sub.add_parser("mine-pairs", help="Preview validated DB training pairs")
    l_mine.add_argument("--connection", required=True)
    l_mine.add_argument("--sample-limit", type=int, default=5)
    l_mine.add_argument("--max-tables", type=int, default=40)
    l_mine.add_argument("--format", choices=["table", "json"], default="table")
    l_rs = llm_sub.add_parser("rag-status", help="Show LLM RAG training index status")
    l_rs.add_argument("--connection", default="")
    l_ir = llm_sub.add_parser("index-rag", help="Index RAG before LLM training")
    l_ir.add_argument("--connection", required=True)
    l_ir.add_argument("--rebuild", action="store_true")

    l_hv = llm_sub.add_parser(
        "harvest",
        help="Auto-harvest a validated NL->SQL corpus (curated + AI question "
             "bank + backend generation) and train local models",
    )
    l_hv.add_argument("--connection", required=True)
    l_hv.add_argument("--also-connection", action="append", default=[],
                      dest="extra_connections",
                      help="Additional connection to span for advanced training "
                           "(repeatable); each dialect is dry-run validated on its "
                           "matching connection")
    l_hv.add_argument("--training-depth", default="", dest="training_depth",
                      choices=["", "offline", "online"],
                      help="offline = template/mining only (no AI); "
                           "online = also use the backend AI agent")
    l_hv.add_argument("--name", action="append", default=[], dest="train_llm",
                      help="Target model name (repeatable)")
    l_hv.add_argument("--new-name", default="", dest="train_new_name")
    l_hv.add_argument("--engine", default="", dest="train_engine")
    l_hv.add_argument("--backend", default="",
                      help="AI backend for generation (e.g. cursor)")
    l_hv.add_argument("--complexity", default="",
                      help="Comma list: basic,advanced,complex")
    l_hv.add_argument("--generated-questions", type=int, default=0,
                      help="How many questions the backend should invent")
    l_hv.add_argument("--max-questions", type=int, default=200,
                      help="Cap on backend generation calls")
    l_hv.add_argument("--question", action="append", default=[], dest="questions",
                      help="Extra user-supplied question (repeatable)")
    l_hv.add_argument("--questions-file", default="", dest="questions_file",
                      help="Load questions from text/CSV/JSON/JSONL file")
    l_hv.add_argument("--multi-dialect", action="store_true",
                      help="Seed all SQL dialect templates (advanced training)")
    l_hv.add_argument("--multi-syntax", action="store_true",
                      help="Harvest alternative SQL syntax variants via backend AI")
    l_hv.add_argument("--advanced-training", action="store_true",
                      help="Enable advanced full/incremental multi-dialect training")
    l_hv.add_argument("--no-curated", action="store_true",
                      help="Skip the curated seed corpus")
    l_hv.add_argument("--no-captures", action="store_true",
                      help="Skip capture replay")
    l_hv.add_argument("--no-followups", action="store_true",
                      help="Skip follow-up threads")
    l_hv.add_argument("--mine-db", action="store_true",
                      help="Also run the DB miner")
    l_hv.add_argument("--no-rag", action="store_true")
    l_hv.add_argument("--no-train", action="store_true",
                      help="Harvest + validate only; do not train")
    l_hv.add_argument("--train-mode", default="", dest="train_mode",
                      choices=["", "full", "incremental"])
    l_hv.add_argument("--gen-workers", type=int, default=0,
                      help="Parallel backend workers (0 = config default)")
    l_hv.add_argument("--gen-timeout", type=int, default=0,
                      help="Per-question backend timeout seconds (0 = config)")
    l_hv.add_argument("--gen-retries", type=int, default=-1,
                      help="In-run retries after timeout/error (-1 = config)")
    l_hv.add_argument("--no-retry-backlog", action="store_true",
                      help="Do not persist failed questions for next run")
    l_hv.add_argument("--max-consecutive-failures", type=int, default=-1,
                      help="Circuit breaker threshold (0 = config, -1 = config)")
    l_hv.add_argument("--format", choices=["table", "json"], default="table")
    l_hv.add_argument("--template-mode", default="", dest="template_mode",
                      choices=["", "concrete", "placeholder", "both"],
                      help="Template training mode: concrete, placeholder, or both")

    l_sched = llm_sub.add_parser("harvest-schedule", help="Start/stop nightly harvest scheduler")
    l_sched_sub = l_sched.add_subparsers(dest="schedule_action", required=True)
    l_sched_sub.add_parser("status", help="Show scheduler status")
    l_sched_sub.add_parser("start", help="Start scheduler thread")
    l_sched_sub.add_parser("stop", help="Stop scheduler thread")

    l_enr = llm_sub.add_parser(
        "enrich-templates",
        help="Use the AI backend to enrich the reusable per-dialect NL->SQL "
             "template library (trained on the next harvest)",
    )
    l_enr.add_argument("--backend", default="",
                       help="AI backend for enrichment (e.g. cursor)")
    l_enr.add_argument("--db-type", action="append", default=[], dest="db_types",
                       help="Dialect to enrich (repeatable); default = all SQL "
                            "dialects (PostgreSQL, MySQL, MariaDB, ...)")
    l_enr.add_argument("--connection", action="append", default=[],
                       dest="connections",
                       help="Connection(s) for optional live catalog validation "
                            "(repeatable)")
    l_enr.add_argument("--questions-file", default="", dest="questions_file",
                       help="Intents file (text/CSV/JSON/JSONL); default = "
                            "built-in intent set")
    l_enr.add_argument("--limit-per-type", type=int, default=0,
                       dest="limit_per_type",
                       help="Cap intents per dialect (0 = all)")
    l_enr.add_argument("--no-persist", action="store_true",
                       help="Validate only; do not write to the template store")
    l_enr.add_argument("--format", choices=["table", "json"], default="table")

    l_tstore = llm_sub.add_parser(
        "templates", help="Inspect/clear the AI-enriched template store")
    l_tstore_sub = l_tstore.add_subparsers(dest="templates_action", required=True)
    l_ts_show = l_tstore_sub.add_parser("status", help="Show enriched-template counts")
    l_ts_show.add_argument("--format", choices=["table", "json"], default="table")
    l_tstore_sub.add_parser("clear", help="Remove all enriched templates")

    mcfg_p = ai_sub.add_parser("config", help="View/edit ai_query/config.ini")
    mcfg_sub = mcfg_p.add_subparsers(dest="ai_config_action", required=True)
    mcfg_show = mcfg_sub.add_parser("show", help="Show module config")
    mcfg_show.add_argument("--format", choices=["table", "json"], default="table")
    mcfg_show.add_argument("--section", default="")
    mcfg_set = mcfg_sub.add_parser("set", help="Set one config value")
    mcfg_set.add_argument("section")
    mcfg_set.add_argument("key")
    mcfg_set.add_argument("value")
    mcfg_sub.add_parser("restore", help="Restore config.ini from .example")

    # Backend listing + shared one-shot flags. The free-text question itself is
    # owned by the dedicated ``ask`` subparser (see ``inject_oneshot_ask``) so it
    # cannot collide with the subcommand choices on this parser.
    ai_p.add_argument("--conn", metavar="NAME",
                      help="Connection to use (required unless --list-backends / session)")
    ai_p.add_argument("--backend", default="", metavar="NAME",
                      help="AI backend to use (default: auto-select)")
    ai_p.add_argument("--list-backends", action="store_true", dest="list_backends",
                      help="List configured AI backends and exit")
    ai_p.add_argument("--format", choices=["table", "json", "csv"], default="table")
    _add_sql_mode_args(ai_p)


def _service():
    from ai_query.service import make_service

    return make_service()




def _dispatch_session(args) -> int:
    svc = _service()
    cmd = getattr(args, "session_cmd", None)
    if cmd == "list":
        r = svc.ai_session_list()
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        rows = [[s["tab_number"], s["session_id"][:8], s.get("connection_name", ""), s.get("backend", ""), s.get("status", "")]
                for s in r.get("sessions") or []]
        cliutil.print_table(rows, ["tab", "session", "connection", "backend", "status"], args.format)
        return 0
    if cmd == "show":
        r = svc.ai_session_get(args.session)
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        if args.format == "json":
            print(json.dumps(r.get("session") or {}, indent=2, default=str))
            return 0
        s = r.get("session") or {}
        cliutil.info(f"session={s.get('session_id', '')[:12]} tab={s.get('tab_number')} "
                     f"conn={s.get('connection_name', '')} backend={s.get('backend', '')}")
        cliutil.info(f"  mode={s.get('sql_mode')} status={s.get('status', '')}")
        return 0
    if cmd == "new":
        r = svc.ai_session_create(
            args.conn,
            args.backend or None,
            isolated=args.isolated,
            sql_mode=_resolve_sql_mode(args),
            sql_execution_rules=_resolve_execution_rules(args),
        )
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        s = r["session"]
        cliutil.info(
            f"Created tab {s['tab_number']} session {s['session_id']} "
            f"mode={s.get('sql_mode', 'summary')}"
        )
        return 0
    if cmd == "ask":
        q = " ".join(args.question)
        r = svc.ai_session_ask(args.session, q, mode="ask")
        return _print_ai_result(r)
    if cmd == "follow-up":
        m = " ".join(args.message)
        r = svc.ai_session_follow_up(args.session, m)
        return _print_ai_result(r)
    if cmd == "set-mode":
        fields = {}
        mode = _resolve_sql_mode(args)
        rules = _resolve_execution_rules(args)
        if mode:
            fields["sql_mode"] = mode
        if rules is not None:
            fields["sql_execution_rules"] = rules
        if not fields:
            cliutil.err("Provide --sql-mode and/or --execution-rules")
            return 2
        r = svc.ai_session_update(args.session, **fields)
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        s = r["session"]
        cliutil.info(f"Updated session mode={s.get('sql_mode')} rules={'yes' if s.get('sql_execution_rules') else 'no'}")
        return 0
    if cmd == "execute-sql":
        r = svc.ai_session_execute_sql(args.session, args.sql)
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        if r.get("explain_output"):
            print(cliutil.bold("EXPLAIN:"))
            print(r["explain_output"])
        result = r.get("result") or {}
        if result.get("message"):
            print(result["message"])
        elif result.get("columns"):
            cliutil.print_table(
                result.get("rows") or [],
                result.get("columns") or [],
                args.format,
            )
        return 0
    if cmd == "cross":
        instr = " ".join(args.instruction)
        r = svc.ai_session_cross_tab(args.session, instr)
        if r.get("error") and not r.get("routed"):
            cliutil.err(r["error"])
            return 1
        cliutil.info(str(r))
        return 0
    if cmd == "close":
        r = svc.ai_session_delete(args.session)
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        cliutil.info("Session closed.")
        return 0
    if cmd == "save":
        r = svc.ai_session_save(args.file or None)
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        cliutil.info(f"Saved to {r['path']}")
        return 0
    if cmd == "load":
        r = svc.ai_session_load(args.file or None)
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        cliutil.info(f"Loaded from {r['path']} ({len(r.get('sessions') or [])} sessions)")
        return 0
    cliutil.err("Unknown session command")
    return 2


def _print_ai_result(r) -> int:
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    if r.get("sql"):
        print(cliutil.bold("Generated SQL:"))
        print(r["sql"])
    if r.get("explanation"):
        print()
        print(cliutil.bold("Explanation:"))
        print(r["explanation"])
    for msg in (r.get("cross_tab") or {}).get("messages") or []:
        print(cliutil.bold("Cross-tab:"), msg)
    return 0

def _load_sql_arg(args) -> tuple[str | None, int]:
    """Return (sql_text, error_code). error_code==0 means OK."""
    from pathlib import Path

    sql = getattr(args, "sql", "") or ""
    sql_file = getattr(args, "sql_file", "") or ""
    if sql_file:
        try:
            sql = Path(sql_file).read_text(encoding="utf-8")
        except OSError as exc:
            cliutil.err(str(exc))
            return None, 1
    if not (sql or "").strip():
        cliutil.err("Provide SQL via --sql or --sql-file.")
        return None, 2
    return sql, 0


def _load_rules_arg(args) -> str:
    from pathlib import Path

    rules = getattr(args, "rules", "") or ""
    rules_file = getattr(args, "rules_file", "") or ""
    if rules_file:
        try:
            rules = Path(rules_file).read_text(encoding="utf-8")
        except OSError as exc:
            cliutil.err(str(exc))
            return ""
    return rules


def _dispatch_explain(args) -> int:
    sql, code = _load_sql_arg(args)
    if sql is None:
        return code
    r = _service().explain_sql(sql, connection=args.conn or "",
                               db_type=args.db_type or "")
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    print(cliutil.bold(f"Explanation ({r.get('db_type', 'SQL')}):"))
    print(r.get("explanation") or "")
    return 0


def _dispatch_optimize(args) -> int:
    sql, code = _load_sql_arg(args)
    if sql is None:
        return code
    r = _service().optimize_sql(sql, connection=args.conn or "",
                                db_type=args.db_type or "")
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    print(cliutil.bold(f"Optimizations ({r.get('db_type', 'SQL')}):"))
    print(r.get("suggestions") or "")
    return 0


def _dispatch_review(args) -> int:
    sql, code = _load_sql_arg(args)
    if sql is None:
        return code
    rules = _load_rules_arg(args)
    r = _service().review_sql(
        sql, rules=rules, connection=args.conn or "",
        db_type=args.db_type or "", timeout=args.timeout,
    )
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    print(cliutil.bold(f"SQL Review ({r.get('db_type', 'SQL')}):"))
    print(r.get("review") or "")
    return 0


def _dispatch_configure(args) -> int:
    r = _service().configure_ai_backend(args.backend, verify=not args.no_verify)
    (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
    cliutil.info(f"Active backend: {r.get('active') or '(none)'}")
    return 0 if r["ok"] else 1


def _dispatch_fallback(args) -> int:
    # No backend and not clearing => show the current fallback.
    if not args.clear and not (args.backend or "").strip():
        info = _service().list_ai_backends()
        if not info.get("available"):
            cliutil.err(info.get("error") or "AI not available.")
            return 1
        cliutil.info(f"Fallback backend: {info.get('fallback') or '(none)'}")
        return 0
    backend = "" if args.clear else args.backend
    r = _service().configure_ai_fallback_backend(backend, verify=not args.no_verify)
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    (cliutil.ok if r.get("ok") or not backend else cliutil.err)(r.get("message"))
    cliutil.info(f"Fallback backend: {r.get('fallback') or '(none)'}")
    return 0 if (r.get("ok") or not backend) else 1


def _dispatch_correct(args) -> int:
    sql, code = _load_sql_arg(args)
    if sql is None:
        return code
    r = _service().correct_sql(
        args.question, sql,
        connection=args.conn, db_type=args.db_type,
        error_text=args.error_text, mode=args.mode, backend=args.backend,
    )
    if not r.get("ok"):
        cliutil.err(r.get("error") or "Correction failed.")
        return 1
    cliutil.ok(f"Corrected by {r.get('backend_used') or '(backend)'}")
    print((r.get("sql") or "").strip())
    if r.get("explanation"):
        cliutil.info(r["explanation"])
    return 0


def _dispatch_cache(args) -> int:
    svc = _service()
    act = getattr(args, "cache_action", None)
    if act == "info":
        r = svc.get_ai_cache_info()
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        rows = [
            [e.get("connection", ""), e.get("db_type", ""),
             e.get("timestamp", ""), e.get("table_count", 0)]
            for e in r.get("entries") or []
        ]
        cliutil.print_table(
            rows, ["connection", "db_type", "cached_at", "tables"], args.format,
        )
        cliutil.info(f"{r.get('count', 0)} cache entries.")
        return 0
    if act == "clear":
        r = svc.clear_ai_cache(args.conn or None)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if act == "show":
        r = svc.show_ai_cache(args.conn or "")
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        import json
        if args.format == "json":
            print(json.dumps(
                {k: v for k, v in r.items() if k != "error"},
                indent=2, default=str,
            ))
        else:
            if r.get("schema") is not None:
                print(json.dumps(r["schema"], indent=2, default=str))
            else:
                print(r.get("schema_last_sent") or "")
        return 0
    cliutil.err("Unknown cache action.")
    return 2


def _dispatch_pii(args) -> int:
    svc = _service()
    act = getattr(args, "pii_action", None)
    if act == "status":
        r = svc.get_pii_masking()
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        cliutil.info(f"PII masking: {'on' if r['enabled'] else 'off'}")
        return 0
    if act == "on":
        r = svc.set_pii_masking(True)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    if act == "off":
        r = svc.set_pii_masking(False)
        (cliutil.ok if r["ok"] else cliutil.err)(r["message"])
        return 0 if r["ok"] else 1
    cliutil.err("Unknown pii action.")
    return 2


def _dispatch_ai_config(args) -> int:
    import json
    from ai_query import module_config as mc

    act = args.ai_config_action
    if act == "show":
        sec_filter = (getattr(args, "section", "") or "").strip()
        if args.format == "json":
            out = {s: {k: mc.get(s, k) for k in keys} for s, keys in mc.DEFAULTS.items()}
            if sec_filter:
                out = {sec_filter: out.get(sec_filter, {})}
            print(json.dumps(out, indent=2))
            return 0
        for sec, keys in sorted(mc.DEFAULTS.items()):
            if sec_filter and sec != sec_filter:
                continue
            cliutil.info(f"[{sec}]")
            cliutil.print_table(
                [[k, mc.get(sec, k)] for k in sorted(keys)], ["key", "value"], "table"
            )
        return 0
    if act == "set":
        mc.set_value(args.section, args.key, args.value)
        cliutil.ok(f"{args.section}.{args.key} saved.")
        return 0
    if act == "restore":
        mc.restore_defaults()
        cliutil.ok("ai_query/config.ini restored from .example.")
        return 0
    return 2


def dispatch_cli(args) -> int:
    sub = getattr(args, "ai_subcommand", None)
    if sub == "session":
        return _dispatch_session(args)
    if sub == "explain":
        return _dispatch_explain(args)
    if sub == "optimize":
        return _dispatch_optimize(args)
    if sub == "review":
        return _dispatch_review(args)
    if sub == "configure":
        return _dispatch_configure(args)
    if sub == "fallback":
        return _dispatch_fallback(args)
    if sub == "correct":
        return _dispatch_correct(args)
    if sub == "cache":
        return _dispatch_cache(args)
    if sub == "pii":
        return _dispatch_pii(args)
    if sub == "config":
        return _dispatch_ai_config(args)
    if sub == "rag":
        return _dispatch_rag(args)
    if sub == "llm":
        return _dispatch_llm(args)
    if sub == "ask":
        return _dispatch_oneshot(args)
    if getattr(args, "list_backends", False):
        info = _service().list_ai_backends()
        if not info.get("available"):
            cliutil.err(info.get("error") or "AI not available.")
            return 1
        options = info.get("options") or []
        rows = []
        if options:
            # Expanded view: local-llm shows one row per trained model, so the
            # user can pick "<model> (local <engine>)" as the backend.
            for opt in options:
                status = "ready" if opt.get("ready") else "not verified"
                mark = " *" if opt.get("active") else ""
                rows.append([opt.get("value", "") + mark, opt.get("label", ""), status])
            cliutil.print_table(rows, ["backend", "label", "status"], args.format)
        else:
            ready = set(info.get("ready") or [])
            active = info.get("active") or ""
            for b in (info.get("all") or []):
                status = "ready" if b in ready else "not verified"
                mark = " *" if b == active else ""
                rows.append([b + mark, status])
            cliutil.print_table(rows, ["backend", "status"], args.format)
        cliutil.info("* = active backend (use `dbtool ai configure --backend <name>`)")
        return 0
    return _dispatch_oneshot(args)


def _dispatch_rag(args) -> int:
    import json

    svc = _service()
    act = getattr(args, "rag_action", None)
    if act == "index":
        r = svc.rag_index(args.conn, rebuild=args.rebuild)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Indexing failed.")
            return 1
        cliutil.ok(
            f"Indexed {r.get('indexed', 0)} docs for '{args.conn}' "
            f"(provider={r.get('provider')}, dim={r.get('dim')})."
        )
        return 0
    if act == "status":
        r = svc.rag_status(args.conn or "")
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Status failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        if args.conn:
            mm = r.get("embedder_mismatch") or {}
            cliutil.info(
                f"{args.conn}: indexed={r.get('indexed')} "
                f"docs={r.get('doc_count')}"
            )
            if mm.get("mismatch"):
                cliutil.warn(mm.get("message") or "Embedder mismatch — re-index recommended.")
        else:
            rows = [[m.get("connection", ""), m.get("doc_count", 0),
                     m.get("provider", ""), m.get("indexed_at", "")]
                    for m in r.get("connections") or []]
            cliutil.print_table(
                rows, ["connection", "docs", "provider", "indexed_at"], "table")
        return 0
    if act in ("search", "context"):
        q = " ".join(args.query)
        r = (svc.rag_search if act == "search" else svc.rag_context)(
            args.conn, q, k=args.k)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Retrieval failed.")
            return 1
        if act == "context":
            print(r.get("context") or "")
        else:
            if r.get("preview"):
                print(r.get("preview"))
            else:
                for h in r.get("hits") or []:
                    print(f"  {h['score']:>6}  {h['kind']:<9} {h['ref']}")
        return 0
    if act == "ask":
        q = " ".join(args.question)
        r = svc.rag_ask(
            args.conn, q, k=args.k, backend=args.backend or None,
            auto_index=not getattr(args, "no_auto_index", False),
        )
        if r.get("error"):
            cliutil.err(r["error"])
            return 1
        if r.get("sql"):
            print(cliutil.bold("Generated SQL:"))
            print(r["sql"])
        if r.get("explanation"):
            print("\n" + cliutil.bold("Explanation:"))
            print(r["explanation"])
        return 0
    if act == "add-example":
        r = svc.rag_add_example(args.conn, args.question, args.sql, args.note)
        (cliutil.ok if r.get("ok") else cliutil.err)(
            r.get("doc_id") or r.get("error") or "")
        return 0 if r.get("ok") else 1
    if act == "add-examples-file":
        r = svc.rag_add_examples_from_file(
            args.conn, args.file, fmt=args.format,
            standalone=bool(args.standalone))
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Example import failed.")
            return 1
        msg = (
            f"Imported {r.get('added', 0)} example(s) "
            f"(parsed={r.get('parsed', 0)}, skipped={r.get('skipped', 0)}) "
            f"into '{args.conn}'."
        )
        reasons = r.get("reasons") or {}
        if reasons:
            msg += " Skips: " + ", ".join(f"{k}={v}" for k, v in reasons.items())
        cliutil.ok(msg)
        return 0
    if act == "add-glossary":
        r = svc.rag_add_glossary(args.conn, args.term, args.definition)
        (cliutil.ok if r.get("ok") else cliutil.err)(
            r.get("doc_id") or r.get("error") or "")
        return 0 if r.get("ok") else 1
    if act == "clear":
        r = svc.rag_clear(args.conn)
        (cliutil.ok if r.get("ok") else cliutil.err)(
            f"Removed {r.get('removed', 0)} docs." if r.get("ok")
            else (r.get("error") or ""))
        return 0 if r.get("ok") else 1
    if act == "add-document":
        if not (args.file or args.text):
            cliutil.err("Provide --file or --text.")
            return 2
        r = svc.rag_add_document(
            args.scope, text=args.text or None, file_path=args.file or None,
            title=args.title, source=args.source, standalone=args.standalone)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Add document failed.")
            return 1
        cliutil.ok(f"Indexed '{r.get('source')}' as {r.get('chunks')} chunk(s) "
                   f"in scope '{args.scope}'.")
        return 0
    if act == "list-docs":
        r = svc.rag_documents(args.scope)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "List failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        rows = [[d.get("source", ""), d.get("title", ""), d.get("chunks", 0)]
                for d in r.get("documents") or []]
        cliutil.print_table(rows, ["source", "title", "chunks"], "table")
        return 0
    if act == "remove-doc":
        r = svc.rag_remove_document(args.scope, args.source)
        (cliutil.ok if r.get("ok") else cliutil.err)(
            f"Removed {r.get('removed', 0)} chunk(s)." if r.get("ok")
            else (r.get("error") or ""))
        return 0 if r.get("ok") else 1
    if act == "analytics":
        r = svc.rag_analytics_library()
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        for q in r.get("queries") or []:
            print(f"  [{q['category']:<12}] {q['question']}")
        cliutil.info(f"{len(r.get('queries') or [])} analytical patterns "
                     f"across {len(r.get('categories') or [])} categories.")
        return 0
    if act == "seed-analytics":
        cats = [c for c in (args.categories or "").split(",") if c.strip()]
        r = svc.rag_seed_analytics(args.scope, cats or None, standalone=args.standalone)
        (cliutil.ok if r.get("ok") else cliutil.err)(
            f"Seeded {r.get('seeded', 0)} analytical patterns into '{args.scope}'."
            if r.get("ok") else (r.get("error") or ""))
        return 0 if r.get("ok") else 1
    if act == "breakdown":
        r = svc.rag_breakdown(args.scope)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Breakdown failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        rows = [[k, v] for k, v in sorted((r.get("counts") or {}).items())]
        cliutil.print_table(rows, ["kind", "count"], "table")
        cliutil.info(f"total docs: {r.get('total', 0)}")
        mm = r.get("embedder_mismatch") or {}
        if mm.get("mismatch"):
            cliutil.warn(mm.get("message") or "Embedder mismatch — re-index recommended.")
        return 0
    if act == "overview":
        r = svc.rag_scope_overview(args.scope)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Overview failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        st = r.get("status") or {}
        br = r.get("breakdown") or {}
        cliutil.info(
            f"scope={args.scope} indexed={st.get('indexed')} "
            f"docs={st.get('doc_count')}"
        )
        for k, v in sorted((br.get("counts") or {}).items()):
            print(f"  {k:<12} {v}")
        mm = (st.get("embedder_mismatch") or br.get("embedder_mismatch") or {})
        if mm.get("mismatch"):
            cliutil.warn(mm.get("message") or "Re-index recommended.")
        return 0
    if act == "preview":
        q = " ".join(args.query)
        r = svc.rag_preview(args.conn, q, k=args.k)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Preview failed.")
            return 1
        print(r.get("preview") or "")
        print("\n" + cliutil.bold("Context block:"))
        print(r.get("context") or "")
        return 0
    if act == "search-multi":
        scopes = [s.strip() for s in (args.scopes or "").split(",") if s.strip()]
        q = " ".join(args.query)
        r = svc.rag_preview_multi(scopes, q, k=args.k)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Multi-scope search failed.")
            return 1
        print(r.get("preview") or "")
        print("\n" + cliutil.bold("Context block:"))
        print(r.get("context") or "")
        return 0
    if act == "add-codebase":
        mf = args.max_files if getattr(args, "max_files", 0) > 0 else None
        r = svc.rag_add_codebase(
            args.folder, args.scope,
            standalone=bool(args.standalone or True),
            replace=not getattr(args, "no_replace", False),
            max_files=mf,
        )
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Codebase indexing failed.")
            return 1
        cliutil.ok(
            f"Indexed {r.get('chunks', 0)} code chunk(s) from "
            f"{r.get('files_scanned', 0)} file(s) into scope '{args.scope}'."
        )
        return 0
    if act == "eval":
        gold = None
        gold_path = getattr(args, "gold", "") or ""
        if gold_path:
            from pathlib import Path
            try:
                raw = Path(gold_path).expanduser().read_text(encoding="utf-8")
            except Exception as exc:  # noqa: BLE001
                cliutil.err(f"Could not read gold file: {exc}")
                return 1
            gold = _parse_gold(raw)
            if not gold:
                cliutil.err("No usable gold cases parsed from file.")
                return 1
        r = svc.rag_eval(args.connection, gold=gold, k=args.k,
                         per_case=bool(getattr(args, "per_case", False)))
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Eval failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        m = r.get("metrics") or {}
        cliutil.info(
            f"connection={args.connection} cases={m.get('cases', 0)} "
            f"k={r.get('k')} "
            f"(seeded_from_examples={r.get('seeded_from_examples')})"
        )
        print(f"  recall@k          {m.get('recall_at_k', 0.0):.4f}")
        print(f"  MRR               {m.get('mrr', 0.0):.4f}")
        print(f"  context precision {m.get('context_precision', 0.0):.4f}")
        for c in (r.get("cases_detail") or []):
            print(f"    - r@k={c['recall_at_k']:.2f} rr={c['reciprocal_rank']:.2f} "
                  f"cp={c['context_precision']:.2f}  {c.get('question', '')[:60]}")
        return 0
    if act == "drift":
        r = svc.rag_drift(args.connection)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Drift check failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        (cliutil.warn if r.get("changed") else cliutil.ok)(r.get("message") or "")
        return 0
    if act == "reindex-stale":
        conns = list(getattr(args, "connections", []) or []) or None
        r = svc.rag_reindex_stale(conns, force=bool(getattr(args, "force", False)))
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Reindex failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        cliutil.ok(f"Re-indexed {r.get('reindexed', 0)} connection(s).")
        for res in (r.get("results") or []):
            tag = "skipped" if res.get("skipped") else "reindexed"
            print(f"  {res.get('connection')}: {tag} ({res.get('reason')})")
        return 0
    if act == "reindex-schedule":
        sub = getattr(args, "schedule_action", "status")
        if sub == "status":
            r = svc.rag_reindex_schedule_status()
        elif sub == "start":
            r = svc.rag_reindex_schedule_start()
        elif sub == "stop":
            r = svc.rag_reindex_schedule_stop()
        else:
            cliutil.err("Unknown schedule action.")
            return 2
        if getattr(args, "format", "table") == "json":
            print(json.dumps(r, indent=2, default=str))
        else:
            cliutil.info(
                f"enabled={r.get('enabled')} running={r.get('running')} "
                f"start={r.get('start_time')} duration_h={r.get('duration_hours')} "
                f"next_run={r.get('next_run')} last_run={r.get('last_run_date')}"
            )
        return 0 if r.get("ok") else 1
    cliutil.err("Unknown rag action.")
    return 2


def _parse_gold(raw: str) -> list[dict]:
    """Parse a gold-set file (JSON array or JSONL) into eval cases."""
    raw = (raw or "").strip()
    if not raw:
        return []
    out: list[dict] = []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict)]
        if isinstance(data, dict):
            return [data]
    except Exception:  # noqa: BLE001
        pass
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                out.append(obj)
        except Exception:  # noqa: BLE001
            continue
    return out


def _dispatch_llm(args) -> int:
    import json

    svc = _service()
    act = getattr(args, "llm_action", None)
    if act == "engines":
        r = svc.llm_engines()
        rows = [[e["name"], e.get("stage", ""),
                 "yes" if e.get("available") else "no", e.get("reason", "")]
                for e in r.get("engines") or []]
        cliutil.print_table(rows, ["engine", "stage", "available", "note"], "table")
        return 0
    if act == "train":
        cliutil.info("Training local model… (this can take a moment)")
        r = svc.llm_train(
            name=args.name,
            engine=args.engine or None,
            include_sample=not args.no_sample,
            dataset_path=args.dataset or None,
            rag_connection=args.rag_conn or "",
        )
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Training failed.")
            return 1
        pairs = r.get("num_pairs", r.get("pairs", "?"))
        loss = r.get("final_loss", r.get("loss", "?"))
        elapsed = r.get("elapsed_sec")
        cliutil.ok(
            f"Trained '{r.get('name')}' engine={r.get('engine')} "
            f"pairs={pairs} loss={loss}"
            + (f" in {elapsed}s" if elapsed is not None else "")
            + f" -> {r.get('path')}"
        )
        if r.get("engine_fallback"):
            cliutil.info(
                f"(requested '{r.get('engine_requested')}' was unavailable; "
                f"used '{r.get('engine')}')")
        ev = r.get("eval") or {}
        if ev:
            cliutil.info(
                f"Accuracy meter ({ev.get('mode', 'eval')}): "
                f"parse={ev.get('parse_ok_rate')} exec={ev.get('executable_rate')} "
                f"norm_match={ev.get('normalized_match_rate')} "
                f"EX={ev.get('execution_exact_rate')}")
        return 0
    if act == "status":
        r = svc.llm_status(args.name)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Status failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
        elif not r.get("trained"):
            cliutil.info(f"Model '{args.name}' is not trained yet.")
        else:
            cliutil.info(
                f"Model '{args.name}': engine={r.get('engine')} "
                f"trained_at={(r.get('meta') or {}).get('trained_at', '')}")
        return 0
    if act == "list":
        r = svc.llm_list()
        rows = [[m["name"], m.get("engine", ""), m.get("trained_at", "")]
                for m in r.get("models") or []]
        cliutil.print_table(rows, ["name", "engine", "trained_at"], "table")
        return 0
    if act == "verify":
        r = svc.llm_model_dataset(name=args.name, query=args.query or "",
                                  limit=args.limit or 0)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Dataset lookup failed.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        if not r.get("available"):
            cliutil.info(r.get("reason") or "No saved training dataset for this model.")
            return 0
        if args.query:
            if r.get("matched"):
                cliutil.ok(f"Found {r.get('shown')} matching pair(s) in '{args.name}' "
                           f"(of {r.get('total')} total).")
            else:
                cliutil.err(f"No pair in model '{args.name}' matches '{args.query}' "
                            f"({r.get('total')} pairs trained).")
        else:
            cliutil.info(f"Model '{args.name}' trained on {r.get('total')} pair(s):")
        rows = [[(p.get("question") or "")[:60], (p.get("sql") or "")[:80]]
                for p in r.get("pairs") or []]
        cliutil.print_table(rows, ["question", "sql"], "table")
        return 0
    if act == "versions":
        r = svc.llm_model_versions(name=args.name)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Could not list versions.")
            return 1
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0
        versions = r.get("versions") or []
        if not versions:
            cliutil.info(f"No saved versions for model '{args.name}'.")
            return 0
        rows = [[v.get("version", ""), v.get("reason", ""), v.get("created", "")]
                for v in versions]
        cliutil.print_table(rows, ["version", "reason", "created"], "table")
        return 0
    if act == "restore":
        r = svc.llm_model_restore(name=args.name, version=args.version)
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Restore failed.")
            return 1
        cliutil.ok(f"Model '{args.name}' restored to version '{r.get('restored')}'.")
        return 0
    if act == "generate":
        q = " ".join(args.question)
        r = svc.llm_generate(
            q, name=args.name, engine=args.engine or None,
            max_new=args.max_new, temperature=args.temperature,
            connection=args.connection or "",
            alternatives=bool(getattr(args, "alternatives", False)),
        )
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Generation failed.")
            return 1
        valid = r.get("valid")
        reason = r.get("reason") or ""
        print(cliutil.bold("Generated SQL:"))
        print(r.get("sql") or "")
        if valid is not None:
            cliutil.info(f"Validation: {'valid' if valid else 'invalid'}" +
                         (f" ({reason})" if reason and not valid else ""))
        if r.get("resolved") and r.get("mappings"):
            cliutil.info(
                "Placeholder resolution: "
                + ", ".join(f"{k}→{v}" for k, v in (r.get("mappings") or {}).items()))
        elif r.get("ambiguous"):
            cliutil.info("Placeholder resolution: ambiguous (could not map confidently).")
        elif r.get("resolution_error"):
            cliutil.info(f"Placeholder resolution: {r['resolution_error']}")
        alts = r.get("alternatives") or []
        if alts:
            print(cliutil.bold(f"\nAlternative SQL syntaxes ({len(alts)}):"))
            for i, a in enumerate(alts, 1):
                tag = f" [{a.get('db_type')}]" if a.get("db_type") else ""
                print(f"  {i}.{tag} {a.get('sql')}")
        return 0
    if act == "eval":
        r = svc.llm_eval(
            name=args.name,
            connection=args.connection or "",
            depth=args.depth or None,
            include_sample=not args.no_sample,
            rag_connection=args.rag_conn or "",
        )
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Evaluation failed.")
            return 1
        s = r.get("summary") or {}
        cliutil.ok(
            f"Eval ({s.get('mode', '?')}, n={s.get('count', 0)}): "
            f"parse={s.get('parse_ok_rate')} exec={s.get('executable_rate')} "
            f"norm_match={s.get('normalized_match_rate')} "
            f"EX={s.get('execution_exact_rate')} soft_f1={s.get('soft_f1_avg')}")
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
        return 0
    if act == "export":
        r = svc.llm_export(args.out, include_sample=not args.no_sample,
                           rag_connection=args.rag_conn or "")
        (cliutil.ok if r.get("ok") else cliutil.err)(
            f"Exported {r.get('count', 0)} pairs -> {r.get('path')}"
            if r.get("ok") else (r.get("error") or ""))
        return 0 if r.get("ok") else 1
    if act == "dataset":
        r = svc.llm_dataset(
            include_sample=not args.no_sample,
            rag_connection=args.rag_conn or "",
        )
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Dataset export failed.")
            return 1
        print(r.get("content") or "")
        return 0
    if act == "train-llm":
        def progress(ev: dict):
            if args.format == "json":
                return
            etype = ev.get("type")
            if etype == "training_capture":
                status = ev.get("status")
                if status == "collecting":
                    cliutil.info("  collecting training data…")
                elif status == "captured":
                    cliutil.info(
                        f"  collected {ev.get('pairs', 0)} pair(s) "
                        f"({ev.get('source', '')}); training…")
            elif etype == "training_rag":
                rag_status = ev.get("status")
                if rag_status == "indexing_parallel":
                    cliutil.info(f"  indexing RAG for '{ev.get('connection', '')}'…")
                elif rag_status == "indexed":
                    cliutil.info("  RAG indexing complete.")
            elif etype == "training_progress":
                cliutil.info(f"  training {ev.get('model', 'model')}…")
            elif etype == "training_epoch":
                cliutil.info(
                    f"  {ev.get('model', 'model')}: epoch {ev.get('epoch', '?')}, "
                    f"loss {ev.get('loss', '?')}")

        r = svc.llm_train_rich({
            "mode": args.mode,
            "description": args.description,
            "connections": [args.connection] if args.connection else [],
            "codebase_path": args.codebase_path,
            "train_llm": args.train_llm,
            "train_new_name": args.train_new_name,
            "train_engine": args.train_engine,
            "use_rag": bool(args.use_rag),
            "index_rag": bool(args.index_rag),
            "rag_strategy": args.rag_strategy,
            "mine_db": not bool(args.no_mine),
            "train_sample_limit": args.sample_limit,
            **({"train_mode": args.train_mode} if args.train_mode else {}),
        }, progress=progress)
        if not r.get("ok"):
            cliutil.err(r.get("error") or r.get("reason") or "Training failed.")
            return 1
        cliutil.ok(
            f"Trained {len(r.get('models') or [])} model(s) on {r.get('pairs')} "
            f"pairs ({r.get('source', '')}); "
            f"already={r.get('already_trained', 0)} new={r.get('new_pairs', 0)}")
        cliutil.info(f"  reason: {r.get('reason', '')}")
        return 0
    if act == "train-multi":
        r = svc.llm_train_multi({
            "connections": list(args.connections or []),
            "train_llm": args.train_llm,
            "train_new_name": args.train_new_name,
            "train_engine": args.train_engine,
            "gen_workers": args.gen_workers,
            "train_sample_limit": args.sample_limit,
        })
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0 if r.get("ok") else 1
        if not r.get("ok"):
            cliutil.err(r.get("error") or r.get("reason") or "Multi-connection training failed.")
            return 1
        cliutil.ok(
            f"Trained {len([m for m in (r.get('models') or []) if m.get('ok')])} "
            f"model(s) from {len(r.get('connections') or [])} connection(s)")
        for m in r.get("models") or []:
            cliutil.info(f"  {m.get('name')}: ok={m.get('ok')} "
                         f"merged_pairs={m.get('merged_pairs')} "
                         f"shards={m.get('committed_shards')}")
        return 0
    if act == "mine-pairs":
        r = svc.llm_mine_pairs({
            "connections": [args.connection],
            "train_sample_limit": args.sample_limit,
            "train_max_tables": args.max_tables,
        })
        if args.format == "json":
            print(json.dumps(r, indent=2))
            return 0 if r.get("ok") else 1
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Mining failed.")
            return 1
        stats = r.get("stats") or {}
        cliutil.ok(
            f"Mined {stats.get('kept', 0)} validated pairs "
            f"({stats.get('validated', 0)}/{stats.get('candidates', 0)} passed)")
        return 0
    if act == "rag-status":
        r = svc.llm_rag_status(args.connection or "")
        print(json.dumps(r, indent=2))
        return 0 if r.get("ok", True) else 1
    if act == "index-rag":
        r = svc.llm_index_rag(args.connection, rebuild=bool(args.rebuild))
        (cliutil.ok if r.get("ok") else cliutil.err)(
            f"Indexed RAG for '{args.connection}'" if r.get("ok")
            else (r.get("error") or "RAG indexing failed."))
        return 0 if r.get("ok") else 1
    if act == "harvest":
        complexity = [c.strip() for c in (args.complexity or "").split(",") if c.strip()]
        def progress(ev: dict):
            if args.format == "json":
                return
            etype = ev.get("type")
            if etype == "harvest_offline_collected":
                cliutil.info(
                    f"  offline corpus: {ev.get('pairs', 0)} validated pair(s); training…")
            elif etype == "harvest_train_done":
                phase = str(ev.get("phase") or "training").replace("_", " ")
                status = "ok" if ev.get("ok") else "failed"
                cliutil.info(f"  {phase} training {status}")
            elif etype == "training_epoch":
                cliutil.info(
                    f"  {ev.get('model', 'model')}: epoch {ev.get('epoch', '?')}, "
                    f"loss {ev.get('loss', '?')}")
            elif etype == "harvest_backend_start":
                cliutil.info("  backend enrichment starting…")
            elif etype == "harvest_question_bank":
                if ev.get("status") == "generating":
                    cliutil.info(
                        f"  asking AI to invent {ev.get('count', 0)} questions… "
                        "(this can take a while)")
                elif ev.get("status") == "generated":
                    cliutil.info(f"  AI proposed {ev.get('questions', 0)} questions")
            elif etype == "harvest_followup":
                q = (ev.get("question") or "").strip()
                tail = f": {q[:60]}" if q else ""
                cliutil.info(
                    f"  follow-up thread {ev.get('done', 0)}/{ev.get('total', 0)} "
                    f"[{ev.get('category', '')}]{tail}")
            elif etype == "harvest_generate":
                if ev.get("status") == "planned":
                    cliutil.info(
                        f"  prepared {ev.get('total', 0)} backend question(s); "
                        f"generating with {ev.get('workers', 1)} worker(s)…")
                elif ev.get("status") == "generating":
                    q = (ev.get("question") or "").strip()
                    tail = f" — {q[:60]}" if q else ""
                    cliutil.info(
                        f"  backend generation {ev.get('done', 0)}/{ev.get('total', 0)} "
                        f"(kept {ev.get('kept', 0)}){tail}")

        # Graceful Ctrl-C: first interrupt requests a stop (finish the current
        # step, then save the model); a second Ctrl-C aborts hard.
        import signal

        stop_flag = {"stop": False}

        def _on_sigint(_signum, _frame):
            if stop_flag["stop"]:
                raise KeyboardInterrupt
            stop_flag["stop"] = True
            cliutil.info("\n  stop requested — finishing current step, then saving the model "
                         "(press Ctrl-C again to abort)…")

        prev_handler = signal.getsignal(signal.SIGINT)
        try:
            signal.signal(signal.SIGINT, _on_sigint)
        except (ValueError, OSError):
            prev_handler = None  # not on the main thread; skip graceful handler

        try:
            _hv_conns = [args.connection] + [
                c for c in (getattr(args, "extra_connections", []) or [])
                if c and c != args.connection
            ]
            hv_body: dict = {
                "connection": args.connection,
                "connections": _hv_conns,
                "backend": args.backend,
                "train_llm": args.train_llm,
                "train_new_name": args.train_new_name,
                "train_engine": args.train_engine,
                "complexity": complexity or None,
                "generated_questions": args.generated_questions,
                "max_questions": args.max_questions,
                "questions": args.questions,
                "questions_file": getattr(args, "questions_file", "") or "",
                "use_curated": not bool(args.no_curated),
                "use_captures": not bool(args.no_captures),
                "followups": not bool(args.no_followups),
                "mine_db": bool(args.mine_db),
                "use_rag": not bool(args.no_rag),
                "do_train": not bool(args.no_train),
            }
            if args.train_mode:
                hv_body["train_mode"] = args.train_mode
            if getattr(args, "training_depth", ""):
                hv_body["training_depth"] = args.training_depth
            if getattr(args, "advanced_training", False):
                hv_body["advanced_training"] = True
            if getattr(args, "multi_dialect", False):
                hv_body["multi_dialect"] = True
            if getattr(args, "multi_syntax", False):
                hv_body["multi_syntax"] = True
            if args.gen_workers:
                hv_body["gen_workers"] = args.gen_workers
            if args.gen_timeout:
                hv_body["gen_timeout"] = args.gen_timeout
            if args.gen_retries >= 0:
                hv_body["gen_retries"] = args.gen_retries
            if args.no_retry_backlog:
                hv_body["retry_backlog"] = False
            if args.max_consecutive_failures >= 0:
                hv_body["max_consecutive_failures"] = args.max_consecutive_failures
            if getattr(args, "template_mode", ""):
                hv_body["template_mode"] = args.template_mode
            r = svc.llm_harvest(hv_body, progress=progress, should_stop=lambda: stop_flag["stop"])
        finally:
            if prev_handler is not None:
                try:
                    signal.signal(signal.SIGINT, prev_handler)
                except (ValueError, OSError):
                    pass
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0 if r.get("ok") else 1
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Harvest failed.")
            return 1
        srcs = r.get("sources") or {}
        if r.get("stopped"):
            cliutil.info("  (stopped early on request — model trained on pairs collected so far)")
        cliutil.ok(
            f"Harvested {r.get('pairs', 0)} validated pairs "
            f"(offline {r.get('offline_pairs', 0)}, "
            f"backend {r.get('backend_pairs', 0)}, "
            f"skipped-known {r.get('skipped_known', 0)}, "
            f"already={r.get('already_trained', 0)} new={r.get('new_pairs', 0)}, "
            f"rejected {r.get('rejected', 0)}) from {args.connection}")
        cliutil.info("  sources: " + ", ".join(f"{k}={v}" for k, v in srcs.items()))
        if r.get("trained"):
            cliutil.info(f"  trained {len(r.get('models') or [])} model(s)")
        elif not args.no_train:
            cliutil.info(f"  training skipped: {r.get('error') or r.get('train_reason') or ''}")
        return 0
    if act == "harvest-schedule":
        sub = getattr(args, "schedule_action", "status")
        if sub == "status":
            r = svc.llm_harvest_schedule_status()
        elif sub == "start":
            r = svc.llm_harvest_schedule_start()
        elif sub == "stop":
            r = svc.llm_harvest_schedule_stop()
        else:
            cliutil.err("Unknown schedule action.")
            return 2
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
        else:
            cliutil.info(str(r))
        return 0 if r.get("ok") else 1
    if act == "enrich-templates":
        def progress(ev):
            if ev.get("type") == "enrich_template" and ev.get("status") in (
                    "accepted", "rejected"):
                mark = "✓" if ev["status"] == "accepted" else "✗"
                line = f"  {mark} [{ev.get('db_type')}] {ev.get('intent')}"
                if ev.get("reason"):
                    line += f" — {ev['reason']}"
                cliutil.info(line)
        r = svc.llm_enrich_templates({
            "backend": args.backend or "",
            "db_types": list(args.db_types or []),
            "connections": list(args.connections or []),
            "questions_file": args.questions_file or "",
            "limit_per_type": args.limit_per_type,
            "persist": not args.no_persist,
        }, progress=progress)
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
        else:
            if r.get("error") and not r.get("accepted"):
                cliutil.err(r["error"])
                return 1
            cliutil.info(
                f"Enriched templates: accepted {r.get('accepted', 0)}, "
                f"rejected {r.get('rejected', 0)}. Store: {r.get('store') or {}}")
        return 0 if r.get("ok") else 1
    if act == "templates":
        sub = getattr(args, "templates_action", "status")
        if sub == "status":
            r = svc.llm_template_store_summary()
            if getattr(args, "format", "table") == "json":
                print(json.dumps(r, indent=2, default=str))
            else:
                cliutil.info(str(r))
            return 0 if r.get("ok") else 1
        if sub == "clear":
            r = svc.llm_template_store_clear()
            cliutil.info(str(r))
            return 0 if r.get("ok") else 1
        cliutil.err("Unknown templates action.")
        return 2
    cliutil.err("Unknown llm action.")
    return 2


def _dispatch_oneshot(args) -> int:
    """Stateless one-shot natural-language question (``ai ask "…"``)."""
    if not args.conn:
        cliutil.err("--conn is required to ask a question.")
        return 2
    question = " ".join(getattr(args, "question", []) or [])
    if not question:
        cliutil.err("Provide a question, or use --list-backends.")
        return 2

    cliutil.info(f"Asking AI: {question}")
    r = _service().ai_query(
        args.conn,
        question,
        backend=args.backend or None,
        sql_mode=_resolve_sql_mode(args),
        sql_execution_rules=_resolve_execution_rules(args),
    )
    if r.get("error"):
        cliutil.err(r["error"])
        return 1
    if r.get("sql"):
        print(cliutil.bold("Generated SQL:"))
        print(r["sql"])
    if r.get("explanation"):
        print()
        print(cliutil.bold("Explanation:"))
        print(r["explanation"])
    if r.get("prompt_tokens_est"):
        print()
        print(f"Estimated prompt tokens: {r['prompt_tokens_est']}")
    return 0
