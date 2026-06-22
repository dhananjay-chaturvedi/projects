"""Loader for the AI Query module's config.ini (independently shippable)."""

from __future__ import annotations

from pathlib import Path

from common.config.module_ini import ModuleIniConfig

_DIR = Path(__file__).resolve().parent

DEFAULTS: dict[str, dict[str, str]] = {
    "ai": {
        "default_backend": "auto",
        "mask_pii": "true",
        "max_sessions": "20",
        # Fallback timeout for the shared subprocess runner when a backend does
        # not pass an explicit per-call timeout.
        "default_backend_timeout": "120",
        # Extra directories scanned (after PATH) when resolving backend CLIs.
        "cli_search_paths": "~/.local/bin,~/bin,~/.claude/local,/opt/homebrew/bin,/usr/local/bin,/usr/bin,/opt/local/bin",
    },
    "ai.claude": {
        "cli_path": "",
        "cli_test_timeout": "5",
        "timeout": "120",
        "simple_query_timeout": "120",
        "complex_query_timeout": "180",
        "followup_timeout": "180",
        "max_output_tokens": "4000",
    },
    "ai.cursor": {
        "model": "auto",
        "timeout": "60",
        # `cursor agent --version` probe timeout
        "agent_version_timeout": "8",
        # `cursor --version` fallback probe timeout
        "version_timeout": "5",
    },
    "ai.codex": {
        "cli_path": "",
        "model": "",
        "timeout": "120",
        # `codex --version` probe timeout
        "version_timeout": "5",
        # API endpoint reachability ping timeout
        "connectivity_timeout": "3",
        # Codex config file (blank => ~/.codex/config.toml)
        "config_path": "",
    },
    "ai.cache": {
        "max_tables_fetch": "50",
        "max_tables_detailed": "10",
        "max_tables_display": "100",
        "max_constraints_display": "50",
        "max_indexes_display": "50",
        "top_tables_count": "15",
        "max_users_roles_display": "15",
        # Lightweight getTables signature check to detect DDL drift (off by default)
        "schema_drift_check": "false",
    },
    "ai.prompt": {
        "compact_schema": "true",
        "consolidate_instructions": "true",
        "dedup_followup_schema": "true",
        "dedup_crosstab_schema": "true",
        "full_format_block": "auto",
        "progressive_escalation": "true",
    },
    "ai.meter": {
        "enabled": "true",
    },
    "ui.ai_query": {
        "auto_execute_ai_loop": "false",
        "auto_execute_summary_sql": "false",
        "auto_loop_max_iterations": "5",
        "default_sql_mode": "summary",
        "sql_execution_rules": "",
        # Thread-pool size for cross-tab delegation
        "cross_tab_max_workers": "4",
        # Default timeout (seconds) for the "Run Review" SQL review action
        "sql_review_timeout": "60",
    },
    "ai.limits": {
        "max_stored_sessions": "0",
        # Fallback session cap when max_stored_sessions is 0/disabled
        "default_max_stored_sessions": "50",
        # Conversation messages persisted to disk per session
        "max_history_on_disk": "20",
        # Filename for the persisted sessions registry
        "sessions_filename": "sessions.json",
    },
    # ── RAG (retrieval-augmented Generate SQL) ───────────────────────────────
    "ai.rag": {
        "enabled": "true",
        "embedding_provider": "hash",          # hash | sentence-transformers
        "embedding_model": "all-MiniLM-L6-v2",
        "embedding_dim": "256",
        "max_tables": "500",
        "sample_values": "true",
        "sample_limit": "5",
        "lexical_alpha": "0.3",                # vector vs lexical blend (0..1)
        "top_k": "8",
        # Skip or reduce RAG for simple questions (saves prompt tokens)
        "gate_by_complexity": "true",
        # Document ingestion (uploaded reference files / pasted text)
        "chunk_size": "1000",                  # max characters per document chunk
        "chunk_overlap": "150",                # overlap characters between chunks
        "max_doc_chars": "2000000",            # safety cap on a single document
        "use_rrf": "true",                     # reciprocal-rank fusion for hybrid search
        "rrf_k": "60",                         # RRF constant (standard default)
        "query_expansion": "true",             # expand query with matched glossary defs
        "entity_linking": "true",              # link question nouns to schema object names
        # Semantic schema cards
        "column_comments": "true",             # index column comments/descriptions
        "enum_max_distinct": "12",             # <= this distinct count => enum doc
        "mask_samples": "false",               # PII-mask sampled values (also honours [ai] mask_pii)
        "table_purpose": "false",              # AI one-line table purpose (needs a backend)
        # Reranking (optional quality tier)
        "rerank": "false",                     # rerank top-N after fusion
        "rerank_top_n": "20",                  # candidates passed to the reranker
        "rerank_model": "cross-encoder/ms-marco-MiniLM-L-6-v2",
        # Approximate-nearest-neighbour store (scale)
        "ann": "false",                        # use FAISS ANN when available
        # Freshness / lifecycle
        "stale_after_days": "7",               # status flags index as stale beyond this
        # Observability
        "log_retrievals": "false",             # append retrieval hits+scores to a log
        # Codebase ingestion limits
        "codebase_max_files": "500",
        "codebase_max_file_bytes": "512000",
    },
    # ── App Builder (bounded DB/codebase profiling) ───────────────────────────
    "ai.app_builder": {
        "db_sample_rows": "10",
        "db_profile_row_cap": "1000",
        "db_max_tables": "25",
        "profile_timeout": "30",
        "use_system_views": "true",
        # Row counts come from system catalogs (approximate, last-analyzed)
        # whenever the engine exposes them — no full-table aggregation. An exact
        # COUNT(*) is only used as a fallback for engines without a catalog stat
        # (e.g. SQLite), and only when db_exact_row_counts is on.
        "db_exact_row_counts": "true",
        # Deep per-column profiling (full-table NULL ratio + distinct counts) is
        # OFF by default: it runs heavy aggregations per column. When off,
        # approximate null/distinct are derived from the sampled rows for free.
        "db_deep_column_profiling": "false",
        # AI interpretation batching: how many tables to interpret per AI Query
        # Assistant call, all within a SINGLE reused session. 10 = batch 10
        # tables/query; 1 = one table per query (still one session); 0 = all
        # tables in a single query.
        "interpret_tables_per_query": "10",
        "codebase_max_files": "400",
        "codebase_max_depth": "4",
        "default_build_profile": "prototype",
        "default_variant": "application",
        # Post-build repair attempts when A/B/C do not agree complete (auto-build).
        "max_finalize_repairs": "2",
        # Host/port the packaged app is served on.
        "host": "127.0.0.1",
        "default_port": "8000",
        # Per-call agent/build subprocess timeout (seconds).
        "agent_timeout": "300",
        # Agentic build round caps: prototype vs full build profiles.
        "max_rounds": "4",
        "full_max_rounds": "6",
        # Build acceptance targets (0..1).
        "target_score": "0.9",
        "target_coverage": "0.9",
        # Total build wall-clock cap (seconds) and stagnation guards.
        "max_wall_clock_seconds": "1800",
        "max_no_progress_rounds": "3",
        "max_validations": "12",
        # Retained build-job TTL (seconds) and max retained jobs.
        "job_ttl_seconds": "3600",
        "max_jobs": "32",
    },
    # ── Local trainable NL->SQL LLM ──────────────────────────────────────────
    "ai.llm": {
        "engine": "pytorch",                   # python | numpy | pytorch | ollama
        "engine_fallback": "python",
        "active_model": "default",             # model used by the local-llm backend
        "context": "0",
        "max_context": "256",                  # cap for numpy/python context window
        "emb_dim": "12",
        "hidden": "48",
        "epochs": "150",
        "batch_size": "32",
        "lr": "0.02",
        "seed": "1234",
        "min_loss": "0.05",
        "log_every": "10",
        "min_freq": "1",
        "max_new_tokens": "512",               # cap on generated SQL tokens (long health SQL)
        "temperature": "0.0",
        "no_repeat_ngram": "2",                 # block short (2-token) repeat loops
        "repetition_penalty": "1.3",
        "top_k": "0",
        # Stage 2 (pytorch nano-GPT) — sized for long multi-subquery SQL
        "pt_block_size": "256",
        "pt_n_layer": "3",
        "pt_n_head": "4",
        "pt_n_embd": "128",
        "pt_dropout": "0.0",
        "pt_batch_size": "16",
        "pt_lr": "0.0003",
        "pt_max_iters": "1000",
        "pt_grad_clip": "1.0",
        "pt_device": "auto",
        # Stage 3 (ollama)
        "ollama_host": "http://localhost:11434",
        "ollama_model": "qwen2.5-coder:1.5b",
        "ollama_timeout": "120",
        # Ollama availability-probe timeout (seconds)
        "ollama_health_timeout": "5",
        # Max NL->SQL example pairs embedded in a generated Modelfile
        "ollama_modelfile_max_pairs": "20",
        # Prefix for the customized Ollama model name
        "ollama_model_prefix": "dbtool-",
        # full = train on collected pairs only; incremental = skip ledger-known
        # questions during harvest and union with the model's saved dataset.
        "train_mode": "full",
    },
    "ai.llm.eval": {
        "depth": "lightweight",                # lightweight | full
        "dev_split": "0.15",
        "execution_accuracy": "true",
        "soft_f1": "true",
        "esm": "true",
        "benchmark_path": "",                  # empty => session/llm/benchmarks/<conn>.jsonl
    },
    # ── Auto-harvest (curated corpus + AI question bank -> validate -> train) ──
    # Runs ONLY under an explicit Train-LLM action. Off by default so live AI
    # Query ask/follow-up behaviour is never altered implicitly.
    "ai.llm.harvest": {
        "enabled": "false",                    # master opt-in for the harvest action
        "use_curated": "true",                 # render the curated seed corpus
        "use_captures": "true",                # replay captured chats/follow-ups
        "use_followups": "true",               # run uninterrupted follow-up threads
        "mine_db": "false",                    # also run the DB miner
        "complexity": "basic,advanced,complex",
        "generated_questions": "40",           # questions the backend invents
        "max_questions": "200",                # cap on backend generation calls
        "sample_limit": "5",                   # row cap for bounded sample queries
        "max_tables": "40",                    # tables considered per connection
        "gen_workers": "4",                    # parallel backend SQL generation
        "gen_timeout": "120",                  # per-question backend timeout (seconds)
        "gen_retries": "1",                    # in-run retries after timeout/error
        "retry_backlog": "true",               # persist failed questions for next run
        "max_consecutive_failures": "0",       # 0 = disabled circuit breaker
    },
    # ── Scheduled RAG re-index (incremental; refresh stale/drifted indexes) ──
    "ai.rag.reindex.schedule": {
        "enabled": "false",                    # master opt-in for scheduled re-index
        "start_time": "02:00",                 # daily window start (HH:MM)
        "duration_hours": "1",                 # window length; run happens once inside it
        "window_end": "",                      # legacy; honored only if duration unset
        "connections": "",                     # comma list; blank = all indexed connections
        "force": "false",                      # re-index regardless of staleness/drift
    },
}

_cfg = ModuleIniConfig(_DIR, defaults=DEFAULTS)

get = _cfg.get
get_int = _cfg.get_int
get_float = _cfg.get_float
get_bool = _cfg.get_bool
set_value = _cfg.set_value
restore_defaults = _cfg.restore_defaults
reload = _cfg.reload
config_path = _cfg.config_path
live_path = _cfg.live_path
