# Configuration Reference

DbAssistant uses a **layered, module-owned** configuration model. The shared
core owns `config.ini` and `properties.ini`; every feature module ships and owns
its own `*.ini`. Each file has a committed, annotated `*.example` that is the
**source of truth** — the tool reads the example directly until you save an edit,
at which point a live file is created from it.

Every setting is editable through **all surfaces**:

```bash
python dbtool.py config show                       # core
python dbtool.py config set <section> <key> <val>
python dbtool.py ai config set <section> <key> <val>
python dbtool.py migrator config set <section> <key> <val>
python dbtool.py monitor-config set <section> <key> <val>
```

…and via the matching settings panel in each UI and the `/api/.../config`
routes.

| File | Example | Owner |
|------|---------|-------|
| `config.ini` | `common/config/config.ini.example` | Core (engines, ports, timeouts) |
| `properties.ini` | `common/config/properties.ini.example` | Core (UI look & feel) |
| `schema_converter/config.ini` | `schema_converter/config.ini.example` | Data Migration |
| `ai_query/config.ini` | `ai_query/config.ini.example` | AI Query / RAG / LLM / App Builder |
| `monitoring/monitor_config.ini` | `monitoring/monitor_config.ini.example` | Monitoring |
| `monitoring/monitor_thresholds.ini` | shipped defaults | Monitoring thresholds |

---

## 1. Core — `config.ini`

```ini
[paths]
oracle_client_path =            ; blank = oracledb thin mode; set for thick/11g

[database.ports]
oracle = 1521
mysql = 3306
postgresql = 5432
sqlserver = 1433
mongodb = 27017

[database.connection]
connection_timeout = 30.0
query_timeout = 0               ; 0 = no client-side limit
default_autocommit = true
max_connection_attempts = 8
```

## 2. Core UI — `properties.ini`

```ini
[ui.window]
main_window_width = 1150
main_window_height = 780

[ui.colors.primary]
primary = #2196F3
primary_dark = #1976D2

[logging]
enable_stdout = false           ; suppress console chatter
```

---

## 3. Data Migration — `schema_converter/config.ini`

| Section / key | Default | Purpose |
|---------------|---------|---------|
| `[schema.conversion] compare_sample_size` | `10` | Rows compared per table in sample compare mode. |
| `zero_date_strategy` | `quote` | MySQL/MariaDB zero-date handling: `quote`/`null`/`omit`. |
| `parallel_workers` | `2` | Parallel table transfers (`1` = serial). |
| `type_overrides` | _(blank)_ | Default type rules, e.g. `varchar2:text, int:decimal`. |
| `conversion_charset` | `utf-8` | Charset for cross-engine text transfer. |
| `overflow_policy` | `fail` | Oversized value: `fail`/`truncate`/`skip`. |
| `null_policy` | `keep` | `keep`/`empty_to_null`/`null_to_empty`. |
| `bool_policy` | `auto` | Boolean normalization: `auto`/`int`/`true_false`. |
| `timezone_policy` | `preserve` | `preserve`/`naive`/`utc`/`target`. |
| `target_timezone` | _(blank)_ | Used when `timezone_policy = target`. |
| `continue_on_error` | `false` | Keep transferring after a bad row. |
| `reset_sequences` | `false` | Reset target sequences after load. |
| `transfer_error_limit` | `1000` | Max per-table errors retained. |
| `max_compare_mismatches` | `20` | Max mismatches reported by data compare. |
| `[schema.runtime] checkpoint_dir` | `dbtool_migrate_checkpoints` | Checkpoint dir (under temp). |

---

## 4. AI Query / RAG / LLM / App Builder — `ai_query/config.ini`

This is the largest config. Highlights below; see `ai_query/config.ini.example`
for the fully annotated file and [RAG.md](RAG.md) for the RAG keys.

### Backends — `[ai]`, `[ai.claude]`, `[ai.cursor]`, `[ai.codex]`

| Key | Default | Purpose |
|-----|---------|---------|
| `[ai] default_backend` | `auto` | `auto`/`claude`/`cursor`/`codex`. |
| `[ai] fallback_backend` | _(blank)_ | Failover + SQL corrector (name or `local-llm::<model>`). |
| `[ai] mask_pii` | `true` | Mask secrets in text sent to backends. |
| `[ai] default_backend_timeout` | `120` | Subprocess timeout (s). |
| `[ai] cli_search_paths` | _(list)_ | Extra dirs to resolve backend CLIs. |
| `[ai.claude] cli_path` / `[ai.codex] cli_path` | _(blank)_ | Explicit binary path override. |

### Schema cache — `[ai.cache]`

`max_tables_fetch`, `max_tables_detailed`, `ttl_seconds` (0 = never expire),
`schema_drift_check` and related display caps.

### Prompt shaping — `[ai.prompt]`, behavior — `[ui.ai_query]`, limits — `[ai.limits]`

| Key | Default | Purpose |
|-----|---------|---------|
| `[ui.ai_query] auto_execute_ai_loop` | `false` | Auto-run follow-up loop until satisfied. |
| `[ui.ai_query] auto_execute_summary_sql` | `false` | Auto-run generated summary SQL. |
| `[ui.ai_query] auto_loop_max_iterations` | `5` | Cap on the auto loop. |
| `[ui.ai_query] default_sql_mode` | `summary` | `strict_summary`/`summary`/`open`. |
| `[ui.ai_query] sql_review_timeout` | `60` | "Run Review" timeout (s). |
| `[ai.limits] max_stored_sessions` | `0` | Sessions kept on save (0 = all). |
| `[ai.limits] max_history_on_disk` | `20` | Messages persisted per session. |

### RAG — `[ai.rag]`

See the full table in [RAG.md → Configuration reference](RAG.md#configuration-reference).
Key toggles: `embedding_provider`, `use_rrf`, `query_expansion`, `entity_linking`,
`column_comments`, `enum_max_distinct`, `mask_samples`, `table_purpose`, `rerank`,
`ann`, `stale_after_days`, `log_retrievals`, `top_k`.

### RAG re-index scheduler — `[ai.rag.reindex.schedule]`

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `false` | Opt in to scheduled incremental re-index. |
| `start_time` | `02:00` | Daily window start (HH:MM). |
| `duration_hours` | `1` | Window length; the run fires once inside it. |
| `window_end` | _(blank)_ | Legacy; honored only if `duration_hours` unset. |
| `connections` | _(blank)_ | Comma list; blank = every indexed connection. |
| `force` | `false` | Re-index regardless of staleness/drift. |

### Local LLM — `[ai.llm]`, `[ai.llm.eval]`, `[ai.llm.harvest]`, `[ai.llm.capacity]`, `[ai.llm.versions]`

| Key | Default | Purpose |
|-----|---------|---------|
| `[ai.llm] engine` | `pytorch` | `python`/`numpy`/`pytorch`/`ollama`. |
| `[ai.llm] engine_fallback` | `python` | Fallback engine. |
| `[ai.llm] active_model` | `default` | Model for the "Local LLM (trained)" backend. |
| `[ai.llm] auto_fix_train` | `false` | Train on verified fallback corrections. |
| `[ai.llm] template_mode` | `both` | `concrete`/`placeholder`/`both`. |
| `[ai.llm.harvest] enabled` | `false` | Enable curated+AI corpus harvest. |
| `[ai.llm.harvest] training_depth` | `offline` | `offline` (no AI) / `online`. |
| `[ai.llm.versions] keep` | `5` | Model snapshots retained (0 = unlimited). |

### LLM harvest scheduler — `[ai.llm.harvest.schedule]`

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `false` | Opt in to the nightly harvest+train. |
| `start_time` | `01:00` | Daily window start. |
| `duration_hours` | `4` | Graceful duration cap. |
| `connection` / `connections` | _(blank)_ | Single or comma-list of connections. |
| `train_mode` | `incremental` | `incremental`/`full`. |
| `training_depth` | `offline` | `offline`/`online`. |

### App Builder — `[ai.app_builder]`

Bounded DB profiling + codebase scanning and build-job governance:
`db_sample_rows`, `db_max_tables`, `default_build_profile`
(`prototype`/`full`), `default_variant` (`application`/`explorer`),
`max_rounds`, `target_score`, `target_coverage`, `max_wall_clock_seconds`,
`job_ttl_seconds`, `host`/`default_port`. See the annotated example for all keys.

---

## 5. Monitoring — `monitoring/monitor_config.ini`

Sections (see the annotated example for every key):

| Section | Covers |
|---------|--------|
| `[monitoring]` | Refresh cadence, keepalive intervals, poll defaults, disk path, sustained-breach TTLs. |
| `[ssh.connection]` | SSH timeouts, `strict_host_key`, default port, control persist. |
| `[monitoring.graphs]` | Graph dimensions. |
| `[monitoring.limits]` | Graph points, alert listing limits. |
| `[cloud.lookback]` | Per-provider metric lookback windows (minutes). |
| `[cloud.aws]` / `[cloud.azure]` / `[cloud.gcp]` | Region, login timeouts, metric periods, PI breakdown, log limits. |
| `[notifications]` | Teams/email delivery, severity, retries. **Secrets are encrypted under `~/.dbassistant`, never in this file.** |

### Thresholds — `monitoring/monitor_thresholds.ini`

Threshold rules (warn/critical bounds per metric) live here and are managed via
`dbtool thresholds …`, the Monitor settings UI, or `/api/thresholds`.

---

## Environment variables

| Variable | Effect |
|----------|--------|
| `DBASSISTANT_HOME` | Override the data directory (default `~/.dbassistant`). Essential for tests and isolated installs. |
| `DBTOOL_API_KEY` | Convenience var used in docs/examples for the REST `X-API-Key` header. |

---

## Where live config & data live

```
project/                         ~/.dbassistant/
  config.ini                       keys/         (encryption keys, chmod 600)
  properties.ini                   connections/  (encrypted profiles)
  schema_converter/config.ini      runtime/      (daemon PID, metrics.json, logs)
  ai_query/config.ini              session/      (AI sessions, RAG index, layout)
  monitoring/monitor_config.ini    version
```

See also: [RAG.md](RAG.md), [HOW_TO_USE.md](../HOW_TO_USE.md), and each module's
annotated `*.ini.example`.
