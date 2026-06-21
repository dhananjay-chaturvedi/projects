# TEST TYPE REFERENCE

> **Read this file FIRST, before starting ANY testing task.**
> It is the single source of truth for *what* "testing" means in this repo,
> *which scope* to run, *which test types* to cover, and *how* to report.
>
> When the user says **"full testing / test all modules and all functionalities"**,
> execute **Scope A** below end-to-end — nothing less.

---

## 0. How to use this document

1. Identify the **scope** the user asked for (see §3):
   - "full test / all modules / all functionality / leave no stone unturned" → **Scope A — FULL**
   - "test the X module" → **Scope B — MODULE-WISE**
   - "test what we changed / test this fix" → **Scope C — CHANGES-ONLY**
2. Run **every test type** in §2 that applies to that scope.
3. Exercise **every surface** in §4 (CLI, API, UI, shell-UI).
4. Use **real connections + real data** (§1) — never only mocks for a full test.
5. Capture output, compare against expected, and report using the format in §6.
6. Ignore the **known pre-existing failures** in §7 (don't re-flag them as new).
7. If you find a real bug: fix it **and** add a regression test (§5 lists examples).

---

## 1. Environment, real connections & real data

| Item | Value |
|---|---|
| venv | `source .venv/bin/activate` (run all commands from repo root) |
| Core DB connection | `local_mariadb` (MariaDB @ localhost:3306, user `dheeru`) |
| Cloud connection | `my_gcp_postgres` (GCP PostgreSQL) |
| Test database | `test` (tables: `PRODUCTS`, `DEPARTMENTS`, `EMPLOYEES`) |
| Monitor-only DB conns | `local_mariadb_run_monitor` (db=`test`), `local_mariadb_monitor` |
| conftest live creds | host=localhost port=3306 user=`dheeru` pass=`dheeru` db=`test` (override via `MYSQL_TEST_*` env) |

**Rules for real data:**
- Use existing saved connections for read/verify paths.
- **Do NOT auto-create new credentialed connections** (auto-review blocks this).
  If a test needs a different default DB, pass it via SQL (`USE`/`information_schema`)
  or use the existing monitor-db profiles, or ask the user to authorize seeding.
- Live AI / cloud / SSH tests may **skip** when the backend/credential is absent —
  a skip is expected, not a failure.

---

## 2. Test types (cover ALL of these in a full test)

| # | Type | What it means here | Example |
|---|---|---|---|
| 1 | **Positive** | Valid input → expected success/output | `query --sql "SELECT 1"` returns `1` |
| 2 | **Negative** | Invalid input → graceful error, no crash | `connections test no_such_conn` → clear error |
| 3 | **False-positive guard** | Make sure we DON'T alert/flag when we shouldn't | cumulative byte counters must not fire CRITICAL |
| 4 | **False-negative guard** | Make sure we DO alert/flag when we should | cpu=99 vs threshold 90 → CRITICAL fires |
| 5 | **Regression** | Old fixed bugs stay fixed | `monitor-config` dispatch, OpenAPI generation |
| 6 | **Smoke** | App imports, launches, basic command runs | `dbtool.py --help`, UI constructs |
| 7 | **Integration / live** | Real DB / cloud / SSH round-trip | metrics from `local_mariadb`, `my_gcp_postgres` |
| 8 | **Boundary / validation** | Empty body, missing args, bad types → 4xx/422 | `POST /api/query` with no `connection` |
| 9 | **Idempotency / isolation** | Monitor-db store isolated from core; repeat-safe | `test_monitor_db_isolation.py` |
| 10 | **Performance / lightweight** | DB metric queries are cheap/fast, no heavy scans | engine system-view metrics only |

---

## 3. Scopes

### Scope A — FULL TEST (all modules, all functionalities)
Run **everything** below, in order. This is the canonical "full test".

```bash
source .venv/bin/activate

# A1. Full automated suite (unit + integration + matrices).
#     Exclude the Tk-fork test that aborts a headless pytest process.
python -m pytest tests/ -q -p no:cacheprovider --ignore=tests/test_sql_editor_pane.py

# A2. Exhaustive CLI sweep (every subcommand/action, real connections).
python tests/_exhaustive_cli.py

# A3. Exhaustive API sweep (every route family, positive+negative, in-process).
PYTHONPATH=. python tests/_exhaustive_api.py

# A4. Exhaustive UI construction (full + each standalone module; builds every screen).
for m in full migrator ai monitor; do python tests/_exhaustive_ui.py "$m"; done

# A5. Live integration / module runners (skip cleanly when creds absent).
python tests/run_live_integration.py
python tests/run_monitoring_live_full.py        # monitoring, live
python tests/run_sql_modes_live_report.py       # AI SQL modes, live (optional)
```

**Expected baseline (record actuals each run):**
- A1: ~1070 passed, ~20 skipped, **3 pre-existing failures only** (see §7).
- A2: all PASS except intentional negative cases.
- A3: 60/60 calls return an expected status (200/201 or expected 4xx).
- A4: `UI_OK` for all 4 modes; all tabs construct + switch.

A full test MUST also cover, per module, every item in §4 surfaces and §2 types.

### Scope B — MODULE-WISE TEST
Run the slice of Scope A for one module + shared core. Each module = its own
business surface **plus** the shared core tabs (Connections, Objects, SQL Editor).

| Module | Package | pytest selector | CLI entry | UI mode |
|---|---|---|---|---|
| **Core** (Connections, DB Objects, SQL Editor, Dashboard, Settings) | `common/` | `-k "dbtool or headless or db_service or dashboard or connection or objects"` | `dbtool.py connections/query/objects/...` | `full` (core tabs) |
| **Data Migration** | `schema_converter` | `-k "schema or migrator or comprehensive_schema"` | `python -m schema_converter migrator ...` | `migrator` |
| **AI Query Assistant** | `ai_query` | `-k "ai_ or sql_mode or sessions or pii or response_parser"` | `python -m ai_query ai ...` | `ai` |
| **Monitoring** | `monitoring` | `-k "monitor or threshold or cloud or daemon or alerts or db_metric or os_"` | `python -m monitoring monitor/cloud/thresholds/...` | `monitor` |

Module-wise template:
```bash
python -m pytest tests/ -q -k "<selector>" --ignore=tests/test_sql_editor_pane.py
# + the relevant CLI block from tests/_exhaustive_cli.py
# + relevant API routes from tests/_exhaustive_api.py
# + python tests/_exhaustive_ui.py <ui mode>
```

### Scope C — CHANGES-ONLY TEST
Fastest loop: test only what was just modified.
1. Run the **new/affected regression tests** for the change.
2. Run the pytest files that import the changed modules (`-k` on the area).
3. Re-run the matching exhaustive slice (CLI/API/UI) for the touched surface.
4. Always finish with a **smoke** (`dbtool.py --help`, target UI mode constructs).

Example (the monitor-config + OpenAPI fixes from this session):
```bash
python -m pytest tests/test_standalone_command_parity.py tests/test_openapi_schema_generation.py -q
python -m monitoring monitor-config show          # was the bug
PYTHONPATH=. python -c "from fastapi.testclient import TestClient; \
from common.headless.app_factory import create_app; \
print(TestClient(create_app()).get('/openapi.json').status_code)"   # expect 200
```

---

## 4. Surfaces (exercise every one in a full/module test)

1. **CLI** — `dbtool.py` (full tool) AND `python -m <module>` (standalone).
   - Bug class to watch: a command registered in the parser but missing from the
     module's `_*_COMMANDS` set in `__main__.py` → misroutes to top-level help.
     Guarded by `tests/test_standalone_command_parity.py`.
2. **REST API** — `common.headless.app_factory.create_app()` (composite) and
   per-module `create_app(module_key=...)`. Hit `/openapi.json`, `/docs`, `/redoc`
   too (Pydantic request models must be module-level, never local classes).
     Guarded by `tests/test_openapi_schema_generation.py`.
3. **Desktop UI (Tkinter)** — construct `UnifiedDBManagerUI` headless in a
   **subprocess** (Tk aborts inside a forked pytest worker, but works in a plain
   subprocess). Pump the event loop, switch every notebook tab. Use `os._exit`
   on teardown to avoid hanging on the monitoring poll thread.
   Interaction coverage lives in `tests/test_ui_widget_interactions.py`.
4. **Shell UI** — `--shell-ui` / `--lite-ui` bash menus (smoke: launches, no tkinter).

---

## 5. Standing regression tests (keep these green)

| Area | Test |
|---|---|
| CLI command routing parity (all 3 modules) | `tests/test_standalone_command_parity.py` |
| OpenAPI / docs generation (composite + each module) | `tests/test_openapi_schema_generation.py` |
| Monitor-db store isolation | `tests/test_monitor_db_isolation.py` |
| DB metric config (OS/DB separation, db_type paths) | `tests/test_db_metric_config.py` |
| Engine-specific threshold paths + fallback | `tests/test_threshold_db_engine_path.py` |
| DB host resolution across stores | `tests/test_get_db_host_resolution.py` |
| Live DB metric refactor (MariaDB + GCP) | `tests/test_live_db_metrics_refactor.py` |
| Master UI widget interactions | `tests/test_ui_widget_interactions.py` |
| Destructive/state-changing API endpoints | `tests/test_destructive_api_endpoints.py` |
| Comprehensive CLI / API / schema matrices | `tests/test_comprehensive_*.py` |

**Reusable exhaustive harnesses** (not collected by pytest; `_`-prefixed):
`tests/_exhaustive_cli.py`, `tests/_exhaustive_api.py`, `tests/_exhaustive_ui.py`.

---

## 6. Reporting format (what to hand back to the user)

For every scope run, report a per-module table:

| Module | Surface | Input (real) | Expected | Actual | Verdict |
|---|---|---|---|---|---|

Plus:
- Headline counts (passed / skipped / failed) for the pytest run.
- Any **new** bug found → root cause, fix, and the regression test added.
- Explicit list of **skips** with the reason (missing cred/service is OK).
- Re-confirm the §7 pre-existing failures are unchanged (not regressions).

---

## 7. Known PRE-EXISTING failures (do NOT report as new regressions)

These fail due to the local environment/config, not product code. Confirm they
are unchanged; only investigate if the count or identity changes.

| Test | Root cause | Current status |
|---|---|---|
| `test_additional_suite.py::...::test_get_compare_sample_size_reads_properties` | `common.config_loader` compatibility gap between legacy `properties.ini` and module config | Fixed; must pass |
| `test_additional_suite.py::TestMariaDBIntegration::test_get_users` | DB user `dheeru` previously lacked `SELECT` on `mysql.user` | Fixed after grant; must pass |
| `test_full_suite.py::TestMySQLIntegration::test_get_users_includes_dheeru` | same grant issue | Fixed after grant; must pass |

Also: `tests/test_sql_editor_pane.py` aborts a headless pytest process (Tk in a
forked worker). Exclude with `--ignore` in headless runs; exercise the SQL Editor
screen via the UI subprocess harness (Scope A4) instead.

---

## 8. Gaps / missing tests to add over time (TODO backlog)

These extend coverage toward truly exhaustive. Add as opportunity allows:

- [x] Widget-interaction tests for the master UI shell and high-value callbacks
      (`tests/test_ui_widget_interactions.py`). Remaining work: deeper
      per-screen domain actions such as run SQL from SQL Editor, save a
      connection dialog, migration preview, and monitor start/stop.
- [ ] CLI matrix for every `--format {table,json,csv}` on every list/show command.
- [ ] API negative-auth / malformed-payload coverage for every POST/PUT/DELETE
      (not just the representative ones in `_exhaustive_api.py`).
- [x] Destructive endpoints against throwaway/seeded targets
      (`tests/test_destructive_api_endpoints.py`) for connection delete,
      dashboard save/reset, clear caches, query commit/rollback, object
      import/export, monitor connection add/update/delete, monitor-db add/delete,
      and alert log/clear. Remaining work: config restore endpoints with
      isolated module config snapshots.
- [ ] Object types per engine end-to-end against a connection with a default DB
      set (tables/views/procs/functions/indexes/triggers/sequences/constraints).
- [ ] Data Migration full round-trip: convert → apply → transfer-data →
      compare-schema → compare-data between two real connections.
      *(Unicode/type-map live script: `tests/run_migration_unicode_live.py`)*
- [ ] Type override unit tests — `tests/test_type_overrides.py`
- [ ] Charset transfer unit tests — `tests/test_charset_transfer.py`
- [ ] Document adapter tests — `tests/test_document_adapters.py`
- [ ] Transfer options / per-value policies (G1-G7) — `tests/test_transfer_options.py`
- [ ] Migration report + checkpoint (G9/G10) — `tests/test_migration_report.py`
- [ ] Continue-on-error, sequence reset, column limits, dry-run (G3/G4/G5/G8) — `tests/test_migration_gaps.py`
- [ ] Transfer options UI/CLI/API parity + dry-run route — `tests/test_migration_surfaces.py`
- [ ] AI Query Assistant live-backend tests for every SQL mode
      (strict_summary / summary / open) — currently skip without a backend.
- [ ] Monitoring: live SSH OS metrics from a real remote Monitor target.
- [ ] Per-engine DB metric coverage for Oracle / PostgreSQL / SQLite
      (MariaDB/MySQL is covered live; others need a reachable instance).

---

*Last validated: focused follow-up — previous 3 failures now pass; UI widget
interaction tests pass; destructive API endpoint tests pass. Last full Scope A
run before these fixes: 1070 passed / 20 skipped / 3 failures (now resolved);
CLI, API (60/60), and UI (4/4 modes) sweeps green; 2 real bugs found & fixed
(`monitor-config` dispatch, OpenAPI `_CfgSet`).*
