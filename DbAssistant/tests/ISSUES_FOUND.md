# Test Suite Report — DbManagementTool

**Generated:** 2026-05-26  
**Scope:** `tests/` only (no production code modified)  
**Logs:** `tests/_last_run.log` (full), `tests/_last_unit_run.log` (`-m "not integration"`)

## Executive summary

| Run | Command | Result | Duration |
|-----|---------|--------|----------|
| Full | `pytest tests/ -v --maxfail=0 -p no:cacheprovider` | **481 passed, 3 skipped**, 23 warnings | ~15.5s |
| Unit | `pytest tests/ -v --maxfail=0 -m "not integration"` | **419 passed, 1 skipped**, 64 deselected | ~6.8s |

All tests pass after fixing one test-harness bug (see §1). No production regressions were introduced by the test work.

---

## 1. Test failures (resolved)

| Test | Symptom | Root cause | Category |
|------|---------|------------|----------|
| `test_get_db_metrics_gating.py::test_ping_skipped_when_recent_sql_ok` | `get_db_metrics` returned `None`; `not enough values to unpack (expected 2, got 0)` | `db.execute_query` was not mocked to return `(result, error)`; `_locked_execute` unpacked a bare `MagicMock` | **Test bug** (fixed: `MagicMock(return_value=({"rows": [[1]]}, None))`) |

No remaining test failures.

---

## 2. Skipped tests

| Location | Reason | Intentional? | Action |
|----------|--------|--------------|--------|
| `tests/test_drivers_ping_reconnect.py` (module) | `pytest.importorskip("cx_Oracle")` — Oracle driver not installed | Yes | Install `oracledb`/`cx_Oracle` in CI image to exercise Oracle ping/reconnect |
| `tests/test_cloud_providers_aws.py::test_aws_live_caller_identity` | `aws_available` fixture — no `~/.aws` creds on this host | Yes | Passes when AWS default chain works |
| `tests/test_mysql_integration.py::test_mysql_integration_connect_and_basic_ops` | `MYSQL_CONN` env var not set | Yes | Set `MYSQL_CONN=mysql://dheeru:dheeru@localhost:3306/test` to enable |

**Note:** `test_headless_api.py` was skipped when `fastapi` was missing from `.venv`; installing `fastapi` removed that skip (2 API tests now run).

Integration tests in `test_full_suite.py` / `test_additional_suite.py` **ran successfully** against local MySQL (`localhost:3306`, db `test`, user `dheeru`).

---

## 3. Production bugs / unsafe behavior (surfaced by tests)

### 3.1 DB liveness false-positive (design issue — mitigated in production)

**File:** `db_metric_config.collect_metrics`  
**Behavior:** When every SQL metric spec fails, `raw_floats` can still be non-empty from host/psutil metrics.  
**Risk:** Gating on `len(raw_floats) > 0` would treat a dead DB connection as “alive.”  
**Mitigation in production:** `get_db_metrics` uses `sql_ok_count` (successful `execute_query` calls), not `raw_floats`.  
**Tests:** `test_db_metric_config.py::test_collect_metrics_psutil_only_no_sql_ok`, `test_get_db_metrics_gating.py`

### 3.2 SSH ControlPath collision (multi-instance)

**File:** `server_monitor/server_monitor_ui.py`  
**Pattern:** `ControlPath={tempdir}/ssh_monitor_{conn_name}`  
**Risk:** Two GUI instances monitoring the same logical connection name share one control socket → cross-talk, surprise disconnects, or auth confusion.  
**Tests:** `test_server_monitor_ui_helpers.py` asserts command shape only; no multi-process test yet.

### 3.3 `datetime.utcnow()` deprecation

**Files:** `server_monitor_ui.py` (`_seconds_until_expiry`), `cloud_providers/gcp_provider.py`  
**Behavior:** Naive UTC comparisons work today but emit `DeprecationWarning` on Python 3.12+.  
**Tests:** `test_liveness_gating.py`, GCP refresh tests trigger warnings.  
**Recommendation:** Migrate to `datetime.now(datetime.UTC)` and timezone-aware credential expiry.

### 3.4 Cloud reconnect threshold

**File:** `server_monitor_ui.py` (~6742)  
**Behavior:** `"Reconnect required"` after `fails >= 3` consecutive keepalive failures.  
**Tests:** `test_ssh_cloud_gate.py` validates counter logic in isolation; full `_cloud_keepalive_loop` thread not driven end-to-end.

### 3.5 Stale test assumptions (documentation / API drift)

| Old assumption | Actual API | Fixed in tests |
|----------------|------------|----------------|
| `ThresholdRule.threshold` | `critical` / `warning` / `info` | `test_full_suite.py` |
| `alert.level` | `alert.severity` | `test_full_suite.py` |
| `monitoring_utils._sustained_store` | `_store` | `test_monitoring_utils.py`, `test_additional_suite.py` |
| MySQL db `pushdb` | db `test` per plan | `test_full_suite.py`, `test_additional_suite.py`, `conftest.py` |
| Teams webhook assert on stdout | notification goes to stderr | `test_additional_suite.py` |

These were **test/documentation drift**, not runtime bugs, but they hid real API contracts.

---

## 4. Coverage gaps (branches not exercised without prod changes or heavy infra)

| Area | Gap | Why |
|------|-----|-----|
| **Tk `ServerMonitorUI`** | Widget layout, event loop, dialogs | Out of scope; only static helpers tested via `liveness_ui` / bound methods |
| **Oracle driver** | `pingOracle` / `reconnectOracle` | Module skipped without `cx_Oracle` |
| **Postgres live** | No live Postgres instance | Mocked in `test_drivers_ping_reconnect.py` |
| **Azure live** | Mocked SDK only | By design |
| **`_update_monitor_metrics_thread`** | Full SSH subprocess branches | Partially covered via helper/state tests; not full thread integration |
| **`_cloud_keepalive_loop`** | Background thread + timer | Counter/status logic only |
| **`fetch_metrics` per cloud** | Deep CloudWatch / Monitor / GCP time-series | Mocked smoke shapes; not full metric catalog |
| **Concurrent `get_db_metrics`** | Per-DB lock under real threads | `_locked_execute` counter tested lightly; no stress test |
| **Multi-process daemon/GUI** | PID file + port + SSH path collisions | Daemon PID tests mocked; no two-process GUI test |
| **schema_converter / DataConverter** | Unknown types, NULL roundtrip edge cases | Partially in `test_additional_suite.py`; not exhaustive |
| **fcntl lock contention** | Two writers corrupting JSON | Connection-manager tests mock `fcntl`; no true parallel writers |

---

## 5. Performance smells

| Observation | Detail |
|-------------|--------|
| Full suite ~15.5s | Acceptable; dominated by MySQL integration (~62 tests) and CLI subprocess `--help` calls |
| No test > 2s individually | Nothing flagged as a microbenchmark violation |
| Liveness helpers | Table-driven unit tests complete in &lt;1ms each (negligible overhead confirmed) |
| `test_dbtool_cli.py` | Spawns `python -m dbtool` per subcommand — could be parametrized/single-session if suite grows |

---

## 6. Concurrency / ordering

| Item | Status |
|------|--------|
| `_locked_execute` / `sql_ok_count` under wrapper | Covered in `test_get_db_metrics_gating.py` with mocked DB |
| `fcntl.flock` on connection JSON | Encrypt/decrypt round-trip tested; parallel save not stress-tested |
| Sustained breach window | `test_threshold_checker_advanced.py`, `test_monitoring_utils.py` |
| Flaky tests observed | **None** in final runs |

---

## 7. Multi-instance issues

| Component | Issue | Test coverage |
|-----------|-------|---------------|
| SSH `ControlPath` | Shared temp path per connection **name** | Documented only |
| Headless daemon PID file | Stale PID cleanup, double-start | `test_headless_daemon.py` (mocked `os.kill`) |
| Cloud/monitor connection JSON | `fcntl` serializes writes | Unit tests, not multi-process |
| GUI config writes | Not tested | Gap |

---

## 8. Test harness / environment notes

- **Deps used:** `pytest`, `httpx`, `fastapi` (for `test_headless_api.py`). Consider adding to `requirements-dev.txt`.
- **`drivers/` on `sys.path`:** `conftest.py` inserts project root + `drivers/` (fixes `from drivers import conMysql` vs package layout).
- **GCP live:** `test_gcp_adc_credentials_load` passes when `~/.config/gcloud/application_default_credentials.json` exists.
- **Warnings (23):** Mostly `datetime.utcnow()` deprecation from production + test code paths.

---

## 9. Files added or updated

### New modules

`conftest.py`, `test_liveness_gating.py`, `test_server_monitor_ui_helpers.py`, `test_drivers_ping_reconnect.py`, `test_db_manager.py`, `test_db_metric_config.py`, `test_db_os_collector.py`, `test_metrics_visualizer.py`, `test_cloud_providers_{aws,azure,gcp}.py`, `test_cloud_provider_registry.py`, `test_cloud_connection_manager.py`, `test_monitor_{aws,azure,gcp}.py`, `test_monitor_connection_manager.py`, `test_threshold_checker_advanced.py`, `test_ai_query_agent.py`, `test_headless_{daemon,db_service,api}.py`, `test_dbtool_cli.py`, `test_stop_py.py`, `test_ssh_cloud_gate.py`, `test_get_db_metrics_gating.py`

### Updated legacy suites

`test_full_suite.py`, `test_additional_suite.py`, `test_conmysql_unit.py`, `test_mysql_integration.py`, `test_monitoring_utils.py`

---

## 10. Recommended follow-up (production agent mode)

1. Replace `datetime.utcnow()` with timezone-aware UTC in expiry helpers.
2. Namespace SSH `ControlPath` with PID or instance id for multi-GUI safety.
3. Add `requirements-dev.txt` with `pytest`, `httpx`, `fastapi`, optional `oracledb`.
4. Optional: thread-level test for `_cloud_keepalive_loop` with injected clock.
5. Optional: multi-process test for SSH ControlPath isolation.

---

*This report is input for a follow-up session that may modify production code. No production files were changed during test authoring.*
