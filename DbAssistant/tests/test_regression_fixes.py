"""Regression tests for bugs fixed in the security/data-integrity/threading audit.

Each test class documents which bug it covers so future regressions are easy
to trace back to the original issue.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Bug #1 — session_manager: concurrent write_sessions_file used fixed .tmp name
# ---------------------------------------------------------------------------

class TestWriteSessionsFileRace:
    def test_unique_tmp_names(self, tmp_path):
        """Two concurrent writes must not clobber each other's .tmp file."""
        from ai_query.session_manager import write_sessions_file

        path = tmp_path / "sessions.json"
        records_a = [{"session_id": "a", "x": 1}]
        records_b = [{"session_id": "b", "x": 2}]

        errors = []
        def write(recs):
            try:
                write_sessions_file(recs, path)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=write, args=(records_a,))
        t2 = threading.Thread(target=write, args=(records_b,))
        t1.start(); t2.start()
        t1.join(); t2.join()

        assert not errors
        data = json.loads(path.read_text())
        assert isinstance(data, list)
        assert len(data) == 1  # last writer wins; file is valid JSON

    def test_no_stale_tmp_files(self, tmp_path):
        """No .tmp files should be left after a successful write."""
        from ai_query.session_manager import write_sessions_file

        path = tmp_path / "sessions.json"
        write_sessions_file([{"session_id": "x"}], path)

        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == [], f"Stale .tmp files: {tmp_files}"


# ---------------------------------------------------------------------------
# Bug #6 — agent.py: _gen_err captured AFTER parsed_result["error"] = None
# (verified indirectly via the capture pipeline and response shape)
# ---------------------------------------------------------------------------

class TestResponseParserFallback:
    """Bug #7 — response_parser: empty SUMMARY_SQL body used raw response as SQL."""

    def test_legacy_no_sql_header_returns_none_sql(self):
        """If the legacy fallback has no SQL: header, summary_sql must be None."""
        from ai_query.response_parser import parse_structured_ai_response

        # Pure prose response, no SQL header, no structured sections.
        prose = "I'm sorry, I don't have enough information to answer that."
        result = parse_structured_ai_response(prose)
        assert result["summary_sql"] is None, (
            "Prose response should never become SQL"
        )

    def test_legacy_with_sql_header_extracts_sql(self):
        """Legacy SQL: header in the fallback path must still work."""
        from ai_query.response_parser import parse_structured_ai_response

        legacy = "SQL:\nSELECT 1;\n\nEXPLANATION:\nA simple query.\n"
        result = parse_structured_ai_response(legacy)
        assert result["summary_sql"] == "SELECT 1;"

    def test_explanation_preserved_when_no_sql(self):
        """When no SQL header matches, prose becomes the explanation, not the SQL."""
        from ai_query.response_parser import parse_structured_ai_response

        prose = "This question cannot be answered with a single SQL query."
        result = parse_structured_ai_response(prose)
        assert result["summary_sql"] is None
        assert result["explanation"] is not None
        assert "cannot" in result["explanation"].lower()


# ---------------------------------------------------------------------------
# Bug #2 — capture store: concurrent JSONL appends (fcntl lock)
# ---------------------------------------------------------------------------

class TestCaptureStoreConcurrentWrite:
    def test_concurrent_appends_produce_valid_jsonl(self, tmp_path):
        """N threads appending simultaneously must produce N valid JSONL lines."""
        from ai_assistant.capture.record import CaptureRecord
        from ai_assistant.capture.store import IsolatedCaptureStore

        store = IsolatedCaptureStore(tmp_path / "capture")
        n = 20
        errors = []

        def do_append(i):
            rec = CaptureRecord(
                project_id="proj",
                connection_name="conn",
                database="db",
                question=f"q{i}",
                sql=f"SELECT {i}",
                raw_response=f"raw{i}",
            )
            try:
                store.append(rec)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=do_append, args=(i,)) for i in range(n)]
        for t in threads: t.start()
        for t in threads: t.join()

        assert not errors
        records = list(store.iter_records("proj"))
        assert len(records) == n


# ---------------------------------------------------------------------------
# Bug #3 — capture store: corrupt JSONL line aborts iter_records
# ---------------------------------------------------------------------------

class TestCaptureStoreCorruptLine:
    def test_corrupt_line_is_skipped_not_aborted(self, tmp_path):
        """A corrupt JSONL line must be skipped; valid lines after it are yielded."""
        from ai_assistant.capture.record import CaptureRecord
        from ai_assistant.capture.store import IsolatedCaptureStore

        store = IsolatedCaptureStore(tmp_path / "capture")
        path = store._path("proj", "conn", "db")
        path.parent.mkdir(parents=True, exist_ok=True)

        rec = CaptureRecord(
            project_id="proj", connection_name="conn", database="db",
            question="valid", sql="SELECT 1", raw_response="r",
        )
        path.write_text(
            "NOT JSON AT ALL\n" + rec.to_json_line() + "\n",
            encoding="utf-8",
        )

        records = list(store.iter_records("proj"))
        assert len(records) == 1
        assert records[0].question == "valid"


# ---------------------------------------------------------------------------
# Bug #5 — orchestrator: _agreement/_final_quality/_mode_quality not reset
# ---------------------------------------------------------------------------

class TestOrchestratorStateReset:
    def test_state_cleared_at_run_start(self, tmp_path):
        """run() must reset _agreement/_final_quality/_mode_quality each call."""
        from unittest.mock import patch
        from ai_assistant.app_builder.orchestrator import AppBuildOrchestrator
        from ai_assistant.app_builder.engine import AppBlueprint, BuildMode, EngineVerdict

        bp = AppBlueprint(
            name="test-app",
            description="test",
            mode=BuildMode.FROM_SCRATCH,
            connections=[],
        )
        # Hook engine.validate_blueprint (called after the resets) to capture state.
        captured = {}
        original_validate = None

        def capturing_validate(blueprint):
            captured["agreement"] = probe._agreement
            captured["final_quality"] = probe._final_quality
            captured["mode_quality"] = probe._mode_quality
            # Return a failing verdict so run() exits immediately.
            return EngineVerdict(accepted=False, score=0.0, issues=["test probe"])

        probe = AppBuildOrchestrator(max_rounds=0)
        probe._agreement = {"poisoned": True}
        probe._final_quality = {"poisoned": True}
        probe._mode_quality = {"poisoned": True}

        with patch.object(probe.engine, "validate_blueprint", side_effect=capturing_validate):
            probe.run(bp, tmp_path / "ws")

        # The resets happen before validate_blueprint is called.
        assert captured.get("agreement") is None, "agreement not reset"
        assert captured.get("final_quality") is None, "final_quality not reset"
        assert captured.get("mode_quality") is None, "mode_quality not reset"


# ---------------------------------------------------------------------------
# Bug #8 — interaction.py: AUTO level auto-approves critical decisions when _ask=None
# ---------------------------------------------------------------------------

class TestBuildDeciderSafeDefault:
    def test_critical_decision_defaults_to_false_in_auto_headless(self):
        """Critical decisions in AUTO without an ask callback must default to False."""
        from ai_assistant.app_builder.interaction import (
            AUTO, BuildDecider, BuildDecision,
        )

        decider = BuildDecider(level=AUTO, ask=None)
        critical = BuildDecision(
            id="confirm_deploy_schema",
            question="Deploy schema to live DB?",
            default=True,
            critical=True,
        )
        answer = decider.decide(critical)
        assert answer is False, "Critical headless AUTO decisions must be False by default"

    def test_non_critical_decision_keeps_its_default_in_auto(self):
        """Non-critical decisions in AUTO keep their configured default."""
        from ai_assistant.app_builder.interaction import AUTO, BuildDecider, BuildDecision

        decider = BuildDecider(level=AUTO, ask=None)
        non_critical = BuildDecision(
            id="use_sample_data",
            question="Use sample data?",
            default=True,
            critical=False,
        )
        assert decider.decide(non_critical) is True

    def test_uninterrupted_still_uses_default(self):
        """Uninterrupted mode always uses the default regardless of critical flag."""
        from ai_assistant.app_builder.interaction import BuildDecider, BuildDecision, UNINTERRUPTED

        decider = BuildDecider(level=UNINTERRUPTED)
        critical = BuildDecision(
            id="deploy",
            question="Deploy?",
            default=True,
            critical=True,
        )
        # Uninterrupted is expected to use default (True) — it's the opt-in
        # "trust me" mode where the caller explicitly chose full automation.
        assert decider.decide(critical) is True


# ---------------------------------------------------------------------------
# Bug #12 — connection_manager: get_connection shallow copy leaks nested state
# ---------------------------------------------------------------------------

class TestConnectionManagerDeepCopy:
    def test_mutation_does_not_affect_stored_profile(self, tmp_path):
        """Mutating the returned profile must not modify the in-memory store."""
        from common.connection_manager import ConnectionManager
        from common.connection_params import ConnectionParams

        cm = ConnectionManager(config_file=str(tmp_path / "conns.json"))
        params = ConnectionParams(
            name="myconn",
            db_type="PostgreSQL",
            host="localhost",
            port=5432,
            ssh_tunnel={
                "ssh_host": "bastion",
                "ssh_user": "user",
                "ssh_port": 22,
            },
        )
        cm.add_connection(params, persist=False)

        profile = cm.get_connection("myconn")
        assert profile is not None
        assert profile.get("ssh_tunnel") is not None, "ssh_tunnel missing from profile"
        profile["ssh_tunnel"]["ssh_host"] = "HACKED"

        profile2 = cm.get_connection("myconn")
        assert profile2["ssh_tunnel"]["ssh_host"] == "bastion", (
            "Mutating the returned copy must not affect the stored profile"
        )

    def test_top_level_mutation_does_not_affect_stored(self, tmp_path):
        """Mutating a top-level key in the returned dict must not affect the store."""
        from common.connection_manager import ConnectionManager
        from common.connection_params import ConnectionParams

        cm = ConnectionManager(config_file=str(tmp_path / "conns.json"))
        params = ConnectionParams(name="c2", db_type="MySQL", host="dbhost", port=3306)
        cm.add_connection(params, persist=False)

        profile = cm.get_connection("c2")
        profile["host"] = "mutated"

        profile2 = cm.get_connection("c2")
        assert profile2["host"] == "dbhost"


# ---------------------------------------------------------------------------
# Bug #23 — converter.py: validate_data_transfer(None, N) false mismatch
# ---------------------------------------------------------------------------

class TestValidateDataTransfer:
    def test_none_source_count_is_not_a_mismatch(self):
        from schema_converter.converter import ConversionValidator

        assert ConversionValidator.validate_data_transfer(None, 5) is None

    def test_none_target_count_is_not_a_mismatch(self):
        from schema_converter.converter import ConversionValidator

        assert ConversionValidator.validate_data_transfer(10, None) is None

    def test_both_none_is_not_a_mismatch(self):
        from schema_converter.converter import ConversionValidator

        assert ConversionValidator.validate_data_transfer(None, None) is None

    def test_matching_counts_return_none(self):
        from schema_converter.converter import ConversionValidator

        assert ConversionValidator.validate_data_transfer(100, 100) is None

    def test_mismatched_counts_return_error_string(self):
        from schema_converter.converter import ConversionValidator

        result = ConversionValidator.validate_data_transfer(100, 95)
        assert result is not None
        assert "100" in result and "95" in result


# ---------------------------------------------------------------------------
# Bug #13 — training_service: _live_validate_pairs aborts on first failure
# ---------------------------------------------------------------------------

class TestLiveValidatePairsPartialFailure:
    def test_partial_failure_keeps_valid_pairs(self):
        """When some pairs fail live validation, valid pairs must be returned."""
        import sqlite3
        from ai_assistant.llm.training_service import LlmTrainingService

        class FakeCore:
            def __init__(self):
                self._conn = sqlite3.connect(":memory:")
                self._conn.execute("CREATE TABLE items (id INTEGER)")
                self._conn.commit()

            def execute(self, name, sql):
                try:
                    cur = self._conn.execute(sql)
                    return {"columns": [], "rows": [], "rowcount": cur.rowcount}
                except Exception as e:
                    return {"error": str(e)}

            def get_connection_profile(self, name):
                return {"db_type": "SQLite", "name": name}

        svc = LlmTrainingService(FakeCore())
        pairs = [
            {"question": "good query", "sql": "SELECT id FROM items"},
            {"question": "bad query", "sql": "SELECT * FROM nonexistent_xyz"},
        ]
        valid, warning = svc._live_validate_pairs(pairs, connection="test")
        assert len(valid) == 1
        assert valid[0]["question"] == "good query"
        assert warning is not None
        assert "rejected" in warning.lower() or "1" in warning

    def test_all_fail_returns_empty_and_error(self):
        """When ALL pairs fail, an error message is returned."""
        import sqlite3
        from ai_assistant.llm.training_service import LlmTrainingService

        class FakeCore:
            def execute(self, name, sql):
                return {"error": "no such table"}
            def get_connection_profile(self, name):
                return {"db_type": "SQLite", "name": name}

        svc = LlmTrainingService(FakeCore())
        pairs = [{"question": "q", "sql": "SELECT * FROM no_such_table"}]
        valid, error = svc._live_validate_pairs(pairs, connection="test")
        assert valid == []
        assert error is not None


# ---------------------------------------------------------------------------
# Bug #16 — GenerationConfig.from_params: unhandled ValueError on bad input
# ---------------------------------------------------------------------------

class TestGenerationConfigFromParams:
    def test_bad_int_param_uses_default(self):
        from ai_assistant.llm.decode import GenerationConfig

        cfg = GenerationConfig.from_params({"max_new": "not_a_number"})
        assert cfg.max_new == 64

    def test_bad_float_param_uses_default(self):
        from ai_assistant.llm.decode import GenerationConfig

        cfg = GenerationConfig.from_params({"temperature": "hot"})
        assert cfg.temperature == 0.0

    def test_valid_params_are_used(self):
        from ai_assistant.llm.decode import GenerationConfig

        cfg = GenerationConfig.from_params({"max_new": "128", "temperature": "0.5"})
        assert cfg.max_new == 128
        assert cfg.temperature == 0.5


# ---------------------------------------------------------------------------
# Bug #24 — database_registry: _ensure_initialized data race
# ---------------------------------------------------------------------------

class TestDatabaseRegistryThreadSafe:
    def test_concurrent_initialization_is_idempotent(self):
        """Concurrent calls to get_all_types must all succeed without double-init."""
        from common.database_registry import DatabaseRegistry

        # Reset state so we exercise the initialization path.
        DatabaseRegistry._initialized = False

        results = []
        errors = []

        def get_types():
            try:
                t = DatabaseRegistry.get_all_types()
                results.append(t)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=get_types) for _ in range(10)]
        for t in threads: t.start()
        for t in threads: t.join()

        assert not errors
        assert all(len(r) > 0 for r in results)
        # All threads should see the same set of types.
        first = sorted(results[0])
        assert all(sorted(r) == first for r in results)
