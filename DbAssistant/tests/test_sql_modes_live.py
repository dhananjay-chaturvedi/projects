"""
Live integration tests for SQL modes and execution rules against local MariaDB.

Uses the same credentials as tests/conftest.py:
  localhost:3306  user=dheeru  password=dheeru  db=test
"""

from __future__ import annotations

import pytest

from ai_query.agent import AIQueryAgent
from ai_query.response_parser import response_format_instructions
from ai_query.session_manager import AISessionManager
from ai_query.sql_execution_rules import (
    build_explain_sql,
    evaluate_execution_rules,
    format_explain_output,
)
from ai_query.sql_modes import (
    execution_rules_apply,
    is_strict_summary,
    migrate_stored_sql_mode,
    normalize_sql_mode,
)
from common.db_manager import DatabaseManager

pytestmark = pytest.mark.integration

MYSQL = dict(
    host="localhost",
    port=3306,
    username="dheeru",
    password="dheeru",
    database="test",
)

USER_TABLE = "EMPLOYEES"
JOIN_SQL = (
    "SELECT e.EMP_ID, d.DEPT_NAME, p.PRODUCT_NAME "
    "FROM EMPLOYEES e "
    "JOIN DEPARTMENTS d ON d.DEPT_ID = 1 "
    "JOIN PRODUCTS p ON p.PRODUCT_ID = 1 "
    "LIMIT 5"
)


@pytest.fixture(scope="module")
def db_manager():
    mgr = DatabaseManager("MariaDB")
    mgr.connect(**MYSQL)
    yield mgr
    mgr.disconnect()


@pytest.fixture(scope="module")
def user_table_names(db_manager):
    result, error = db_manager.execute_query("SHOW TABLES")
    assert error is None, error
    return [row[0] for row in result.get("rows", [])]


@pytest.fixture(scope="module")
def schema_context(user_table_names):
    return {
        "database_type": "MariaDB",
        "schema": {"tables": user_table_names, "table_schemas": {}},
    }


class TestLocalDbConnectivity:
    def test_catalog_query_runs(self, db_manager):
        result, error = db_manager.execute_query(
            "SELECT COUNT(*) AS table_count "
            "FROM information_schema.tables "
            "WHERE table_schema = DATABASE()"
        )
        assert error is None, error
        assert int(result["rows"][0][0]) >= 1

    def test_user_table_query_runs_with_limit(self, db_manager):
        sql = f"SELECT * FROM {USER_TABLE} LIMIT 5"
        result, error = db_manager.execute_query(sql)
        assert error is None, error
        assert "columns" in result


class TestStrictSummaryValidationLive:
    def test_blocks_user_table_sql(self, schema_context, user_table_names):
        agent = AIQueryAgent.__new__(AIQueryAgent)
        result = {
            "summary_sql": f"SELECT COUNT(*) FROM {USER_TABLE}",
            "explanation": "count employees",
        }
        out = agent._apply_sql_mode_validation(
            result, schema_context, "strict_summary"
        )
        assert out.get("summary_mode_blocked") is True

    def test_allows_information_schema(self, schema_context):
        agent = AIQueryAgent.__new__(AIQueryAgent)
        sql = (
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = DATABASE()"
        )
        result = {"summary_sql": sql, "explanation": "metadata"}
        out = agent._apply_sql_mode_validation(
            result, schema_context, "strict_summary"
        )
        assert not out.get("summary_mode_blocked")

    def test_summary_mode_does_not_block_user_table(self, schema_context):
        agent = AIQueryAgent.__new__(AIQueryAgent)
        result = {
            "summary_sql": f"SELECT * FROM {USER_TABLE} LIMIT 10",
            "explanation": "sample rows",
        }
        out = agent._apply_sql_mode_validation(result, schema_context, "summary")
        assert not out.get("summary_mode_blocked")

    def test_open_mode_does_not_block_user_table(self, schema_context):
        agent = AIQueryAgent.__new__(AIQueryAgent)
        result = {
            "summary_sql": f"SELECT * FROM {USER_TABLE} LIMIT 10",
            "explanation": "sample rows",
        }
        out = agent._apply_sql_mode_validation(result, schema_context, "open")
        assert not out.get("summary_mode_blocked")


class TestExecutionRulesLive:
    RULES = (
        "Always use LIMIT clause for SELECT on user tables\n"
        "Always check EXPLAIN plan before running SQL with 2+ JOIN tables"
    )

    def test_blocks_select_without_limit_on_user_table(self, user_table_names):
        sql = f"SELECT * FROM {USER_TABLE}"
        check = evaluate_execution_rules(
            sql,
            self.RULES,
            user_table_names=user_table_names,
            db_type="MariaDB",
        )
        assert check.allowed is False
        assert "LIMIT" in check.blocked_reason

    def test_allows_select_with_limit(self, user_table_names):
        sql = f"SELECT * FROM {USER_TABLE} LIMIT 10"
        check = evaluate_execution_rules(
            sql,
            self.RULES,
            user_table_names=user_table_names,
            db_type="MariaDB",
        )
        assert check.allowed is True

    def test_explain_triggered_for_multi_join(self, user_table_names):
        check = evaluate_execution_rules(
            JOIN_SQL,
            self.RULES,
            user_table_names=user_table_names,
            db_type="MariaDB",
        )
        assert check.allowed is True
        assert check.run_explain_first is True
        assert check.explain_sql.upper().startswith("EXPLAIN")

    def test_explain_runs_on_live_db(self, db_manager, user_table_names):
        check = evaluate_execution_rules(
            JOIN_SQL,
            self.RULES,
            user_table_names=user_table_names,
            db_type="MariaDB",
        )
        exp_result, exp_error = db_manager.execute_query(check.explain_sql)
        assert exp_error is None, exp_error
        formatted = format_explain_output(exp_result)
        assert "EXPLAIN" in formatted

        main_result, main_error = db_manager.execute_query(JOIN_SQL)
        assert main_error is None, main_error
        assert main_result.get("rows") is not None

    def test_strict_summary_skips_execution_rules_flag(self):
        assert execution_rules_apply("strict_summary") is False
        assert execution_rules_apply("summary") is True
        assert execution_rules_apply("open") is True


class TestPromptModesLive:
    def test_three_mode_prompts_distinct(self):
        strict = response_format_instructions("MariaDB", sql_mode="strict_summary")
        summary = response_format_instructions("MariaDB", sql_mode="summary")
        open_ = response_format_instructions("MariaDB", sql_mode="open")
        assert "NEVER user-schema" in strict or "ONLY catalog" in strict
        assert "user-schema tables when required" in summary
        assert "directly answers" in open_ or "no artificial" in open_.lower()

    def test_execution_rules_injected_in_summary_prompt(self):
        rules = "Always use LIMIT on user tables"
        text = response_format_instructions(
            "MariaDB", sql_mode="summary", execution_rules=rules
        )
        assert rules in text
        strict = response_format_instructions(
            "MariaDB", sql_mode="strict_summary", execution_rules=rules
        )
        assert rules not in strict


class TestSessionMigrationLive:
    def test_legacy_session_sql_mode_migration(self):
        assert migrate_stored_sql_mode("summary", sql_modes_v2=False) == "strict_summary"
        assert migrate_stored_sql_mode("open", sql_modes_v2=False) == "summary"
        assert migrate_stored_sql_mode("open", sql_modes_v2=True) == "open"

    def test_session_roundtrip_sql_mode_and_rules(self):
        mgr = AISessionManager()
        sess = mgr.create(connection_name="local_mariadb")
        sess.sql_mode = "open"
        sess.sql_execution_rules = "Always use LIMIT on user tables"
        sess.sql_modes_v2 = True
        exported = mgr.export_state()
        mgr2 = AISessionManager()
        mgr2.import_state(exported)
        loaded = mgr2.get(sess.session_id)
        assert loaded.sql_mode == "open"
        assert "LIMIT" in loaded.sql_execution_rules
        assert loaded.sql_modes_v2 is True


class TestEndToEndExecutionPipelineLive:
    """Simulates manual/auto execute gate + EXPLAIN + query."""

    RULES = (
        "Always use LIMIT clause for SELECT on user tables\n"
        "Always check EXPLAIN plan before running SQL with 2+ JOIN tables"
    )

    def _run_pipeline(self, db_manager, sql, user_table_names):
        if not execution_rules_apply("summary"):
            return None, "mode skip"
        check = evaluate_execution_rules(
            sql,
            self.RULES,
            user_table_names=user_table_names,
            db_type="MariaDB",
        )
        if not check.allowed:
            return None, check.blocked_reason
        parts = []
        if check.run_explain_first and check.explain_sql:
            exp, err = db_manager.execute_query(check.explain_sql)
            parts.append(format_explain_output(exp, err))
        result, error = db_manager.execute_query(sql)
        return (parts, result, error)

    def test_pipeline_blocks_bad_query(self, db_manager, user_table_names):
        sql = f"SELECT * FROM {USER_TABLE}"
        out = self._run_pipeline(db_manager, sql, user_table_names)
        assert out[0] is None
        assert "LIMIT" in out[1]

    def test_pipeline_runs_good_join_query(self, db_manager, user_table_names):
        parts, result, error = self._run_pipeline(
            db_manager, JOIN_SQL, user_table_names
        )
        assert error is None, error
        assert parts and "EXPLAIN" in parts[0]
        assert result.get("rows") is not None

    def test_strict_summary_sql_executable_on_db(self, db_manager):
        sql = (
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = DATABASE() LIMIT 10"
        )
        result, error = db_manager.execute_query(sql)
        assert error is None, error
        assert len(result.get("rows", [])) >= 1


class TestModeNormalizationLive:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("strict_summary", "strict_summary"),
            ("summary", "summary"),
            ("open", "open"),
            ("strict", "strict_summary"),
        ],
    )
    def test_normalize(self, raw, expected):
        assert normalize_sql_mode(raw) == expected

    def test_is_strict_only_for_strict(self):
        assert is_strict_summary("strict_summary")
        assert not is_strict_summary("summary")
        assert not is_strict_summary("open")

    def test_build_explain_mariadb(self):
        assert build_explain_sql("SELECT 1", "MariaDB").startswith("EXPLAIN ")
