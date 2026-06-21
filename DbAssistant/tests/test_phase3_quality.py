from __future__ import annotations


def test_shared_sql_splitter_handles_strings_comments_and_procedures():
    from common.sql_splitter import split_sql_statements

    sql = "SELECT ';' AS semi; -- ignore ; in comment\nSELECT 2;"
    assert split_sql_statements(sql) == ["SELECT ';' AS semi", "-- ignore ; in comment\nSELECT 2"]

    proc = """
    CREATE OR REPLACE FUNCTION f()
    RETURNS void AS $$
    BEGIN
      PERFORM 1;
    END;
    $$ LANGUAGE plpgsql;
    """
    assert len(split_sql_statements(proc)) == 1


def test_db_manager_and_headless_use_shared_splitter():
    from common.db_manager import DatabaseManager
    from common.headless.db_service import CoreDBService
    from common.sql_splitter import split_sql_statements

    sql = "SELECT 1; SELECT 'x;y';"
    assert DatabaseManager("SQLite")._split_sql_statements(sql) == split_sql_statements(sql)
    assert CoreDBService._split_sql_statements(sql) == split_sql_statements(sql)


def test_schema_validator_ignores_common_sql_functions():
    from ai_query.agent import AIQueryAgent

    agent = AIQueryAgent.__new__(AIQueryAgent)
    context = {
        "schema": {
            "table_schemas": {
                "orders": [
                    {"name": "created_at"},
                    {"name": "amount"},
                    {"name": "customer_name"},
                ]
            }
        }
    }
    sql = (
        "SELECT YEAR(created_at), MONTH(created_at), IFNULL(customer_name, 'x'), "
        "CONCAT(customer_name, '-ok'), ROUND(SUM(amount), 2), NOW() FROM orders"
    )
    warnings = agent._validate_sql_against_schema(sql, context)
    joined = "\n".join(warnings)
    for fn in ["YEAR", "MONTH", "IFNULL", "CONCAT", "ROUND", "NOW"]:
        assert fn not in joined


def test_daemon_closes_devnull_descriptor():
    src = open("monitoring/daemon.py", encoding="utf-8").read()
    assert "devnull.close()" in src
