import os
import pytest
from urllib.parse import urlparse

from common.drivers import conMysql


@pytest.mark.integration
def test_mysql_integration_connect_and_basic_ops():
    conn_str = os.environ.get("MYSQL_CONN")
    if not conn_str:
        pytest.skip("MYSQL_CONN not set")

    p = urlparse(conn_str)
    if p.scheme != "mysql":
        pytest.skip("MYSQL_CONN must be a mysql:// URL")

    host = p.hostname
    port = p.port or 3306
    user = p.username
    password = p.password
    database = p.path.lstrip("/") if p.path else None

    # Attempt connection using conMysql helper
    conn = conMysql.connectMysql(
        database=database, host=host, user=user, password=password, port=port
    )
    assert conn is not None and conn.is_connected()

    cur = conn.cursor()
    try:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS pytest_tmp_integration (id INT PRIMARY KEY AUTO_INCREMENT, val VARCHAR(64))"
        )
        cur.execute("INSERT INTO pytest_tmp_integration (val) VALUES ('x')")
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM pytest_tmp_integration")
        row = cur.fetchone()
        assert row is not None
        assert int(row[0]) >= 1
    finally:
        # Cleanup
        try:
            cur.execute("DROP TABLE IF EXISTS pytest_tmp_integration")
            conn.commit()
        except Exception:
            pass
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
