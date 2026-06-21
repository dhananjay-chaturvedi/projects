"""PostgreSQL schema enrichment regression tests."""

from __future__ import annotations


class _Cursor:
    def __init__(self):
        self.queries: list[str] = []
        self._fetchone_count = 0

    def execute(self, query, params=None):
        self.queries.append(query)

    def fetchone(self):
        self._fetchone_count += 1
        if self._fetchone_count == 1:
            return ("table comment", "UTF8")
        return None

    def fetchall(self):
        return []

    def close(self):
        pass


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_enrich_postgres_schema_uses_server_encoding_not_relencoding(monkeypatch):
    import schema_converter.schema_full as sf

    # Keep this test focused on the top-level table metadata query.
    monkeypatch.setattr(sf, "_pg_unique_constraints", lambda *a: [])
    monkeypatch.setattr(sf, "_pg_foreign_keys", lambda *a: [])
    monkeypatch.setattr(sf, "_pg_check_constraints", lambda *a: [])
    monkeypatch.setattr(sf, "_pg_indexes", lambda *a: [])
    monkeypatch.setattr(sf, "_pg_partition", lambda *a: None)
    monkeypatch.setattr(sf, "_pg_sequences", lambda *a: [])
    monkeypatch.setattr(sf, "_pg_triggers", lambda *a: [])

    cursor = _Cursor()
    schema = {
        "columns": [],
        "related_objects": {},
    }

    out = sf.enrich_postgres_schema(schema, _Conn(cursor), "public.orders")

    metadata_query = cursor.queries[0]
    assert "relencoding" not in metadata_query
    assert "current_setting('server_encoding')" in metadata_query
    assert out["table_comment"] == "table comment"
    assert out["table_charset"] == "UTF8"
