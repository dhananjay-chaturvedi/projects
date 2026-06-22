"""Real end-to-end migration test for the source/target schema dropdowns.

Uses saved connection profiles (source PostgreSQL, target MariaDB/MySQL by
default). Override profile names with environment variables when your local
names differ.

Run:  PYTHONPATH=. .venv/bin/python scripts/e2e_migration_dropdown_test.py
"""

from __future__ import annotations

import os
import sys
import traceback

from common.connection_manager import ConnectionManager
from common.db_manager import DatabaseManager
from schema_converter.schema_converter_ui import SchemaConverterUI
from schema_converter.converter import SchemaConverter, DataConverter


SRC_NAME = os.environ.get("DBTOOL_E2E_SRC_CONN", "source_postgres")
TGT_NAME = os.environ.get("DBTOOL_E2E_TGT_CONN", "target_mysql")
TARGET_DB = "test"
SRC_TABLE = "public.dbtool_e2e_migration"
SRC_TABLE_BARE = "dbtool_e2e_migration"

ROWS = [
    (1, "Alice", "100.50", True),
    (2, "Bob", "0.00", False),
    (3, "Carol O'Hara", "-25.25", True),
    (4, "Dan \"Q\"", "999999.99", False),
    (5, None, None, None),
]

PASS = []
FAIL = []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(name)
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  -- {detail}" if detail else ""))


def connect(conn_dict):
    mgr = DatabaseManager(conn_dict["db_type"])
    mgr.connect(
        host=conn_dict.get("host"),
        port=conn_dict.get("port"),
        username=conn_dict.get("username"),
        password=conn_dict.get("password"),
        database=conn_dict.get("service_or_db") or None,
    )
    return mgr


class FakeCombo:
    def __init__(self, value=""):
        self._v = value
        self._values = []

    def get(self):
        return self._v

    def set(self, v):
        self._v = v

    def __setitem__(self, k, v):
        if k == "values":
            self._values = list(v)

    def __getitem__(self, k):
        if k == "values":
            return self._values
        raise KeyError(k)


def main():
    cm = ConnectionManager()
    by_name = {c["name"]: c for c in cm.get_all_connections()}
    if SRC_NAME not in by_name or TGT_NAME not in by_name:
        print(f"Missing connections. Found: {list(by_name)}")
        return 2

    print(f"Connecting source={SRC_NAME} target={TGT_NAME} ...")
    src = connect(by_name[SRC_NAME])
    tgt = connect(by_name[TGT_NAME])
    print(f"  source db_type={src.db_type} target db_type={tgt.db_type}")

    ui = SchemaConverterUI.__new__(SchemaConverterUI)
    ui.source_database_combo = FakeCombo()
    ui.target_database_combo = FakeCombo()
    ui.target_prefix_entry = FakeCombo()  # only .get() used
    ui.target_suffix_entry = FakeCombo()

    try:
        # --- 0. Clean any leftovers from a prior run ---
        _drop_pg(src, SRC_TABLE)
        _drop_mysql(tgt, f"{TARGET_DB}.{SRC_TABLE_BARE}")

        # --- 1. Create a REAL source table with data in PostgreSQL public ---
        print("\n[1] Create real source table + data in PostgreSQL")
        c = src.conn.cursor()
        c.execute(
            f"CREATE TABLE {SRC_TABLE} ("
            "  id integer PRIMARY KEY,"
            "  name varchar(100),"
            "  amount numeric(12,2),"
            "  active boolean"
            ")"
        )
        for r in ROWS:
            c.execute(
                f"INSERT INTO {SRC_TABLE} (id, name, amount, active) VALUES (%s,%s,%s,%s)",
                r,
            )
        src.conn.commit()
        c.close()
        src_count = _count(src, SRC_TABLE)
        check("source table created with rows", src_count == len(ROWS),
              f"rows={src_count}")

        # --- 2. Dropdown population: source schemas, target databases ---
        print("\n[2] Dropdown population + auto-select")
        ui._populate_namespace_combo(ui.source_database_combo, src)
        ui._populate_namespace_combo(ui.target_database_combo, tgt)
        src_ns = ui.source_database_combo["values"]
        tgt_ns = ui.target_database_combo["values"]
        check("source dropdown lists schemas incl public", "public" in src_ns,
              f"{src_ns}")
        check("source dropdown auto-selects live schema 'public'",
              ui.source_database_combo.get() == "public")
        check("target dropdown lists databases incl 'test'", TARGET_DB in tgt_ns,
              f"{tgt_ns}")
        check("target dropdown excludes system DBs",
              "information_schema" not in tgt_ns and "mysql" not in tgt_ns)

        # --- 3. Source-namespace table filtering ---
        print("\n[3] Source table list filtered by selected schema")
        all_tables = src.conn  # placeholder
        from common.database_registry import DatabaseRegistry
        tables = DatabaseRegistry.execute_operation("PostgreSQL", "getTables", src.conn) or []
        filtered = ui._filter_tables_by_source_namespace(tables)
        check("our public table present after filter", SRC_TABLE in filtered,
              f"{len(filtered)} tables")
        check("filter keeps only public.* names",
              all(t.split(".")[0] == "public" for t in filtered))

        # --- 4. Target qualification (the original bug) ---
        print("\n[4] Target table name qualification")
        ui.target_database_combo.set(TARGET_DB)  # user picks 'test'
        target_table = ui.get_target_table_name(SRC_TABLE, tgt)
        check("target name qualified as test.<table>",
              target_table == f"{TARGET_DB}.{SRC_TABLE_BARE}", target_table)

        # --- 5. Run the REAL schema conversion (UI code path) ---
        print("\n[5] Convert schema PostgreSQL -> MariaDB")
        converter = SchemaConverter(src, tgt)
        source_schema = converter.get_table_schema(SRC_TABLE)
        check("source schema introspected", bool(source_schema),
              f"cols={len(source_schema.get('columns', [])) if source_schema else 0}")
        converted = converter.convert_schema(
            source_schema,
            table_name_map=ui._build_table_name_map([SRC_TABLE], tgt),
        )
        converted["table_name"] = target_table
        _drop_mysql(tgt, target_table)
        all_ddl = converter.generate_all_table_ddl(converted)
        print("    DDL:", all_ddl[0][:120].replace("\n", " "), "...")
        executed, ddl_errors, table_created = ui._execute_all_schema_ddl(
            tgt, all_ddl, create_indexes=True
        )
        check("CREATE TABLE executed on target", table_created,
              f"executed={executed} errors={ddl_errors}")

        # --- 6. Transfer the REAL data ---
        print("\n[6] Transfer data")
        dc = DataConverter(src, tgt)
        from schema_converter.transfer_options import TransferRuntime

        moved = dc.transfer_table_data(
            SRC_TABLE, target_table, runtime=TransferRuntime(batch_size=100)
        )
        tgt_count = dc.get_row_count(target_table, is_source=False)
        check("rows transferred matches source", tgt_count == len(ROWS),
              f"moved={moved} target_count={tgt_count}")

        # --- 7. Verify actual data values landed correctly ---
        print("\n[7] Verify data values in MariaDB test table")
        tc = tgt.conn.cursor()
        tc.execute(f"SELECT id, name, amount, active FROM {target_table} ORDER BY id")
        got = tc.fetchall()
        tc.close()
        by_id = {row[0]: row for row in got}
        check("row id=1 name correct", by_id.get(1, [None, None])[1] == "Alice",
              str(by_id.get(1)))
        check("row id=3 keeps apostrophe", by_id.get(3, [None, None])[1] == "Carol O'Hara",
              str(by_id.get(3)))
        check("row id=4 keeps double-quote", by_id.get(4, [None, None])[1] == 'Dan "Q"',
              str(by_id.get(4)))
        check("row id=5 NULL name preserved", by_id.get(5, [None, "x"])[1] is None,
              str(by_id.get(5)))
        amt1 = by_id.get(1, [None, None, None])[2]
        check("numeric amount preserved (100.50)", str(amt1) in ("100.50", "100.5"),
              str(amt1))

        # --- 8. Negative: missing target DB raises clear error ---
        print("\n[8] Negative test: no target db selected on MariaDB")
        ui.target_database_combo.set("")
        # MariaDB connection has no default db -> should raise ValueError
        raised = False
        try:
            ui.get_target_table_name(SRC_TABLE, tgt)
        except ValueError:
            raised = True
        check("missing target db raises ValueError for MariaDB", raised)
        ui.target_database_combo.set(TARGET_DB)

    except Exception:
        print("\nUNEXPECTED ERROR:\n" + traceback.format_exc())
        FAIL.append("unexpected-exception")
    finally:
        print("\n[cleanup] dropping test tables")
        _drop_pg(src, SRC_TABLE)
        _drop_mysql(tgt, f"{TARGET_DB}.{SRC_TABLE_BARE}")
        src.disconnect()
        tgt.disconnect()

    print("\n" + "=" * 60)
    print(f"RESULT: {len(PASS)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED:", FAIL)
    print("=" * 60)
    return 0 if not FAIL else 1


def _count(mgr, table):
    cur = mgr.conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    n = cur.fetchone()[0]
    cur.close()
    return n


def _drop_pg(mgr, table):
    try:
        cur = mgr.conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mgr.conn.commit()
        cur.close()
    except Exception:
        try:
            mgr.conn.rollback()
        except Exception:
            pass


def _drop_mysql(mgr, table):
    try:
        cur = mgr.conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")
        mgr.conn.commit()
        cur.close()
    except Exception:
        pass


if __name__ == "__main__":
    sys.exit(main())
