#!/usr/bin/env python3
"""Live e2e: multibyte data migration with type-map override (GCP Postgres -> MariaDB)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SOURCE_CONN = "my-gcp-pg-db"
TARGET_CONN = "local_mariadb"
SOURCE_TABLE = "public.migrator_unicode_e2e"
TARGET_DB = "test"
TARGET_TABLE = f"{TARGET_DB}.migrator_unicode_e2e"

SAMPLES = [
    ("jp", "こんにちは世界"),
    ("cn", "你好世界"),
    ("hi", "नमस्ते दुनिया"),
    ("em", "Hello 🌍✨"),
]


def main() -> int:
    from schema_converter.bridge import make_service

    svc = make_service()
    core = svc._core if hasattr(svc, "_core") else None
    if core is None:
        from common.headless.db_service import CoreDBService

        core = CoreDBService()
        svc = make_service(core)

    src = core.get_manager(SOURCE_CONN)
    tgt = core.get_manager(TARGET_CONN)

    src_cur = src.conn.cursor()
    tgt_cur = tgt.conn.cursor()

    print("Creating source table on GCP Postgres...")
    src_cur.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
    src_cur.execute(
        f"""
        CREATE TABLE {SOURCE_TABLE} (
            id serial PRIMARY KEY,
            lang varchar(8) NOT NULL,
            message character varying(200) NOT NULL
        )
        """
    )
    for lang, msg in SAMPLES:
        src_cur.execute(
            f"INSERT INTO {SOURCE_TABLE} (lang, message) VALUES (%s, %s)",
            (lang, msg),
        )
    src.conn.commit()
    src_cur.close()

    print("Converting schema with type-map varchar:text ...")
    from schema_converter.table_naming import TargetNaming

    conv = svc.convert_schema(
        SOURCE_CONN,
        "MariaDB",
        SOURCE_TABLE,
        naming=TargetNaming(target_db=TARGET_DB),
        type_map="varchar:text,character varying:text",
    )
    if conv.get("error"):
        print("CONVERT ERROR:", conv["error"])
        return 1
    ddl = "\n\n".join(conv.get("all_ddl") or [conv.get("ddl") or ""])
    print("Applying DDL on MariaDB target...")
    tgt_cur.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    tgt.conn.commit()
    apply = svc.apply_ddl_to_target(TARGET_CONN, ddl)
    if apply.get("error"):
        print("APPLY ERROR:", apply["error"])
        return 1

    print("Transferring data...")
    from schema_converter.transfer_options import TransferRequest

    xfer = svc.transfer_data(
        TransferRequest(
            SOURCE_CONN,
            TARGET_CONN,
            SOURCE_TABLE,
            target_table=TARGET_TABLE,
        ),
    )
    if not xfer.get("ok"):
        print("TRANSFER ERROR:", xfer.get("message"))
        return 1
    print("Transferred rows:", xfer.get("rows_transferred"))

    tgt_cur = tgt.conn.cursor()
    tgt_cur.execute(f"SELECT lang, message FROM {TARGET_TABLE} ORDER BY id")
    rows = tgt_cur.fetchall()
    tgt_cur.close()

    expected = {lang: msg for lang, msg in SAMPLES}
    ok = True
    for lang, message in rows:
        exp = expected.get(lang)
        if message != exp:
            ok = False
            print(f"MISMATCH lang={lang!r}: got {message!r}, expected {exp!r}")

    print("Cleaning up...")
    src_cur = src.conn.cursor()
    src_cur.execute(f"DROP TABLE IF EXISTS {SOURCE_TABLE}")
    src.conn.commit()
    src_cur.close()
    tgt_cur = tgt.conn.cursor()
    tgt_cur.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    tgt.conn.commit()
    tgt_cur.close()

    if ok:
        print("LIVE E2E PASSED: multibyte strings transferred exactly.")
        return 0
    print("LIVE E2E FAILED")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
