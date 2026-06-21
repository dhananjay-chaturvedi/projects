#!/usr/bin/env python3
"""Live parity test: multi-table column rename (G2) across UI engine / CLI / API.

Verifies the fix that makes ``column_map`` (column rename) apply to *every*
selected table, while row filter (WHERE) and column subset stay single-table
only. Runs the same transfer through three surfaces and checks that the renamed
target column holds the source data (including multibyte text):

  * core/bridge  -> ``SchemaBridge.transfer_data_multi`` (the engine the UI uses)
  * CLI          -> ``dbtool.py migrator transfer-data --tables ... --column-map``
  * API          -> ``POST /api/migrator/transfer-data-multi`` (in-process)

Uses only the existing saved connection ``local_mariadb`` (source == target,
inside the ``test`` schema). Self-cleans all scratch tables.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CONN = "local_mariadb"
SCHEMA = "test"
SRC_TABLES = ["cr_src_a", "cr_src_b"]
RENAME = "name:full_name"

# Per-table multibyte sample rows: (id, name, note)
SAMPLES = {
    "cr_src_a": [
        (1, "こんにちは世界", "jp"),
        (2, "你好世界", "cn"),
        (3, "नमस्ते दुनिया", "hi"),
        (4, "Hello 🌍✨", "em"),
    ],
    "cr_src_b": [
        (1, "Café déjà vü", "fr"),
        (2, "Ω≈ç√∫˜µ", "sym"),
        (3, "한국어 테스트", "kr"),
    ],
}

failures: list[str] = []


def check(label: str, cond: bool, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {label}" + (f" :: {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(label)


def _exec(cur, sql, params=None):
    cur.execute(sql, params or ())


def setup_sources(tgt) -> None:
    cur = tgt.conn.cursor()
    for t in SRC_TABLES:
        _exec(cur, f"DROP TABLE IF EXISTS {SCHEMA}.{t}")
        _exec(
            cur,
            f"""
            CREATE TABLE {SCHEMA}.{t} (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                note VARCHAR(50) NOT NULL
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
        )
        for row in SAMPLES[t]:
            _exec(cur, f"INSERT INTO {SCHEMA}.{t} (id, name, note) VALUES (%s, %s, %s)", row)
    tgt.conn.commit()
    cur.close()


def precreate_targets(tgt, prefix: str) -> None:
    """Create target tables with the *renamed* column (full_name)."""
    cur = tgt.conn.cursor()
    for t in SRC_TABLES:
        name = f"{SCHEMA}.{prefix}{t}"
        _exec(cur, f"DROP TABLE IF EXISTS {name}")
        _exec(
            cur,
            f"""
            CREATE TABLE {name} (
                id INT PRIMARY KEY,
                full_name VARCHAR(100) NOT NULL,
                note VARCHAR(50) NOT NULL
            ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
        )
    tgt.conn.commit()
    cur.close()


def verify_target(tgt, prefix: str, surface: str) -> None:
    cur = tgt.conn.cursor()
    for t in SRC_TABLES:
        name = f"{SCHEMA}.{prefix}{t}"
        # Renamed column must exist and hold the source 'name' values.
        _exec(cur, f"SELECT id, full_name, note FROM {name} ORDER BY id")
        rows = cur.fetchall()
        expected = SAMPLES[t]
        got = [(int(r[0]), r[1], r[2]) for r in rows]
        check(
            f"{surface}: {t} row count",
            len(got) == len(expected),
            f"got {len(got)} expected {len(expected)}",
        )
        check(
            f"{surface}: {t} renamed column data (multibyte exact)",
            got == expected,
            f"got {got!r}",
        )
    cur.close()


def drop_targets(tgt, prefixes: list[str]) -> None:
    cur = tgt.conn.cursor()
    for prefix in prefixes:
        for t in SRC_TABLES:
            _exec(cur, f"DROP TABLE IF EXISTS {SCHEMA}.{prefix}{t}")
    for t in SRC_TABLES:
        _exec(cur, f"DROP TABLE IF EXISTS {SCHEMA}.{t}")
    tgt.conn.commit()
    cur.close()


def main() -> int:
    from schema_converter.bridge import make_service

    svc = make_service()
    core = getattr(svc, "_core", None)
    if core is None:
        from common.headless.db_service import CoreDBService

        core = CoreDBService()
        svc = make_service(core)

    tgt = core.get_manager(CONN)

    qualified = [f"{SCHEMA}.{t}" for t in SRC_TABLES]
    prefixes = {"core": "crcore_", "cli": "crcli_", "api": "crapi_"}

    print("=" * 78)
    print("SETUP: source tables + multibyte sample data")
    print("=" * 78)
    setup_sources(tgt)

    # ── 1) CORE / BRIDGE (engine the UI drives) ───────────────────────────
    print("\n--- CORE/BRIDGE multi-table transfer with column_map ---")
    precreate_targets(tgt, prefixes["core"])
    from schema_converter.transfer_options import (
        TransferMultiRequest,
        options_from_mapping,
    )

    from schema_converter.table_naming import TargetNaming

    r = svc.transfer_data_multi(
        TransferMultiRequest(
            CONN,
            CONN,
            qualified,
            naming=TargetNaming(target_db=SCHEMA, prefix=prefixes["core"]),
        ),
        options_from_mapping({"column_map": RENAME}),
    )
    check("core: transfer ok", bool(r.get("ok")), json.dumps(r)[:200])
    check("core: 2 tables succeeded", int(r.get("successful", 0)) == 2, json.dumps(r)[:200])
    verify_target(tgt, prefixes["core"], "core")

    # ── 2) CLI ────────────────────────────────────────────────────────────
    print("\n--- CLI multi-table transfer with --column-map ---")
    precreate_targets(tgt, prefixes["cli"])
    p = subprocess.run(
        [
            sys.executable, "dbtool.py", "--format", "json", "migrator", "transfer-data",
            "--source-conn", CONN, "--target-conn", CONN,
            "--tables", ",".join(qualified),
            "--target-db", SCHEMA, "--prefix", prefixes["cli"],
            "--column-map", RENAME,
        ],
        cwd=ROOT, capture_output=True, text=True, timeout=120,
    )
    out = (p.stdout or "") + (p.stderr or "")
    check("cli: exit 0", p.returncode == 0, out[:200])
    verify_target(tgt, prefixes["cli"], "cli")

    # CLI guard rails: WHERE / columns must be rejected for multi-table.
    g1 = subprocess.run(
        [
            sys.executable, "dbtool.py", "migrator", "transfer-data",
            "--source-conn", CONN, "--target-conn", CONN,
            "--tables", ",".join(qualified), "--where", "id > 1",
        ],
        cwd=ROOT, capture_output=True, text=True, timeout=60,
    )
    check("cli: --where rejected with --tables", g1.returncode == 1,
          (g1.stdout + g1.stderr)[:160])
    g2 = subprocess.run(
        [
            sys.executable, "dbtool.py", "migrator", "transfer-data",
            "--source-conn", CONN, "--target-conn", CONN,
            "--tables", ",".join(qualified), "--columns", "id,name",
        ],
        cwd=ROOT, capture_output=True, text=True, timeout=60,
    )
    check("cli: --columns rejected with --tables", g2.returncode == 1,
          (g2.stdout + g2.stderr)[:160])

    # ── 3) API (in-process) ───────────────────────────────────────────────
    print("\n--- API multi-table transfer with column_map ---")
    from fastapi.testclient import TestClient

    from common.headless.app_factory import create_app

    client = TestClient(create_app())
    precreate_targets(tgt, prefixes["api"])
    resp = client.post(
        "/api/migrator/transfer-data-multi",
        json={
            "source_conn": CONN,
            "target_conn": CONN,
            "tables": qualified,
            "target_db": SCHEMA,
            "prefix": prefixes["api"],
            "column_map": RENAME,
        },
    )
    check("api: HTTP 200", resp.status_code == 200, f"{resp.status_code} {resp.text[:160]}")
    if resp.status_code == 200:
        body = resp.json()
        check("api: transfer ok", bool(body.get("ok")), json.dumps(body)[:200])
    verify_target(tgt, prefixes["api"], "api")

    # API contract: column_map present on multi model, where/columns absent.
    from schema_converter.api import DataTransferMultiRequest

    fields = DataTransferMultiRequest.model_fields
    check("api: model has column_map", "column_map" in fields)
    check("api: model has no where", "where" not in fields)
    check("api: model has no columns", "columns" not in fields)

    # ── CLEANUP ───────────────────────────────────────────────────────────
    print("\nCleaning up scratch tables...")
    drop_targets(tgt, list(prefixes.values()))

    print("\n" + "=" * 78)
    if failures:
        print(f"PARITY LIVE: FAILED ({len(failures)}): {failures}")
        return 1
    print("PARITY LIVE: ALL CHECKS PASSED — UI engine / CLI / API in sync.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
