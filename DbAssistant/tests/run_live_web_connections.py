#!/usr/bin/env python3
"""Live smoke for Web UI backend + Connections tab (direct / remote / cloud).

Uses the developer's real ~/.dbassistant store (set DBASSISTANT_HOME or default).
Does not print passwords or secret keys.
"""
from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CORE_CONN = os.environ.get("DBTOOL_LIVE_CONN", "local_mariadb")
CLOUD_CONN = os.environ.get("DBTOOL_LIVE_CLOUD", "my_gcp_postgres")


def _client():
    from starlette.testclient import TestClient
    from common.ui.web.backend import build_web_backend

    return TestClient(build_web_backend())


def main() -> int:
    failures: list[str] = []
    c = _client()

    def check(name: str, cond: bool, detail: str = "") -> None:
        mark = "PASS" if cond else "FAIL"
        print(f"[{mark}] {name}" + (f" — {detail[:200]}" if detail else ""))
        if not cond:
            failures.append(name + (": " + detail if detail else ""))

    # SPA + config
    html = c.get("/").text
    check("GET / serves SPA", "conn-connect-form" in html and "cloud-connect" in html)
    cfg = c.get("/ui/config").json()
    secs = [s["id"] for s in cfg["specs"]["connection"]["sections"]]
    check("ui/config sections", secs == ["active", "direct", "remote", "cloud"], str(secs))

    # Metadata + saved list
    check("GET /api/connections/metadata", c.get("/api/connections/metadata").status_code == 200)
    saved = c.get("/api/connections").json()
    check("GET /api/connections", isinstance(saved, list) and len(saved) >= 1, f"{len(saved)} profiles")

    # Cloud schemas + list
    schemas = c.get("/api/cloud/schemas").json()
    check("GET /api/cloud/schemas", "AWS" in schemas.get("providers", {}))
    cloud_list = c.get("/api/cloud/connections").json()
    check("GET /api/cloud/connections", isinstance(cloud_list, list), f"{len(cloud_list)} cloud")

    # Live query on primary connection
    if any(x.get("name") == CORE_CONN for x in saved):
        r = c.post("/api/query", json={"connection": CORE_CONN, "sql": "SELECT 1 AS n"}).json()
        check(f"POST /api/query ({CORE_CONN})", "columns" in r and not r.get("error"), str(r.get("error", "")))
        r2 = c.post(f"/api/connections/{CORE_CONN}/test").json()
        check(f"POST test {CORE_CONN}", r2.get("ok") is True, r2.get("message", ""))
    else:
        print(f"[SKIP] core conn '{CORE_CONN}' not in saved list")

    # Temp direct connection round-trip (create → test → delete)
    tmp = f"web_e2e_{uuid.uuid4().hex[:8]}"
    body = {
        "name": tmp, "db_type": "MariaDB", "host": "localhost", "port": "3306",
        "user": "dheeru", "password": "dheeru", "service": "test",
        "save_password": False,
    }
    cr = c.post("/api/connections", json=body)
    check(f"POST create temp {tmp}", cr.status_code == 201, cr.text[:200])
    if cr.status_code == 201:
        tr = c.post("/api/connections/test-inline", json=body).json()
        check(f"POST test-inline temp {tmp}", tr.get("ok") is True, tr.get("message", ""))
        dr = c.delete(f"/api/connections/{tmp}")
        check(f"DELETE temp {tmp}", dr.status_code == 200, dr.text[:120])

    # Cloud profile load (if exists)
    if cloud_list:
        name = cloud_list[0].get("name")
        prof = c.get(f"/api/cloud/connections/{name}").json()
        check(f"GET cloud profile {name}", prof.get("display_name") == name or prof.get("provider"))
        # Test DB login only (no connect) using stored profile shape
        if prof.get("sql_connection", {}).get("host"):
            td = c.post("/api/cloud/test-db", json=prof).json()
            check(f"POST cloud/test-db {name}", "ok" in td, td.get("message", "")[:200])
    elif CLOUD_CONN:
        print(f"[SKIP] no cloud profiles listed")

    # JS asset sanity
    js = c.get("/ui/app.js").text
    for fn in ("upsertConnection", "loadCloudSchemas", "collectCloudProfile", "buildRemoteBody"):
        check(f"app.js has {fn}", fn in js)

    print("\n" + ("ALL PASSED" if not failures else f"FAILED ({len(failures)}): " + "; ".join(failures)))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
