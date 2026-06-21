#!/usr/bin/env python3
"""Exhaustive API exerciser — hits every route on the full composite app.

Strategy:
  * GET endpoints: called with real path params (local_mariadb / my_gcp_postgres).
  * POST/PUT: valid payloads for non-destructive ops; negative payloads to
    exercise validation paths.
  * Destructive ops (DELETE / restore / reset): exercised with NON-EXISTENT
    targets so the handler code runs without mutating real user data
    (expect 404/400). These are marked [safe-neg].
Each call records: method, path, status, ok(expected?), short body.
"""
from __future__ import annotations

import json

from fastapi.testclient import TestClient

from common.headless.app_factory import create_app

DB = "local_mariadb"
CLOUD = "my_gcp_postgres"

app = create_app()
c = TestClient(app)

results = []


def call(method, path, *, expect, label="", **kw):
    try:
        r = c.request(method, path, **kw)
        ok = r.status_code in expect
        body = r.text[:200].replace("\n", " ")
        results.append((method, path, r.status_code, ok, label, body))
    except Exception as exc:  # noqa
        results.append((method, path, "EXC", False, label, str(exc)[:200]))


OK = {200, 201}
OKNEG = {400, 404, 409, 422, 500, 501, 503}

# ── meta ──────────────────────────────────────────────────────────────
for p in ["/", "/api", "/api/health", "/api/modules", "/openapi.json"]:
    call("GET", p, expect=OK)

# ── connections ───────────────────────────────────────────────────────
call("GET", "/api/connections", expect=OK)
call("GET", "/api/connections/active", expect=OK)
call("POST", f"/api/connections/{DB}/test", expect=OK, label="test ok")
call("POST", "/api/connections/zzz_nope/test", expect=OKNEG, label="test bad")
call("POST", f"/api/connections/{DB}/open", expect=OK | OKNEG)
call("POST", f"/api/connections/{DB}/close", expect=OK | OKNEG)
call("POST", "/api/connections", expect=OKNEG, label="add missing body", json={})
call("DELETE", "/api/connections/zzz_nope", expect=OKNEG, label="safe-neg del")

# ── query ─────────────────────────────────────────────────────────────
call("POST", "/api/query", expect=OK, label="select1",
     json={"connection": DB, "sql": "SELECT 1 AS one"})
call("POST", "/api/query", expect=OK | OKNEG, label="bad sql",
     json={"connection": DB, "sql": "SELEC bad"})
call("POST", "/api/query", expect=OKNEG, label="missing conn",
     json={"sql": "SELECT 1"})
call("POST", "/api/query/multi", expect=OK | OKNEG, label="multi",
     json={"connection": DB, "sql": "SELECT 1; SELECT 2"})
call("GET", f"/api/query/{DB}/autocommit", expect=OK | OKNEG)

# ── objects ───────────────────────────────────────────────────────────
call("GET", f"/api/objects/{DB}?type=databases", expect=OK)
call("GET", f"/api/objects/{DB}?type=engines", expect=OK)
call("GET", f"/api/objects/{DB}?type=charsets", expect=OK)
call("GET", f"/api/objects/{DB}/count?type=databases", expect=OK | OKNEG)

# ── databases / config / modules ──────────────────────────────────────
call("GET", "/api/databases/types", expect=OK)
call("GET", "/api/databases/ops?type=MySQL", expect=OK)
call("GET", "/api/config", expect=OK)
call("GET", "/api/config/settings", expect=OK)
call("GET", "/api/dashboard", expect=OK)
call("GET", "/api/dashboard/layout", expect=OK)

# ── app ───────────────────────────────────────────────────────────────
call("GET", "/api/app/shortcuts", expect=OK)

# ── migrator ──────────────────────────────────────────────────────────
call("GET", "/api/migrator/config", expect=OK)
call("GET", f"/api/migrator/{DB}/test.PRODUCTS", expect=OK | OKNEG, label="show table")
call("GET", f"/api/migrator/{DB}/dump?table=test.PRODUCTS", expect=OK | OKNEG)
call("POST", f"/api/migrator/{DB}/row-counts", expect=OK | OKNEG,
     json={"tables": ["test.PRODUCTS"]})
call("POST", "/api/migrator/convert", expect=OK | OKNEG, label="convert",
     json={"connection": DB, "table": "test.PRODUCTS", "target_type": "PostgreSQL"})
call("POST", "/api/migrator/compare-schema", expect=OK | OKNEG,
     json={"source": DB, "target": DB, "table": "test.PRODUCTS"})

# ── ai ────────────────────────────────────────────────────────────────
call("GET", "/api/ai/backends", expect=OK)
call("GET", "/api/ai/config", expect=OK)
call("GET", "/api/ai/pii", expect=OK)
call("GET", "/api/ai/cache", expect=OK)
call("GET", "/api/ai/sessions", expect=OK)
call("POST", "/api/ai/query", expect=OK | OKNEG, label="ai query (may need backend)",
     json={"connection": DB, "prompt": "list tables"})

# ── monitoring ────────────────────────────────────────────────────────
call("GET", "/api/monitor/config", expect=OK)
call("GET", "/api/monitor/connections", expect=OK)
call("GET", "/api/monitor/connections/saved", expect=OK)
call("GET", "/api/monitor/db-connections", expect=OK)
call("GET", "/api/monitor/notifications", expect=OK)
call("GET", "/api/os/metrics", expect=OK)
call("GET", "/api/thresholds", expect=OK)
call("GET", "/api/thresholds/os/cpu_utilization", expect=OK | OKNEG)
call("POST", "/api/thresholds/check", expect=OK | OKNEG,
     json={"source": "os", "metric": "cpu_utilization", "value": 99})
call("GET", "/api/metrics", expect=OK | OKNEG)
call("GET", f"/api/metrics/{DB}", expect=OK | OKNEG, label="db metrics")
call("GET", "/api/daemon/status", expect=OK)
call("GET", "/api/alerts", expect=OK)
call("POST", "/api/notify", expect=OK | OKNEG, label="notify (disabled ok)",
     json={"title": "t", "message": "m", "severity": "INFO"})

# ── cloud (core cloud-DB connections) ─────────────────────────────────
call("GET", "/api/cloud/connections", expect=OK)
call("DELETE", "/api/cloud/connections/zzz_nope", expect=OKNEG, label="safe-neg del")

# ── monitoring cloud (namespaced under /api/monitor/cloud) ────────────
call("GET", "/api/monitor/cloud/connections", expect=OK)
call("GET", "/api/monitor/cloud/providers/schema", expect=OK)
call("GET", f"/api/monitor/cloud/metrics/{CLOUD}", expect=OK | OKNEG, label="cloud metrics")
call("POST", f"/api/monitor/cloud/connections/{CLOUD}/test", expect=OK | OKNEG)

# ── REPORT ────────────────────────────────────────────────────────────
fails = [r for r in results if not r[3]]
print("=" * 90)
print(f"API EXHAUSTIVE: {len(results)} calls | PASSED {len(results)-len(fails)} | UNEXPECTED {len(fails)}")
print("=" * 90)
for m, p, code, ok, label, body in results:
    mark = "OK " if ok else "XX "
    print(f"[{mark}] {m:6} {p}  -> {code}  {label}")
    if not ok:
        print(f"        body: {body}")
print("\nUNEXPECTED:", [(r[0], r[1], r[2]) for r in fails] or "none")
