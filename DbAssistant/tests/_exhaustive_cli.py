#!/usr/bin/env python3
"""Exhaustive CLI exerciser — runs every CLI action and records output.

Uses only EXISTING saved connections (no new credentialed connections created):
  - local_mariadb   (core DB profile)
  - my_gcp_postgres (cloud profile)
Read/list/show/help actions for every subcommand across all modules + dbtool.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable
DB = "local_mariadb"
CLOUD = "my_gcp_postgres"

results: list[tuple[str, str, bool, str]] = []


def run(label: str, args: list[str], *, expect_ok=True, timeout=90, contains=None):
    try:
        p = subprocess.run([PY, *args], cwd=ROOT, capture_output=True,
                           text=True, timeout=timeout)
        out = (p.stdout or "") + (p.stderr or "")
        ok = (p.returncode == 0) if expect_ok else (p.returncode != 0)
        if ok and contains is not None:
            ok = contains.lower() in out.lower()
        results.append((label, " ".join(args[1:]), ok, out.strip()[:240]))
    except subprocess.TimeoutExpired:
        results.append((label, " ".join(args[1:]), False, "TIMEOUT"))
    except Exception as exc:  # noqa
        results.append((label, " ".join(args[1:]), False, str(exc)))


SC = ["-m", "schema_converter"]
AI = ["-m", "ai_query"]
MON = ["-m", "monitoring"]
DT = ["dbtool.py"]

# ── CORE: connections ────────────────────────────────────────────────
run("connections.list", DT + ["connections", "list"], contains="local_mariadb")
run("connections.list.json", DT + ["--format", "json", "connections", "list"])
run("connections.test.ok", DT + ["connections", "test", DB], contains="MariaDB")
run("connections.test.bad", DT + ["connections", "test", "no_such_conn"], contains="not found")

# ── CORE: query ──────────────────────────────────────────────────────
run("query.select", DT + ["query", "--conn", DB, "--sql", "SELECT 1 AS one", "--format", "json"], contains='"one"')
run("query.version", DT + ["query", "--conn", DB, "--sql", "SELECT VERSION() AS v", "--format", "json"], contains="mariadb")
run("query.infoschema", DT + ["query", "--conn", DB, "--sql",
    "SELECT table_name FROM information_schema.tables WHERE table_schema='test'", "--format", "json"])
run("query.csv", DT + ["query", "--conn", DB, "--sql", "SELECT 1 AS a, 2 AS b", "--format", "csv"], contains="a")
run("query.syntax_err", DT + ["query", "--conn", DB, "--sql", "SELEC 1"], expect_ok=True)  # error in payload, exit may be 0

# ── CORE: objects ────────────────────────────────────────────────────
for t in ("databases", "engines", "charsets", "processlist", "users"):
    run(f"objects.{t}", DT + ["objects", "--conn", DB, "--type", t, "--format", "json"])
run("objects.bad_type", DT + ["objects", "--conn", DB, "--type", "nonsense", "--format", "json"])

# ── CORE: databases / config / app ───────────────────────────────────
run("databases.types", DT + ["databases", "types"], contains="MySQL")
run("databases.ops", DT + ["databases", "ops", "--type", "MySQL"])
run("config.show", DT + ["config", "show"])
run("config.list", DT + ["config", "list"])
run("config.describe", DT + ["config", "describe"])
run("app.shortcuts", DT + ["app", "shortcuts"])
run("app.dashboard-layout.show", DT + ["app", "dashboard-layout", "show"])
run("modules.list", DT + ["modules"])

# ── MIGRATOR ─────────────────────────────────────────────────────────
run("migrator.config", DT + ["migrator", "config", "show"])
run("migrator.row-counts", DT + ["migrator", "row-counts", "--conn", DB,
    "--tables", "test.PRODUCTS"], timeout=60)
run("migrator.sample", DT + ["migrator", "sample", "--conn", DB,
    "--tables", "test.PRODUCTS", "--limit", "3"], timeout=60)
run("migrator.show", DT + ["migrator", "show", "--conn", DB, "--table", "test.PRODUCTS"], timeout=60)
run("migrator.dump", DT + ["migrator", "dump", "--conn", DB, "--table", "test.PRODUCTS"], timeout=60)

# ── AI ───────────────────────────────────────────────────────────────
run("ai.list-backends", AI + ["ai", "--list-backends"], contains="backend")
run("ai.session.list", AI + ["ai", "session", "list"])
run("ai.session.new", AI + ["ai", "session", "new", "--conn", DB], contains="session")

# ── MONITORING ───────────────────────────────────────────────────────
run("mon.connections.list", MON + ["monitor-connections", "list"], contains="local_mariadb")
run("mon.connections.names", MON + ["monitor-connections", "names"])
run("mon.monitor.once", MON + ["monitor", "--conn", DB, "--once"], timeout=60, contains="Connections")
run("mon.monitor.json", MON + ["monitor", "--conn", DB, "--once", "--format", "json"], timeout=60)
run("mon.monitor-db.list", MON + ["monitor-db", "list"])
run("mon.os.metrics", MON + ["os", "metrics"], contains="cpu_utilization")
run("mon.thresholds.list", MON + ["thresholds", "list"], contains="cpu")
run("mon.thresholds.show", MON + ["thresholds", "show", "--source", "os", "--metric", "cpu_utilization"])
run("mon.thresholds.check", MON + ["thresholds", "check", "--source", "os",
    "--metric", "cpu_utilization", "--value", "99"])
run("mon.thresholds.show.dbpath", MON + ["thresholds", "show", "--source", "db",
    "--metric", "buffer_pool_usage_pct", "--path", "mysql"])
run("mon.alerts.list", MON + ["alerts", "list"])
run("mon.monitor-config.show", MON + ["monitor-config", "show"])
run("mon.notify.config", MON + ["notify", "config", "show"])
run("mon.daemon.status", MON + ["daemon", "status"])
run("mon.cloud.connections.list", MON + ["cloud", "connections", "list"], contains=CLOUD)
run("mon.cloud.metrics", MON + ["cloud", "metrics", "--name", CLOUD], timeout=120, contains="Cpu")

# ── per-module core dispatch parity (schema_converter / ai_query) ─────
run("sc.connections.list", SC + ["connections", "list"])
run("ai.connections.list", AI + ["connections", "list"])
run("sc.databases.types", SC + ["databases", "types"])
run("ai.query.select", AI + ["query", "--conn", DB, "--sql", "SELECT 1 AS one", "--format", "json"])


# ── REPORT ───────────────────────────────────────────────────────────
passed = sum(1 for *_, ok, _ in [(r[0], r[1], r[2], r[3]) for r in results] if ok)
fails = [r for r in results if not r[2]]
print("=" * 78)
print(f"CLI EXHAUSTIVE: {len(results)} commands | PASSED {len(results)-len(fails)} | FAILED {len(fails)}")
print("=" * 78)
for label, args, ok, out in results:
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {label}")
    if not ok:
        print(f"       args: {args}")
        print(f"       out : {out[:200]}")
print("\nFAILED SUMMARY:", [r[0] for r in fails] or "none")
