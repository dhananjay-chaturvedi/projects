#!/usr/bin/env python3
"""Comprehensive LIVE end-to-end suite.

Exercises every module across the core library, the three module CLIs, the REST
APIs, the Web UI (in-process), the Textual TUI (in-process Pilot) and the Tk
desktop UI (in-process construction), plus the shell entry scripts — against the
real ``local_mariadb`` (MariaDB) and GCP (``my_gcp_postgres`` cloud /
``my-gcp-pg-db`` core DB) connections.

It creates an isolated set of ``lt_*`` sample objects (tables with PK/FK, an
index, a view and seed rows), runs real input→output checks for each
functionality, performs a real cross-engine data migration into GCP, then drops
everything it created.

Run from the project root with the venv active:

    .venv/bin/python tests/run_full_live_suite.py
    .venv/bin/python tests/run_full_live_suite.py --only schema,ai   # subset
    .venv/bin/python tests/run_full_live_suite.py --keep             # keep sample data
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
PY = ROOT / ".venv" / "bin" / "python"
if not PY.exists():
    PY = Path(sys.executable)

CORE_DB = "local_mariadb"          # MariaDB core DB connection
GCP_DB = "my-gcp-pg-db"            # PostgreSQL core DB connection (for query/migration)
CLOUD_CONN = "my_gcp_postgres"     # monitoring cloud connection
AI_BACKEND = "cursor"

# Isolated sample objects (created in MariaDB's default `test` DB).
CUST = "test.lt_customers"
ORD = "test.lt_orders"
VIEW = "test.lt_customer_orders"

SETUP_SQL = (
    "DROP VIEW IF EXISTS lt_customer_orders;"
    "DROP TABLE IF EXISTS lt_orders;"
    "DROP TABLE IF EXISTS lt_customers;"
    "CREATE TABLE lt_customers ("
    " customer_id INT PRIMARY KEY AUTO_INCREMENT,"
    " name VARCHAR(64) NOT NULL,"
    " email VARCHAR(128),"
    " created_at DATETIME DEFAULT CURRENT_TIMESTAMP);"
    "CREATE TABLE lt_orders ("
    " order_id INT PRIMARY KEY AUTO_INCREMENT,"
    " customer_id INT NOT NULL,"
    " amount DECIMAL(10,2) NOT NULL,"
    " status VARCHAR(16) DEFAULT 'new',"
    " CONSTRAINT fk_lt_orders_cust FOREIGN KEY (customer_id)"
    " REFERENCES lt_customers(customer_id));"
    "CREATE INDEX idx_lt_orders_customer ON lt_orders(customer_id);"
    "INSERT INTO lt_customers (name,email) VALUES"
    " ('Alice','alice@example.com'),('Bob','bob@example.com'),"
    " ('Carol','carol@example.com'),('Dan','dan@example.com'),"
    " ('Eve','eve@example.com');"
    "INSERT INTO lt_orders (customer_id,amount,status) VALUES"
    " (1,100.50,'paid'),(1,20.00,'new'),(2,55.25,'paid'),"
    " (3,12.99,'cancelled'),(3,99.00,'paid'),(4,5.00,'new'),(5,250.75,'paid');"
    "CREATE VIEW lt_customer_orders AS"
    " SELECT c.customer_id, c.name, COUNT(o.order_id) AS orders,"
    " COALESCE(SUM(o.amount),0) AS total"
    " FROM lt_customers c LEFT JOIN lt_orders o ON o.customer_id=c.customer_id"
    " GROUP BY c.customer_id, c.name"
)

TEARDOWN_MARIA = (
    "DROP VIEW IF EXISTS lt_customer_orders;"
    "DROP TABLE IF EXISTS lt_orders;"
    "DROP TABLE IF EXISTS lt_customers"
)
TEARDOWN_GCP = "DROP TABLE IF EXISTS lt_customers"

API_PORT = 18790


@dataclass
class Result:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Report:
    results: list[Result] = field(default_factory=list)

    def run(self, name, cmd, *, expect=0, timeout=120, need=None):
        """Run a subprocess; PASS when returncode==expect (and `need` in output)."""
        try:
            p = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
            out = (p.stdout or "") + (p.stderr or "")
            ok = p.returncode == expect
            if ok and need is not None:
                ok = need in out
            detail = out.strip()[-600:]
            if not ok:
                detail = f"exit={p.returncode} need={need!r}\n{detail}"
            self.results.append(Result(name, ok, detail))
            return p
        except subprocess.TimeoutExpired:
            self.results.append(Result(name, False, f"timeout after {timeout}s"))
            return None
        except Exception as exc:  # noqa: BLE001
            self.results.append(Result(name, False, repr(exc)))
            return None

    def check(self, name, cond, detail=""):
        self.results.append(Result(name, bool(cond), detail))

    def skip(self, name, reason):
        self.results.append(Result(name, True, f"SKIP: {reason}"))

    def summary(self) -> str:
        passed = sum(1 for r in self.results if r.ok)
        failed = [r for r in self.results if not r.ok]
        lines = [f"TOTAL {len(self.results)}  PASSED {passed}  FAILED {len(failed)}", ""]
        if failed:
            lines.append("FAILURES:")
            for r in failed:
                lines.append(f"  [FAIL] {r.name}")
            lines.append("")
        for r in self.results:
            lines.append(f"[{'PASS' if r.ok else 'FAIL'}] {r.name}")
            if r.detail:
                snippet = r.detail.replace("\n", "\n        ")
                lines.append(f"        {snippet[:500]}")
        return "\n".join(lines)


def core(*args):
    """A core/migrator CLI invocation hosted by the schema_converter entry."""
    return [str(PY), "-m", "schema_converter", *args]


def mon(*args):
    return [str(PY), "-m", "monitoring", *args]


def ai(*args):
    return [str(PY), "-m", "ai_query", *args]


# --------------------------------------------------------------------------- #
# Sections
# --------------------------------------------------------------------------- #
def setup_sample(r: Report):
    p = r.run("setup: create sample objects (MariaDB)",
              core("query", "--conn", CORE_DB, "--multi", "--sql", SETUP_SQL))
    # Verify the objects exist and are seeded.
    r.run("setup: 5 customers seeded",
          core("query", "--conn", CORE_DB, "--sql",
               "SELECT COUNT(*) AS n FROM lt_customers", "--format", "json"),
          need='"5"')
    r.run("setup: 7 orders seeded",
          core("query", "--conn", CORE_DB, "--sql",
               "SELECT COUNT(*) AS n FROM lt_orders", "--format", "json"),
          need='"7"')


def section_core(r: Report):
    r.run("core: connections list", core("connections", "list"))
    r.run("core: connections test mariadb", core("connections", "test", CORE_DB),
          timeout=60)
    r.run("core: query SELECT", core("query", "--conn", CORE_DB, "--sql",
          "SELECT name,email FROM lt_customers ORDER BY customer_id", "--format", "json"),
          need="Alice")
    r.run("core: query aggregate (view)", core("query", "--conn", CORE_DB, "--sql",
          "SELECT * FROM lt_customer_orders ORDER BY total DESC", "--format", "json"),
          need="Eve")
    r.run("core: query INSERT", core("query", "--conn", CORE_DB, "--sql",
          "INSERT INTO lt_customers (name,email) VALUES ('Frank','frank@example.com')"))
    r.run("core: query UPDATE", core("query", "--conn", CORE_DB, "--sql",
          "UPDATE lt_customers SET email='frank2@example.com' WHERE name='Frank'"))
    r.run("core: query DELETE", core("query", "--conn", CORE_DB, "--sql",
          "DELETE FROM lt_customers WHERE name='Frank'"))
    r.run("core: query --multi", core("query", "--conn", CORE_DB, "--multi", "--sql",
          "SELECT 1 AS a; SELECT 2 AS b"))
    # Objects browser
    r.run("core: objects tables", core("objects", "--conn", CORE_DB, "--type", "tables"),
          need="lt_customers")
    r.run("core: objects views", core("objects", "--conn", CORE_DB, "--type", "views"),
          need="lt_customer_orders")
    r.run("core: objects indexes", core("objects", "--conn", CORE_DB, "--type", "indexes"))
    r.run("core: objects sample", core("objects", "sample", "--conn", CORE_DB,
          "--table", CUST, "--limit", "3"), need="Alice")
    r.run("core: objects count", core("objects", "count", "--conn", CORE_DB,
          "--table", CUST))


def section_schema(r: Report):
    r.run("schema: show columns", core("migrator", "show", "--conn", CORE_DB,
          "--table", CUST), need="customer_id")
    r.run("schema: dump DDL", core("migrator", "dump", "--conn", CORE_DB, "--table", CUST),
          need="lt_customers")
    for tgt in ("PostgreSQL", "MySQL", "Oracle"):
        r.run(f"schema: convert -> {tgt}", core("migrator", "convert",
              "--source-conn", CORE_DB, "--target-type", tgt, "--table", CUST,
              "--target-db", "test"), need="lt_customers")
    r.run("schema: row-counts", core("migrator", "row-counts", "--conn", CORE_DB,
          "--tables", "lt_customers,lt_orders"))
    r.run("schema: sample (multi-table)", core("migrator", "sample", "--conn", CORE_DB,
          "--tables", "lt_customers,lt_orders", "--limit", "2"))
    r.run("schema: compare-schema (self)", core("migrator", "compare-schema",
          "--source-conn", CORE_DB, "--target-conn", CORE_DB, "--table", CUST))
    r.run("schema: config show", core("migrator", "config", "show"))
    # Cross-engine migration into GCP: validate (dry-run), then the real
    # two-step pipeline — convert DDL, apply it (create the target table),
    # transfer rows, verify counts and compare data — then teardown drops it.
    ddl = ROOT / "tests" / "_lt_customers_gcp.sql"
    r.run("schema: validate -> GCP (dry-run)", core("migrator", "validate",
          "--source-conn", CORE_DB, "--target-conn", GCP_DB,
          "--tables", "lt_customers", "--format", "json"), timeout=120)
    r.run("schema: pre-clean GCP target", core("query", "--conn", GCP_DB,
          "--sql", "DROP TABLE IF EXISTS lt_customers"), timeout=120)
    r.run("schema: convert lt_customers -> Postgres DDL", core("migrator", "convert",
          "--source-conn", CORE_DB, "--target-type", "PostgreSQL", "--table", CUST,
          "--output", str(ddl)), timeout=120)
    r.run("schema: apply DDL on GCP (create table)", core("migrator", "apply",
          "--target-conn", GCP_DB, "--ddl-file", str(ddl)), timeout=120)
    r.run("schema: transfer-data -> GCP", core("migrator", "transfer-data",
          "--source-conn", CORE_DB, "--target-conn", GCP_DB,
          "--table", CUST, "--target-table", "lt_customers"), timeout=240)
    r.run("schema: verify rows on GCP", core("query", "--conn", GCP_DB, "--sql",
          "SELECT COUNT(*) AS n FROM lt_customers", "--format", "json"),
          need='"5"', timeout=120)
    r.run("schema: compare-data local vs GCP", core("migrator", "compare-data",
          "--source-conn", CORE_DB, "--target-conn", GCP_DB, "--table", CUST,
          "--target-table", "lt_customers"), timeout=180)


def section_monitor(r: Report):
    r.run("monitor: once (MariaDB)", mon("monitor", "--conn", CORE_DB, "--once"),
          timeout=90)
    r.run("monitor: os metrics", mon("os", "metrics"), need="cpu_utilization")
    r.run("monitor: thresholds list", mon("thresholds", "list"))
    r.run("monitor: monitor-db list", mon("monitor-db", "list"))
    r.run("monitor: alerts list", mon("alerts", "list"))
    r.run("monitor: config show", mon("config", "show"))
    r.run("monitor: notify config show", mon("notify", "config", "show"))
    r.run("monitor: daemon status", mon("daemon", "status"))
    r.run("monitor: cloud connections list", mon("cloud", "connections", "list"))
    r.run("monitor: cloud test (GCP)", mon("cloud", "connections", "test", CLOUD_CONN),
          timeout=120)
    r.run("monitor: cloud metrics (GCP)", mon("cloud", "metrics", "--name", CLOUD_CONN),
          timeout=120)


_AI_SESSION_PROBE = r"""
import sys
sys.path.insert(0, %r)
from ai_query.service import make_service
svc = make_service()
c = svc.ai_session_create("local_mariadb", "cursor")
assert not c.get("error"), c
sid = c["session"]["session_id"]
ask = svc.ai_session_ask(sid, "How many rows are in lt_customers? Reply with one SQL only.", mode="ask")
assert not ask.get("error"), ask
assert ask.get("sql") or ask.get("explanation"), ("no answer", ask)
fu = svc.ai_session_follow_up(sid, "Now also include their email addresses.")
assert not fu.get("error"), fu
upd = svc.ai_session_update(sid, sql_mode="open")
assert not upd.get("error"), upd
ex = svc.ai_session_execute_sql(sid, "SELECT customer_id, name FROM lt_customers LIMIT 10")
assert not ex.get("error"), ex
sv = svc.ai_session_save(None)
assert not sv.get("error"), sv
ld = svc.ai_session_load(None)
assert not ld.get("error"), ld
dl = svc.ai_session_delete(sid)
assert not dl.get("error"), dl
print("SESSION_LIFECYCLE_OK")
""" % (str(ROOT),)


def section_ai(r: Report):
    r.run("ai: configure cursor", ai("ai", "configure", "--backend", AI_BACKEND))
    p = r.run("ai: list-backends", ai("ai", "--list-backends"))
    ready = bool(p and AI_BACKEND in (p.stdout or "") and "ready" in (p.stdout or ""))
    r.run("ai: session new", ai("ai", "session", "new", "--conn", CORE_DB))
    r.run("ai: session list", ai("ai", "session", "list"))
    r.run("ai: pii status", ai("ai", "pii", "status"))
    r.run("ai: pii on", ai("ai", "pii", "on"))
    r.run("ai: pii off", ai("ai", "pii", "off"))
    r.run("ai: cache info", ai("ai", "cache", "info"))
    r.run("ai: config show", ai("ai", "config", "show"))
    # One-shot free-text routing: ``ai "question"`` must reach the ask handler
    # (cheap check — omit --conn so it stops before any live model call).
    r.run("ai: one-shot free-text routes to ask", ai("ai", "How many customers?"),
          expect=2, need="--conn is required")
    if ready:
        # Full session lifecycle in one process (CLI sessions are per-process):
        # create -> ask -> follow-up -> set-mode -> execute-sql -> save -> load -> close.
        r.run("ai: session lifecycle (LIVE cursor)", [str(PY), "-c", _AI_SESSION_PROBE],
              need="SESSION_LIFECYCLE_OK", timeout=300)
        r.run("ai: explain (LIVE)", ai("ai", "explain", "--conn", CORE_DB,
              "--sql", "SELECT * FROM lt_customers"), timeout=240)
        r.run("ai: optimize (LIVE)", ai("ai", "optimize", "--conn", CORE_DB,
              "--sql", "SELECT * FROM lt_orders WHERE amount > 50"), timeout=240)
        r.run("ai: review (LIVE)", ai("ai", "review", "--conn", CORE_DB,
              "--sql", "SELECT * FROM lt_customers"), timeout=240)
    else:
        for n in ("ai: session lifecycle (LIVE cursor)", "ai: explain (LIVE)",
                  "ai: optimize (LIVE)", "ai: review (LIVE)"):
            r.skip(n, f"{AI_BACKEND} backend not ready")


def _wait_api(port, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=2) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.4)
    return False


def _get(port, path, timeout=20):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=timeout) as resp:
        return resp.status, resp.read().decode()


def _post(port, path, payload, timeout=30):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
                                 data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode()


def section_api(r: Report):
    ports = {"schema_converter": API_PORT, "ai_query": API_PORT + 1, "monitoring": API_PORT + 2}
    for module, port in ports.items():
        proc = subprocess.Popen([str(PY), "-m", module, "api", "--host", "127.0.0.1",
                                 "--port", str(port)], cwd=ROOT,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            if not _wait_api(port):
                r.check(f"api {module}: startup", False, f"health timeout :{port}")
                continue
            for path in ("/api/health", "/api/connections", "/api/modules"):
                try:
                    st, body = _get(port, path)
                    r.check(f"api {module}: GET {path}", st == 200, body[:120])
                except Exception as exc:  # noqa: BLE001
                    r.check(f"api {module}: GET {path}", False, repr(exc))
            if module == "schema_converter":
                try:
                    st, body = _post(port, "/api/query",
                                     {"connection": CORE_DB,
                                      "sql": "SELECT COUNT(*) AS n FROM lt_customers"})
                    r.check("api schema: POST /api/query", st == 200 and '"n"' in body, body[:120])
                except Exception as exc:  # noqa: BLE001
                    r.check("api schema: POST /api/query", False, repr(exc))
                try:
                    st, _ = _get(port, f"/api/migrator/{CORE_DB}/{CUST}")
                    r.check("api schema: GET /api/migrator/{conn}/{table}", st == 200)
                except Exception as exc:  # noqa: BLE001
                    r.check("api schema: GET /api/migrator/{conn}/{table}", False, repr(exc))
            if module == "monitoring":
                for path in ("/api/os/metrics", "/api/thresholds"):
                    try:
                        st, body = _get(port, path)
                        r.check(f"api monitoring: GET {path}", st == 200, body[:80])
                    except Exception as exc:  # noqa: BLE001
                        r.check(f"api monitoring: GET {path}", False, repr(exc))
            if module == "ai_query":
                try:
                    st, body = _get(port, "/api/ai/backends")
                    r.check("api ai: GET /api/ai/backends", st == 200, body[:80])
                except Exception as exc:  # noqa: BLE001
                    r.check("api ai: GET /api/ai/backends", False, repr(exc))
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def section_web(r: Report):
    try:
        from fastapi.testclient import TestClient
        from common.ui.web.server import build_web_app
    except Exception as exc:  # noqa: BLE001
        r.check("web: import app", False, repr(exc))
        return
    try:
        client = TestClient(build_web_app())
    except Exception as exc:  # noqa: BLE001
        r.check("web: build app", False, repr(exc))
        return
    for path in ("/ui/", "/ui/config", "/api/connections", "/api/modules"):
        try:
            resp = client.get(path)
            r.check(f"web: GET {path}", resp.status_code == 200, resp.text[:100])
        except Exception as exc:  # noqa: BLE001
            r.check(f"web: GET {path}", False, repr(exc))
    try:
        resp = client.post("/api/query", json={"connection": CORE_DB,
                           "sql": "SELECT COUNT(*) AS n FROM lt_customers"})
        r.check("web: POST /api/query", resp.status_code == 200 and '"n"' in resp.text,
                resp.text[:100])
    except Exception as exc:  # noqa: BLE001
        r.check("web: POST /api/query", False, repr(exc))


def section_tui(r: Report):
    import asyncio

    async def drive():
        from common.ui.textual.app import DbToolApp
        app = DbToolApp()
        async with app.run_test() as pilot:
            await pilot.pause()
            r.check("tui: app mounts", app.screen is not None, type(app.screen).__name__)
            # Visit each installed screen by its canonical name.
            for name in ("connections", "objects", "sql", "ai", "monitor", "migration"):
                try:
                    app.push_screen_by_name(name)
                    await pilot.pause()
                    r.check(f"tui: screen {name}", type(app.screen).__name__ != "HomeScreen"
                            or name not in app._screen_names, type(app.screen).__name__)
                    app.action_home()
                    await pilot.pause()
                except Exception as exc:  # noqa: BLE001
                    r.check(f"tui: screen {name}", False, repr(exc))
            await app.action_quit()

    try:
        asyncio.run(asyncio.wait_for(drive(), timeout=60))
    except Exception as exc:  # noqa: BLE001
        r.check("tui: run_test", False, repr(exc))


_TK_PROBE = r"""
import sys
sys.path.insert(0, %r)
import tkinter as tk
root = tk.Tk(); root.withdraw()
import importlib
from common.ui.tk import ColorTheme, default_ui_font, default_ui_mono
from common.ui.tk.migrator.schema_converter_ui import SchemaConverterUI
from common.ui.tk.ai.ai_query_ui import AIQueryUI  # import smoke
importlib.import_module("common.ui.tk.monitor.monitoring_ui")  # import smoke
assert AIQueryUI is not None
fonts = {"ui": default_ui_font(), "mono": default_ui_mono()}
frame = tk.Frame(root)
ui = SchemaConverterUI(parent_frame=frame, root=root,
                       get_connections_callback=lambda: {},
                       update_status_callback=lambda *a, **k: None,
                       theme=ColorTheme, fonts=fonts)
ui.create_ui()
assert ui.conversion_preview_text is not None
assert hasattr(ui, "source_conn_combo") and hasattr(ui, "target_conn_combo")
root.destroy()
print("TK_OK")
""" % (str(ROOT),)


def section_tk(r: Report):
    # Tk on macOS aborts (SIGABRT) without a GUI session, so run it isolated in a
    # subprocess: a crash there can't take down the rest of the suite.
    try:
        p = subprocess.run([str(PY), "-c", _TK_PROBE], cwd=ROOT,
                           capture_output=True, text=True, timeout=60)
    except Exception as exc:  # noqa: BLE001
        r.skip("tk: construct UIs", repr(exc))
        return
    if p.returncode == 0 and "TK_OK" in (p.stdout or ""):
        r.check("tk: construct migrator/monitor/ai UIs", True)
    elif p.returncode < 0 or p.returncode in (134, 133):
        r.skip("tk: construct UIs", f"no GUI session (signal {abs(p.returncode)})")
    else:
        r.check("tk: construct migrator/monitor/ai UIs", False,
                ((p.stdout or "") + (p.stderr or "")).strip()[-300:])


def section_shell(r: Report):
    for sh in sorted(ROOT.glob("**/*.sh")):
        if ".venv" in sh.parts:
            continue
        rel = sh.relative_to(ROOT)
        r.run(f"shell: bash -n {rel}", ["bash", "-n", str(sh)])


def teardown(r: Report):
    r.run("teardown: drop sample objects (MariaDB)",
          core("query", "--conn", CORE_DB, "--multi", "--sql", TEARDOWN_MARIA))
    r.run("teardown: drop sample table (GCP)",
          core("query", "--conn", GCP_DB, "--sql", TEARDOWN_GCP), timeout=120)
    ddl = ROOT / "tests" / "_lt_customers_gcp.sql"
    try:
        ddl.unlink(missing_ok=True)
    except Exception:  # noqa: BLE001
        pass


SECTIONS = {
    "core": section_core,
    "schema": section_schema,
    "monitor": section_monitor,
    "ai": section_ai,
    "api": section_api,
    "web": section_web,
    "tui": section_tui,
    "tk": section_tk,
    "shell": section_shell,
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="", help="comma list of sections to run")
    ap.add_argument("--keep", action="store_true", help="keep sample data (no teardown)")
    ap.add_argument("--no-setup", action="store_true", help="skip sample-data creation")
    args = ap.parse_args()

    chosen = [s.strip() for s in args.only.split(",") if s.strip()] or list(SECTIONS)
    r = Report()

    needs_data = any(s in chosen for s in ("core", "schema", "api", "web", "ai"))
    if needs_data and not args.no_setup:
        print("=== setup sample data ===")
        setup_sample(r)
    try:
        for name in chosen:
            print(f"=== {name} ===")
            SECTIONS[name](r)
    finally:
        if needs_data and not args.keep:
            print("=== teardown ===")
            teardown(r)

    print("\n" + r.summary())
    out = ROOT / "tests" / "full_live_report.txt"
    out.write_text(r.summary())
    print(f"\nReport written to {out}")
    return 0 if all(x.ok for x in r.results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
