#!/usr/bin/env python3
"""Live integration smoke — run from project root with .venv active."""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / ".venv" / "bin" / "python"
if not PY.exists():
    PY = Path(sys.executable)

CORE_DB = "local_mariadb"
CLOUD_CONN = "my_gcp_postgres"
TEST_TABLE = "test.PRODUCTS"
API_PORT = 18765


@dataclass
class Result:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Report:
    results: list[Result] = field(default_factory=list)

    def run(self, name: str, cmd: list[str], *, expect: int = 0, timeout: int = 120) -> subprocess.CompletedProcess | None:
        try:
            p = subprocess.run(
                cmd,
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            ok = p.returncode == expect
            detail = (p.stdout or "")[-800:] + (p.stderr or "")[-400:]
            if not ok:
                detail = f"exit={p.returncode}\n{detail}"
            self.results.append(Result(name, ok, detail.strip()))
            return p
        except subprocess.TimeoutExpired:
            self.results.append(Result(name, False, "timeout"))
            return None
        except Exception as exc:
            self.results.append(Result(name, False, str(exc)))
            return None

    def ok(self, name: str, cond: bool, detail: str = "") -> None:
        self.results.append(Result(name, cond, detail))

    def skip(self, name: str, reason: str) -> None:
        self.results.append(Result(name, True, f"SKIP: {reason}"))

    def summary(self) -> str:
        passed = sum(1 for r in self.results if r.ok)
        failed = [r for r in self.results if not r.ok]
        lines = [
            f"TOTAL {len(self.results)}  PASSED {passed}  FAILED {len(failed)}",
            "",
        ]
        for r in self.results:
            mark = "PASS" if r.ok else "FAIL"
            lines.append(f"[{mark}] {r.name}")
            if r.detail:
                lines.append(f"       {r.detail[:500]}")
        return "\n".join(lines)


def _mod(module: str, *args: str) -> list[str]:
    return [str(PY), "-m", module, *args]


def _wait_for_api(port: int, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    url = f"http://127.0.0.1:{port}/api/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.5)
    return False


def test_core_cli(r: Report) -> None:
    for conn in [CORE_DB]:
        p = r.run(
            f"core connections test {conn}",
            _mod("schema_converter", "connections", "test", conn),
        )
        r.run(
            f"core query {conn}",
            _mod("schema_converter", "query", "--conn", conn, "--sql", "SELECT 1 AS n", "--format", "json"),
        )
        r.run(
            f"core objects tables {conn}",
            _mod("schema_converter", "objects", "--conn", conn, "--type", "tables", "--format", "json"),
        )


def test_schema_module(r: Report) -> None:
    conn = CORE_DB
    r.run("schema tables", _mod("schema_converter", "objects", "--conn", conn, "--type", "tables"))
    r.run("migrator show", _mod("schema_converter", "migrator", "show", "--conn", conn, "--table", TEST_TABLE))
    out = ROOT / "tmp_schema_convert_test.sql"
    r.run(
        "migrator convert",
        _mod(
            "schema_converter",
            "migrator",
            "convert",
            "--source-conn",
            conn,
            "--target-type",
            "MySQL",
            "--table",
            TEST_TABLE,
            "--output",
            str(out),
        ),
    )
    r.ok("migrator convert output file", out.exists() and out.stat().st_size > 0, str(out))


def test_ai_module(r: Report) -> None:
    conn = CORE_DB
    r.run("ai list-backends", _mod("ai_query", "ai", "--list-backends"))
    r.run("dbtool ai list-backends", [str(PY), "dbtool.py", "ai", "--list-backends"])
    r.run("ai session list", _mod("ai_query", "ai", "session", "list"))
    r.run(
        "ai session new",
        _mod("ai_query", "ai", "session", "new", "--conn", conn),
    )
    p = subprocess.run(
        _mod("ai_query", "ai", "--list-backends"),
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    out = (p.stdout or "").lower()
    has_ready_backend = p.returncode == 0 and any(
        " ready" in line or line.strip().endswith("ready")
        for line in (p.stdout or "").splitlines()
    )
    if has_ready_backend:
        r.run(
            "ai ask one-shot",
            _mod("ai_query", "ai", "--conn", conn, "How many tables are in this database? Reply with one SQL only."),
            timeout=180,
        )
    else:
        r.skip("ai ask one-shot", p.stdout[:200] or "no backend configured")


def test_monitor_module(r: Report) -> None:
    for conn in [CORE_DB]:
        r.run(
            f"monitor once {conn}",
            _mod("monitoring", "monitor", "--conn", conn, "--once"),
            timeout=90,
        )
    r.run("os metrics", _mod("monitoring", "os", "metrics"))
    r.run("thresholds list", _mod("monitoring", "thresholds", "list"))
    r.run("cloud connections list", _mod("monitoring", "cloud", "connections", "list"))
    r.run(
        f"cloud test {CLOUD_CONN}",
        _mod("monitoring", "cloud", "connections", "test", CLOUD_CONN),
        timeout=120,
    )
    r.run(
        f"cloud metrics {CLOUD_CONN}",
        _mod("monitoring", "cloud", "metrics", "--name", CLOUD_CONN),
        timeout=120,
    )
    r.run("daemon status", _mod("monitoring", "daemon", "status"))


def test_api(module: str, r: Report) -> None:
    port = API_PORT + {"schema_converter": 0, "ai_query": 1, "monitoring": 2}[module]
    proc = subprocess.Popen(
        _mod(module, "api", "--host", "127.0.0.1", "--port", str(port)),
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not _wait_for_api(port):
            r.ok(f"api {module} startup", False, f"health check timeout on port {port}")
            return
        for path in ("/api/health", "/api/connections", "/api/modules"):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as resp:
                    body = resp.read().decode()
                    r.ok(f"api {module} GET {path}", resp.status == 200, body[:120])
            except urllib.error.URLError as exc:
                r.ok(f"api {module} GET {path}", False, str(exc))
        if module == "schema_converter":
            conn = CORE_DB
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/query",
                data=json.dumps({"connection": conn, "sql": "SELECT 1 AS n"}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    r.ok(f"api {module} POST /api/query", resp.status == 200)
            except Exception as exc:
                r.ok(f"api {module} POST /api/query", False, str(exc))
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/api/migrator/{conn}/{TEST_TABLE}",
                    timeout=30,
                ) as resp:
                    r.ok(f"api {module} GET /api/migrator/{{conn}}/{{table}}", resp.status == 200)
            except Exception as exc:
                r.ok(f"api {module} GET /api/migrator/{{conn}}/{{table}}", False, str(exc))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_ui_imports(r: Report) -> None:
    script = """
from common.ui.tk.launcher import launch_desktop_ui
from common.ui.tk.cloud_connection_dialog import CloudConnectionWizardAdapter, run_cloud_connection_wizard
from schema_converter.schema_converter_ui import launch_ui as schema_ui
from ai_query.ai_query_ui import launch_ui as ai_ui
from monitoring.monitoring_ui import launch_ui as mon_ui
import inspect
assert hasattr(CloudConnectionWizardAdapter, '_open_cloud_provider_form')
assert hasattr(CloudConnectionWizardAdapter, 'add_cloud_database')
print('ui imports ok')
"""
    r.run("ui imports + wizard adapter", [str(PY), "-c", script])


def main() -> int:
    r = Report()
    print("=== Core CLI (via schema_converter) ===")
    test_core_cli(r)
    print("=== Schema module ===")
    test_schema_module(r)
    print("=== AI module ===")
    test_ai_module(r)
    print("=== Monitoring module ===")
    test_monitor_module(r)
    print("=== API per module ===")
    for mod in ("schema_converter", "ai_query", "monitoring"):
        test_api(mod, r)
    print("=== UI entry points ===")
    test_ui_imports(r)
    print("\n" + r.summary())
    out = ROOT / "tests" / "live_integration_report.txt"
    out.write_text(r.summary())
    print(f"\nReport written to {out}")
    return 0 if all(x.ok for x in r.results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
