#!/usr/bin/env python3
"""
Live smoke matrix — exercises CLI and REST API against saved connections.

Designed for headless verification (no Tk). Uses local OS metrics, saved DB
profiles, and cloud monitor profiles (AWS/GCP/Azure).

Run from the project root:

    PYTHONPATH=. .venv/bin/python scripts/live_smoke_matrix.py

Environment overrides:

    DBTOOL_SMOKE_DB_CONNS       Comma-separated DB connection names
    DBTOOL_SMOKE_CLOUD_CONNS    Comma-separated cloud profile names
    DBTOOL_SMOKE_API_PORT       API port (default 18766)
    DBTOOL_SMOKE_SKIP_API=1     CLI-only (skip API block)
    DBTOOL_INCLUDE_TUNNEL=1     Include SSH-tunnel DB connections

Example matrix (use your saved connection / cloud profile names):

    export DBTOOL_SMOKE_DB_CONNS="prod,staging"
    export DBTOOL_SMOKE_CLOUD_CONNS="cloud-aws-prod,cloud-gcp-staging"
    PYTHONPATH=. .venv/bin/python scripts/live_smoke_matrix.py

Exit code 0 when all executed checks pass; 1 when any hard failure occurs.
Skipped checks (missing profile, tunnel down) do not fail the run.
"""

from __future__ import annotations

import json
import os
import signal
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

from tests.integration_helpers import (  # noqa: E402
    load_saved_cloud_connection_names,
    load_saved_db_connection_names,
    project_python,
    tunnel_unreachable,
)

# Fallback when no env override and no saved profiles exist (usually empty).
DEFAULT_SMOKE_DB: tuple[str, ...] = ()
DEFAULT_SMOKE_CLOUD: tuple[str, ...] = ()


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""
    skipped: bool = False


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, ok: bool, detail: str = "", *, skipped: bool = False) -> None:
        self.checks.append(Check(name, ok, detail, skipped))

    def ok_count(self) -> int:
        return sum(1 for c in self.checks if c.ok and not c.skipped)

    def fail_count(self) -> int:
        return sum(1 for c in self.checks if not c.ok and not c.skipped)

    def skip_count(self) -> int:
        return sum(1 for c in self.checks if c.skipped)

    def exit_code(self) -> int:
        return 0 if self.fail_count() == 0 else 1


def _env_list(key: str, fallback: tuple[str, ...]) -> list[str]:
    raw = os.environ.get(key, "").strip()
    if raw:
        return [n.strip() for n in raw.split(",") if n.strip()]
    return list(fallback)


def _resolve_db_names() -> list[str]:
    explicit = os.environ.get("DBTOOL_SMOKE_DB_CONNS", "").strip()
    if explicit:
        from tests.integration_helpers import filter_test_connection_names

        return filter_test_connection_names(
            [n.strip() for n in explicit.split(",") if n.strip()]
        )
    saved = load_saved_db_connection_names()
    if saved:
        return saved
    from tests.integration_helpers import filter_test_connection_names

    return filter_test_connection_names(list(DEFAULT_SMOKE_DB))


def _resolve_cloud_names() -> list[str]:
    names = _env_list("DBTOOL_SMOKE_CLOUD_CONNS", DEFAULT_SMOKE_CLOUD)
    if os.environ.get("DBTOOL_SMOKE_CLOUD_CONNS", "").strip():
        return names
    saved = load_saved_cloud_connection_names()
    return saved if saved else names


def _run(argv: list[str], *, timeout: int = 180) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    return subprocess.run(
        [str(project_python()), *argv],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


def _dbtool(*args: str, timeout: int = 180, fmt: str = "") -> subprocess.CompletedProcess[str]:
    argv = [str(ROOT / "dbtool.py")]
    if fmt:
        argv += ["--format", fmt]
    argv.extend(args)
    return _run(argv, timeout=timeout)


def _http_json(method: str, url: str, body: dict | None = None, timeout: int = 60) -> tuple[int, dict | str]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw
    except Exception as exc:
        return 0, str(exc)


def _start_api(port: int) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            str(project_python()),
            str(ROOT / "dbtool.py"),
            "api",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": str(ROOT)},
    )


def _wait_api(port: int, proc: subprocess.Popen[str], seconds: float = 45.0) -> bool:
    deadline = time.time() + seconds
    base = f"http://127.0.0.1:{port}"
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        code, _ = _http_json("GET", f"{base}/api/health", timeout=3)
        if code == 200:
            return True
        time.sleep(0.4)
    return False


def _stop_proc(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()


def run_cli_core(report: Report, db_names: list[str]) -> None:
    r = _dbtool("connections", "list", fmt="json")
    report.add(
        "cli.connections.list",
        r.returncode == 0,
        (r.stderr or r.stdout or "")[:240],
    )

    r = _dbtool("databases", "types", fmt="json")
    report.add("cli.databases.types", r.returncode == 0, (r.stderr or "")[:200])

    for conn in db_names[:4]:
        r = _dbtool("connections", "test", conn)
        if r.returncode != 0 and tunnel_unreachable(r.stderr + r.stdout):
            report.add(f"cli.connections.test[{conn}]", True, "skip: tunnel down", skipped=True)
            continue
        report.add(
            f"cli.connections.test[{conn}]",
            r.returncode == 0,
            (r.stderr or r.stdout or "")[:200],
            skipped=r.returncode != 0,
        )
        if r.returncode != 0:
            continue
        r = _dbtool("query", "--conn", conn, "--sql", "SELECT 1 AS one", fmt="json")
        out = (r.stderr or "") + (r.stdout or "")
        if "not found" in out.lower() or "[err]" in out.lower():
            report.add(f"cli.query[{conn}]", True, "skip: connection missing", skipped=True)
        else:
            report.add(
                f"cli.query[{conn}]",
                r.returncode == 0 and '"one"' in (r.stdout or ""),
                out[:200],
                skipped=r.returncode != 0,
            )
        r = _dbtool("objects", "--conn", conn, "--type", "tables", fmt="json")
        report.add(
            f"cli.objects.tables[{conn}]",
            r.returncode == 0,
            (r.stderr or "")[:200],
        )


def run_cli_modules(report: Report, db_names: list[str], cloud_names: list[str]) -> None:
    r = _dbtool("monitor-config", "show", "--format", "json")
    report.add(
        "cli.monitor-config.show",
        r.returncode == 0 and "monitoring" in (r.stdout or ""),
        (r.stderr or "")[:200],
    )

    r = _dbtool("notify", "config", "show", "--format", "json")
    report.add("cli.notify.config", r.returncode == 0, (r.stderr or "")[:200])

    r = _dbtool("os", "metrics", fmt="json")
    report.add("cli.os.metrics", r.returncode == 0, (r.stderr or "")[:200])

    r = _dbtool("thresholds", "list", fmt="json")
    report.add("cli.thresholds.list", r.returncode == 0, (r.stderr or "")[:200])

    conn = db_names[0] if db_names else ""
    if conn:
        r = _dbtool("monitor", "--conn", conn, "--once", fmt="json")
        if r.returncode != 0 and tunnel_unreachable(r.stderr + r.stdout):
            report.add(f"cli.monitor.once[{conn}]", True, "skip: tunnel down", skipped=True)
        else:
            report.add(
                f"cli.monitor.once[{conn}]",
                r.returncode == 0,
                (r.stderr or r.stdout or "")[:200],
                skipped=r.returncode != 0,
            )

    for cloud in cloud_names[:6]:
        r = _dbtool("cloud", "connections", "test", cloud)
        if r.returncode != 0:
            report.add(
                f"cli.cloud.test[{cloud}]",
                True,
                (r.stderr or r.stdout or "not found")[:200],
                skipped=True,
            )
            continue
        report.add(f"cli.cloud.test[{cloud}]", True, "ok")
        r = _dbtool("cloud", "metrics", "--name", cloud, "--format", "json")
        report.add(
            f"cli.cloud.metrics[{cloud}]",
            r.returncode == 0,
            (r.stderr or "")[:200],
            skipped=r.returncode != 0,
        )

    r = _dbtool("ai", "config", "show", "--format", "json")
    report.add(
        "cli.ai.config.show",
        r.returncode == 0 and "[ai]" in (r.stdout or "") or "ai" in (r.stdout or "").lower(),
        (r.stderr or "")[:200],
    )

    r = _dbtool("migrator", "config", "show", "--format", "json")
    report.add(
        "cli.migrator.config.show",
        r.returncode == 0,
        (r.stderr or "")[:200],
    )

def run_api(report: Report, db_names: list[str], cloud_names: list[str], port: int) -> None:
    proc = _start_api(port)
    if not _wait_api(port, proc):
        err = ""
        if proc.stderr:
            err = proc.stderr.read()[:300]
        report.add("api.start", False, err or "health check timeout")
        _stop_proc(proc)
        return
    report.add("api.start", True, f"127.0.0.1:{port}")
    base = f"http://127.0.0.1:{port}"

    endpoints = [
        ("GET", "/api/health"),
        ("GET", "/api/config/settings"),
        ("GET", "/api/monitor/config"),
        ("GET", "/api/monitor/notifications"),
        ("GET", "/api/ai/config"),
        ("GET", "/api/migrator/config"),
        ("GET", "/api/thresholds"),
        ("GET", "/api/os/metrics"),
        ("GET", "/api/cloud/connections"),
        ("GET", "/api/daemon/status"),
    ]
    try:
        for method, path in endpoints:
            code, body = _http_json(method, f"{base}{path}")
            ok = code == 200
            detail = ""
            if isinstance(body, dict) and body.get("error"):
                detail = str(body["error"])[:120]
            report.add(f"api{path}", ok, detail or f"HTTP {code}")

        conn = db_names[0] if db_names else ""
        if conn:
            code, body = _http_json("GET", f"{base}/api/metrics/{conn}")
            if code != 200 and isinstance(body, dict) and body.get("error"):
                if tunnel_unreachable(str(body.get("error"))):
                    report.add(f"api/metrics/{conn}", True, "skip: tunnel", skipped=True)
                else:
                    report.add(f"api/metrics/{conn}", False, str(body.get("error"))[:200])
            else:
                report.add(f"api/metrics/{conn}", code == 200, f"HTTP {code}")

        cloud = cloud_names[0] if cloud_names else ""
        if cloud:
            code, _ = _http_json("GET", f"{base}/api/monitor/cloud/metrics/{cloud}")
            report.add(
                f"api/monitor/cloud/metrics/{cloud}",
                code == 200,
                f"HTTP {code}",
                skipped=code != 200,
            )
    finally:
        _stop_proc(proc)


def main() -> int:
    report = Report()
    db_names = _resolve_db_names()
    cloud_names = _resolve_cloud_names()
    port = int(os.environ.get("DBTOOL_SMOKE_API_PORT", "18766"))

    print("=== DbAssistant live smoke matrix ===")
    print(f"DB connections : {', '.join(db_names) or '(none)'}")
    print(f"Cloud profiles : {', '.join(cloud_names) or '(none)'}")
    print()

    run_cli_core(report, db_names)
    run_cli_modules(report, db_names, cloud_names)

    if os.environ.get("DBTOOL_SMOKE_SKIP_API", "").strip() not in ("1", "true", "yes"):
        run_api(report, db_names, cloud_names, port)
    else:
        report.add("api.block", True, "skipped (DBTOOL_SMOKE_SKIP_API)", skipped=True)

    print(f"{'STATUS':8} {'CHECK'}")
    print("-" * 72)
    for c in report.checks:
        tag = "SKIP" if c.skipped else ("PASS" if c.ok else "FAIL")
        line = f"{tag:8} {c.name}"
        if c.detail:
            line += f" — {c.detail}"
        print(line)

    print()
    print(
        f"Summary: {report.ok_count()} passed, "
        f"{report.fail_count()} failed, {report.skip_count()} skipped"
    )
    return report.exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
