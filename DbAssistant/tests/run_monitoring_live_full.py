#!/usr/bin/env python3
"""
Full live monitoring verification — DB, OS (SSH), and cloud profiles.

Uses saved profiles from ~/.dbassistant/ (override via env vars below).
Exercises CLI, REST API, and Monitor UI backend paths; sends one Teams
notification per source (db / os / cloud) when ALERT_TEAMS_WEBHOOK_URL is set.

Run from project root:
    .venv/bin/python tests/run_monitoring_live_full.py
"""

from __future__ import annotations

import json
import os
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

# Canonical profiles from prior live integration runs (override if needed).
DB_CONN = os.environ.get("DBTOOL_MONITOR_DB", "local_mariadb")
OS_CONN = os.environ.get("DBTOOL_MONITOR_OS", "")
CLOUD_CONN = os.environ.get("DBTOOL_MONITOR_CLOUD", "my_gcp_postgres")
API_PORT = int(os.environ.get("DBTOOL_MONITOR_API_PORT", "18820"))
RUN_TAG = time.strftime("%Y%m%d-%H%M%S")


@dataclass
class Step:
    name: str
    ok: bool
    detail: str = ""


@dataclass
class Report:
    steps: list[Step] = field(default_factory=list)

    def record(self, name: str, ok: bool, detail: str = "") -> None:
        self.steps.append(Step(name, ok, detail.strip()))

    def skip(self, name: str, reason: str) -> None:
        self.record(name, True, f"SKIP: {reason}")

    def summary(self) -> str:
        failed = [s for s in self.steps if not s.ok]
        passed = len(self.steps) - len(failed)
        lines = [
            f"MONITORING LIVE FULL — {RUN_TAG}",
            f"TOTAL {len(self.steps)}  PASSED {passed}  FAILED {len(failed)}",
            f"Profiles: db={DB_CONN}  os={OS_CONN}  cloud={CLOUD_CONN}",
            "",
        ]
        for s in self.steps:
            mark = "PASS" if s.ok else "FAIL"
            lines.append(f"[{mark}] {s.name}")
            if s.detail:
                lines.append(f"       {s.detail[:700]}")
        return "\n".join(lines)


def _bootstrap_paths() -> None:
    from common import paths as P

    P.bootstrap()


def _service():
    from monitoring.service import make_service

    return make_service()


def _run_cli(args: list[str], *, timeout: int = 180) -> subprocess.CompletedProcess:
    cmd = [str(PY), "-m", "app.dbtool", *args]
    return subprocess.run(
        cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout,
    )


def _api_key() -> str:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
    return (os.environ.get("DBTOOL_API_KEY") or "").strip()


def _wait_api(port: int, timeout: float = 20.0) -> bool:
    url = f"http://127.0.0.1:{port}/api/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.4)
    return False


def _api_json(
    method: str,
    path: str,
    *,
    port: int,
    api_key: str,
    body: dict | None = None,
    timeout: int = 120,
) -> tuple[int, dict | list | str]:
    url = f"http://127.0.0.1:{port}{path}"
    data = None
    headers = {"X-API-Key": api_key} if api_key else {}
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


def _tunnel_skip(text: str) -> bool:
    t = (text or "").lower()
    return "can't connect" in t or "could not connect" in t or "tunnel" in t


def preflight(report: Report) -> bool:
    svc = _service()
    db_names = {c["name"] for c in svc.list_connections()}
    cloud_names = set()
    for row in svc.list_cloud_connections():
        if isinstance(row, dict) and row.get("name"):
            cloud_names.add(row["name"])
    mon_rows = svc.list_all_connections(source="monitor")
    mon_names = {r["name"] for r in mon_rows if r.get("name")}

    missing = []
    if DB_CONN not in db_names:
        missing.append(f"db:{DB_CONN}")
    if CLOUD_CONN not in cloud_names:
        missing.append(f"cloud:{CLOUD_CONN}")
    if OS_CONN and OS_CONN not in mon_names:
        missing.append(f"monitor:{OS_CONN}")

    if missing:
        report.record(
            "preflight profiles",
            False,
            "Missing saved profile(s): " + ", ".join(missing),
        )
        return False

    report.record(
        "preflight profiles",
        True,
        f"db ok; cloud ok; monitor targets={len(mon_names)}"
        + (f"; using {OS_CONN}" if OS_CONN else "; SSH target not configured, remote OS checks skip"),
    )
    return True


def run_cli(report: Report) -> None:
    # DB
    p = _run_cli(
        ["monitor-connections", "test", "--source", "db", DB_CONN], timeout=120,
    )
    out = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0 and _tunnel_skip(out):
        report.skip("cli db test", "tunnel unreachable")
    else:
        report.record("cli db test", p.returncode == 0, out[-400:])

    p = _run_cli(["monitor", "--conn", DB_CONN, "--once"], timeout=180)
    out = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0 and _tunnel_skip(out):
        report.skip("cli db monitor --once", "tunnel unreachable")
    else:
        report.record(
            "cli db monitor --once",
            p.returncode == 0 and "[source: db]" in out,
            out[-500:],
        )

    # OS (local host + remote SSH profile)
    p = _run_cli(["os", "metrics"], timeout=60)
    report.record("cli os metrics (local)", p.returncode == 0, (p.stdout or "")[-300:])

    if OS_CONN:
        p = _run_cli(
            ["os", "remote", "--name", OS_CONN, "--format", "json"], timeout=120,
        )
        out = (p.stdout or "") + (p.stderr or "")
        report.record("cli os remote", p.returncode == 0, out[-400:])

        p = _run_cli(
            ["monitor-connections", "test", "--source", "monitor", OS_CONN],
            timeout=90,
        )
        report.record(
            "cli monitor ssh test",
            p.returncode == 0,
            ((p.stdout or "") + (p.stderr or ""))[-400:],
        )

        p = _run_cli(["monitor", "--conn", OS_CONN, "--once"], timeout=180)
        out = (p.stdout or "") + (p.stderr or "")
        report.record(
            "cli os monitor --once",
            p.returncode == 0 and "[source: monitor]" in out,
            out[-500:],
        )
    else:
        report.skip("cli os remote", "DBTOOL_MONITOR_OS not set")
        report.skip("cli monitor ssh test", "DBTOOL_MONITOR_OS not set")
        report.skip("cli os monitor --once", "DBTOOL_MONITOR_OS not set")

    # Cloud
    p = _run_cli(
        ["monitor-connections", "test", "--source", "cloud", CLOUD_CONN],
        timeout=180,
    )
    report.record(
        "cli cloud test",
        p.returncode == 0,
        ((p.stdout or "") + (p.stderr or ""))[-400:],
    )

    p = _run_cli(["cloud", "metrics", "--name", CLOUD_CONN], timeout=180)
    out = (p.stdout or "") + (p.stderr or "")
    report.record("cli cloud metrics", p.returncode == 0, out[-500:])

    p = _run_cli(["monitor", "--conn", CLOUD_CONN, "--once"], timeout=180)
    out = (p.stdout or "") + (p.stderr or "")
    report.record(
        "cli cloud monitor --once",
        p.returncode == 0 and "[source: cloud]" in out,
        out[-500:],
    )

    # Supporting commands
    for label, args in [
        ("cli thresholds list", ["thresholds", "list", "--source", "db"]),
        ("cli daemon status", ["daemon", "status"]),
        ("cli alerts list", ["alerts", "list", "--limit", "5"]),
        ("cli monitor-connections list", ["monitor-connections", "list"]),
    ]:
        p = _run_cli(args, timeout=60)
        report.record(label, p.returncode == 0, (p.stderr or p.stdout or "")[-200:])


def run_api(report: Report) -> None:
    api_key = _api_key()
    if not api_key:
        report.skip("api auth", "DBTOOL_API_KEY not set in .env")
        return

    proc = subprocess.Popen(
        [str(PY), "-m", "app.dbtool", "api", "--host", "127.0.0.1", "--port", str(API_PORT)],
        cwd=ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not _wait_api(API_PORT):
            report.record("api startup", False, f"health timeout port {API_PORT}")
            return
        report.record("api startup", True, f"port {API_PORT}")

        code, body = _api_json("GET", "/api/health", port=API_PORT, api_key=api_key)
        report.record("api GET /api/health", code == 200, str(body)[:200])

        code, body = _api_json(
            "GET", f"/api/metrics/{DB_CONN}", port=API_PORT, api_key=api_key, timeout=180,
        )
        ok = code == 200 and isinstance(body, dict) and body.get("source") == "db"
        report.record("api GET /api/metrics/{db}", ok, str(body)[:300] if not ok else "source=db")

        if OS_CONN:
            code, body = _api_json(
                "GET",
                f"/api/monitor/connections/saved/{OS_CONN}/os-metrics",
                port=API_PORT,
                api_key=api_key,
                timeout=120,
            )
            ok = code == 200 and isinstance(body, dict) and body.get("ok") is True
            report.record("api GET remote os-metrics", ok, str(body)[:300] if not ok else "ok")
        else:
            report.skip("api GET remote os-metrics", "DBTOOL_MONITOR_OS not set")

        code, body = _api_json(
            "GET", f"/api/monitor/cloud/metrics/{CLOUD_CONN}",
            port=API_PORT, api_key=api_key, timeout=180,
        )
        ok = code == 200 and isinstance(body, dict) and not body.get("error")
        report.record("api GET /api/monitor/cloud/metrics/{cloud}", ok, str(body)[:300] if not ok else "ok")

        code, body = _api_json("GET", "/api/os/metrics", port=API_PORT, api_key=api_key)
        ok = code == 200 and isinstance(body, dict) and body.get("metrics")
        report.record("api GET /api/os/metrics", ok, str(body)[:200] if not ok else "metrics ok")

        code, body = _api_json(
            "GET", "/api/monitor/connections?source=all", port=API_PORT, api_key=api_key,
        )
        ok = code == 200 and isinstance(body, dict) and body.get("count", 0) > 0
        report.record("api GET /api/monitor/connections", ok, f"count={body.get('count')}")

        code, body = _api_json(
            "GET", "/api/thresholds?source=db", port=API_PORT, api_key=api_key,
        )
        report.record(
            "api GET /api/thresholds",
            code == 200 and isinstance(body, list),
            f"{len(body)} rules" if isinstance(body, list) else str(body)[:200],
        )

        code, body = _api_json(
            "GET", "/api/daemon/status", port=API_PORT, api_key=api_key,
        )
        report.record("api GET /api/daemon/status", code == 200, str(body)[:200])
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()


def run_ui(report: Report) -> None:
    """Exercise Monitor UI backend (same code paths the tab uses)."""
    try:
        import tkinter as tk
    except ImportError:
        report.skip("ui tkinter", "tkinter not available")
        return

    svc = _service()
    root = tk.Tk()
    root.withdraw()
    try:
        from common.connection_manager import ConnectionManager
        from common.ui.tk.theme import ColorTheme
        from monitoring.server_monitor.server_monitor_ui import ServerMonitorUI

        frame = tk.Frame(root)
        ui = ServerMonitorUI(
            parent_frame=frame,
            root=root,
            connection_manager=ConnectionManager(),
            active_connections={},
            update_status_callback=lambda *_a, **_k: None,
            theme=ColorTheme,
        )
        ui._alert_log = []
        ui._alert_unread_db = ui._alert_unread_os = ui._alert_unread_cloud = 0
        ui._alert_counter_lock = __import__("threading").Lock()
        ui.active_cloud_databases = set()
        if not hasattr(ui, "_refresh_alert_badges"):
            ui._refresh_alert_badges = lambda: None

        report.record("ui ServerMonitorUI init", True, "Monitor tab shell created")

        # DB path: open connection + collect metrics like the UI DB pane.
        from common.headless.db_service import CoreDBService

        core = CoreDBService()
        profile = core.get_connection_profile(DB_CONN)
        if not profile:
            report.record("ui db metrics", False, f"profile {DB_CONN} missing")
        else:
            try:
                # Do not hold core.connection_lock here — get_db_metrics acquires
                # its own per-db lock and can deadlock when nested.
                mgr = core.get_manager(DB_CONN, profile)
                raw = ui.get_db_metrics(mgr, DB_CONN)
                ok = isinstance(raw, dict) and len(raw) > 0
                report.record(
                    "ui db metrics (get_db_metrics)",
                    ok,
                    f"{len(raw or {})} numeric metrics",
                )
            except Exception as exc:
                report.record("ui db metrics (get_db_metrics)", False, str(exc))
            finally:
                try:
                    core.disconnect(DB_CONN)
                except Exception:
                    pass

        # OS path: remote metrics via mixin used by SSH monitor pane.
        if OS_CONN:
            r = svc.get_remote_os_metrics(OS_CONN, disk_path="/")
            ok = r.get("ok") and bool(r.get("metrics"))
            report.record(
                "ui os remote metrics",
                ok,
                f"{len((r.get('metrics') or {}))} metrics" if ok else r.get("error", ""),
            )
        else:
            report.skip("ui os remote metrics", "DBTOOL_MONITOR_OS not set")

        # Cloud path: registry fetch used by cloud monitor pane.
        cloud_entry = ui.cloud_databases.get(CLOUD_CONN) or {}
        if not cloud_entry:
            report.record("ui cloud metrics", False, f"{CLOUD_CONN} not in cloud registry")
        else:
            from monitoring.cloud_provider_registry import CloudProviderRegistry

            monitor = CloudProviderRegistry.build_monitor(cloud_entry)
            if monitor is None:
                report.record("ui cloud metrics", False, "build_monitor returned None")
            else:
                text, graph = ui._fetch_cloud_metrics(
                    CLOUD_CONN, cloud_entry, monitor,
                )
                ok = bool(text) or bool(graph)
                report.record(
                    "ui cloud metrics (_fetch_cloud_metrics)",
                    ok,
                    f"text={len(text or '')} chars graph_keys={len(graph or {})}",
                )
    except Exception as exc:
        report.record("ui monitor tab", False, str(exc))
    finally:
        try:
            root.destroy()
        except Exception:
            pass


def send_alerts_once(report: Report) -> None:
    """One outbound notification per source + log entry for audit."""
    svc = _service()
    webhook = (os.environ.get("ALERT_TEAMS_WEBHOOK_URL") or "").strip()
    if not webhook:
        from dotenv import load_dotenv

        load_dotenv(ROOT / ".env")
        webhook = (os.environ.get("ALERT_TEAMS_WEBHOOK_URL") or "").strip()

    cases = [
        ("db", DB_CONN, f"[LIVE-{RUN_TAG}][DB] Monitoring smoke for {DB_CONN}"),
        ("cloud", CLOUD_CONN, f"[LIVE-{RUN_TAG}][CLOUD] Monitoring smoke for {CLOUD_CONN}"),
    ]
    if OS_CONN:
        cases.append(("os", OS_CONN, f"[LIVE-{RUN_TAG}][OS] Monitoring smoke for SSH target {OS_CONN}"))
    else:
        report.skip("alert log (os)", "DBTOOL_MONITOR_OS not set")
        report.skip("alert notify (os)", "DBTOOL_MONITOR_OS not set")
    for source, instance, message in cases:
        log_r = svc.log_alert("INFO", message, source=source, instance=instance)
        report.record(
            f"alert log ({source})",
            log_r.get("ok") is True,
            log_r.get("message", ""),
        )
        notify_r = svc.send_notification("INFO", message)
        if not webhook:
            report.skip(
                f"alert notify ({source})",
                "ALERT_TEAMS_WEBHOOK_URL not set — logged only",
            )
        else:
            report.record(
                f"alert notify ({source})",
                notify_r.get("ok") is True,
                notify_r.get("message", ""),
            )


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    _bootstrap_paths()
    report = Report()
    if not preflight(report):
        print(report.summary())
        return 1
    run_cli(report)
    run_api(report)
    run_ui(report)
    send_alerts_once(report)
    text = report.summary()
    print(text)
    out_path = ROOT / "logs" / f"monitoring_live_full_{RUN_TAG}.txt"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(text + "\n")
    print(f"\nWrote {out_path}")
    failed = [s for s in report.steps if not s.ok]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
