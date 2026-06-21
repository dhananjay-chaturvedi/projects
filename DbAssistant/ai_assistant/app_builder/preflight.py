"""Deterministic compile + import dry-run for built apps.

This is Session C's *code-level* gate — run before the build is accepted and
before the app is launched. It catches the failure class that otherwise only
shows up as a silent crash when the user clicks "Start app":

* every generated ``.py`` must COMPILE (no syntax errors), and
* the ASGI app (``src.app:app``) plus each top-level ``src`` module must
  IMPORT cleanly (no import-time crash) on the prototype's default SQLite DB.

All checks are deterministic (no AI/tokens). The orchestrator feeds the result
into the gate + the validation evidence handed to Session C, and the UI reuses
``launch_env`` / ``import_app_check`` so launching the app uses the exact same
environment that was just verified.
"""

from __future__ import annotations

import importlib.util
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# Directories that are never part of the generated app's own source.
_SKIP_DIRS = frozenset({".venv", "venv", "__pycache__", "node_modules",
                        ".git", ".pytest_cache", ".mypy_cache", "build", "dist"})


@dataclass
class PreflightResult:
    """Outcome of the deterministic compile + import dry-run."""

    compiled: bool = True
    imported: bool = True
    syntax_errors: list[str] = field(default_factory=list)
    import_error: str = ""
    module_errors: list[str] = field(default_factory=list)
    checked_modules: list[str] = field(default_factory=list)
    boot: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.compiled and self.imported and not self.module_errors

    def issues(self) -> list[str]:
        out: list[str] = []
        for e in self.syntax_errors[:8]:
            out.append(f"syntax error: {e}")
        if self.import_error:
            out.append(f"app import failed: {self.import_error.splitlines()[-1][:300]}"
                       if self.import_error.strip() else "app import failed")
        for e in self.module_errors[:8]:
            out.append(f"module import failed: {e}")
        return out

    def digest(self) -> str:
        """One compact block of evidence for Session C."""
        lines = [
            f"compiles (no syntax errors): {self.compiled}",
            f"app imports (src.app:app dry-run): {self.imported}",
        ]
        if self.checked_modules:
            ok_mods = len(self.checked_modules) - len(self.module_errors)
            lines.append(
                f"modules importable: {ok_mods}/{len(self.checked_modules)}")
        for e in self.syntax_errors[:6]:
            lines.append(f"  syntax: {e}")
        if self.import_error:
            lines.append(f"  import: {self.import_error.strip().splitlines()[-1][:200]}"
                         if self.import_error.strip() else "  import: failed")
        for e in self.module_errors[:6]:
            lines.append(f"  module: {e}")
        return "\n".join(lines)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "compiled": self.compiled,
            "imported": self.imported,
            "syntax_errors": list(self.syntax_errors),
            "import_error": self.import_error,
            "module_errors": list(self.module_errors),
            "checked_modules": list(self.checked_modules),
            "boot": dict(self.boot),
        }


@dataclass
class BootResult:
    """Outcome of a TestClient lifespan boot check."""

    ok: bool = True
    health_status: int = 0
    health_payload: dict[str, Any] = field(default_factory=dict)
    root_status: int = 0
    root_body_len: int = 0
    error: str = ""

    def digest(self) -> str:
        if self.ok:
            return "boot check: PASSED"
        bits = ["boot check: FAILED"]
        if self.health_status:
            bits.append(f"  GET /health: HTTP {self.health_status}")
        if self.root_status:
            bits.append(f"  GET /: HTTP {self.root_status}")
        if self.error:
            bits.append(f"  error: {self.error}")
        return "\n".join(bits)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "health_status": self.health_status,
            "health_payload": dict(self.health_payload),
            "root_status": self.root_status,
            "root_body_len": self.root_body_len,
            "error": self.error,
        }


def _iter_py_files(workspace: Path):
    for path in sorted(workspace.rglob("*.py")):
        if _SKIP_DIRS & set(path.parts):
            continue
        yield path


def launch_env(workspace: Path, *, use_real_db: bool = False) -> dict[str, str]:
    """Environment for importing/launching the prototype.

    By default the prototype runs on a local SQLite file inside its own
    workspace, so it ALWAYS boots (with the schema/sample-data its
    ``connection.py`` seeds) and never crashes trying to reach a remote
    MariaDB or because ``DATABASE_URL`` points elsewhere. Pass
    ``use_real_db=True`` to honor an externally provided ``DATABASE_URL``.
    """
    env: dict[str, str] = dict(os.environ)
    env["APP_WORKSPACE"] = str(workspace)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if not use_real_db:
        env.pop("DATABASE_URL", None)
        var_dir = workspace / "var"
        var_dir.mkdir(parents=True, exist_ok=True)
        env["APP_DB_PATH"] = str(var_dir / "app.db")
    return env


def test_db_env(workspace: Path) -> dict[str, str]:
    """Environment for generated app tests and boot checks.

    Uses a file-backed SQLite DB under ``var/`` so schema created during
    lifespan/startup is visible to later requests. ``:memory:`` creates a fresh
    database per connection and can hide or create false launch failures.
    """
    env = launch_env(workspace)
    var_dir = workspace / "var"
    var_dir.mkdir(parents=True, exist_ok=True)
    db_path = var_dir / "test_runtime.db"
    try:
        db_path.unlink()
    except FileNotFoundError:
        pass
    except OSError:
        pass
    env["APP_DB_PATH"] = str(db_path)
    env.pop("DBASSIST_DB_PATH", None)
    return env


def compile_check(
    workspace: Optional[Path] = None,
    *,
    files: Optional[dict[str, str]] = None,
) -> list[str]:
    """Compile every ``.py`` (in-memory map or on disk); return syntax errors."""
    errors: list[str] = []
    if files is not None:
        for rel, content in files.items():
            if not rel.endswith(".py"):
                continue
            try:
                compile(content or "", rel, "exec")
            except SyntaxError as exc:  # noqa: PERF203
                errors.append(f"{rel}:{exc.lineno}: {exc.msg}")
        return errors
    if workspace is None:
        return errors
    for path in _iter_py_files(workspace):
        try:
            src = path.read_text(encoding="utf-8")
        except OSError:
            continue
        rel = path.relative_to(workspace)
        try:
            compile(src, str(rel), "exec")
        except SyntaxError as exc:
            errors.append(f"{rel}:{exc.lineno}: {exc.msg}")
    return errors


def _module_names(workspace: Path) -> list[str]:
    """Top-level importable ``src`` modules (dotted), excluding ``__init__``."""
    src = workspace / "src"
    if not src.is_dir():
        return []
    mods: list[str] = []
    for path in sorted(src.rglob("*.py")):
        if _SKIP_DIRS & set(path.parts):
            continue
        if path.name == "__init__.py":
            continue
        rel = path.relative_to(workspace).with_suffix("")
        mods.append(".".join(rel.parts))
    return mods


def import_app_check(
    workspace: Path,
    *,
    module: str = "src.app",
    attr: str = "app",
    timeout: int = 60,
    use_real_db: bool = False,
) -> tuple[bool, str]:
    """Import ``module:attr`` in a clean subprocess; return (ok, error_text)."""
    code = (
        "import importlib\n"
        f"m = importlib.import_module({module!r})\n"
        f"obj = getattr(m, {attr!r}, None)\n"
        f"assert obj is not None, 'missing {attr} in {module}'\n"
        "print('IMPORT_OK')\n"
    )
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code],
            cwd=str(workspace),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=launch_env(workspace, use_real_db=use_real_db),
        )
    except Exception as exc:  # noqa: BLE001
        return False, f"import dry-run error: {exc}"
    if proc.returncode == 0 and "IMPORT_OK" in (proc.stdout or ""):
        return True, ""
    return False, (proc.stderr or proc.stdout or "import failed").strip()[-2000:]


def boot_check(
    workspace: Path,
    *,
    timeout: int = 60,
    use_real_db: bool = False,
) -> BootResult:
    """Boot ``src.app:app`` with TestClient so lifespan/startup actually runs."""
    result = BootResult()
    if not (workspace / "src" / "app.py").is_file():
        result.ok = False
        result.error = "no src/app.py — cannot boot-check app"
        return result
    code = (
        "import importlib, json\n"
        "from fastapi.testclient import TestClient\n"
        "payload = {'ok': True, 'health_status': 0, 'health_payload': {}, "
        "'root_status': 0, 'root_body_len': 0, 'error': ''}\n"
        "try:\n"
        "    mod = importlib.import_module('src.app')\n"
        "    app = getattr(mod, 'app', None)\n"
        "    assert app is not None, 'missing app in src.app'\n"
        "    with TestClient(app, raise_server_exceptions=False) as client:\n"
        "        h = client.get('/health')\n"
        "        payload['health_status'] = int(h.status_code)\n"
        "        try:\n"
        "            payload['health_payload'] = h.json()\n"
        "        except Exception:\n"
        "            payload['health_payload'] = {}\n"
        "        r = client.get('/')\n"
        "        payload['root_status'] = int(r.status_code)\n"
        "        payload['root_body_len'] = len(r.text or '')\n"
        "        payload['ok'] = h.status_code == 200 and r.status_code < 500\n"
        "except Exception as exc:\n"
        "    payload['ok'] = False\n"
        "    payload['error'] = type(exc).__name__ + ': ' + str(exc)\n"
        "print('BOOT_JSON:' + json.dumps(payload))\n"
    )
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code],
            cwd=str(workspace),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=(launch_env(workspace, use_real_db=use_real_db)
                 if use_real_db else test_db_env(workspace)),
        )
    except Exception as exc:  # noqa: BLE001
        result.ok = False
        result.error = f"boot check error: {exc}"
        return result
    marker = "BOOT_JSON:"
    payload: dict[str, Any] = {}
    for line in (proc.stdout or "").splitlines():
        if line.startswith(marker):
            try:
                payload = json.loads(line[len(marker):])
            except ValueError:
                payload = {}
            break
    if not payload:
        result.ok = False
        err = (proc.stderr or proc.stdout or "boot check failed").strip()
        result.error = err[-2000:]
        return result
    result.ok = bool(payload.get("ok"))
    result.health_status = int(payload.get("health_status") or 0)
    result.health_payload = dict(payload.get("health_payload") or {})
    result.root_status = int(payload.get("root_status") or 0)
    result.root_body_len = int(payload.get("root_body_len") or 0)
    result.error = str(payload.get("error") or "")
    if proc.returncode != 0 and not result.error:
        result.ok = False
        result.error = (proc.stderr or proc.stdout or "boot check failed").strip()[-2000:]
    return result


def module_smoke(
    workspace: Path, *, timeout: int = 60, use_real_db: bool = False,
) -> tuple[list[str], list[str]]:
    """Import each top-level ``src`` module in one clean subprocess.

    Returns ``(checked_modules, errors)`` where each error is
    ``"<module>: <last error line>"``. This is the deterministic "dry run of
    every code block" — it proves each module loads before any unit test runs.
    """
    mods = _module_names(workspace)
    if not mods:
        return [], []
    code = (
        "import importlib, json\n"
        f"mods = {mods!r}\n"
        "errs = {}\n"
        "for name in mods:\n"
        "    try:\n"
        "        importlib.import_module(name)\n"
        "    except Exception as exc:\n"
        "        errs[name] = type(exc).__name__ + ': ' + str(exc)\n"
        "print('SMOKE_JSON:' + json.dumps(errs))\n"
    )
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code],
            cwd=str(workspace),
            capture_output=True,
            text=True,
            timeout=timeout,
            env=launch_env(workspace, use_real_db=use_real_db),
        )
    except Exception as exc:  # noqa: BLE001
        return mods, [f"<smoke run>: {exc}"]
    marker = "SMOKE_JSON:"
    errors: list[str] = []
    for line in (proc.stdout or "").splitlines():
        if line.startswith(marker):
            import json
            try:
                data = json.loads(line[len(marker):])
            except ValueError:
                data = {}
            errors = [f"{k}: {str(v).splitlines()[-1][:200]}"
                      for k, v in data.items()]
            break
    else:
        # The subprocess crashed before reporting (e.g. a module ran code at
        # import that hard-exited): surface stderr so it is not silent.
        tail = (proc.stderr or "").strip().splitlines()
        if tail:
            errors = [f"<smoke run>: {tail[-1][:200]}"]
    return mods, errors


def dry_run(
    workspace: Path,
    *,
    files: Optional[dict[str, str]] = None,
    timeout: int = 60,
    use_real_db: bool = False,
    quick: bool = False,
) -> PreflightResult:
    """Deterministic preflight: compile + app import (+ per-module smoke).

    ``quick=True`` runs only compile + the ``src.app:app`` import dry-run (one
    subprocess) — cheap enough to run EVERY changed round during the build so
    issues are caught and conveyed to Session A continuously. The per-module
    smoke (a second subprocess) is reserved for the closing full check.
    """
    result = PreflightResult()
    result.syntax_errors = compile_check(workspace, files=files)
    result.compiled = not result.syntax_errors
    # A syntax error means imports cannot succeed; skip the (doomed) subprocess.
    if not result.compiled:
        result.imported = False
        result.import_error = "skipped — fix syntax errors first"
        return result
    ok, err = import_app_check(
        workspace, timeout=timeout, use_real_db=use_real_db)
    result.imported = ok
    result.import_error = err
    if ok and (workspace / "src" / "app.py").is_file():
        result.boot = boot_check(
            workspace, timeout=timeout, use_real_db=use_real_db).as_dict()
    if quick:
        return result
    mods, mod_errs = module_smoke(
        workspace, timeout=timeout, use_real_db=use_real_db)
    result.checked_modules = mods
    result.module_errors = mod_errs
    return result


@dataclass
class SmokeResult:
    """Outcome of an HTTP launch smoke test (uvicorn boot + a GET route crawl)."""

    ok: bool = True
    skipped: bool = False
    skip_reason: str = ""
    checks: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def digest(self) -> str:
        if self.skipped:
            return f"launch smoke: SKIPPED ({self.skip_reason or 'unavailable'})"
        lines = [f"launch smoke: {'PASSED' if self.ok else 'FAILED'}"]
        for chk in self.checks:
            path = chk.get("path", "?")
            code = chk.get("status_code", 0)
            lines.append(f"  GET {path}: HTTP {code}")
        for err in self.errors[:6]:
            lines.append(f"  error: {err}")
        return "\n".join(lines)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "checks": list(self.checks),
            "errors": list(self.errors),
        }


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _http_get(url: str, *, timeout: float = 5.0) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return int(getattr(resp, "status", 200) or 200), ""
    except urllib.error.HTTPError as exc:
        return int(exc.code), str(exc)
    except Exception as exc:  # noqa: BLE001
        return 0, str(exc)


def _http_fetch(url: str, *, timeout: float = 5.0) -> tuple[int, str, str]:
    """GET *url* returning ``(status_code, body, error)`` (0 on connect error)."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace")
            return int(getattr(resp, "status", 200) or 200), body, ""
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            pass
        return int(exc.code), body, str(exc)
    except Exception as exc:  # noqa: BLE001
        return 0, "", str(exc)


def _discover_get_routes(base: str, *, timeout: float = 5.0) -> list[str]:
    """Return param-free GET paths from the app's OpenAPI schema (best-effort).

    Routes with a required path/query parameter are skipped (we cannot guess a
    valid value), so the crawl only exercises pages/endpoints that should serve
    on their own — the ones a user lands on first.
    """
    code, body, _ = _http_fetch(f"{base}/openapi.json", timeout=timeout)
    if code != 200 or not body:
        return []
    try:
        spec = json.loads(body)
    except Exception:  # noqa: BLE001
        return []
    routes: list[str] = []
    for path, methods in (spec.get("paths") or {}).items():
        if not isinstance(path, str) or "{" in path:  # required path param — skip
            continue
        op = (methods or {}).get("get")
        if not isinstance(op, dict):
            continue
        params = op.get("parameters") or []
        if any(isinstance(p, dict) and p.get("required") for p in params):
            continue
        routes.append(path)
    return routes


def http_smoke(
    workspace: Path,
    *,
    timeout: int = 30,
    use_real_db: bool = False,
) -> SmokeResult:
    """Start uvicorn briefly, crawl the app's GET routes, then terminate.

    Boots the app, confirms ``GET /health`` is 200 and ``GET /`` renders a
    non-empty page, then crawls every param-free GET route discovered from the
    app's OpenAPI schema — any ``5xx`` is treated as a broken flow and fails the
    check. Best-effort: when uvicorn is not installed the check is *skipped*
    (not a failure). A missing ``src/app.py`` or a server that never boots is a
    failure.
    """
    result = SmokeResult()
    if not (workspace / "src" / "app.py").is_file():
        result.ok = False
        result.errors.append("no src/app.py — cannot smoke-test launch")
        return result
    if importlib.util.find_spec("uvicorn") is None:
        result.skipped = True
        result.skip_reason = "uvicorn not installed"
        return result

    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    cmd = [
        sys.executable, "-m", "uvicorn",
        "src.app:app",
        "--host", "127.0.0.1",
        "--port", str(port),
        "--log-level", "error",
    ]
    proc: Optional[subprocess.Popen[str]] = None
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(workspace),
            env=launch_env(workspace, use_real_db=use_real_db),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + max(5, timeout)
        booted = False
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                tail = (proc.stderr.read() if proc.stderr else "")[-500:]
                result.ok = False
                result.errors.append(
                    "uvicorn exited before boot"
                    + (f": {tail.strip()}" if tail.strip() else ""))
                return result
            code, _ = _http_get(f"{base}/health", timeout=2.0)
            if code == 200:
                booted = True
                break
            time.sleep(0.25)
        if not booted:
            result.ok = False
            result.errors.append(
                f"server did not respond on GET /health within {timeout}s")
            return result
        # Crawl the app's own param-free GET routes (discovered from OpenAPI) on
        # top of /health and /. A UI that looks fine but errors on its real routes
        # must fail the gate — not just a missing /health.
        routes = ["/health", "/"]
        for r in _discover_get_routes(base, timeout=5.0):
            if r not in routes:
                routes.append(r)
        for path in routes[:25]:
            code, body, err = _http_fetch(f"{base}{path}", timeout=5.0)
            result.checks.append({
                "path": path, "status_code": code,
                "body_len": len(body), "error": err,
            })
        # /health must report 200 (platform monitoring depends on it).
        health = next((c for c in result.checks if c.get("path") == "/health"), None)
        if not health or health.get("status_code") != 200:
            result.ok = False
            code = (health or {}).get("status_code", 0)
            msg = f"GET /health returned HTTP {code}"
            if health and health.get("error"):
                msg += f" ({health['error']})"
            result.errors.append(msg)
        # The landing page must render real content, not a blank shell.
        root = next((c for c in result.checks if c.get("path") == "/"), None)
        if root and root.get("status_code") == 200 and not root.get("body_len"):
            result.ok = False
            result.errors.append("GET / returned an empty page (no UI content)")
        # Any route that 5xx-es is a broken flow — the app does not actually work.
        broken = [c for c in result.checks
                  if 500 <= int(c.get("status_code") or 0) < 600]
        if broken:
            result.ok = False
            for c in broken[:6]:
                result.errors.append(
                    f"GET {c['path']} returned HTTP {c['status_code']} "
                    "(server error — broken flow)")
    except Exception as exc:  # noqa: BLE001
        result.ok = False
        result.errors.append(f"launch smoke error: {exc}")
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
    return result
