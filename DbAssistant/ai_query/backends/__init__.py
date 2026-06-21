"""
ai_query/backends/__init__.py
==============================
AI backend abstraction layer.

Every backend exposes the same two methods:
    is_available() -> bool
    call(prompt, timeout) -> {"response": str | None, "error": str | None}

The registry auto-detects which CLIs are installed and returns only
the ones that actually work on this machine.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import threading
from typing import Any, Optional

# Backend session IDs come back from a CLI and are later fed to ``--resume``.
# Restrict them to a safe character set so a corrupted/tampered session file can
# never smuggle extra CLI tokens into the subprocess invocation.
_SAFE_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")

from common.config_loader import console_print
from ai_query import module_config as mc


# ── CLI resolution ──────────────────────────────────────────────────────────────
#
# GUI / launchd processes on macOS (and terminals opened before a PATH change)
# frequently run with a minimal PATH (e.g. /usr/bin:/bin) that does NOT include
# user install dirs like ~/.local/bin.  Relying on shutil.which() alone then makes
# perfectly-installed CLIs look "not installed".  resolve_cli() falls back to a set
# of well-known install locations and honours an explicit config override.

_DEFAULT_COMMON_BIN_DIRS = [
    "~/.local/bin",
    "~/bin",
    "~/.claude/local",
    "/opt/homebrew/bin",
    "/usr/local/bin",
    "/usr/bin",
    "/opt/local/bin",
]


def _common_bin_dirs() -> list[str]:
    """Directories scanned (after PATH) when resolving a backend CLI.

    Configurable via ``[ai] cli_search_paths`` (comma-separated). Falls back to
    the built-in well-known install locations.
    """
    raw = mc.get("ai", "cli_search_paths", default="")
    dirs = [d.strip() for d in (raw or "").split(",") if d.strip()]
    return dirs or list(_DEFAULT_COMMON_BIN_DIRS)


def resolve_cli(command: str, override: str = "") -> Optional[str]:
    """
    Locate *command* and return an absolute path, or None if not found.

    Resolution order:
      1. explicit override (a full path to the binary, or a directory holding it)
      2. shutil.which() against the current PATH
      3. a scan of common user/system install directories
    """
    def _ok(p: str) -> bool:
        return bool(p) and os.path.isfile(p) and os.access(p, os.X_OK)

    if override:
        cand = os.path.expanduser(override)
        if _ok(cand):
            return cand
        joined = os.path.join(cand, command)
        if _ok(joined):
            return joined

    found = shutil.which(command)
    if found:
        return found

    for d in _common_bin_dirs():
        cand = os.path.join(os.path.expanduser(d), command)
        if _ok(cand):
            return cand
    return None


# ── Base class ────────────────────────────────────────────────────────────────

class AIBackend:
    """Abstract base for all AI CLI backends."""

    name: str = "base"          # short id used in dropdown / config
    display_name: str = "AI"    # human-readable label
    cli_command: str = ""       # executable to look for in PATH
    # Whether this backend can resume a prior conversation by replaying a
    # backend session id (so follow-ups keep the model's context). Backends
    # that ignore ``resume_session_id`` (stateless or non-conversational, e.g.
    # the local NL->SQL model) MUST leave this False so the app never pretends a
    # session can be resumed.
    supports_resume: bool = False

    def __init__(self):
        self._cli_path: Optional[str] = None
        self._available: Optional[bool] = None  # None = not yet checked
        self._unavail_reason: str = ""           # populated when _detect fails
        # Availability is written from a background probe thread and read from
        # the main/UI thread; guard it so reads/writes are consistently ordered
        # across threads and Python implementations (not just CPython's GIL).
        self._avail_lock = threading.Lock()

    # ── availability ─────────────────────────────────────────────────────────

    def is_available(self) -> bool:
        """Return cached availability. Does NOT probe if never checked."""
        with self._avail_lock:
            return bool(self._available)

    def is_checked(self) -> bool:
        """True if availability has already been determined (cached)."""
        with self._avail_lock:
            return self._available is not None

    def check_availability(self, force: bool = False) -> bool:
        """
        Run the actual availability probe (subprocess / network).
        Cached after first call unless force=True is passed.
        Call this explicitly — e.g. when the user selects a backend.
        """
        with self._avail_lock:
            if self._available is not None and not force:
                return self._available
        # Probe outside the lock (it may shell out / hit the network).
        reason = ""
        try:
            available = self._detect()
            reason = self._unavail_reason
        except Exception as exc:                     # noqa: BLE001
            available = False
            reason = self._unavail_reason or f"detection error: {exc}"
        with self._avail_lock:
            self._available = available
            self._unavail_reason = reason
        return available

    def get_unavailable_reason(self) -> str:
        return self._unavail_reason

    def _detect(self) -> bool:
        """Override to add extra checks beyond CLI resolution."""
        self._cli_path = resolve_cli(self.cli_command, self._cli_path_override())
        if self._cli_path is None:
            self._unavail_reason = f"'{self.cli_command}' not found on PATH"
            return False
        return True

    def _cli_path_override(self) -> str:
        """Optional explicit CLI path from module config (subclasses set section)."""
        return ""

    def _resolve_executable(self) -> str:
        """
        Return an absolute path to the CLI, resolving lazily if a probe hasn't
        run yet.  Falls back to the bare command name as a last resort so the
        subprocess can still surface a clear FileNotFoundError.
        """
        if not self._cli_path:
            self._cli_path = resolve_cli(self.cli_command, self._cli_path_override())
        return self._cli_path or self.cli_command

    # ── prompt call ──────────────────────────────────────────────────────────

    def call(
        self,
        prompt: str,
        timeout: int = 120,
        resume_session_id: Optional[str] = None,
    ) -> dict:
        """
        Send *prompt* to the AI and return:
            {"response": str, "error": None, "backend_session_id": str|None} on success
            {"response": None, "error": str, "backend_session_id": None} on failure
        """
        raise NotImplementedError

    # ── metadata ─────────────────────────────────────────────────────────────

    def get_info(self) -> dict:
        """Return display info for the status bar."""
        return {
            "provider": self.display_name,
            "model":    "default",
            "status":   "Connected" if self.is_available() else "Not Available",
            "note":     "CLI-based — no API key required",
            "resume_supported": bool(self.supports_resume),
        }

    # ── helpers ───────────────────────────────────────────────────────────────

    def _run(self, cmd: list[str], stdin_text: Optional[str] = None,
             timeout: Optional[int] = None) -> dict:
        """Shared subprocess runner used by concrete backends.

        Runs the CLI in its own process group (``start_new_session=True``) so a
        timeout can tear down the whole process tree — including any helper
        processes the CLI spawned — instead of orphaning them.
        """
        if timeout is None:
            timeout = mc.get_int("ai", "default_backend_timeout", default=120)
        proc: Optional[subprocess.Popen] = None
        try:
            console_print(
                f"[{self.name}] calling: {' '.join(cmd[:3])}... "
                f"(prompt {len(stdin_text or '')} chars, timeout {timeout}s)"
            )
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            try:
                stdout, stderr = proc.communicate(input=stdin_text, timeout=timeout)
            except subprocess.TimeoutExpired:
                self._terminate_process_tree(proc)
                msg = f"{self.display_name} timed out after {timeout}s"
                console_print(f"[{self.name}] {msg}")
                return {"response": None, "error": msg, "backend_session_id": None}

            if proc.returncode == 0:
                return {
                    "response": (stdout or "").strip(),
                    "error": None,
                    "backend_session_id": None,
                }
            err = (stderr or "").strip() or f"exit code {proc.returncode}"
            console_print(f"[{self.name}] error: {err}")
            return {
                "response": None,
                "error": f"{self.display_name} error: {err}",
                "backend_session_id": None,
            }

        except FileNotFoundError:
            msg = f"{self.display_name} CLI not found ({self.cli_command})"
            return {"response": None, "error": msg, "backend_session_id": None}
        except Exception as exc:
            if proc is not None:
                self._terminate_process_tree(proc)
            msg = f"{self.display_name} unexpected error: {exc}"
            console_print(f"[{self.name}] {msg}")
            return {"response": None, "error": msg, "backend_session_id": None}

    @staticmethod
    def _terminate_process_tree(proc: subprocess.Popen) -> None:
        """Kill *proc* and every process in its group, then reap it."""
        try:
            if os.name == "posix":
                try:
                    pgid = os.getpgid(proc.pid)
                except ProcessLookupError:
                    return
                try:
                    os.killpg(pgid, signal.SIGTERM)
                except ProcessLookupError:
                    return
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            else:
                proc.kill()
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        # Drain pipes / reap the zombie so the FD and PID are released.
        try:
            proc.communicate(timeout=3)
        except Exception:
            pass

    @staticmethod
    def _safe_resume_id(resume_session_id: Optional[str]) -> str:
        """Return the session id only if it matches a safe format, else ""."""
        sid = (resume_session_id or "").strip()
        return sid if _SAFE_SESSION_ID_RE.match(sid) else ""

    @staticmethod
    def _parse_json_payload(raw: str) -> tuple[str, Optional[str]]:
        """Extract text response and optional session id from JSON CLI output."""
        try:
            payload: Any = json.loads(raw)
        except json.JSONDecodeError:
            return raw, None
        if isinstance(payload, dict):
            text = (
                payload.get("result")
                or payload.get("response")
                or payload.get("content")
                or payload.get("text")
            )
            if text is None and payload.get("message"):
                text = payload.get("message")
            sid = payload.get("session_id") or payload.get("chat_id") or payload.get("id")
            if isinstance(text, str) and text.strip():
                return text.strip(), str(sid) if sid else None
        return raw, None


# ── Registry ──────────────────────────────────────────────────────────────────

class AIBackendRegistry:
    """
    Registers all known AI backends.  Detection is *lazy* — no subprocess
    or network calls happen until check_one() is called explicitly.

    Usage:
        registry = AIBackendRegistry()
        names    = registry.list_all_names()    # all configured backends
        backend  = registry.get("claude")
        ok       = registry.check_one("claude") # fires actual probe
    """

    def __init__(self):
        from ai_query.backends.claude_cli  import ClaudeCliBackend
        from ai_query.backends.cursor_backend import CursorBackend
        from ai_query.backends.codex_backend  import CodexBackend
        from ai_query.backends.local_llm_backend import LocalLlmBackend

        # Ordered preference: fastest / most reliable first.
        # NOTE: instantiation MUST be cheap — no subprocess, no I/O.
        self._all: list[AIBackend] = [
            ClaudeCliBackend(),
            CursorBackend(),
            CodexBackend(),
            LocalLlmBackend(),
        ]
        self._by_name: dict[str, AIBackend] = {b.name: b for b in self._all}

    # ── enumeration (no probing) ─────────────────────────────────────────────

    def list_all_names(self) -> list[str]:
        """Return ALL configured backend names — does not probe."""
        return [b.name for b in self._all]

    def list_all_backends(self) -> list[AIBackend]:
        return list(self._all)

    def get(self, name: str) -> Optional[AIBackend]:
        return self._by_name.get(name)

    # ── on-demand probing ────────────────────────────────────────────────────

    def check_one(self, name: str, force: bool = False) -> bool:
        """Run the availability probe for a single backend (explicit user action)."""
        b = self._by_name.get(name)
        if not b:
            return False
        return b.check_availability(force=force)

    def detect_all(self) -> None:
        """
        Eagerly probe every backend.  Kept for headless CLI / debugging only —
        the GUI does NOT call this at startup anymore.
        """
        console_print("\n=== AI Backend Detection ===")
        for backend in self._all:
            ok = backend.check_availability()
            console_print(
                f"  {'✓' if ok else '✗'} {backend.display_name:<18} "
                f"({'available' if ok else backend.get_unavailable_reason() or 'not found'})"
            )
        console_print("=" * 30 + "\n")

    # ── back-compat helpers (still lazy) ─────────────────────────────────────

    def available_names(self) -> list[str]:
        """Return names of backends already verified available (no new probe)."""
        return [b.name for b in self._all if b.is_available()]

    def available_backends(self) -> list[AIBackend]:
        return [b for b in self._all if b.is_available()]

    def get_default_name(self) -> str:
        """
        Return the configured default backend name (no probing).
        Returns empty string if config says 'auto'.
        """
        preferred = mc.get("ai", "default_backend", default="auto").strip()
        if preferred and preferred != "auto" and preferred in self._by_name:
            return preferred
        return ""
