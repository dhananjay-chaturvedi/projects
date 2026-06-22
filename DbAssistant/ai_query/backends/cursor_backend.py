"""
ai_query/backends/cursor_backend.py
=====================================
Cursor Agent backend — uses the locally installed `cursor` CLI.
"""

from __future__ import annotations

from typing import Optional

from ai_query import module_config as mc
from ai_query.backends import AIBackend


class CursorBackend(AIBackend):

    name         = "cursor"
    display_name = "Cursor Agent"
    cli_command  = "cursor"
    # Cursor agent resumes a chat via `--resume <session_id>` and returns a
    # session id we persist for follow-ups.
    supports_resume = True

    def __init__(self):
        super().__init__()
        self._model   = mc.get("ai.cursor", "model",   default="auto")
        self._timeout = mc.get_int("ai.cursor", "timeout", default=60)
        self._version: str = ""

    def _detect(self) -> bool:
        import shutil, subprocess
        self._cli_path = shutil.which("cursor")
        if not self._cli_path:
            self._unavail_reason = "Cursor CLI not installed (https://cursor.com)"
            return False
        try:
            r = subprocess.run(
                ["cursor", "agent", "--version"],
                capture_output=True, text=True,
                timeout=mc.get_int("ai.cursor", "agent_version_timeout", default=8),
            )
            if r.returncode == 0:
                self._version = r.stdout.strip() or "cursor agent"
                return True
        except Exception:
            pass
        try:
            r = subprocess.run(
                ["cursor", "--version"],
                capture_output=True, text=True,
                timeout=mc.get_int("ai.cursor", "version_timeout", default=5),
            )
            if r.returncode == 0:
                ver_line = (r.stdout.strip() or r.stderr.strip()).splitlines()
                self._version = ver_line[0] if ver_line else "Cursor"
                return True
            self._unavail_reason = f"`cursor --version` failed: exit {r.returncode}"
        except Exception as exc:
            self._unavail_reason = f"`cursor agent` not available: {exc}"
        return False

    def call(
        self,
        prompt: str,
        timeout: int = 0,
        resume_session_id: Optional[str] = None,
    ) -> dict:
        t = timeout or self._timeout
        cmd = [
            "cursor", "agent",
            "--print",
            "--output-format", "json",
            "--mode", "ask",
            "--model", self._model,
        ]
        safe_resume = self._safe_resume_id(resume_session_id)
        if safe_resume:
            cmd.extend(["--resume", safe_resume])
        # ``--`` ends option parsing so a prompt that happens to start with
        # ``-`` can never be misread as a CLI flag.
        cmd.append("--")
        cmd.append(prompt)
        raw = self._run(cmd, stdin_text=None, timeout=t)
        if raw.get("error"):
            return raw
        text, sid = self._parse_json_payload(raw.get("response") or "")
        out = {"response": text, "error": None,
               "backend_session_id": sid or safe_resume or None}
        return out

    def get_info(self) -> dict:
        model_label = self._model if self._model != "auto" else "auto (Cursor picks)"
        return {
            "provider": self.display_name,
            "model":    model_label,
            "status":   "Connected" if self.is_available() else "Not Available",
            "note":     "CLI-based — uses your Cursor login. ~15-25s per query.",
            "resume_supported": bool(self.supports_resume),
        }
