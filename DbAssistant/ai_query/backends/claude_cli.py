"""
ai_query/backends/claude_cli.py
================================
Claude CLI backend — uses the locally installed `claude` command.
"""

from __future__ import annotations

from typing import Optional

from ai_query import module_config as mc
from ai_query.backends import AIBackend, resolve_cli


class ClaudeCliBackend(AIBackend):

    name         = "claude"
    display_name = "Claude (CLI)"
    cli_command  = "claude"
    # Claude CLI resumes a conversation via `--resume <session_id>` and returns
    # a session id we persist for follow-ups.
    supports_resume = True

    def __init__(self):
        super().__init__()
        self._timeout = mc.get_int("ai.claude", "timeout", default=120)
        self._version: str = ""

    def _cli_path_override(self) -> str:
        return mc.get("ai.claude", "cli_path", default="")

    def _detect(self) -> bool:
        import subprocess
        self._cli_path = resolve_cli("claude", self._cli_path_override())
        if not self._cli_path:
            self._unavail_reason = "Claude CLI not installed (https://claude.ai/download)"
            return False
        try:
            test_timeout = mc.get_int("ai.claude", "cli_test_timeout", default=5)
            r = subprocess.run(
                [self._cli_path, "--version"],
                capture_output=True, text=True, timeout=test_timeout
            )
            if r.returncode == 0:
                self._version = (r.stdout.strip() or r.stderr.strip()).splitlines()[0]
                return True
            self._unavail_reason = f"`claude --version` failed: exit {r.returncode}"
        except Exception as exc:
            self._unavail_reason = f"`claude --version` failed: {exc}"
        return False

    def call(
        self,
        prompt: str,
        timeout: int = 0,
        resume_session_id: Optional[str] = None,
    ) -> dict:
        t = timeout or self._timeout
        cmd = [self._resolve_executable(), "-p", "--output-format", "json"]
        safe_resume = self._safe_resume_id(resume_session_id)
        if safe_resume:
            cmd.extend(["--resume", safe_resume])
        raw = self._run(cmd, stdin_text=prompt, timeout=t)
        if raw.get("error"):
            return raw
        text, sid = self._parse_json_payload(raw.get("response") or "")
        return {
            "response": text,
            "error": None,
            "backend_session_id": sid or safe_resume or None,
        }

    def get_info(self) -> dict:
        return {
            "provider": self.display_name,
            "model":    self._version or "claude",
            "status":   "Connected" if self.is_available() else "Not Available",
            "note":     "CLI-based — no API key required",
            "resume_supported": bool(self.supports_resume),
        }
