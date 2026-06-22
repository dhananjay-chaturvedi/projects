"""
ai_query/backends/codex_backend.py
====================================
OpenAI Codex CLI backend — uses the locally installed `codex` command.

Call method: subprocess, prompt passed as a positional argument
    codex exec --dangerously-bypass-approvals-and-sandbox "<prompt>"

Authentication: Codex's own login session (openai auth).
No API key needed by this tool.

⚠  Important limitation:
    Codex is designed as an agentic coding tool (edit files, run shell
    commands) rather than a pure Q&A assistant.  In non-interactive mode
    (`codex exec`) it starts an approval-loop process that can take
    30–120 s or timeout entirely on simple Q&A prompts.

    It IS listed as available if installed, so users can try it, but
    Claude CLI or Cursor Agent will be faster and more reliable for
    SQL generation queries.

    The prompt is written to instruct Codex to answer in plain text
    without executing any shell commands, which works best in practice.
"""

from __future__ import annotations

import os
import subprocess
from typing import Optional

from common.config_loader import console_print
from ai_query import module_config as mc
from ai_query.backends import AIBackend, resolve_cli


_CODEX_PREAMBLE = (
    "IMPORTANT: Do NOT run any shell commands or edit any files. "
    "Reply ONLY with plain text. "
)

def _connectivity_timeout() -> int:
    """Seconds for the API ping check (configurable)."""
    return mc.get_int("ai.codex", "connectivity_timeout", default=3)


def _read_codex_api_url() -> str:
    """
    Parse the Codex config.toml to find the configured base_url.
    Returns empty string if not found or file unreadable.
    """
    try:
        import tomllib                          # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib             # pip install tomli (3.9/3.10)
        except ImportError:
            tomllib = None

    override = mc.get("ai.codex", "config_path", default="")
    cfg_path = os.path.realpath(os.path.expanduser(override or "~/.codex/config.toml"))
    # Keep the config strictly under the user's home directory so a tampered
    # ``config_path`` override can't be pointed at arbitrary files (e.g.
    # /etc/passwd) whose parse errors might leak fragments into messages.
    home = os.path.realpath(os.path.expanduser("~"))
    if os.path.commonpath([home, cfg_path]) != home:
        console_print(f"[codex] ignoring config_path outside home: {cfg_path}")
        return ""
    if not os.path.exists(cfg_path):
        return ""

    # Fast path: parse with tomllib if available
    if tomllib is not None:
        try:
            with open(cfg_path, "rb") as f:
                data = tomllib.load(f)
            provider_name = data.get("model_provider", "")
            providers = data.get("model_providers", {})
            if provider_name in providers:
                return providers[provider_name].get("base_url", "")
        except Exception:
            pass

    # Fallback: naive line scan (no extra dependency)
    try:
        in_provider_section = False
        with open(cfg_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("[model_providers."):
                    in_provider_section = True
                elif line.startswith("[") and not line.startswith("[model_providers."):
                    in_provider_section = False
                if in_provider_section and line.startswith("base_url"):
                    _, _, val = line.partition("=")
                    return val.strip().strip('"').strip("'")
    except Exception:
        pass
    return ""


def _is_safe_probe_url(url: str) -> bool:
    """True only for http(s) URLs whose host is not loopback/private/link-local.

    Prevents the Codex connectivity probe from being turned into an SSRF vector
    by a tampered ``base_url`` pointing at internal infrastructure.
    """
    import ipaddress
    from urllib.parse import urlparse

    try:
        parsed = urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return False
    if host in ("localhost", "ip6-localhost", "ip6-loopback"):
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return True  # a DNS name we won't resolve here — allow the probe
    return not (
        ip.is_private or ip.is_loopback or ip.is_link_local
        or ip.is_reserved or ip.is_multicast or ip.is_unspecified
    )


def _check_api_reachable(url: str, timeout: int | None = None) -> bool:
    """HEAD/GET the base URL with a short timeout to verify the API is up."""
    if timeout is None:
        timeout = _connectivity_timeout()
    if not url:
        return True   # can't check — assume OK
    import urllib.request, urllib.error
    probe = url.rstrip("/")
    if not probe.startswith("http"):
        return True
    if not _is_safe_probe_url(probe):
        console_print(f"[codex] refusing to probe unsafe base_url: {probe}")
        return True   # don't probe, but don't mark unavailable either
    try:
        req = urllib.request.Request(probe, method="HEAD")
        urllib.request.urlopen(req, timeout=timeout)
        return True
    except urllib.error.HTTPError:
        return True   # got a response (even 401/403) → server is reachable
    except Exception:
        return False


class CodexBackend(AIBackend):

    name         = "codex"
    display_name = "Codex (CLI)"
    cli_command  = "codex"
    # `codex exec` runs as a one-shot agentic process and does not expose a
    # stable conversation session id we can replay, so resume is unsupported.
    # Each follow-up is sent as a fresh prompt (with prior context inlined by
    # the caller). Revisit if the Codex CLI gains `exec resume` with a usable
    # session id in JSON output.
    supports_resume = False

    def __init__(self):
        super().__init__()
        self._model        = mc.get("ai.codex", "model",   default="")
        self._timeout      = mc.get_int("ai.codex", "timeout", default=120)
        self._version: str = ""

    def _cli_path_override(self) -> str:
        return mc.get("ai.codex", "cli_path", default="")

    def _detect(self) -> bool:
        self._cli_path = resolve_cli("codex", self._cli_path_override())
        if not self._cli_path:
            self._unavail_reason = "Codex CLI not installed (https://github.com/openai/codex)"
            return False

        try:
            r = subprocess.run(
                [self._cli_path, "--version"],
                capture_output=True, text=True,
                timeout=mc.get_int("ai.codex", "version_timeout", default=5),
            )
            if r.returncode == 0:
                lines = (r.stdout.strip() or r.stderr.strip()).splitlines()
                self._version = lines[0] if lines else "codex"
            else:
                self._unavail_reason = f"`codex --version` failed: exit {r.returncode}"
                return False
        except Exception as exc:
            self._unavail_reason = f"`codex --version` failed: {exc}"
            return False

        api_url = _read_codex_api_url()
        if api_url:
            console_print(f"[codex] checking API endpoint: {api_url} ...")
            if not _check_api_reachable(api_url, _connectivity_timeout()):
                self._unavail_reason = f"API unreachable: {api_url}"
                console_print(f"[codex] ✗ {self._unavail_reason}")
                return False
            console_print(f"[codex] ✓ API endpoint reachable.")

        return True

    def call(
        self,
        prompt: str,
        timeout: int = 0,
        resume_session_id: Optional[str] = None,
    ) -> dict:
        if not self.is_available() and self._unavail_reason:
            return {
                "response": None,
                "error": f"Codex unavailable: {self._unavail_reason}",
            }

        t = timeout or self._timeout
        full_prompt = _CODEX_PREAMBLE + prompt

        cmd = [self._resolve_executable(), "exec", "--dangerously-bypass-approvals-and-sandbox"]
        if self._model:
            cmd += ["--model", self._model]
        # ``--`` ends option parsing so the prompt can't be misread as a flag.
        cmd.append("--")
        cmd.append(full_prompt)

        # stdin=None in _run means the child inherits parent stdin.
        # Codex reads extra context from stdin; pass empty string so it
        # gets an immediate EOF and doesn't block.
        return self._run(cmd, stdin_text="", timeout=t)

    def get_info(self) -> dict:
        if self._unavail_reason:
            return {
                "provider": self.display_name,
                "model":    self._model or "default",
                "status":   "Not Available",
                "note":     self._unavail_reason,
                "resume_supported": bool(self.supports_resume),
            }
        return {
            "provider": self.display_name,
            "model":    self._model or "default",
            "status":   "Connected" if self.is_available() else "Not Available",
            "note":     "CLI-based — uses your Codex login. May be slow for Q&A queries.",
            "resume_supported": bool(self.supports_resume),
        }
