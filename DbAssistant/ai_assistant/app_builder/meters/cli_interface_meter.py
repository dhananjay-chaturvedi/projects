"""cli_interface_meter — CLI (Command-Line Interface Score).

Scores whether the app exposes a real, usable command-line interface. Modeled on
the developer-experience expectations for CLI tools (argparse/click/typer with
subcommands, arguments, help text and a runnable entry point).

Requirement-aware: a CLI is only *expected* when the user asked for one (the
description/features mention a CLI / command line / terminal) **or** when CLI
code is actually present. When neither is true the meter reports
``applicable=False`` with a neutral score so web-only apps are not penalized.

Deterministic: regex/structural analysis over produced files, never a model.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from ai_assistant.meters.base import Meter, Measurement, weighted_score

_ARGPARSE_RE = re.compile(r"\bArgumentParser\b|\bimport\s+argparse\b")
_ADD_ARG_RE = re.compile(r"\.add_argument\(")
_SUBPARSER_RE = re.compile(r"\.add_subparsers\(")
_CLICK_RE = re.compile(r"\bimport\s+click\b|@click\.(command|group)\b")
_CLICK_OPT_RE = re.compile(r"@click\.(option|argument)\b")
_CLICK_GROUP_RE = re.compile(r"@click\.group\b")
_TYPER_RE = re.compile(r"\bimport\s+typer\b|typer\.Typer\(")
_TYPER_CMD_RE = re.compile(r"@\w+\.command\(")
_MAIN_GUARD_RE = re.compile(r"if\s+__name__\s*==\s*['\"]__main__['\"]")
_CONSOLE_SCRIPTS_RE = re.compile(
    r"console_scripts|\[project\.scripts\]|\[tool\.poetry\.scripts\]")
_HELP_RE = re.compile(r"help\s*=|add_help|--help|\"\"\"|'''")

#: words in the requirement that mean "this app needs a CLI".
_CLI_REQUEST_WORDS = (
    "cli", "command line", "command-line", "commandline", "terminal",
    "console", "argparse", "click", "typer", "subcommand", "shell command",
)


class CliInterfaceMeter(Meter):
    """Score the presence and quality of a command-line interface."""

    name = "cli_interface_meter"
    default_threshold = 0.7

    def measure(
        self,
        files: Mapping[str, str],
        *,
        description: str = "",
        features: Iterable[str] = (),
        threshold: float | None = None,
    ) -> Measurement:
        thr = self.default_threshold if threshold is None else threshold
        py = {p: c for p, c in files.items() if p.endswith(".py") and c}
        blob = "\n".join(py.values())
        manifest = "\n".join(
            c for p, c in files.items()
            if p.endswith(("setup.py", "pyproject.toml", "setup.cfg")) and c)
        paths = [p.lower() for p in files]

        framework = bool(_ARGPARSE_RE.search(blob) or _CLICK_RE.search(blob)
                         or _TYPER_RE.search(blob))
        has_cli_module = any(
            ("cli" in p or p.endswith("__main__.py") or "/cmd" in p
             or "console" in p) and p.endswith(".py") for p in paths)
        has_args = bool(_ADD_ARG_RE.search(blob) or _CLICK_OPT_RE.search(blob)
                        or _TYPER_CMD_RE.search(blob))
        has_subcommands = bool(
            _SUBPARSER_RE.search(blob) or _CLICK_GROUP_RE.search(blob)
            or len(_TYPER_CMD_RE.findall(blob)) >= 2)
        has_entry = bool(_MAIN_GUARD_RE.search(blob)
                         or _CONSOLE_SCRIPTS_RE.search(manifest))
        has_help = bool(_HELP_RE.search(blob))

        present = framework or has_cli_module or has_args

        # Requirement awareness: is a CLI expected here?
        req_text = (description + " " + " ".join(features)).lower()
        requested = any(w in req_text for w in _CLI_REQUEST_WORDS)
        applicable = requested or present

        if not applicable:
            return Measurement(
                meter=self.name, score=1.0,
                components={}, weights={},
                evidence={"applicable": False, "requested": False,
                          "present": False},
                issues=[], threshold=thr)

        components = {
            "cli_framework": 1.0 if framework else 0.0,
            "entry_point": 1.0 if has_entry else 0.0,
            "arguments": 1.0 if has_args else 0.0,
            "subcommands": 1.0 if has_subcommands else 0.0,
            "help_text": 1.0 if has_help else 0.0,
        }
        weights = {
            "cli_framework": 3.0, "entry_point": 2.0, "arguments": 2.0,
            "subcommands": 1.5, "help_text": 1.0,
        }
        score = weighted_score(components, weights)

        issues: list[str] = []
        if requested and not present:
            issues.append("a CLI was requested but none was found")
        if present and not framework:
            issues.append("CLI present but no argparse/click/typer framework")
        if present and not has_entry:
            issues.append("no runnable CLI entry point (__main__ / console_scripts)")
        if present and not has_subcommands:
            issues.append("CLI exposes no subcommands")

        return Measurement(
            meter=self.name, score=score, components=components, weights=weights,
            evidence={"applicable": True, "requested": requested,
                      "present": present, "framework": framework,
                      "has_module": has_cli_module, "subcommands": has_subcommands,
                      "entry_point": has_entry},
            issues=issues, threshold=thr)
