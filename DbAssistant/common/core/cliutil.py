"""
Tiny shared CLI output helpers used by the master CLI and every module CLI.

Kept dependency-free (tabulate is optional) so a single module can ship with
only the core bundle.
"""

from __future__ import annotations

import csv as _csv
import io
import json
import sys

try:
    from tabulate import tabulate as _tabulate
    _HAVE_TABULATE = True
except Exception:  # pragma: no cover - optional dep
    _HAVE_TABULATE = False


def _supports_color() -> bool:
    return sys.stdout.isatty()


def _c(code: str, text: str) -> str:
    if not _supports_color():
        return text
    return f"\033[{code}m{text}\033[0m"


def ok(msg: str) -> None:
    print(_c("92", "[OK]  ") + msg)


def err(msg: str) -> None:
    print(_c("91", "[ERR] ") + msg, file=sys.stderr)


def info(msg: str) -> None:
    print(_c("96", "[   ] ") + msg)


def warn(msg: str) -> None:
    print(_c("93", "[WARN]") + " " + msg)


def bold(msg: str) -> str:
    return _c("1", msg)


def print_table(rows: list[list], headers: list[str], fmt: str = "table") -> None:
    """Render *rows* as a table / json / csv block."""
    if fmt == "json":
        print(json.dumps([dict(zip(headers, r)) for r in rows], indent=2, default=str))
        return
    if fmt == "csv":
        buf = io.StringIO()
        w = _csv.writer(buf)
        w.writerow(headers)
        w.writerows(rows)
        print(buf.getvalue().rstrip())
        return
    if _HAVE_TABULATE:
        print(_tabulate(rows, headers=headers, tablefmt="github"))
    else:
        widths = [len(h) for h in headers]
        for r in rows:
            for i, cell in enumerate(r):
                widths[i] = max(widths[i], len(str(cell)))
        line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
        print(line)
        print("  ".join("-" * widths[i] for i in range(len(headers))))
        for r in rows:
            print("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(r)))
