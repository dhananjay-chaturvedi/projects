"""Application display-timezone helpers.

The tool's display timezone is configured via ``config.ini`` ``[project]``
``timezone``. Accepted values:

* blank             -> system local timezone
* ``UTC`` / ``Z``   -> UTC
* UTC offset        -> ``+5:30``, ``+05:30``, ``-08:00``, ``+0530``, ``-8`` ...
* IANA name         -> ``Asia/Kolkata`` (when zoneinfo data is available)

Offsets are interpreted relative to UTC, i.e. ``local = UTC + offset``. So
``+5:30`` yields Indian Standard Time. Use :func:`now` to get the current
time already converted into the configured timezone.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Optional

# Accepts: +5, -8, +05, +5:30, +05:30, +0530, -0800
_OFFSET_RE = re.compile(r"^([+-])(\d{1,2})(?::?([0-5]\d))?$")

# Real-world UTC offsets span -12:00 .. +14:00.
_MAX_OFFSET_MIN = 14 * 60


def parse_offset(value: str) -> Optional[timedelta]:
    """Parse a UTC-offset string into a :class:`~datetime.timedelta`.

    Returns ``None`` when ``value`` is not a recognisable offset (e.g. it is
    blank or an IANA name).
    """
    s = (value or "").strip()
    if not s:
        return None
    if s.upper() in ("UTC", "Z", "GMT"):
        return timedelta(0)
    m = _OFFSET_RE.match(s)
    if not m:
        return None
    sign, hh = m.group(1), int(m.group(2))
    mm = int(m.group(3) or 0)
    total = hh * 60 + mm
    if sign == "-":
        total = -total
    if abs(total) > _MAX_OFFSET_MIN:
        return None
    return timedelta(minutes=total)


def format_offset(td: timedelta) -> str:
    """Canonicalise a timedelta into ``+HH:MM`` / ``-HH:MM`` form."""
    total = int(td.total_seconds() // 60)
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    return f"{sign}{total // 60:02d}:{total % 60:02d}"


def is_valid(value: str) -> bool:
    """Return ``True`` when ``value`` is blank, a UTC offset, or an IANA name."""
    s = (value or "").strip()
    if not s:
        return True
    if parse_offset(s) is not None:
        return True
    try:
        from zoneinfo import ZoneInfo

        ZoneInfo(s)
        return True
    except Exception:
        return False


def canonical(value: str) -> str:
    """Normalise for storage: offsets become ``+HH:MM``; names are kept as-is."""
    s = (value or "").strip()
    if not s:
        return ""
    off = parse_offset(s)
    if off is not None:
        return format_offset(off)
    return s


def _configured_value() -> str:
    try:
        from common.config_loader import config

        return (config.get("project", "timezone", "") or "").strip()
    except Exception:
        return ""


def get_tzinfo() -> tzinfo:
    """Resolve the configured display timezone.

    Falls back to the system local timezone when unset/invalid, and to UTC as
    a last resort.
    """
    raw = _configured_value()
    if raw:
        off = parse_offset(raw)
        if off is not None:
            return timezone(off)
        try:
            from zoneinfo import ZoneInfo

            return ZoneInfo(raw)
        except Exception:
            pass
    return datetime.now().astimezone().tzinfo or timezone.utc


def now() -> datetime:
    """Current wall-clock time in the configured display timezone."""
    return datetime.now(timezone.utc).astimezone(get_tzinfo())
