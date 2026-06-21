"""Comment-preserving, surgical INI value editor.

``configparser`` can write INI files, but it discards every comment and blank
line in the process. Our ``config.ini`` / ``properties.ini`` carry a lot of
hand-written documentation in comments, and the Settings UI only ever changes
a handful of keys at a time, so a full rewrite would be destructive.

:func:`set_ini_value` performs a *targeted* edit of the raw file text:

* it changes only the ``key = value`` line for the requested ``(section, key)``,
* it preserves all other lines verbatim (comments, blanks, ordering, casing),
* it keeps the line's original indentation and any trailing inline comment,
* it creates the section / key when missing (appending in a sensible place).

Writes are crash-safe (temp file + ``os.replace``).
"""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

# A "key = value" assignment line (configparser also allows ':' as a delimiter).
_ASSIGN_RE = re.compile(r"^(?P<indent>\s*)(?P<key>[^#;=:\s][^=:]*?)\s*(?P<sep>[=:])\s*(?P<val>.*)$")
_SECTION_RE = re.compile(r"^\s*\[(?P<name>[^\]]+)\]\s*$")
_COMMENT_RE = re.compile(r"^\s*[#;]")


def _split_inline_comment(value_part: str) -> tuple[str, str]:
    """Split a raw value into (value, inline_comment_including_marker).

    configparser is configured with ``inline_comment_prefixes`` only in some
    places, so we are conservative: only treat ``#`` / ``;`` as an inline
    comment when it is preceded by whitespace (so URLs with ``#`` survive).
    """
    m = re.search(r"\s+[#;].*$", value_part)
    if not m:
        return value_part.rstrip(), ""
    return value_part[: m.start()].rstrip(), value_part[m.start():].rstrip("\n")


def set_ini_value(path: str | Path, section: str, key: str, value: str) -> bool:
    """Set ``[section] key = value`` in ``path``, preserving comments/layout.

    Returns ``True`` on success. Creates the file, the section, and/or the key
    if they do not already exist.
    """
    path = Path(path)
    value = "" if value is None else str(value)

    if path.exists():
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines(keepends=True)
    else:
        lines = []

    newline = "\n"
    # Detect dominant newline style from the file if present.
    if lines and lines[0].endswith("\r\n"):
        newline = "\r\n"

    section_start = -1
    section_end = len(lines)  # exclusive
    for i, line in enumerate(lines):
        m = _SECTION_RE.match(line)
        if not m:
            continue
        if m.group("name").strip() == section:
            section_start = i
            # find the end of this section (next header or EOF)
            section_end = len(lines)
            for j in range(i + 1, len(lines)):
                if _SECTION_RE.match(lines[j]):
                    section_end = j
                    break
            break

    def _fmt(existing_line: str | None) -> str:
        """Build the replacement assignment line, keeping indent + inline comment."""
        indent, sep, comment = "", "=", ""
        if existing_line is not None:
            am = _ASSIGN_RE.match(existing_line)
            if am:
                indent = am.group("indent")
                sep = am.group("sep")
                _old_val, comment = _split_inline_comment(am.group("val"))
        sep_render = f" {sep} "
        line = f"{indent}{key}{sep_render}{value}"
        if comment:
            line = f"{line} {comment.lstrip()}"
        return line + newline

    if section_start == -1:
        # Section missing: append a new section block at EOF.
        if lines and not lines[-1].endswith(("\n", "\r\n")):
            lines[-1] = lines[-1] + newline
        if lines and lines[-1].strip() != "":
            lines.append(newline)
        lines.append(f"[{section}]{newline}")
        lines.append(_fmt(None))
        return _atomic_write_text(path, "".join(lines))

    # Search for the key within the section.
    key_lower = key.strip().lower()
    for i in range(section_start + 1, section_end):
        if _COMMENT_RE.match(lines[i]):
            continue
        am = _ASSIGN_RE.match(lines[i])
        if am and am.group("key").strip().lower() == key_lower:
            lines[i] = _fmt(lines[i])
            return _atomic_write_text(path, "".join(lines))

    # Key missing in section: insert after the last non-blank line of the section.
    insert_at = section_end
    for j in range(section_end - 1, section_start, -1):
        if lines[j].strip() != "":
            insert_at = j + 1
            break
    lines.insert(insert_at, _fmt(None))
    return _atomic_write_text(path, "".join(lines))


def _atomic_write_text(path: Path, text: str, *, perms: int = 0o600) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        os.fchmod(tmp_fd, perms)
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
        os.replace(tmp_name, str(path))
        return True
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
