"""Shared settings service used by the Settings UI, the ``config`` CLI, and the
read-only config API.

Centralising read/validate/save here means all three surfaces behave
identically (same coercion, same validation, same secret redaction) — the
schema in :mod:`common.config.settings_schema` describes *what* the settings
are, this service performs *operations* on them.
"""

from __future__ import annotations

from typing import Any, Optional

from common.config.settings_schema import (
    SettingSpec,
    all_specs,
    by_group,
    find,
)

_TRUE = {"true", "yes", "1", "on"}
_FALSE = {"false", "no", "0", "off"}


def _loader(target: str):
    from common.config_loader import get_config, get_properties

    return get_config() if target == "config" else get_properties()


# --------------------------------------------------------------------------- #
# Read
# --------------------------------------------------------------------------- #
def current_value(spec: SettingSpec) -> str:
    """Return the current raw string value (default when unset)."""
    if spec.target == "secret":
        return ""  # secrets are never read back
    loader = _loader(spec.target)
    return loader.get(spec.section, spec.key, default=spec.default)


def describe(spec: SettingSpec, *, redact: bool = True) -> dict:
    """Return a JSON-friendly description + current value for one setting."""
    value: Any = current_value(spec)
    if spec.sensitive and redact and value:
        value = "***"

    return {
        "id": spec.id,
        "target": spec.target,
        "section": spec.section,
        "key": spec.key,
        "label": spec.label,
        "description": spec.description,
        "type": spec.type,
        "group": spec.group,
        "default": spec.default,
        "options": list(spec.options),
        "unit": spec.unit,
        "minimum": spec.minimum,
        "maximum": spec.maximum,
        "requires_restart": spec.requires_restart,
        "sensitive": spec.sensitive,
        "value": value,
    }


def describe_all(*, redact: bool = True, include_secrets: bool = True) -> list[dict]:
    return [describe(s, redact=redact) for s in all_specs(include_secrets=include_secrets)]


def grouped(*, redact: bool = True, include_secrets: bool = True) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for group, specs in by_group(include_secrets=include_secrets).items():
        out[group] = [describe(s, redact=redact) for s in specs]
    return out


# --------------------------------------------------------------------------- #
# Validate + coerce
# --------------------------------------------------------------------------- #
def validate(spec: SettingSpec, raw: str) -> Optional[str]:
    """Return an error message, or None if ``raw`` is valid for ``spec``."""
    raw = "" if raw is None else str(raw).strip()

    if spec.type == "bool":
        if raw.lower() not in _TRUE | _FALSE:
            return f"{spec.label} must be true or false."
        return None
    if spec.type == "enum":
        if spec.options and raw not in spec.options:
            return f"{spec.label} must be one of: {', '.join(spec.options)}."
        return None
    if spec.type in ("int", "float"):
        if raw == "":
            return f"{spec.label} requires a number."
        try:
            num = int(raw) if spec.type == "int" else float(raw)
        except ValueError:
            return f"{spec.label} must be a{'n integer' if spec.type == 'int' else ' number'}."
        if spec.minimum is not None and num < spec.minimum:
            return f"{spec.label} must be >= {spec.minimum}."
        if spec.maximum is not None and num > spec.maximum:
            return f"{spec.label} must be <= {spec.maximum}."
        return None
    if spec.type == "tz":
        from common.tzutil import is_valid

        if not is_valid(raw):
            return (f"{spec.label} must be a UTC offset like +5:30 or -08:00, "
                    "an IANA name (e.g. Asia/Kolkata), or blank.")
        return None
    # str / secret — anything is acceptable (including empty).
    return None


def normalize(spec: SettingSpec, raw: str) -> str:
    """Canonicalise a value for storage (e.g. bool -> 'true'/'false')."""
    raw = "" if raw is None else str(raw).strip()
    if spec.type == "bool":
        return "true" if raw.lower() in _TRUE else "false"
    if spec.type == "tz":
        from common.tzutil import canonical

        return canonical(raw)
    return raw


# --------------------------------------------------------------------------- #
# Write
# --------------------------------------------------------------------------- #
def set_value(spec_id: str, value: str) -> dict:
    """Validate and persist a single setting by its dotted id."""
    spec = find(spec_id)
    if spec is None:
        return {"ok": False, "message": f"Unknown setting '{spec_id}'."}
    return set_spec(spec, value)


def set_spec(spec: SettingSpec, value: str) -> dict:
    err = validate(spec, value)
    if err:
        return {"ok": False, "message": err}
    canon = normalize(spec, value)
    loader = _loader(spec.target)
    if not loader.set(spec.section, spec.key, canon):
        return {"ok": False, "message": f"Failed to save {spec.label}."}
    return {"ok": True, "message": f"{spec.label} saved.",
            "requires_restart": spec.requires_restart}


def set_many(values: dict[str, str]) -> dict:
    """Validate ALL values first, then persist. Atomic-ish: nothing is written
    unless every value validates.
    """
    resolved: list[tuple[SettingSpec, str]] = []
    errors: dict[str, str] = {}
    for spec_id, val in values.items():
        spec = find(spec_id)
        if spec is None:
            errors[spec_id] = "unknown setting"
            continue
        if spec.target != "secret":
            err = validate(spec, val)
            if err:
                errors[spec_id] = err
                continue
        resolved.append((spec, val))

    if errors:
        return {"ok": False, "message": "Validation failed.", "errors": errors,
                "saved": []}

    saved, restart = [], False
    for spec, val in resolved:
        r = set_spec(spec, val)
        if not r["ok"]:
            errors[spec.id] = r["message"]
        else:
            saved.append(spec.id)
            restart = restart or bool(r.get("requires_restart"))
    return {"ok": not errors, "message": "Settings saved." if not errors else
            "Some settings failed.", "errors": errors, "saved": saved,
            "requires_restart": restart}


def restore_defaults(target: str = "all") -> dict:
    """Restore config and/or properties from their ``*.ini.example`` defaults."""
    from common.config_loader import get_config, get_properties

    done, failed = [], []
    targets = ["config", "properties"] if target == "all" else [target]
    for t in targets:
        loader = get_config() if t == "config" else get_properties()
        if loader.restore_defaults():
            done.append(t)
        else:
            failed.append(t)
    ok = not failed
    return {"ok": ok, "restored": done, "failed": failed,
            "message": ("Restored: " + ", ".join(done)) if done else
                       "Nothing restored (no example defaults found)."}
