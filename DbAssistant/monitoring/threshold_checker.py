"""
threshold_checker.py
====================
Reads ``monitor_thresholds.ini`` and evaluates metric values against three
severity levels: INFO, WARNING, and CRITICAL.

Each :meth:`ThresholdChecker.check` call returns an :class:`AlertResult`
namedtuple when a sustained breach is detected, or ``None`` when everything is
within limits.

Usage
-----
    from monitoring.threshold_checker import ThresholdChecker, CRITICAL, WARNING, INFO

    checker = ThresholdChecker()

    # OS / DB (3-part sections, no path)
    result = checker.check("os", "cpu_utilization", 95.0, instance_id="my-host")

    # Cloud (variable-length sections with a path)
    result = checker.check(
        "aws", "FreeableMemory", 100 * 1024**2,
        instance_id="my-rds", path=("cloudwatch", "RDS"),
    )

    # Batch evaluation
    results = checker.check_many(
        "azure", metrics_dict,
        instance_id="my-server",
        path=("azuremonitor", "DBforMySQL", "flexibleServers"),
    )

INI grammar
-----------
Sections are dot-separated, variable length, with at least three segments::

    [metric.<source>.<rule_id>]                                   # 3 parts (db/os)
    [metric.<source>.<api>.<...path...>.<rule_id>]                # 4+ parts (cloud)

The first segment is the literal ``metric``.
The second segment is the *source* (``aws``, ``azure``, ``gcp``, ``db``, ``os``).
The last segment is the *rule id* (used as the dict key in :meth:`check_many`).
Anything between forms the *path* — describes API + sub-namespace.

Each section accepts these option keys (all optional unless noted)::

    critical, warning, info   numeric threshold (at least one is required)
    operator                  > >= < <= == !=   (required)
    unit                      display unit
    window                    consecutive breaches before firing (default 3)
    enabled                   true/false (default true)
    description               human-readable label
    metric_name               cloud-API metric name (defaults to <rule_id>)
    namespace                 AWS CloudWatch namespace override
    service_type              AWS Performance Insights service type override
    resource_provider         Azure resource provider override
    resource_type             GCP monitored resource type override

Derivation defaults
-------------------
``namespace``, ``service_type``, ``resource_provider``, ``resource_type`` are
derived from the section path when not given explicitly:

* AWS CloudWatch: ``namespace = "AWS/" + path[1]``           (path[0] == "cloudwatch")
* AWS PI:         ``service_type = path[1]``                 (path[0] == "pi")
* Azure Monitor:  ``resource_provider = "Microsoft." + path[1] + "/" + path[2]``
* GCP Monitoring: ``resource_type = path[1] + "_" + path[2]``

Severity levels (highest → lowest)
------------------------------------
    CRITICAL  — threshold exceeded at the most severe level  (red)
    WARNING   — threshold exceeded at a moderate level       (yellow)
    INFO      — threshold exceeded at an informational level (blue)
"""

from __future__ import annotations

import configparser
import math
import sys
from collections import namedtuple
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

from .monitoring_utils import sustained_breach

CRITICAL = "CRITICAL"
WARNING  = "WARNING"
INFO     = "INFO"

_SEVERITY_LEVELS = (CRITICAL, WARNING, INFO)

AlertResult = namedtuple("AlertResult", ["severity", "message"])

_DEFAULT_CONFIG = Path(__file__).resolve().parent / "monitor_thresholds.ini"


# ----------------------------------------------------------------------------
# Helpers — path / api derivation
# ----------------------------------------------------------------------------

def _coerce_path(path: Optional[Sequence[str]]) -> tuple[str, ...]:
    """Normalise a path argument into an immutable tuple of strings.

    ``None`` and empty inputs both map to the empty tuple, which is the path
    used by 3-part legacy sections (``metric.<source>.<metric>``).
    """
    if not path:
        return ()
    return tuple(str(seg) for seg in path)


def _derive_api(source: str, path: tuple[str, ...]) -> str:
    """Return the canonical API tag (path[0]) for cloud sources."""
    if source in ("aws", "azure", "gcp") and path:
        return path[0]
    return ""


def _derive_metadata(source: str, path: tuple[str, ...]) -> dict:
    """Compute default values for the per-provider metadata fields based on
    the section path. Empty strings mean "not derivable from path"; callers
    will fall back to whatever the section explicitly declared.
    """
    api = _derive_api(source, path)
    out = {
        "namespace": "",
        "service_type": "",
        "resource_provider": "",
        "resource_type": "",
    }
    if source == "aws" and api == "cloudwatch" and len(path) >= 2:
        out["namespace"] = f"AWS/{path[1]}"
    elif source == "aws" and api == "pi" and len(path) >= 2:
        out["service_type"] = path[1]
    elif source == "azure" and api == "azuremonitor" and len(path) >= 3:
        out["resource_provider"] = f"Microsoft.{path[1]}/{path[2]}"
    elif source == "gcp" and api == "cloudmonitoring" and len(path) >= 3:
        out["resource_type"] = f"{path[1]}_{path[2]}"
    return out


# ----------------------------------------------------------------------------
# ThresholdRule
# ----------------------------------------------------------------------------

@dataclass(frozen=True)
class MetricMetadata:
    metric_name: str = ""
    api: str = ""
    namespace: str = ""
    service_type: str = ""
    resource_provider: str = ""
    resource_type: str = ""
    enabled: bool = True
    description: str = ""


@dataclass(frozen=True)
class ThresholdLevels:
    critical: float | None = None
    warning: float | None = None
    info: float | None = None
    operator: str = ">"
    unit: str = ""
    window: int = 3

class ThresholdRule:
    """A single threshold rule parsed from the config file."""

    __slots__ = (
        "source", "path", "metric", "metric_name", "api",
        "namespace", "service_type", "resource_provider", "resource_type",
        "critical", "warning", "info",
        "operator", "unit", "window", "enabled", "description",
    )

    def __init__(
        self,
        source: str,
        path: tuple[str, ...],
        metric: str,
        metadata: MetricMetadata | None = None,
        levels: ThresholdLevels | None = None,
    ) -> None:
        metadata = metadata or MetricMetadata()
        levels = levels or ThresholdLevels()
        self.source = source
        self.path = _coerce_path(path)
        self.metric = metric
        self.metric_name = metadata.metric_name or metric
        self.api = metadata.api or _derive_api(source, self.path)
        derived = _derive_metadata(source, self.path)
        self.namespace = metadata.namespace or derived["namespace"]
        self.service_type = metadata.service_type or derived["service_type"]
        self.resource_provider = metadata.resource_provider or derived["resource_provider"]
        self.resource_type = metadata.resource_type or derived["resource_type"]
        self.critical = levels.critical
        self.warning = levels.warning
        self.info = levels.info
        self.operator = levels.operator
        self.unit = levels.unit
        self.window = levels.window
        self.enabled = metadata.enabled
        self.description = metadata.description or metric

    @property
    def section_id(self) -> str:
        """Return the canonical INI section name for this rule."""
        parts = ["metric", self.source, *self.path, self.metric]
        return ".".join(parts)

    @property
    def path_str(self) -> str:
        """Dot-joined path string, useful for display."""
        return ".".join(self.path)

    def __repr__(self) -> str:
        return (
            f"ThresholdRule(section={self.section_id!r}, "
            f"metric_name={self.metric_name!r}, "
            f"critical={self.critical}, warning={self.warning}, info={self.info}, "
            f"operator={self.operator!r}, enabled={self.enabled})"
        )


# ----------------------------------------------------------------------------
# ThresholdChecker
# ----------------------------------------------------------------------------

class ThresholdChecker:
    """Loads threshold rules from *config_path* and evaluates metric values
    against up to three severity levels per metric.

    Parameters
    ----------
    config_path:
        Path to the INI config file. Defaults to ``monitor_thresholds.ini``
        in the same directory as this module.
    reload_on_check:
        When True the config file is re-read on every call to :meth:`check`.
        Useful for long-running daemons. Defaults to False.
    """

    def __init__(
        self,
        config_path: str | Path | None = None,
        reload_on_check: bool = False,
    ) -> None:
        import threading
        self._config_path = Path(config_path) if config_path else _DEFAULT_CONFIG
        self._reload_on_check = reload_on_check
        self._data_lock = threading.Lock()
        # Primary index — exact (source, path, metric) lookup.
        self._rules: dict[tuple[str, tuple[str, ...], str], ThresholdRule] = {}
        # Helper indexes for path-agnostic lookup (used by legacy callers and
        # the optional path=None branch in get_rule/check).
        self._by_source_metric: dict[tuple[str, str], list[ThresholdRule]] = {}
        self._load()

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def _load(self) -> None:
        if not self._config_path.exists():
            raise FileNotFoundError(
                f"Threshold config not found: {self._config_path}"
            )

        parser = configparser.ConfigParser(inline_comment_prefixes=("#",))
        parser.read(self._config_path, encoding="utf-8")

        rules: dict[tuple[str, tuple[str, ...], str], ThresholdRule] = {}
        by_sm: dict[tuple[str, str], list[ThresholdRule]] = {}

        for section in parser.sections():
            parts = section.split(".")
            if len(parts) < 3 or parts[0] != "metric":
                continue

            source = parts[1]
            path = tuple(parts[2:-1])
            metric = parts[-1]

            try:
                operator = parser.get(section, "operator").strip()
                unit     = parser.get(section, "unit", fallback="").strip()
                window = parser.getint(
                    section, "window",
                    fallback=parser.getint(section, "sustained_window", fallback=3),
                )
                if window < 1:
                    window = 1
                enabled  = parser.getboolean(section, "enabled", fallback=True)
                description = parser.get(section, "description", fallback=metric).strip()

                metric_name = parser.get(section, "metric_name", fallback="").strip()
                namespace = parser.get(section, "namespace", fallback="").strip()
                service_type = parser.get(section, "service_type", fallback="").strip()
                resource_provider = parser.get(section, "resource_provider", fallback="").strip()
                resource_type = parser.get(section, "resource_type", fallback="").strip()

                def _get_level(key: str) -> float | None:
                    raw = parser.get(section, key, fallback=None)
                    return float(raw) if raw is not None else None

                critical = _get_level("critical")
                warning  = _get_level("warning")
                info_thr = _get_level("info")

                if critical is None and warning is None and info_thr is None:
                    legacy = _get_level("threshold")
                    if legacy is not None:
                        critical = legacy

            except (configparser.Error, ValueError) as exc:
                print(f"[ThresholdChecker] Skipping [{section}]: {exc}", file=sys.stderr)
                continue

            if operator not in self._VALID_OPERATORS:
                print(
                    f"[ThresholdChecker] [{section}] unknown operator {operator!r}, "
                    f"must be one of {sorted(self._VALID_OPERATORS)}.  Skipping.",
                    file=sys.stderr,
                )
                continue

            if critical is None and warning is None and info_thr is None:
                print(
                    f"[ThresholdChecker] [{section}] no threshold levels defined. Skipping.",
                    file=sys.stderr,
                )
                continue

            rule = ThresholdRule(
                source=source,
                path=path,
                metric=metric,
                metadata=MetricMetadata(
                    metric_name=metric_name,
                    namespace=namespace,
                    service_type=service_type,
                    resource_provider=resource_provider,
                    resource_type=resource_type,
                    enabled=enabled,
                    description=description,
                ),
                levels=ThresholdLevels(
                    critical=critical,
                    warning=warning,
                    info=info_thr,
                    operator=operator,
                    unit=unit,
                    window=window,
                ),
            )

            rules[(source, path, metric)] = rule
            by_sm.setdefault((source, metric), []).append(rule)

        with self._data_lock:
            self._rules = rules
            self._by_source_metric = by_sm

    def reload(self) -> None:
        self._load()

    # ------------------------------------------------------------------
    # Lookup helpers
    # ------------------------------------------------------------------

    def get_rule(
        self,
        source: str,
        metric: str,
        *,
        path: Sequence[str] | None = None,
        fallback_to_empty: bool = False,
    ) -> Optional[ThresholdRule]:
        """Return the rule for *(source, path, metric)*.

        When *path* is given, an exact match is required. When *path* is
        ``None`` the empty path is preferred (for ``db``/``os`` rules); if no
        rule matches, the first rule with the same ``(source, metric)`` from
        any path is returned (handy for legacy callers).

        ``fallback_to_empty`` enables the per-engine DB lookup pattern: try
        the engine-specific ``[metric.db.<engine>.<rule>]`` first and, when
        absent, fall back to the generic ``[metric.db.<rule>]`` default.
        """
        with self._data_lock:
            rules = self._rules
            by_sm = self._by_source_metric
        if path is not None:
            exact = rules.get((source, _coerce_path(path), metric))
            if exact is not None or not fallback_to_empty:
                return exact
            return rules.get((source, (), metric))
        rule = rules.get((source, (), metric))
        if rule is not None:
            return rule
        candidates = by_sm.get((source, metric))
        return candidates[0] if candidates else None

    def list_rules(
        self,
        source: str | None = None,
        *,
        path: Sequence[str] | None = None,
        api: str | None = None,
        enabled_only: bool = True,
    ) -> list[ThresholdRule]:
        """Return rules optionally filtered by source / path / api."""
        with self._data_lock:
            rules = list(self._rules.values())
        if enabled_only:
            rules = [r for r in rules if r.enabled]
        if source:
            rules = [r for r in rules if r.source == source]
        if path is not None:
            wanted = _coerce_path(path)
            rules = [r for r in rules if r.path == wanted]
        if api:
            rules = [r for r in rules if r.api == api]
        return rules

    def all_rules(self) -> list[ThresholdRule]:
        """Return every parsed rule (including disabled), no filtering."""
        with self._data_lock:
            return list(self._rules.values())

    # ------------------------------------------------------------------
    # Write surface (comment-preserving edits of monitor_thresholds.ini)
    # ------------------------------------------------------------------
    _EDITABLE_FIELDS = {
        "critical": "num", "warning": "num", "info": "num",
        "operator": "op", "unit": "str", "window": "int",
        "enabled": "bool", "description": "str",
    }
    _VALID_OPERATORS = {">", ">=", "<", "<=", "==", "!="}

    def add_rule(
        self,
        source: str,
        metric: str,
        fields: dict,
        *,
        path: Sequence[str] | None = None,
    ) -> dict:
        """Append a new threshold rule section to monitor_thresholds.ini."""
        source = (source or "").strip().lower()
        metric = (metric or "").strip()
        if not source or not metric:
            return {"ok": False, "message": "Source and metric/rule id are required."}
        norm_path = _coerce_path(path)
        if self.get_rule(source, metric, path=norm_path):
            return {"ok": False, "message": "A rule with this source/path/metric already exists."}

        raw_op = str((fields or {}).get("operator", ">")).strip()
        err, operator = self._validate_field("op", raw_op)
        if err:
            return {"ok": False, "message": f"operator: {err}"}

        levels: dict[str, str] = {}
        for lvl in ("critical", "warning", "info"):
            if lvl not in (fields or {}):
                continue
            e, v = self._validate_field("num", fields[lvl])
            if e:
                return {"ok": False, "message": f"{lvl}: {e}"}
            if v:
                levels[lvl] = v
        if not levels:
            return {"ok": False, "message": "At least one of critical/warning/info is required."}

        section = ".".join(["metric", source, *norm_path, metric])
        lines = [f"\n[{section}]", f"operator = {operator}"]
        for lvl, val in levels.items():
            lines.append(f"{lvl} = {val}")

        for opt_key, kind in (
            ("window", "int"), ("enabled", "bool"), ("unit", "str"), ("description", "str"),
            ("metric_name", "str"),
        ):
            if opt_key not in (fields or {}):
                continue
            e, v = self._validate_field(kind, fields[opt_key])
            if e:
                return {"ok": False, "message": f"{opt_key}: {e}"}
            if v or opt_key in ("enabled", "description"):
                lines.append(f"{opt_key} = {v or fields[opt_key]}")

        try:
            with open(self._config_path, "a", encoding="utf-8") as fh:
                fh.write("\n".join(lines) + "\n")
        except Exception as exc:
            return {"ok": False, "message": f"Failed to write new rule: {exc}"}

        self._load()
        return {"ok": True, "message": f"Added rule [{section}].", "section": section}

    def update_rule(
        self,
        source: str,
        metric: str,
        changes: dict,
        *,
        path: Sequence[str] | None = None,
    ) -> dict:
        """Validate and persist field changes for one rule.

        Only the fields in :data:`_EDITABLE_FIELDS` may be changed. The edit is
        surgical (comment-preserving) and the in-memory rules are reloaded so
        subsequent checks use the new values immediately.
        """
        rule = self.get_rule(source, metric, path=path)
        if rule is None:
            return {"ok": False, "message":
                    f"No threshold rule for source={source!r} metric={metric!r}."}

        clean: dict[str, str] = {}
        for raw_field, raw_val in (changes or {}).items():
            field = str(raw_field).strip().lower()
            kind = self._EDITABLE_FIELDS.get(field)
            if kind is None:
                return {"ok": False, "message": f"Field '{raw_field}' is not editable."}
            err, norm = self._validate_field(kind, raw_val)
            if err:
                return {"ok": False, "message": f"{field}: {err}"}
            clean[field] = norm

        if not clean:
            return {"ok": False, "message": "No editable changes supplied."}

        from common.config.ini_writer import set_ini_value

        section = rule.section_id
        try:
            for field, val in clean.items():
                set_ini_value(self._config_path, section, field, val)
        except Exception as exc:
            return {"ok": False, "message": f"Failed to write rule: {exc}"}

        self._load()
        return {"ok": True, "message": f"Rule '{section}' updated.",
                "section": section, "changed": list(clean.keys())}

    def set_enabled(
        self,
        source: str,
        metric: str,
        enabled: bool,
        *,
        path: Sequence[str] | None = None,
    ) -> dict:
        return self.update_rule(
            source, metric, {"enabled": "true" if enabled else "false"}, path=path
        )

    @staticmethod
    def _validate_field(kind: str, value) -> tuple[Optional[str], str]:
        raw = "" if value is None else str(value).strip()
        if kind == "num":
            if raw == "":
                return None, ""  # blank disables that severity level
            try:
                float(raw)
            except ValueError:
                return "must be a number (or blank).", ""
            return None, raw
        if kind == "int":
            try:
                n = int(raw)
            except ValueError:
                return "must be an integer.", ""
            if n < 1:
                return "must be >= 1.", ""
            return None, str(n)
        if kind == "bool":
            low = raw.lower()
            if low in ("true", "yes", "1", "on"):
                return None, "true"
            if low in ("false", "no", "0", "off"):
                return None, "false"
            return "must be true or false.", ""
        if kind == "op":
            if raw not in ThresholdChecker._VALID_OPERATORS:
                return (f"must be one of {' '.join(sorted(ThresholdChecker._VALID_OPERATORS))}.",
                        "")
            return None, raw
        return None, raw

    # ------------------------------------------------------------------
    # Evaluation
    # ------------------------------------------------------------------

    def check(
        self,
        source: str,
        metric: str,
        value: float,
        instance_id: str = "",
        *,
        path: Sequence[str] | None = None,
        fallback_to_empty: bool = False,
        window_override: int | None = None,
    ) -> Optional[AlertResult]:
        """Evaluate *value* against all severity levels for *(source, path, metric)*.

        Returns the highest-severity :class:`AlertResult` when a sustained
        breach is detected, or ``None`` when within limits / rule disabled.
        """
        if self._reload_on_check:
            self._load()

        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(value):
            return None

        rule = self.get_rule(
            source, metric, path=path, fallback_to_empty=fallback_to_empty
        )
        if rule is None or not rule.enabled:
            return None
        # Sentinel thresholds (99999 at all levels) mark display-only /
        # cumulative counters — collect for UI but never fire alerts.
        if (
            rule.critical == 99999
            and rule.warning == 99999
            and rule.info == 99999
        ):
            return None

        id_part = f".{instance_id}" if instance_id else ""

        level_map = {
            CRITICAL: rule.critical,
            WARNING:  rule.warning,
            INFO:     rule.info,
        }
        path_part = f".{rule.path_str}" if rule.path else ""

        # Every severity level gets its own sustained-breach counter.
        # We feed *every* numeric sample to ``sustained_breach`` — even
        # safe ones — so the counter resets to zero on recovery.  That
        # gives strict "N *consecutive* breaches" semantics:  a single
        # non-breaching sample wipes the in-flight count.
        #
        # Severity iteration order is highest → lowest
        # (CRITICAL, WARNING, INFO), so the first level whose counter
        # crosses ``window`` is the one we report.  We continue feeding
        # the remaining levels even after a higher one fires, so their
        # counters stay accurate for the next poll.
        fired: Optional[AlertResult] = None
        for severity in _SEVERITY_LEVELS:
            threshold = level_map[severity]
            if threshold is None:
                continue

            breach_key = f"{source}{path_part}{id_part}.{metric}.{severity}"
            # ``window_override`` lets stateless callers (the manual one-shot
            # "check this value" entry used by the CLI/API/UI) evaluate the
            # breach immediately (window=1) instead of requiring N consecutive
            # samples that can never accumulate across separate processes.
            effective_window = (
                window_override if window_override is not None else rule.window
            )
            hit = sustained_breach(
                breach_key, value, rule.operator, threshold,
                window=effective_window,
            )
            if not hit or fired is not None:
                continue

            display_value     = _format_value(value, rule.unit)
            display_threshold = _format_value(threshold, rule.unit)
            direction = (
                "HIGH" if rule.operator in (">", ">=")
                else "LOW" if rule.operator in ("<", "<=")
                else "MATCH" if rule.operator == "=="
                else "DIFF"
            )
            who = f"{instance_id} | " if instance_id else ""
            api_tag = f"/{rule.api}" if rule.api else ""

            message = (
                f"[{source.upper()}{api_tag}] {who}{rule.description}: "
                f"{direction} {display_value} "
                f"(threshold {rule.operator} {display_threshold})"
            )
            fired = AlertResult(severity=severity, message=message)

        return fired

    def check_many(
        self,
        source: str,
        metrics: dict[str, float | dict],
        instance_id: str = "",
        *,
        path: Sequence[str] | None = None,
        fallback_to_empty: bool = False,
        window_override: int | None = None,
    ) -> list[AlertResult]:
        """Evaluate a dict of metrics in one call.

        *metrics* can be ``{metric_name: float}`` or the richer
        ``{metric_name: {"value": float, "time": str}}``.

        When *path* is supplied (cloud collectors), rules are looked up by
        ``(source, path, metric)``. When *path* is ``None`` (legacy db/os
        callers), the empty-path index is consulted first and then any path
        within that source/metric.
        """
        results: list[AlertResult] = []
        for metric, payload in metrics.items():
            value = payload.get("value") if isinstance(payload, dict) else payload
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(numeric):
                continue
            result = self.check(
                source,
                metric,
                numeric,
                instance_id=instance_id,
                path=path,
                fallback_to_empty=fallback_to_empty,
                window_override=window_override,
            )
            if result:
                results.append(result)
        return results


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def _format_value(value: float, unit: str) -> str:
    """Return a human-friendly string for *value* based on *unit*."""
    unit_lower = unit.lower()

    if unit_lower == "bytes":
        gb = value / (1024 ** 3)
        if gb >= 1:
            return f"{gb:.2f} GB"
        mb = value / (1024 ** 2)
        if mb >= 1:
            return f"{mb:.1f} MB"
        return f"{value:.0f} B"

    if unit_lower in ("bytes/sec",):
        mb = value / (1024 ** 2)
        return f"{mb:.2f} MB/s"

    if unit_lower in ("seconds", "second"):
        if value < 1:
            return f"{value * 1000:.1f} ms"
        return f"{value:.2f} s"

    if unit_lower in ("percent", "%"):
        return f"{value:.1f}%"

    if unit_lower in ("ratio",):
        return f"{value:.3f}"

    if unit_lower == "mb":
        return f"{value:.0f} MB"

    if unit_lower == "gb":
        return f"{value:.2f} GB"

    if value == int(value):
        return f"{int(value)}"
    return f"{value:.2f}"


# ----------------------------------------------------------------------------
# CLI helper — list parsed rules
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    checker = ThresholdChecker(config_path=config_path)

    source_filter = sys.argv[2] if len(sys.argv) > 2 else None
    rules = checker.list_rules(source=source_filter, enabled_only=False)

    header = (
        f"{'SOURCE':<8} {'API':<16} {'PATH':<32} {'METRIC':<30} {'OP':<3} "
        f"{'CRITICAL':<12} {'WARNING':<12} {'INFO':<12} "
        f"{'UNIT':<10} {'WIN':>3} {'ON':>5}"
    )
    print(header)
    print("-" * len(header))
    for r in sorted(rules, key=lambda x: (x.source, x.path, x.metric)):
        def _fmt(v):
            return f"{v}" if v is not None else "-"
        print(
            f"{r.source:<8} {r.api:<16} {r.path_str:<32} {r.metric:<30} {r.operator:<3} "
            f"{_fmt(r.critical):<12} {_fmt(r.warning):<12} {_fmt(r.info):<12} "
            f"{r.unit:<10} {r.window:>3} {str(r.enabled):>5}"
        )
    print(f"\nTotal: {len(rules)} rule(s)")
