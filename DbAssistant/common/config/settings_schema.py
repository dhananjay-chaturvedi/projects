"""Curated, self-describing settings schema.

This is the **single source of truth** the Settings UI, the ``config`` CLI, and
the read-only config API all render from — so the three surfaces stay in
lock-step (the same pattern used for cloud-connection forms).

It intentionally exposes a *curated* subset of the keys in ``config.ini`` /
``properties.ini`` — the ones an operator realistically wants to tune.
Low-level / unsafe keys (file permissions, color hex, font internals, encoding
fallbacks, path redirections) are deliberately left out of the editor; they can
still be changed by hand in the INI files.

Each :class:`SettingSpec` carries everything a generic renderer needs: a human
label, a detailed description ("what it means / what to expect"), a type, the
allowed values for enums, the default, and flags for ``requires_restart`` and
``sensitive``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SettingSpec:
    target: str          # "config" | "properties"
    section: str
    key: str
    label: str
    description: str
    type: str            # "int" | "float" | "bool" | "str" | "enum" | "tz"
    group: str
    default: str = ""
    options: tuple[str, ...] = ()
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    unit: str = ""
    requires_restart: bool = False
    sensitive: bool = False
    placeholder: str = ""

    @property
    def id(self) -> str:
        """Stable dotted identifier, e.g. ``config.database.connection.query_timeout``."""
        return f"{self.target}.{self.section}.{self.key}"


# --------------------------------------------------------------------------- #
# The curated catalogue
# --------------------------------------------------------------------------- #
_BOOL_OPTS = ("true", "false")

SETTINGS: tuple[SettingSpec, ...] = (
    # ---- Database --------------------------------------------------------- #
    SettingSpec(
        "config", "database.connection", "connection_timeout",
        "Connection timeout", "Seconds to wait when opening a new database "
        "connection before giving up. Increase for slow/remote databases.",
        "float", "Database", default="30.0", minimum=1, maximum=600, unit="s",
    ),
    SettingSpec(
        "config", "database.connection", "query_timeout",
        "Query timeout", "Seconds to wait for a single query to finish. "
        "0 means no timeout (wait indefinitely).",
        "int", "Database", default="0", minimum=0, maximum=86400, unit="s",
    ),
    SettingSpec(
        "config", "database.connection", "default_autocommit",
        "Autocommit by default", "When enabled, statements commit immediately "
        "instead of running inside an explicit transaction.",
        "bool", "Database", default="true", options=_BOOL_OPTS,
    ),
    SettingSpec(
        "config", "database.connection", "max_connection_attempts",
        "Max connection attempts", "How many times to retry a failed connection "
        "before reporting an error.",
        "int", "Database", default="8", minimum=1, maximum=50,
    ),
    SettingSpec(
        "config", "paths", "oracle_client_path",
        "Oracle Instant Client path", "Absolute path to the Oracle Instant "
        "Client directory (thick mode). Leave blank to use ORACLE_HOME or "
        "thin mode. Takes effect after restart.",
        "str", "Database", default="", requires_restart=True,
        placeholder="/opt/oracle/instantclient_21_8",
    ),

    # ---- Performance ------------------------------------------------------ #
    SettingSpec(
        "config", "database.performance", "transfer_batch_size",
        "Data transfer batch size", "Rows inserted per batch during data "
        "migration/transfer. Higher = faster but more memory.",
        "int", "Performance", default="1000", minimum=10, maximum=100000, unit="rows",
    ),
    SettingSpec(
        "config", "database.performance", "varchar_max_limit",
        "VARCHAR max length", "Upper bound applied to VARCHAR columns when "
        "converting schemas between databases.",
        "int", "Performance", default="4000", minimum=1, maximum=65535,
    ),
    # NOTE: Data Migration settings live in schema_converter/config.ini (module
    # settings button / ``dbtool migrator config`` / API).

    # NOTE: Monitoring + SSH settings are owned by the Monitoring module and
    # live in ``monitoring/monitor_config.ini`` (edited via the Monitor tab's
    # settings button, ``dbtool monitor config ...``, or the monitoring API) so
    # the module stays independently shippable. They are intentionally NOT in
    # this core schema.

    # NOTE: AI Query settings live in ai_query/config.ini (module settings
    # button / ``dbtool ai config`` / API).

    # ---- Interface / limits ---------------------------------------------- #
    SettingSpec(
        "properties", "ui.limits", "query_result_max_rows",
        "Max result rows", "Maximum number of rows fetched/displayed for a query "
        "result grid.",
        "int", "Interface", default="10000", minimum=1, maximum=10000000, unit="rows",
    ),
    SettingSpec(
        "properties", "ui.limits", "sql_history_limit",
        "SQL history size", "How many past SQL statements to remember.",
        "int", "Interface", default="100", minimum=0, maximum=100000,
    ),
    SettingSpec(
        "properties", "ui.limits", "sql_preview_limit",
        "SQL preview length", "Maximum characters shown for SQL previews in "
        "history, dashboards, and status text. 0 means no truncation.",
        "int", "Interface", default="100", minimum=0, maximum=100000, unit="chars",
    ),
    SettingSpec(
        "properties", "ui.limits", "cell_copy_limit",
        "Copied cell preview length", "Maximum characters shown in the status "
        "bar after copying a cell. The copied value itself is not truncated. "
        "0 means no truncation.",
        "int", "Interface", default="50", minimum=0, maximum=100000, unit="chars",
    ),
    SettingSpec(
        "properties", "ui.window", "main_window_width",
        "Main window width", "Default width of the main window in pixels "
        "(takes effect on next launch).",
        "int", "Interface", default="1150", minimum=640, maximum=10000, unit="px",
        requires_restart=True,
    ),
    SettingSpec(
        "properties", "ui.window", "main_window_height",
        "Main window height", "Default height of the main window in pixels "
        "(takes effect on next launch).",
        "int", "Interface", default="780", minimum=480, maximum=10000, unit="px",
        requires_restart=True,
    ),

    # ---- Logging / General ------------------------------------------------ #
    SettingSpec(
        "properties", "logging", "enable_stdout",
        "Console output", "Print informational messages to stdout. Disable for "
        "quiet/scripted runs (errors still go to stderr).",
        "bool", "General", default="true", options=_BOOL_OPTS,
    ),
    SettingSpec(
        "properties", "logging", "enable_info",
        "Verbose debug output", "Print low-level debug/trace messages (driver "
        "registration, connection traces). Noisy; off by default.",
        "bool", "General", default="false", options=_BOOL_OPTS,
    ),
    SettingSpec(
        "config", "project", "debug_mode",
        "Debug mode", "Enable extra diagnostics across the tool.",
        "bool", "General", default="false", options=_BOOL_OPTS,
    ),
    SettingSpec(
        "config", "project", "version",
        "Application version", "Version advertised by the REST API. Leave blank "
        "to read the top-level VERSION file.",
        "str", "General", default="", placeholder="1.0.0",
    ),
    SettingSpec(
        "config", "api", "host",
        "API default host", "Default host used by `dbtool api` when --host is "
        "not provided.",
        "str", "General", default="127.0.0.1", placeholder="127.0.0.1",
    ),
    SettingSpec(
        "config", "api", "port",
        "API default port", "Default port used by `dbtool api` when --port is "
        "not provided.",
        "int", "General", default="8000", minimum=1, maximum=65535,
    ),
    SettingSpec(
        "config", "api", "cors_origins",
        "API CORS origins", "Comma-separated CORS origins allowed by the REST "
        "API. Use * only for local/development scenarios.",
        "str", "General", default="*", placeholder="http://localhost:3000",
    ),
    SettingSpec(
        "config", "api", "max_body_bytes",
        "API max body size", "Maximum accepted REST API request body size.",
        "int", "General", default="10485760", minimum=1024, maximum=104857600,
        unit="bytes",
    ),
    SettingSpec(
        "config", "project", "timezone",
        "Timezone (UTC offset)",
        "UTC offset applied to displayed timestamps, e.g. +5:30 (IST), "
        "-08:00 (PST) or +00:00 (UTC). Times are computed as UTC + this "
        "offset. Blank uses the system timezone. An IANA name "
        "(e.g. Asia/Kolkata) is also accepted.",
        "tz", "General", default="", placeholder="+5:30",
    ),
    # NOTE: The product name ("Database Assistant - Multi-DB Tool") is a fixed,
    # universal brand defined in ``common.branding`` — intentionally NOT a
    # configurable setting, so it never drifts between surfaces.

    # NOTE: Notification settings ([notifications] + the Teams/SMTP secrets) are
    # owned by the Monitoring module (monitoring/monitor_config.ini + encrypted
    # store) and are edited via the Monitor tab's settings button,
    # ``dbtool monitor notify config ...``, or the monitoring API — not this
    # core schema — so monitoring stays independently shippable.
)


# No core secret settings: notification secrets are owned by the Monitoring
# module (see monitoring.notifications_settings).
SECRET_SETTINGS: tuple[SettingSpec, ...] = ()


# --------------------------------------------------------------------------- #
# Lookup helpers
# --------------------------------------------------------------------------- #
def all_specs(include_secrets: bool = True) -> list[SettingSpec]:
    specs = list(SETTINGS)
    if include_secrets:
        specs += list(SECRET_SETTINGS)
    return specs


def group_order() -> list[str]:
    seen: list[str] = []
    for s in all_specs():
        if s.group not in seen:
            seen.append(s.group)
    return seen


def by_group(include_secrets: bool = True) -> dict[str, list[SettingSpec]]:
    out: dict[str, list[SettingSpec]] = {}
    for s in all_specs(include_secrets=include_secrets):
        out.setdefault(s.group, []).append(s)
    return out


def find(spec_id: str) -> Optional[SettingSpec]:
    for s in all_specs():
        if s.id == spec_id:
            return s
    return None


def find_by(target: str, section: str, key: str) -> Optional[SettingSpec]:
    for s in all_specs():
        if s.target == target and s.section == section and s.key == key:
            return s
    return None
