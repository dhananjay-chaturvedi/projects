"""
db_metric_config.py
====================
Declarative metric definitions for every supported local database engine.

Each entry ties one displayable metric to:
  - an INI key  (source, metric)  →  controls enabled/disabled in monitor_thresholds.ini
  - a display name and section
  - how to collect the value  ("query" or "computed")
  - how to format the raw value for display

The public function ``collect_metrics(db_manager, host, checker)`` returns a
structured list ready for ``ServerMonitorUI._format_metric_block()``.

OS / host metrics (CPU, memory, disk) are collected exclusively via SSH
server monitoring — not through the DB connection path.

INI lookup uses per-engine paths with generic fallback::

    [metric.db.active_connections]           # generic default
    [metric.db.mysql.database_size_mb]     # engine-specific override

Design goals
------------
* Adding a new metric = adding one dict to the right DB list below.
* Disabling a metric = set ``enabled = false`` for its INI key in
  monitor_thresholds.ini.  No code changes needed.
* Same standard sections appear for all DB types; missing data shows "—".
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Section order — controls the visual order in the display block
# ---------------------------------------------------------------------------

SECTION_ORDER = [
    "Connections",
    "Throughput / Performance",
    "Memory / Cache",
    "I/O",
    "Storage",
    "Locks / Waits",
    "Replication",
]


# ---------------------------------------------------------------------------
# Value formatters
# ---------------------------------------------------------------------------

def _fmt_pct(v):
    return f"{float(v):>11.1f} %" if v is not None else "—"

def _fmt_int(v):
    return f"{int(float(v)):>14,}" if v is not None else "—"

def _fmt_mb(v):
    return f"{float(v):>10.1f} MB" if v is not None else "—"

def _fmt_gb(v):
    return f"{float(v):>10.2f} GB" if v is not None else "—"

def _fmt_bytes_to_gb(v):
    return f"{float(v)/(1024**3):>10.2f} GB" if v is not None else "—"

def _fmt_bytes_to_mb(v):
    return f"{float(v)/(1024**2):>10.1f} MB" if v is not None else "—"

def _fmt_float(v, unit=""):
    if v is None:
        return "—"
    suffix = f" {unit}" if unit else ""
    return f"{float(v):>12.2f}{suffix}"

def _fmt_days(v):
    return f"{float(v)/86400:>11.1f} days" if v is not None else "—"

def _fmt_per_s(v):
    return f"{float(v):>10.1f} /s" if v is not None else "—"


# ---------------------------------------------------------------------------
# Metric spec helpers
# ---------------------------------------------------------------------------

def _q(display, section, *parts, extract=None, scale=1.0):
    """SQL-query-based metric.

    *scale* multiplies the raw SQL value before it is stored in *collected*
    for threshold evaluation (e.g. bytes → MB uses ``1/(1024**2)``).
    Display formatting still receives the unscaled raw value.
    """
    ini_source, ini_metric, query, fmt, *rest = parts
    extract = rest[0] if rest else extract
    return {
        "ini": (ini_source, ini_metric),
        "display": display,
        "section": section,
        "source": "query",
        "query": query,
        "extract": extract,   # callable(rows) → scalar; None = rows[0][0] or rows[0][1]
        "format": fmt,
        "scale": scale,
    }


def _cmp(display, section, *parts):
    """Computed metric derived from other collected values."""
    ini_source, ini_metric, deps, compute, fmt = parts
    return {
        "ini": (ini_source, ini_metric),
        "display": display,
        "section": section,
        "source": "computed",
        "deps": deps,          # list of display-name keys needed from already-collected values
        "compute": compute,    # callable(vals_dict) → scalar | None
        "format": fmt,
    }


def db_type_path(db_type: str) -> tuple[str, ...]:
    """Normalise a DB type label into the INI path segment for lookups.

    Each engine has its own threshold namespace under ``[metric.db.<engine>.*]``:
    ``mysql``, ``mariadb``, ``oracle``, ``postgresql``, ``sqlite``. MariaDB and
    MySQL share the same collected metric spec (:data:`METRIC_SPECS`) but use
    separate threshold namespaces so each can be tuned/disabled independently.
    Unknown engine sections fall back to the generic ``[metric.db.<metric>]``.
    """
    t = (db_type or "").strip().lower()
    return (t,) if t else ()


# ---------------------------------------------------------------------------
# Metric definitions per DB type
# ---------------------------------------------------------------------------

# ── MySQL / MariaDB ──────────────────────────────────────────────────────────

_MYSQL_METRICS = [
    _q("Active Connections",   "Connections", "db", "active_connections",
       "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND != 'Sleep'",
       _fmt_int),
    _q("Total Connections",    "Connections", "db", "total_connections",
       "SELECT COUNT(*) FROM information_schema.PROCESSLIST",
       _fmt_int),
    _q("Max Connections",      "Connections", "db", "max_connections",
       "SHOW VARIABLES LIKE 'max_connections'",
       _fmt_int),
    _cmp("Connection Usage",   "Connections", "db", "connection_usage_pct",
         ["Active Connections", "Max Connections"],
         lambda d: (d.get("Active Connections", 0) / d["Max Connections"] * 100)
         if d.get("Max Connections") else None,
         _fmt_pct),

    _q("Queries Per Second",   "Throughput / Performance", "db", "queries_per_sec",
       "SHOW GLOBAL STATUS LIKE 'Questions'", _fmt_int),
    _q("Slow Queries",         "Throughput / Performance", "db", "slow_query_count",
       "SHOW GLOBAL STATUS LIKE 'Slow_queries'", _fmt_int),
    _q("Threads Running",      "Throughput / Performance", "db", "threads_running",
       "SHOW GLOBAL STATUS LIKE 'Threads_running'", _fmt_int),
    _q("Uptime",               "Throughput / Performance", "db", "uptime_sec",
       "SHOW GLOBAL STATUS LIKE 'Uptime'", _fmt_days),

    _q("Buffer Pool Used",     "Memory / Cache", "db", "buffer_pool_used_mb",
       "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_bytes_data'", _fmt_bytes_to_mb,
       scale=1 / (1024 ** 2)),
    _q("Buffer Pool Total",    "Memory / Cache", "db", "buffer_pool_total_mb",
       "SHOW VARIABLES LIKE 'innodb_buffer_pool_size'", _fmt_bytes_to_mb,
       scale=1 / (1024 ** 2)),
    _cmp("Buffer Pool Usage",  "Memory / Cache", "db", "buffer_pool_usage_pct",
         ["Buffer Pool Used", "Buffer Pool Total"],
         lambda d: (d.get("Buffer Pool Used", 0) / d["Buffer Pool Total"] * 100)
         if d.get("Buffer Pool Total") else None,
         _fmt_pct),
    _q("BP Read Requests",     "Memory / Cache", "db", "bp_read_requests",
       "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_read_requests'", _fmt_int),
    _q("BP Disk Reads",        "Memory / Cache", "db", "bp_disk_reads",
       "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_reads'", _fmt_int),
    _cmp("Buffer Pool Hit %",  "Memory / Cache", "db", "cache_hit_ratio",
         ["BP Disk Reads", "BP Read Requests"],
         lambda d: ((1 - d["BP Disk Reads"] / d["BP Read Requests"]) * 100)
         if d.get("BP Read Requests") else None,
         _fmt_pct),
    _q("Query Cache Hits",     "Memory / Cache", "db", "query_cache_hits",
       "SHOW GLOBAL STATUS LIKE 'Qcache_hits'", _fmt_int),

    _q("Bytes Received",       "I/O", "db", "net_bytes_recv",
       "SHOW GLOBAL STATUS LIKE 'Bytes_received'", _fmt_bytes_to_gb),
    _q("Bytes Sent",           "I/O", "db", "net_bytes_sent",
       "SHOW GLOBAL STATUS LIKE 'Bytes_sent'", _fmt_bytes_to_gb),
    _q("InnoDB Data Read",     "I/O", "db", "io_read_mb",
       "SHOW GLOBAL STATUS LIKE 'Innodb_data_read'", _fmt_bytes_to_mb,
       scale=1 / (1024 ** 2)),
    _q("InnoDB Data Written",  "I/O", "db", "io_write_mb",
       "SHOW GLOBAL STATUS LIKE 'Innodb_data_written'", _fmt_bytes_to_mb,
       scale=1 / (1024 ** 2)),
    _q("Temp Tables / sec",    "Storage", "db", "temp_tables_per_sec",
       "SHOW GLOBAL STATUS LIKE 'Created_tmp_tables'", _fmt_int),

    _q("Table Locks Waited",   "Locks / Waits", "db", "table_lock_wait_ratio",
       "SHOW GLOBAL STATUS LIKE 'Table_locks_waited'", _fmt_int),
    _q("Row Lock Waits",       "Locks / Waits", "db", "row_lock_waits",
       "SHOW GLOBAL STATUS LIKE 'Innodb_row_lock_waits'", _fmt_int),
    _q("Deadlocks",            "Locks / Waits", "db", "deadlocks_per_min",
       "SHOW GLOBAL STATUS LIKE 'Innodb_deadlocks'", _fmt_int),

    _q("Replica Lag (sec)",    "Replication", "db", "replication_lag_s",
       "SHOW SLAVE STATUS", _fmt_float,
       extract=lambda rows: rows[0][32] if rows and len(rows[0]) > 32 else None),
]


# ── Oracle ───────────────────────────────────────────────────────────────────

_ORACLE_METRICS = [
    _q("Active Sessions",      "Connections", "db", "active_connections",
       "SELECT COUNT(*) FROM v$session WHERE status = 'ACTIVE'", _fmt_int),
    _q("Total Sessions",       "Connections", "db", "total_connections",
       "SELECT COUNT(*) FROM v$session", _fmt_int),
    _q("Inactive Sessions",    "Connections", "db", "idle_connections",
       "SELECT COUNT(*) FROM v$session WHERE status = 'INACTIVE'", _fmt_int),

    _q("Executions / sec",     "Throughput / Performance", "db", "queries_per_sec",
       "SELECT value FROM v$sysmetric WHERE metric_name='Executions Per Sec' AND group_id=2",
       _fmt_float),
    _q("User Calls / sec",     "Throughput / Performance", "db", "user_calls_per_sec",
       "SELECT value FROM v$sysmetric WHERE metric_name='User Calls Per Sec' AND group_id=2",
       _fmt_float),
    _q("Host CPU Utilization", "Throughput / Performance", "db", "host_cpu_utilization",
       "SELECT value FROM v$sysmetric WHERE metric_name='Host CPU Utilization (%)' AND group_id=2",
       _fmt_pct),
    _q("DB CPU Time Ratio",    "Throughput / Performance", "db", "db_cpu_time_ratio",
       "SELECT value FROM v$sysmetric WHERE metric_name='Database CPU Time Ratio' AND group_id=2",
       _fmt_pct),

    _q("SGA Size",             "Memory / Cache", "db", "sga_size_mb",
       "SELECT ROUND(SUM(value)/1024/1024,2) FROM v$sga", _fmt_float, lambda r: r[0][0] if r else None),
    _q("PGA Used",             "Memory / Cache", "db", "pga_used_mb",
       "SELECT ROUND(value/1024/1024,2) FROM v$pgastat WHERE name='total PGA allocated'",
       _fmt_float, lambda r: r[0][0] if r else None),
    _q("Buffer Cache Hit %",   "Memory / Cache", "db", "cache_hit_ratio",
       "SELECT ROUND((1-(phy.value/(log.value+phy.value)))*100,2) "
       "FROM v$sysstat phy, v$sysstat log "
       "WHERE phy.name='physical reads' AND log.name='db block gets'",
       _fmt_pct, lambda r: r[0][0] if r else None),

    _q("Physical Reads",       "I/O", "db", "io_read_mb",
       "SELECT value FROM v$sysstat WHERE name='physical reads'", _fmt_int),
    _q("Physical Writes",      "I/O", "db", "io_write_mb",
       "SELECT value FROM v$sysstat WHERE name='physical writes'", _fmt_int),
    _q("DB Block Gets",        "I/O", "db", "db_block_gets",
       "SELECT value FROM v$sysstat WHERE name='db block gets'", _fmt_int),

    _q("Enqueue Waits",        "Locks / Waits", "db", "table_lock_wait_ratio",
       "SELECT value FROM v$sysstat WHERE name='enqueue waits'", _fmt_int),
    _q("Lock Wait Time",       "Locks / Waits", "db", "lock_wait_time_ms",
       "SELECT SUM(wait_time+time_waited) FROM v$session_wait WHERE wait_class!='Idle'",
       _fmt_int),
]


# ── PostgreSQL ───────────────────────────────────────────────────────────────

_POSTGRES_METRICS = [
    _q("Active Connections",   "Connections", "db", "active_connections",
       "SELECT count(*) FROM pg_stat_activity WHERE state='active'", _fmt_int),
    _q("Total Connections",    "Connections", "db", "total_connections",
       "SELECT count(*) FROM pg_stat_activity", _fmt_int),
    _q("Idle Connections",     "Connections", "db", "idle_connections",
       "SELECT count(*) FROM pg_stat_activity WHERE state='idle'", _fmt_int),
    _q("Max Connections",      "Connections", "db", "max_connections",
       "SELECT setting::int FROM pg_settings WHERE name='max_connections'", _fmt_int),
    _cmp("Connection Usage",   "Connections", "db", "connection_usage_pct",
         ["Active Connections", "Max Connections"],
         lambda d: (d.get("Active Connections", 0) / d["Max Connections"] * 100)
         if d.get("Max Connections") else None,
         _fmt_pct),

    _q("Transactions Committed",  "Throughput / Performance", "db", "queries_per_sec",
       "SELECT SUM(xact_commit) FROM pg_stat_database WHERE datname=current_database()",
       _fmt_int),
    _q("Transactions Rolled Back","Throughput / Performance", "db", "rollbacks",
       "SELECT SUM(xact_rollback) FROM pg_stat_database WHERE datname=current_database()",
       _fmt_int),

    _q("Shared Buffers",       "Memory / Cache", "db", "shared_buffers_mb",
       "SELECT ROUND(setting::numeric * 8192 / 1024 / 1024, 2) FROM pg_settings WHERE name='shared_buffers'",
       _fmt_float, lambda r: r[0][0] if r else None),
    _q("Blocks Read",          "Memory / Cache", "db", "blocks_read",
       "SELECT SUM(blks_read) FROM pg_stat_database WHERE datname=current_database()",
       _fmt_int),
    _q("Blocks Hit",           "Memory / Cache", "db", "blocks_hit",
       "SELECT SUM(blks_hit) FROM pg_stat_database WHERE datname=current_database()",
       _fmt_int),
    _cmp("Cache Hit Ratio",    "Memory / Cache", "db", "cache_hit_ratio",
         ["Blocks Hit", "Blocks Read"],
         lambda d: (d["Blocks Hit"] / (d["Blocks Hit"] + d.get("Blocks Read", 0)) * 100)
         if (d.get("Blocks Hit") and (d.get("Blocks Hit", 0) + d.get("Blocks Read", 0)) > 0)
         else None,
         _fmt_pct),

    _q("Database Size",        "Storage", "db", "database_size_mb",
       "SELECT ROUND(pg_database_size(current_database())::numeric/1024/1024,2)",
       _fmt_float, lambda r: r[0][0] if r else None),
    _q("Dead Tuples",          "Storage", "db", "dead_tuples",
       "SELECT SUM(n_dead_tup) FROM pg_stat_user_tables", _fmt_int),
    _q("Live Tuples",          "Storage", "db", "live_tuples",
       "SELECT SUM(n_live_tup) FROM pg_stat_user_tables", _fmt_int),

    _q("Active Locks",         "Locks / Waits", "db", "active_locks",
       "SELECT count(*) FROM pg_locks WHERE granted=true", _fmt_int),
    _q("Waiting Locks",        "Locks / Waits", "db", "waiting_locks",
       "SELECT count(*) FROM pg_locks WHERE granted=false", _fmt_int),
    _q("Deadlocks",            "Locks / Waits", "db", "deadlocks_per_min",
       "SELECT SUM(deadlocks) FROM pg_stat_database WHERE datname=current_database()",
       _fmt_int),
]


# ── SQLite ───────────────────────────────────────────────────────────────────

_SQLITE_METRICS = [
    _q("Page Count",           "Storage", "db", "database_size_mb",
       "PRAGMA page_count", _fmt_int),
    _q("Page Size (bytes)",    "Storage", "db", "page_size_bytes",
       "PRAGMA page_size", _fmt_int),
    _q("Free Pages",           "Storage", "db", "free_pages",
       "PRAGMA freelist_count", _fmt_int),
    _q("Journal Mode",         "Storage", "db", "journal_mode",
       "PRAGMA journal_mode", lambda v: f"{'':>10}{v}" if v else "—"),
    _q("WAL Checkpoint",       "Storage", "db", "wal_checkpoint",
       "PRAGMA wal_checkpoint(PASSIVE)", _fmt_int,
       extract=lambda rows: rows[0][1] if rows and len(rows[0]) > 1 else None),
]


# Registry
METRIC_SPECS: dict[str, list] = {
    "MySQL":      _MYSQL_METRICS,
    "MariaDB":    _MYSQL_METRICS,
    "Oracle":     _ORACLE_METRICS,
    "PostgreSQL": _POSTGRES_METRICS,
    "SQLite":     _SQLITE_METRICS,
}


# ---------------------------------------------------------------------------
# Collection engine
# ---------------------------------------------------------------------------

def _extract_scalar(result_data):
    """Pull the first scalar from execute_query() result dict or list."""
    if result_data is None:
        return None
    if isinstance(result_data, dict):
        rows = result_data.get("rows") or result_data.get("data") or []
    elif isinstance(result_data, list):
        rows = result_data
    else:
        return None
    if not rows:
        return None
    row = rows[0]
    # SHOW STATUS / SHOW VARIABLES returns (Variable_name, Value) → use col 1
    if len(row) == 2 and isinstance(row[0], str) and not str(row[0]).lstrip("-").replace(".", "").isdigit():
        val = row[1]
    else:
        val = row[0]
    if val is None:
        return None
    try:
        s = str(val).strip()
        return float(s) if "." in s else int(s)
    except (ValueError, TypeError):
        return str(val)


def collect_metrics(
    db_manager,
    host: str = "",
    checker=None,    # ThresholdChecker | None — for enabled-flag filtering
) -> tuple[list, dict]:
    """
    Collect all enabled DB metrics for *db_manager* and return a 2-tuple:
      (sections, raw_values)

    sections   — list of (section_title, [(display_name, value_str)])
                 ready for _format_metric_block()
    raw_values — flat dict {display_name: numeric_or_str} for threshold evaluation

    Parameters
    ----------
    db_manager : database manager object (has .db_type, .execute_query())
    host       : retained for API compatibility; OS metrics are not collected here
    checker    : ThresholdChecker instance used to filter by enabled flag
    """
    del host  # OS metrics are SSH-only; host is not used for DB collection
    db_type = getattr(db_manager, "db_type", "")
    specs = METRIC_SPECS.get(db_type, [])
    ini_path = db_type_path(db_type)

    collected: dict[str, object] = {}   # display_name → raw numeric
    display: dict[str, str] = {}        # display_name → formatted string

    def _is_enabled(ini_source, ini_metric):
        if checker is None:
            return True
        rule = checker.get_rule(
            ini_source, ini_metric, path=ini_path, fallback_to_empty=True
        )
        return rule is None or rule.enabled  # unknown metrics show by default

    for spec in specs:
        src, metric_key = spec["ini"]
        if not _is_enabled(src, metric_key):
            continue

        name = spec["display"]
        fmt  = spec["format"]
        kind = spec["source"]

        raw = None

        if kind == "query":
            try:
                with _noop_ctx():
                    result_data, error = db_manager.execute_query(spec["query"])
                if error:
                    display[name] = f"Error: {str(error)[:40]}"
                    continue
                extractor = spec.get("extract")
                if extractor:
                    rows = (result_data.get("rows") or result_data.get("data") or []) \
                        if isinstance(result_data, dict) else (result_data or [])
                    raw = extractor(rows)
                else:
                    raw = _extract_scalar(result_data)
            except Exception as exc:
                display[name] = f"Error: {str(exc)[:40]}"
                continue

        elif kind == "computed":
            dep_vals = {d: collected.get(d) for d in spec["deps"]}
            if any(v is None for v in dep_vals.values()):
                display[name] = "—"
                continue
            try:
                raw = spec["compute"](dep_vals)
            except Exception:
                display[name] = "—"
                continue

        if raw is None:
            display[name] = "—"
        else:
            try:
                scale = float(spec.get("scale", 1.0))
                collected[name] = float(raw) * scale
            except (TypeError, ValueError):
                collected[name] = raw
            try:
                display[name] = fmt(raw)
            except Exception:
                display[name] = str(raw)

    # Build ordered sections
    bucket: dict[str, list] = {}
    for spec in specs:
        src, metric_key = spec["ini"]
        if not _is_enabled(src, metric_key):
            continue
        name = spec["display"]
        if name not in display:
            continue
        sec = spec["section"]
        bucket.setdefault(sec, []).append((name, display[name]))

    sections = [
        (sec, bucket[sec])
        for sec in SECTION_ORDER
        if sec in bucket and bucket[sec]
    ]
    # Append any sections not in SECTION_ORDER
    for sec, items in bucket.items():
        if sec not in SECTION_ORDER:
            sections.append((sec, items))

    os_note = ""

    return sections, {k: v for k, v in collected.items() if isinstance(v, float)}, os_note


class _noop_ctx:
    """Trivial context manager used as a placeholder (lock is at the call site)."""
    def __enter__(self): return self
    def __exit__(self, *_): return False
