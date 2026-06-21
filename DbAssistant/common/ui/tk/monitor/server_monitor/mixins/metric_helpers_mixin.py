"""MetricHelpersMixin — ServerMonitorUI mixin."""

from __future__ import annotations


class MetricHelpersMixin:
    _BLOCK_WIDTH = 66

    @staticmethod
    def _format_metric_block(
        db_name: str,
        db_label: str,
        timestamp: str,
        sections: list,
        resource: str = "",
        note: str = "",
    ) -> str:
        """
        Render a standardised metric text block used for BOTH local and cloud DBs.
        """
        W = MetricHelpersMixin._BLOCK_WIDTH
        SEP = "═" * W
        THIN = "─" * W
        lines: list[str] = []

        lines.append(SEP)
        lines.append(f" Database  : {db_name}")
        if resource:
            lines.append(f" Resource  : {resource}")
        lines.append(f" Type      : {db_label}")
        lines.append(f" Updated   : {timestamp}")
        lines.append(SEP)

        for title, items in sections:
            if not items:
                continue
            pad = max(0, W - len(title) - 6)
            lines.append(f"\n  ─ {title} {'─' * pad}")
            for name, val in items:
                lines.append(f"    {name:<32}  {val}")

        if note:
            pad = max(0, W - 9)
            lines.append(f"\n  ─ Note {'─' * pad}")
            lines.append(f"    {note}")

        lines.append(f"\n{THIN}\n")
        return "\n".join(lines)

    @staticmethod
    def _group_local_metrics(db_type: str, stats: dict) -> tuple[list, str]:
        """Convert flat stats dict into (sections, note) for _format_metric_block."""

        def _v(key, default=None):
            val = stats.get(key)
            if val is None:
                return default
            try:
                return float(val)
            except (TypeError, ValueError):
                return val

        def _fmt_int(v):
            if v is None:
                return "—"
            try:
                return f"{int(v):>14,}"
            except Exception:
                return str(v)

        def _fmt_pct(v):
            return "—" if v is None else f"{v:>13.1f} %"

        def _fmt_bytes_mb(v):
            return "—" if v is None else f"{v / (1024**2):>11.1f} MB"

        def _fmt_bytes_gb(v):
            return "—" if v is None else f"{v / (1024**3):>11.2f} GB"

        def _fmt_float(v, unit=""):
            return "—" if v is None else f"{v:>13.2f}{(' ' + unit) if unit else ''}"

        sections: list = []
        note = "Host CPU / memory / disk: use SSH Server Monitoring tab"

        if db_type in ("MySQL", "MariaDB"):
            active = _v("Active Connections")
            total = _v("Total Connections")
            mx = _v("Max Connections")
            usage = (active / mx * 100) if (active is not None and mx) else None
            sections.append(("Connections", [
                ("Active Connections", _fmt_int(active)),
                ("Total Connections", _fmt_int(total)),
                ("Max Connections", _fmt_int(mx)),
                ("Connection Usage", _fmt_pct(usage)),
            ]))
            sections.append(("Throughput / Performance", [
                ("Queries Per Second", _fmt_int(_v("Queries Per Second"))),
                ("Slow Queries", _fmt_int(_v("Slow Queries"))),
                ("Threads Running", _fmt_int(_v("Threads Running"))),
                ("Uptime", f"{_v('Uptime (sec)', 0)/86400:>11.1f} days"
                          if _v("Uptime (sec)") is not None else "—"),
            ]))
            bp_used_bytes = _v("Buffer Pool Used (bytes)")
            bp_total_bytes = _v("Buffer Pool Total (bytes)")
            bp_reads = _v("Buffer Pool Disk Reads")
            bp_requests = _v("Buffer Pool Read Requests")
            bp_hit = None
            if bp_reads is not None and bp_requests and bp_requests > 0:
                bp_hit = max(0.0, (1.0 - bp_reads / bp_requests) * 100)
            sections.append(("Memory / Buffer Cache", [
                ("Buffer Pool Used", _fmt_bytes_mb(bp_used_bytes)),
                ("Buffer Pool Total", _fmt_bytes_mb(bp_total_bytes)),
                ("Buffer Pool Hit Ratio", _fmt_pct(bp_hit)),
                ("Query Cache Hits", _fmt_int(_v("Query Cache Hits"))),
            ]))
            sections.append(("I/O", [
                ("Bytes Received", _fmt_bytes_gb(_v("Bytes Received"))),
                ("Bytes Sent", _fmt_bytes_gb(_v("Bytes Sent"))),
                ("InnoDB Data Read", _fmt_bytes_mb(_v("InnoDB Data Read (bytes)"))),
                ("InnoDB Data Written", _fmt_bytes_mb(_v("InnoDB Data Written (bytes)"))),
            ]))
            sections.append(("Locks", [
                ("Table Locks Waited", _fmt_int(_v("Table Locks Waited"))),
                ("InnoDB Row Lock Waits", _fmt_int(_v("Innodb Row Lock Waits"))),
            ]))

        elif db_type == "Oracle":
            sections.append(("Sessions", [
                ("Active Sessions", _fmt_int(_v("Active Sessions"))),
                ("Total Sessions", _fmt_int(_v("Total Sessions"))),
                ("Inactive Sessions", _fmt_int(_v("Inactive Sessions"))),
            ]))
            sections.append(("Performance", [
                ("Executions Per Sec", _fmt_float(_v("Executions Per Sec"), "/s")),
                ("User Calls Per Sec", _fmt_float(_v("User Calls Per Sec"), "/s")),
                ("Buffer Cache Hit", _fmt_pct(_v("Buffer Cache Hit%"))),
                ("Host CPU Utilization", _fmt_pct(_v("Host CPU Utilization %"))),
            ]))
            sections.append(("I/O", [
                ("Physical Reads", _fmt_int(_v("Physical Reads"))),
                ("Physical Writes", _fmt_int(_v("Physical Writes"))),
                ("DB Block Gets", _fmt_int(_v("DB Block Gets"))),
            ]))
            sections.append(("Memory", [
                ("SGA Size", _fmt_float(_v("SGA Size (MB)"), "MB")),
                ("PGA Used", _fmt_float(_v("PGA Used (MB)"), "MB")),
            ]))
            sections.append(("Locks / Waits", [
                ("Enqueue Waits", _fmt_int(_v("Enqueue Waits"))),
                ("Lock Wait Time", _fmt_int(_v("Lock Wait Time"))),
            ]))
            note = "CPU: Host CPU Utilization shown above (from v$sysmetric)"

        elif db_type == "PostgreSQL":
            active = _v("Active Connections")
            mx = _v("Max Connections")
            usage = (active / mx * 100) if (active is not None and mx) else None
            sections.append(("Connections", [
                ("Active Connections", _fmt_int(active)),
                ("Total Connections", _fmt_int(_v("Total Connections"))),
                ("Idle Connections", _fmt_int(_v("Idle Connections"))),
                ("Max Connections", _fmt_int(mx)),
                ("Connection Usage", _fmt_pct(usage)),
            ]))
            sections.append(("Transactions", [
                ("Committed", _fmt_int(_v("Transactions Committed"))),
                ("Rolled Back", _fmt_int(_v("Transactions Rolled Back"))),
                ("Deadlocks", _fmt_int(_v("Deadlocks"))),
            ]))
            blk_hit = _v("Blocks Hit")
            blk_read = _v("Blocks Read")
            total_blk = (blk_hit or 0) + (blk_read or 0)
            hit_ratio = (blk_hit / total_blk * 100) if total_blk else None
            sections.append(("I/O / Cache", [
                ("Blocks Read", _fmt_int(blk_read)),
                ("Blocks Hit (cached)", _fmt_int(blk_hit)),
                ("Cache Hit Ratio", _fmt_pct(hit_ratio)),
            ]))
            sections.append(("Storage", [
                ("Database Size", _fmt_float(_v("Database Size (MB)"), "MB")),
            ]))
            sections.append(("Locks", [
                ("Active Locks", _fmt_int(_v("Active Locks"))),
                ("Waiting Locks", _fmt_int(_v("Waiting Locks"))),
            ]))
            sections.append(("Table Health", [
                ("Live Tuples", _fmt_int(_v("Total Tuples"))),
                ("Dead Tuples", _fmt_int(_v("Dead Tuples"))),
            ]))

        else:
            sections.append(("Metrics", [(k, str(v)) for k, v in stats.items()]))
            note = ""

        return sections, note
