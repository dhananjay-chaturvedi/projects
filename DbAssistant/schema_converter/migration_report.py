"""Migration run report (G10) and resume/checkpoint store (G9).

* :class:`MigrationReport` accumulates per-table outcomes (rows, skipped,
  errors, durations, source/target counts, mismatches) and writes a JSON
  artifact to disk.
* :class:`CheckpointStore` persists how many rows of each table have been
  committed so an interrupted transfer can resume without re-inserting rows
  that already landed on the target.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timezone


def _atomic_write(path: str, text: str) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


class MigrationReport:
    """Accumulates per-table results and writes a JSON report artifact."""

    def __init__(self, path: str = "", *, source_conn: str = "", target_conn: str = ""):
        self.path = path or ""
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.started_at = datetime.now(timezone.utc).isoformat()
        self._start_perf = time.perf_counter()
        self.tables: list[dict] = []

    def add_table(self, entry: dict) -> None:
        self.tables.append(entry)
        if self.path:
            self.write()

    def summary(self) -> dict:
        total_rows = sum(int(t.get("rows_transferred") or 0) for t in self.tables)
        total_skipped = sum(int(t.get("skipped") or 0) for t in self.tables)
        total_errors = sum(int(t.get("error_count") or 0) for t in self.tables)
        successful = sum(1 for t in self.tables if t.get("ok"))
        failed = sum(1 for t in self.tables if not t.get("ok"))
        mismatches = [
            t for t in self.tables
            if t.get("source_count") is not None
            and t.get("target_count") is not None
            and t.get("source_count") != t.get("target_count")
        ]
        return {
            "started_at": self.started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(time.perf_counter() - self._start_perf, 3),
            "source_conn": self.source_conn,
            "target_conn": self.target_conn,
            "tables_total": len(self.tables),
            "tables_successful": successful,
            "tables_failed": failed,
            "rows_transferred": total_rows,
            "rows_skipped": total_skipped,
            "row_errors": total_errors,
            "count_mismatches": len(mismatches),
        }

    def to_dict(self) -> dict:
        return {"summary": self.summary(), "tables": self.tables}

    def write(self) -> str | None:
        if not self.path:
            return None
        _atomic_write(self.path, json.dumps(self.to_dict(), indent=2, default=str))
        return self.path


class CheckpointStore:
    """JSON-backed map of ``"source->target" -> rows_committed`` for resume."""

    def __init__(self, path: str):
        self.path = path
        self._data: dict = {}
        self._load()

    @staticmethod
    def default_path(source_conn: str, target_conn: str) -> str:
        safe = "".join(c if c.isalnum() else "_" for c in f"{source_conn}__{target_conn}")
        from schema_converter import module_config as _mod_cfg
        dirname = _mod_cfg.get(
            "schema.runtime", "checkpoint_dir", default="dbtool_migrate_checkpoints"
        ) or "dbtool_migrate_checkpoints"
        base = os.path.join(tempfile.gettempdir(), dirname)
        return os.path.join(base, f"{safe}.json")

    def _key(self, source_table: str, target_table: str) -> str:
        return f"{source_table}->{target_table}"

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as handle:
                self._data = json.load(handle) or {}
        except (OSError, ValueError):
            self._data = {}

    def get(self, source_table: str, target_table: str) -> int:
        return int(self._data.get(self._key(source_table, target_table), 0) or 0)

    def set(self, source_table: str, target_table: str, rows_done: int) -> None:
        self._data[self._key(source_table, target_table)] = int(rows_done)
        _atomic_write(self.path, json.dumps(self._data, indent=2))

    def clear(self, source_table: str, target_table: str) -> None:
        self._data.pop(self._key(source_table, target_table), None)
        _atomic_write(self.path, json.dumps(self._data, indent=2))
