"""Isolated JSONL storage — one file per project/connection/database."""

from __future__ import annotations

import fcntl
import json
import logging
import os
from pathlib import Path
from typing import Iterator

from ai_assistant.capture.record import CaptureRecord

_log = logging.getLogger(__name__)


class IsolatedCaptureStore:
    """Append-only store partitioned by project → connection → database."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, project_id: str, connection: str, database: str) -> Path:
        return (
            self.root
            / project_id
            / connection
            / database
            / "samples.jsonl"
        )

    def append(self, record: CaptureRecord) -> Path:
        path = self._path(record.project_id, record.connection_name, record.database)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                fh.write(record.to_json_line() + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)
        return path

    def iter_records(
        self,
        project_id: str,
        *,
        connection: str | None = None,
        database: str | None = None,
        accepted_only: bool = False,
    ) -> Iterator[CaptureRecord]:
        base = self.root / project_id
        if not base.is_dir():
            return
        conn_dirs = [base / connection] if connection else sorted(base.iterdir())
        for cdir in conn_dirs:
            if not cdir.is_dir():
                continue
            db_dirs = [cdir / database] if database else sorted(cdir.iterdir())
            for ddir in db_dirs:
                path = ddir / "samples.jsonl"
                if not path.is_file():
                    continue
                with path.open(encoding="utf-8") as fh:
                    for lineno, line in enumerate(fh, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = CaptureRecord.from_json_line(line)
                        except Exception:
                            _log.warning("Skipping corrupt JSONL line %d in %s", lineno, path)
                            continue
                        if accepted_only and not rec.quality_accepted:
                            continue
                        yield rec

    def stats(self, project_id: str) -> dict:
        total = accepted = 0
        for rec in self.iter_records(project_id):
            total += 1
            if rec.quality_accepted:
                accepted += 1
        return {"project_id": project_id, "total": total, "accepted": accepted}

    def export_training_jsonl(
        self,
        project_id: str,
        out_path: Path,
        *,
        accepted_only: bool = True,
    ) -> int:
        """Export Alpaca-style instruction/input/output lines for fine-tuning."""
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        n = 0
        with out_path.open("w", encoding="utf-8") as fh:
            for rec in self.iter_records(project_id, accepted_only=accepted_only):
                row = {
                    "instruction": rec.question,
                    "input": rec.db_context_excerpt[:4000],
                    "output": rec.sql or rec.raw_response[:8000],
                    "metadata": {
                        "record_id": rec.record_id,
                        "connection": rec.connection_name,
                        "database": rec.database,
                        "quality_score": rec.quality_score,
                    },
                }
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                n += 1
        return n
