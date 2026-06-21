#!/usr/bin/env python3
"""Build a Standard or Advanced distributable tree.

Standard physically omits App Builder, local LLM training, and RAG packages.
Advanced copies the full source tree.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from common.editions import excludes_for


_ALWAYS_EXCLUDE = {
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "logs",
    "built_apps",
    "config.ini",
    "properties.ini",
    ".env",
    ".env.local",
    "tmp_schema_convert_test.sql",
    "shipper.sh",
}


def _is_excluded(rel: Path, excludes: set[str]) -> bool:
    posix = rel.as_posix()
    parts = set(rel.parts)
    if parts & _ALWAYS_EXCLUDE:
        return True
    return any(posix == ex or posix.startswith(ex.rstrip("/") + "/") for ex in excludes)


def build_edition(src: Path, dest: Path, edition: str, *, dry_run: bool = False) -> dict:
    src = src.resolve()
    dest = dest.resolve()
    excludes = excludes_for(edition)
    copied: list[str] = []
    skipped: list[str] = []
    try:
        dest_rel = dest.relative_to(src)
    except ValueError:
        dest_rel = None

    if dest.exists() and not dry_run:
        shutil.rmtree(dest)
    if not dry_run:
        dest.mkdir(parents=True, exist_ok=True)

    for path in src.rglob("*"):
        rel = path.relative_to(src)
        if dest_rel is not None and (rel == dest_rel or dest_rel in rel.parents):
            continue
        if _is_excluded(rel, excludes):
            skipped.append(rel.as_posix())
            continue
        if path.is_dir():
            if not dry_run:
                (dest / rel).mkdir(parents=True, exist_ok=True)
            continue
        copied.append(rel.as_posix())
        if not dry_run:
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)

    marker = {
        "edition": edition,
        "advanced_modules_included": edition == "advanced",
        "excluded": sorted(excludes),
    }
    if not dry_run:
        (dest / "edition.json").write_text(json.dumps(marker, indent=2), encoding="utf-8")
    return {"edition": edition, "dest": str(dest), "copied": len(copied),
            "skipped": sorted(set(skipped)), "marker": marker}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--edition", choices=["standard", "advanced"], required=True)
    parser.add_argument("--dest", required=True)
    parser.add_argument("--src", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = build_edition(
        Path(args.src), Path(args.dest), args.edition, dry_run=bool(args.dry_run))
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
