"""Walk a source tree and produce RAG :class:`Document` chunks (kind=code)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Iterable

from ai_assistant.rag.document_loader import chunk_code_text, chunk_text, load_file
from ai_assistant.rag.documents import Document

# Reuse App Builder ignore rules (keep in sync with codebase_profile.py).
IGNORED_DIRS = frozenset({
    ".git", "__pycache__", ".venv", "venv", "node_modules", "site-packages",
    "dist", "build", ".tox", ".pytest_cache", ".mypy_cache", "coverage",
    ".idea", ".vscode", "__pycache__",
})

CODE_EXTENSIONS = frozenset({
    ".py", ".js", ".ts", ".tsx", ".jsx", ".java", ".go", ".rb", ".cs", ".php",
    ".sql", ".sh", ".bash", ".zsh", ".yaml", ".yml", ".toml", ".rs", ".kt",
    ".swift", ".scala", ".c", ".h", ".cpp", ".hpp", ".m", ".mm",
    ".txt", ".md", ".markdown", ".rst", ".json", ".xml", ".html", ".htm",
    ".ini", ".cfg", ".env.example",
})

_LANG_BY_EXT = {
    ".py": "python", ".js": "javascript", ".ts": "typescript", ".tsx": "tsx",
    ".jsx": "jsx", ".java": "java", ".go": "go", ".rb": "ruby", ".cs": "csharp",
    ".php": "php", ".sql": "sql", ".sh": "shell", ".rs": "rust", ".kt": "kotlin",
    ".swift": "swift", ".scala": "scala", ".c": "c", ".cpp": "cpp", ".h": "c",
    ".md": "markdown", ".json": "json", ".yaml": "yaml", ".yml": "yaml",
}


def _slug_path(rel: str, *, limit: int = 80) -> str:
    s = re.sub(r"\W+", "_", rel.strip().lower()).strip("_")
    return s[:limit] or "file"


def iter_codebase_files(
    root: Path,
    *,
    max_files: int = 500,
    max_file_bytes: int = 512_000,
    extensions: Iterable[str] | None = None,
) -> list[Path]:
    """Return readable source files under *root*, skipping ignored directories."""
    root = root.expanduser().resolve()
    if not root.is_dir():
        return []
    allowed = {e.lower() if e.startswith(".") else f".{e.lower()}"
               for e in (extensions or CODE_EXTENSIONS)}
    out: list[Path] = []
    for p in sorted(root.rglob("*")):
        if len(out) >= max_files:
            break
        if not p.is_file():
            continue
        if p.suffix.lower() not in allowed:
            continue
        try:
            if p.stat().st_size > max_file_bytes:
                continue
        except OSError:
            continue
        rel_parts = p.relative_to(root).parts
        if any(part in IGNORED_DIRS for part in rel_parts):
            continue
        out.append(p)
    return out


def index_codebase(
    folder: str | Path,
    scope: str,
    *,
    chunk_size: int = 1000,
    overlap: int = 150,
    max_files: int = 500,
    max_file_bytes: int = 512_000,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[Document], dict[str, Any]]:
    """Scan *folder* and return code documents + summary stats."""
    root = Path(folder).expanduser().resolve()
    if not root.is_dir():
        return [], {"ok": False, "error": f"Not a directory: {root}"}

    files = iter_codebase_files(root, max_files=max_files, max_file_bytes=max_file_bytes)
    if not files:
        return [], {"ok": False, "error": "No indexable source files found."}

    docs: list[Document] = []
    errors: list[str] = []
    for fp in files:
        rel = str(fp.relative_to(root))
        if on_progress:
            on_progress({"type": "codebase_file", "path": rel})
        text, err = load_file(fp)
        if err or not text:
            if err:
                errors.append(f"{rel}: {err}")
            continue
        lang = _LANG_BY_EXT.get(fp.suffix.lower(), "text")
        if fp.suffix.lower() in {".py", ".js", ".ts", ".tsx", ".java", ".go", ".rb"}:
            chunks = chunk_code_text(text, chunk_size=chunk_size, overlap=overlap, language=lang)
        else:
            chunks = chunk_text(text, chunk_size=chunk_size, overlap=overlap)
        src_slug = _slug_path(rel)
        for i, chunk in enumerate(chunks):
            docs.append(Document(
                doc_id=f"code:{src_slug}:{i}",
                kind="code",
                ref=rel,
                text=f"File: {rel} ({lang})\n{chunk}",
                metadata={
                    "source": rel,
                    "path": rel,
                    "language": lang,
                    "chunk_index": i,
                    "total_chunks": len(chunks),
                },
            ))

    summary = {
        "ok": bool(docs),
        "folder": str(root),
        "scope": scope,
        "files_scanned": len(files),
        "chunks": len(docs),
        "errors": errors[:20],
        "error": None if docs else "No chunks produced from codebase.",
    }
    return docs, summary
