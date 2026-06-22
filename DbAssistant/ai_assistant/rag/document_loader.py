"""
Document ingestion for the RAG pipeline.

Turns an uploaded file (or pasted text) into clean, overlapping text chunks that
can be embedded and stored like any other RAG :class:`~ai_assistant.rag.documents.Document`.

Design goals (mirroring the rest of the RAG layer):
    * **Zero hard dependencies** for the common cases. Plain-text family formats
      (.txt, .md, .sql, .csv, .tsv, .json, .rst, .log, .yaml, .html) are read
      directly with the stdlib.
    * **Optional richer formats**. PDF (.pdf) and Word (.docx) are supported
      *only* when the optional ``pypdf`` / ``python-docx`` packages are
      installed; otherwise a clear, actionable error is returned instead of
      crashing — exactly like the optional ``sentence-transformers`` embedder.

Public surface::

    SUPPORTED_TEXT_EXTS  -> frozenset[str]   (always work)
    OPTIONAL_EXTS        -> frozenset[str]   (need an optional package)
    supported_extensions() -> dict           (for UI/CLI help)
    load_file(path)      -> (text, error)
    chunk_text(text, ...) -> list[str]
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

# Formats we can always read with only the standard library.
SUPPORTED_TEXT_EXTS = frozenset({
    ".txt", ".md", ".markdown", ".rst", ".sql", ".csv", ".tsv",
    ".json", ".log", ".yaml", ".yml", ".ini", ".cfg", ".html", ".htm", ".xml",
    # Source code (codebase RAG)
    ".py", ".js", ".ts", ".tsx", ".jsx", ".java", ".go", ".rb", ".cs", ".php",
    ".sh", ".bash", ".zsh", ".toml", ".rs", ".kt", ".swift", ".scala",
    ".c", ".h", ".cpp", ".hpp", ".m", ".mm",
})

# Formats that require an optional package to be installed.
OPTIONAL_EXTS = frozenset({".pdf", ".docx"})


def supported_extensions() -> dict[str, list[str]]:
    """Return the supported extensions grouped by availability (for help text)."""
    available_optional = []
    if _have_pypdf():
        available_optional.append(".pdf")
    if _have_docx():
        available_optional.append(".docx")
    missing_optional = sorted(OPTIONAL_EXTS - set(available_optional))
    return {
        "text": sorted(SUPPORTED_TEXT_EXTS),
        "optional_available": sorted(available_optional),
        "optional_missing": missing_optional,
    }


def _have_pypdf() -> bool:
    try:
        import pypdf  # type: ignore  # noqa: F401
        return True
    except Exception:
        try:
            import PyPDF2  # type: ignore  # noqa: F401
            return True
        except Exception:
            return False


def _have_docx() -> bool:
    try:
        import docx  # type: ignore  # noqa: F401
        return True
    except Exception:
        return False


def _read_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
    except Exception:
        from PyPDF2 import PdfReader  # type: ignore
    reader = PdfReader(str(path))
    parts = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(parts)


def _read_docx(path: Path) -> str:
    import docx  # type: ignore

    doc = docx.Document(str(path))
    return "\n".join(p.text for p in doc.paragraphs)


def _normalize_text(text: str) -> str:
    """Collapse excessive whitespace while keeping paragraph boundaries."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # collapse runs of blank lines to a single blank line (paragraph break)
    text = re.sub(r"\n{3,}", "\n\n", text)
    # trim trailing spaces on each line
    text = "\n".join(line.rstrip() for line in text.split("\n"))
    return text.strip()


def load_file(path: str | Path) -> tuple[str, Optional[str]]:
    """Load a document file into normalized text.

    Returns ``(text, error)``. On success ``error`` is ``None``; on failure
    ``text`` is empty and ``error`` is a human-readable message.
    """
    p = Path(path).expanduser()
    if not p.exists():
        return "", f"File not found: {p}"
    if not p.is_file():
        return "", f"Not a file: {p}"
    ext = p.suffix.lower()
    try:
        if ext == ".pdf":
            if not _have_pypdf():
                return "", (
                    "PDF support needs the optional 'pypdf' package. "
                    "Install it with: pip install pypdf"
                )
            text = _read_pdf(p)
        elif ext == ".docx":
            if not _have_docx():
                return "", (
                    "DOCX support needs the optional 'python-docx' package. "
                    "Install it with: pip install python-docx"
                )
            text = _read_docx(p)
        elif ext == ".json":
            # Pretty-print JSON so keys/values are individually tokenizable.
            raw = p.read_text(encoding="utf-8", errors="replace")
            try:
                text = json.dumps(json.loads(raw), indent=2, ensure_ascii=False)
            except Exception:
                text = raw
        elif ext in SUPPORTED_TEXT_EXTS or ext == "":
            text = p.read_text(encoding="utf-8", errors="replace")
        else:
            return "", (
                f"Unsupported file type '{ext}'. Supported: "
                f"{', '.join(sorted(SUPPORTED_TEXT_EXTS))}; "
                ".pdf and .docx are available with optional packages."
            )
    except Exception as exc:  # noqa: BLE001
        return "", f"Failed to read {p.name}: {exc}"

    text = _normalize_text(text)
    if not text:
        return "", f"No extractable text in {p.name}."
    return text, None


def chunk_text(
    text: str,
    *,
    chunk_size: int = 1000,
    overlap: int = 150,
) -> list[str]:
    """Split *text* into overlapping chunks, respecting paragraph boundaries.

    Chunks are kept under ``chunk_size`` characters where possible. Consecutive
    chunks share ``overlap`` characters of tail context so a fact spanning a
    boundary is still retrievable from at least one chunk.
    """
    text = _normalize_text(text)
    if not text:
        return []
    chunk_size = max(200, int(chunk_size))
    overlap = max(0, min(int(overlap), chunk_size // 2))

    # Markdown / structured docs: split on headings first.
    if re.search(r"^#{1,6}\s", text, re.MULTILINE):
        return _chunk_markdown(text, chunk_size=chunk_size, overlap=overlap)

    return _chunk_paragraphs(text, chunk_size=chunk_size, overlap=overlap)


def _chunk_paragraphs(text: str, *, chunk_size: int, overlap: int) -> list[str]:
    """Paragraph-pack *text* into overlapping chunks (no heading awareness)."""
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    buf = ""
    for para in paragraphs:
        # A single oversized paragraph is split below (table-aware, then
        # sentence-aware) so we never cut through a markdown table row.
        if len(para) > chunk_size:
            if buf:
                chunks.append(buf)
                buf = ""
            if _is_markdown_table(para):
                chunks.extend(_split_table(para, chunk_size))
            else:
                chunks.extend(_hard_split(para, chunk_size, overlap))
            continue
        if not buf:
            buf = para
        elif len(buf) + 2 + len(para) <= chunk_size:
            buf = f"{buf}\n\n{para}"
        else:
            chunks.append(buf)
            tail = buf[-overlap:] if overlap else ""
            buf = (f"{tail}\n\n{para}" if tail else para)
    if buf:
        chunks.append(buf)
    return [c.strip() for c in chunks if c.strip()]


def _chunk_markdown(text: str, *, chunk_size: int, overlap: int) -> list[str]:
    """Split markdown on heading boundaries, then paragraph-pack each section.

    Oversized sections are paragraph-packed directly (never routed back through
    :func:`chunk_text`) so a heading-led section cannot recurse infinitely.
    """
    sections = re.split(r"(?=^#{1,6}\s)", text, flags=re.MULTILINE)
    sections = [s.strip() for s in sections if s.strip()]
    if not sections:
        return _chunk_paragraphs(text, chunk_size=chunk_size, overlap=overlap)
    out: list[str] = []
    for sec in sections:
        if len(sec) <= chunk_size:
            out.append(sec)
        else:
            out.extend(_chunk_paragraphs(sec, chunk_size=chunk_size, overlap=overlap))
    return out


# Language-specific split hints for code-aware chunking.
_CODE_SPLIT_PATTERNS = {
    "python": re.compile(
        r"^(?=(?:async\s+)?def\s+\w+|^class\s+\w+)", re.MULTILINE),
    "javascript": re.compile(
        r"^(?=(?:export\s+)?(?:async\s+)?function\s|^(?:export\s+)?class\s)",
        re.MULTILINE),
    "typescript": re.compile(
        r"^(?=(?:export\s+)?(?:async\s+)?function\s|^(?:export\s+)?class\s|"
        r"^interface\s+\w+)",
        re.MULTILINE),
    "java": re.compile(r"^(?=(?:public|private|protected)?\s*class\s)", re.MULTILINE),
    "go": re.compile(r"^(?=func\s+(?:\([^)]*\)\s*)?\w+)", re.MULTILINE),
    "ruby": re.compile(r"^(?=def\s+\w+|^class\s+\w+)", re.MULTILINE),
}


def chunk_code_text(
    text: str,
    *,
    chunk_size: int = 1000,
    overlap: int = 150,
    language: str = "text",
) -> list[str]:
    """Split source code on function/class boundaries where possible."""
    text = _normalize_text(text)
    if not text:
        return []
    pat = _CODE_SPLIT_PATTERNS.get(language.lower())
    if pat:
        parts = [p.strip() for p in pat.split(text) if p.strip()]
        if len(parts) > 1:
            chunks: list[str] = []
            for part in parts:
                if len(part) <= chunk_size:
                    chunks.append(part)
                else:
                    chunks.extend(_hard_split(part, chunk_size, overlap))
            return [c for c in chunks if c]
    return chunk_text(text, chunk_size=chunk_size, overlap=overlap)


_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z(\"'`\d])")


def _sentence_split(text: str) -> list[str]:
    """Split prose into sentences; fall back to the whole text when unsure."""
    parts = [s.strip() for s in _SENTENCE_RE.split(text) if s.strip()]
    return parts or [text.strip()]


def _hard_split(text: str, chunk_size: int, overlap: int) -> list[str]:
    """Split an oversized paragraph on sentence boundaries, packing to size.

    Sentences themselves longer than ``chunk_size`` fall back to a plain
    character-window split so a single run-on line is still bounded.
    """
    out: list[str] = []
    buf = ""
    for sent in _sentence_split(text):
        if len(sent) > chunk_size:
            if buf:
                out.append(buf)
                buf = ""
            out.extend(_char_window(sent, chunk_size, overlap))
            continue
        if not buf:
            buf = sent
        elif len(buf) + 1 + len(sent) <= chunk_size:
            buf = f"{buf} {sent}"
        else:
            out.append(buf)
            tail = buf[-overlap:] if overlap else ""
            buf = f"{tail} {sent}".strip() if tail else sent
    if buf:
        out.append(buf)
    return [c.strip() for c in out if c.strip()]


def _char_window(text: str, chunk_size: int, overlap: int) -> list[str]:
    """Last-resort fixed-size character-window split."""
    out: list[str] = []
    step = max(1, chunk_size - overlap)
    for start in range(0, len(text), step):
        piece = text[start:start + chunk_size].strip()
        if piece:
            out.append(piece)
        if start + chunk_size >= len(text):
            break
    return out


def _is_markdown_table(text: str) -> bool:
    """Heuristic: a block is a table when most lines are pipe-delimited rows."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return False
    pipe_rows = sum(1 for ln in lines if ln.count("|") >= 2)
    return pipe_rows >= max(2, int(len(lines) * 0.6))


def _split_table(text: str, chunk_size: int) -> list[str]:
    """Split an oversized markdown table by rows, repeating the header.

    Keeps the header (and its ``---`` separator) at the top of every chunk so
    each piece remains a valid, self-describing table.
    """
    lines = [ln for ln in text.splitlines() if ln.strip()]
    header: list[str] = []
    body_start = 0
    if lines:
        header.append(lines[0])
        body_start = 1
        if len(lines) > 1 and set(lines[1].replace("|", "").strip()) <= set("-: "):
            header.append(lines[1])
            body_start = 2
    head_text = "\n".join(header)
    out: list[str] = []
    buf = head_text
    for row in lines[body_start:]:
        if len(buf) + 1 + len(row) > chunk_size and buf != head_text:
            out.append(buf)
            buf = f"{head_text}\n{row}" if head_text else row
        else:
            buf = f"{buf}\n{row}" if buf else row
    if buf and buf != head_text:
        out.append(buf)
    elif not out and buf:
        out.append(buf)
    return out
