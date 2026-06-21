"""
Word/symbol tokenizer for the tiny LLM.

Keeps things deliberately simple: identifiers and numbers are whole tokens,
and SQL punctuation (``( ) , * = > < ; .``) become individual tokens so the
model can learn SQL structure. Five special tokens frame the sequences:

    <pad>  padding
    <unk>  out-of-vocabulary
    <bos>  beginning of sequence
    <sep>  separates the question from the SQL
    <eos>  end of sequence
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable

PAD, UNK, BOS, SEP, EOS = "<pad>", "<unk>", "<bos>", "<sep>", "<eos>"
SPECIALS = [PAD, UNK, BOS, SEP, EOS]

_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|\d+(?:\.\d+)?|[^\sA-Za-z0-9_]")


def tokenize(text: str) -> list[str]:
    """Split text into word/number/symbol tokens (case-preserving)."""
    return _TOKEN_RE.findall(text or "")


class WordTokenizer:
    def __init__(self):
        self.itos: list[str] = list(SPECIALS)
        self.stoi: dict[str, int] = {t: i for i, t in enumerate(self.itos)}

    # ── ids for special tokens ──────────────────────────────────────────
    @property
    def pad_id(self) -> int: return self.stoi[PAD]
    @property
    def unk_id(self) -> int: return self.stoi[UNK]
    @property
    def bos_id(self) -> int: return self.stoi[BOS]
    @property
    def sep_id(self) -> int: return self.stoi[SEP]
    @property
    def eos_id(self) -> int: return self.stoi[EOS]

    @property
    def vocab_size(self) -> int:
        return len(self.itos)

    # ── vocabulary ──────────────────────────────────────────────────────

    def build(self, texts: Iterable[str], *, min_freq: int = 1) -> "WordTokenizer":
        """Build the vocabulary from a corpus of raw strings."""
        freq: dict[str, int] = {}
        for text in texts:
            for tok in tokenize(text):
                freq[tok] = freq.get(tok, 0) + 1
        for tok in sorted(freq, key=lambda t: (-freq[t], t)):
            if freq[tok] >= min_freq and tok not in self.stoi:
                self.stoi[tok] = len(self.itos)
                self.itos.append(tok)
        return self

    # ── encode / decode ─────────────────────────────────────────────────

    def encode(self, text: str) -> list[int]:
        return [self.stoi.get(t, self.unk_id) for t in tokenize(text)]

    def decode(self, ids: Iterable[int]) -> str:
        toks = [self.itos[i] for i in ids if 0 <= i < len(self.itos)]
        return self._detokenize([t for t in toks if t not in SPECIALS])

    def decode_token(self, token_id: int) -> str:
        if 0 <= token_id < len(self.itos):
            tok = self.itos[token_id]
            if tok not in SPECIALS:
                return tok
        return ""

    @staticmethod
    def _detokenize(tokens: list[str]) -> str:
        """Re-join tokens into readable SQL (sensible spacing around punctuation)."""
        no_space_before = {")", ",", ";", ".", "("}
        no_space_after = {"("}
        out = ""
        prev = None
        for i, tok in enumerate(tokens):
            if i == 0:
                out = tok
            elif tok in no_space_before or prev in no_space_after:
                out += tok
            else:
                out += " " + tok
            prev = tok
        # Collapse spaces inside quoted literals / identifiers:
        # ' x ' -> 'x', " public " -> "public", ` EMPLOYEES ` -> `EMPLOYEES`
        # (the last is MariaDB/MySQL backtick-quoted identifiers).
        out = re.sub(r"'\s*([^']*?)\s*'", r"'\1'", out)
        out = re.sub(r'"\s*([^"]*?)\s*"', r'"\1"', out)
        out = re.sub(r"`\s*([^`]*?)\s*`", r"`\1`", out)
        return out

    # ── persistence ─────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {"itos": self.itos}

    @classmethod
    def from_dict(cls, data: dict) -> "WordTokenizer":
        tok = cls()
        tok.itos = list(data["itos"])
        tok.stoi = {t: i for i, t in enumerate(tok.itos)}
        return tok

    def save(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict()), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> "WordTokenizer":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
