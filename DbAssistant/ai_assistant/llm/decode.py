"""Shared decoding guards for tiny NL->SQL engines (python / numpy).

Adds no-repeat-ngram blocking, optional top-k sampling, repetition penalty,
early stop at statement completion, and post-decode SQL trimming.
"""

from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass, field
from typing import Callable

_SEMICOLON_TOKEN = ";"
_SQL_START = re.compile(r"\b(SELECT|WITH)\b", re.IGNORECASE)


def trim_sql_output(text: str) -> str:
    """Extract the first plausible SQL statement from decoded token text."""
    s = (text or "").strip()
    if not s:
        return ""
    # Drop leading prose before the first SELECT/WITH.
    m = _SQL_START.search(s)
    if m:
        s = s[m.start():]
    # Stop at first semicolon (complete statement).
    if ";" in s:
        s = s.split(";", 1)[0].strip()
    # Collapse whitespace.
    s = re.sub(r"\s+", " ", s).strip()
    return s


def has_repeated_ngram(token_ids: list[int], candidate: int, n: int) -> bool:
    """Return True if adding *candidate* would repeat an n-gram of length *n*."""
    if n <= 0:
        return False
    seq = token_ids + [candidate]
    if len(seq) < 2 * n:
        return False
    tail = tuple(seq[-n:])
    hits = 0
    for i in range(len(seq) - n + 1):
        if tuple(seq[i : i + n]) == tail:
            hits += 1
            if hits >= 2:
                return True
    return False


def apply_repetition_penalty(
    probs: list[float],
    recent: list[int],
    *,
    penalty: float = 1.2,
    window: int = 8,
) -> list[float]:
    """Down-weight tokens that appeared recently in the generated sequence."""
    if penalty <= 1.0 or not recent:
        return probs
    seen = set(recent[-window:])
    out = list(probs)
    for tid in seen:
        if 0 <= tid < len(out):
            out[tid] /= penalty
    total = sum(out)
    if total <= 0:
        return probs
    return [p / total for p in out]


def pick_token(
    probs: list[float],
    *,
    temperature: float = 0.0,
    top_k: int = 0,
    rng: random.Random | None = None,
) -> int:
    """Greedy (temp=0), top-k, or temperature sampling."""
    rng = rng or random.Random(0)
    scored = list(enumerate(probs))
    if top_k > 0 and top_k < len(scored):
        scored.sort(key=lambda x: x[1], reverse=True)
        scored = scored[:top_k]
        total = sum(p for _, p in scored)
        if total <= 0:
            return int(max(range(len(probs)), key=lambda i: probs[i]))
        r = rng.random() * total
        acc = 0.0
        for idx, p in scored:
            acc += p
            if acc >= r:
                return idx
        return scored[-1][0]
    if temperature <= 0.0:
        return int(max(range(len(probs)), key=lambda i: probs[i]))
    logits = [math.log(max(p, 1e-12)) / temperature for p in probs]
    m = max(logits)
    exps = [math.exp(v - m) for v in logits]
    s = sum(exps)
    r = rng.random() * s
    acc = 0.0
    for i, e in enumerate(exps):
        acc += e
        if acc >= r:
            return i
    return len(probs) - 1


@dataclass
class GenerationConfig:
    """Hyperparameters for token generation — groups the tuning knobs."""
    max_new: int = 64
    temperature: float = 0.0
    seed: int = 0
    no_repeat_ngram: int = 3
    repetition_penalty: float = 1.2
    top_k: int = 0

    @classmethod
    def from_params(cls, params: dict) -> "GenerationConfig":
        """Build from a loose params dict (keys are a superset of fields)."""
        def _int(key, default):
            try:
                return int(params.get(key, default))
            except (TypeError, ValueError):
                return default

        def _float(key, default):
            try:
                return float(params.get(key, default))
            except (TypeError, ValueError):
                return default

        return cls(
            max_new=_int("max_new", 64),
            temperature=_float("temperature", 0.0),
            seed=_int("seed", 0),
            no_repeat_ngram=_int("no_repeat_ngram", 3),
            repetition_penalty=_float("repetition_penalty", 1.2),
            top_k=_int("top_k", 0),
        )


def guarded_generate(
    prefix_ids: list[int],
    *,
    predict_proba: Callable[[list[int]], list[float]],
    decode_token: Callable[[int], str],
    pad_id: int,
    eos_id: int,
    context: int,
    config: GenerationConfig | None = None,
) -> list[int]:
    """Generate token ids with repetition guard and early stop."""
    cfg = config or GenerationConfig()
    max_new = cfg.max_new
    temperature = cfg.temperature
    seed = cfg.seed
    no_repeat_ngram = cfg.no_repeat_ngram
    repetition_penalty = cfg.repetition_penalty
    top_k = cfg.top_k
    rng = random.Random(seed)
    seq = list(prefix_ids)
    out: list[int] = []
    decoded_parts: list[str] = []

    def _context_window() -> list[int]:
        if len(seq) >= context:
            return seq[-context:]
        return [pad_id] * (context - len(seq)) + seq

    for _ in range(max_new):
        probs = list(predict_proba(_context_window()))
        probs = apply_repetition_penalty(probs, out, penalty=repetition_penalty)
        # Try up to vocab picks avoiding repeated n-grams.
        nxt = None
        for _try in range(min(len(probs), 8)):
            cand = pick_token(probs, temperature=temperature, top_k=top_k, rng=rng)
            if no_repeat_ngram > 0 and has_repeated_ngram(out, cand, no_repeat_ngram):
                probs[cand] = 0.0
                total = sum(probs)
                if total <= 0:
                    break
                probs = [p / total for p in probs]
                continue
            nxt = cand
            break
        if nxt is None:
            nxt = pick_token(probs, temperature=temperature, top_k=top_k, rng=rng)
        if nxt == eos_id:
            break
        out.append(nxt)
        seq.append(nxt)
        tok = decode_token(nxt)
        decoded_parts.append(tok)
        joined = " ".join(decoded_parts)
        # Only stop on an explicit statement terminator. We deliberately do NOT
        # stop as soon as a FROM clause appears — that truncated legitimate
        # WHERE / GROUP BY / ORDER BY / JOIN tails. The model emits <eos> when
        # done; otherwise generation runs up to max_new and trim_sql_output
        # cleans the result.
        if _SEMICOLON_TOKEN in joined or joined.rstrip().endswith(";"):
            break
    return out
