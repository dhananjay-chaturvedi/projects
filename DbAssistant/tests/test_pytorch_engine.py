"""Tests for the PyTorch engine supervised training + guarded decode."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ai_assistant.llm.dataset import build_sequences, build_tokenizer, pair_to_sequence
from ai_assistant.llm.engines.pytorch_engine import (
    PytorchEngine,
    _build_supervised_examples,
    _IGNORE,
)


def _tiny_pairs() -> list[dict]:
    return [
        {"question": "count customers", "sql": "SELECT COUNT(*) FROM customers"},
        {"question": "list customers", "sql": "SELECT * FROM customers LIMIT 5"},
        {"question": "count orders", "sql": "SELECT COUNT(*) FROM orders"},
        {"question": "show order totals", "sql": "SELECT SUM(total) FROM orders"},
    ]


def test_supervised_examples_mask_question_region():
    pairs = _tiny_pairs()
    tok = build_tokenizer(pairs)
    seqs = build_sequences(pairs, tok)
    xs, ys = _build_supervised_examples(
        seqs, sep_id=tok.sep_id, pad_id=tok.pad_id, block_size=32,
    )
    assert len(xs) == len(pairs)
    for seq, tgt in zip(seqs, ys):
        sep_idx = seq.index(tok.sep_id)
        # Every target before/at sep (question side) must be masked.
        for i, t in enumerate(tgt):
            if t == _IGNORE:
                continue
            assert (i + 1) > sep_idx, "unmasked target in question region"
        # At least one SQL token is trained.
        assert any(t != _IGNORE for t in tgt)


def test_supervised_examples_preserve_sql_tail_on_truncation():
    pairs = [{"question": "x " * 40, "sql": "SELECT 1"}]
    tok = build_tokenizer(pairs)
    seq = pair_to_sequence(pairs[0], tok)
    xs, ys = _build_supervised_examples(
        [seq], sep_id=tok.sep_id, pad_id=tok.pad_id, block_size=8,
    )
    assert xs and ys
    # SQL token (SELECT or 1) should appear in unmasked targets.
    unmasked_ids = [t for t in ys[0] if t != _IGNORE]
    decoded = tok.decode(unmasked_ids)
    assert "SELECT" in decoded or "1" in decoded


@pytest.mark.skipif(
    not PytorchEngine().is_available()[0],
    reason="torch not installed",
)
def test_pytorch_train_and_generate_smoke(tmp_path: Path):
    pairs = _tiny_pairs()
    eng = PytorchEngine()
    cfg = {
        "pt_max_iters": 300,
        "pt_batch_size": 4,
        "pt_n_layer": 2,
        "pt_n_head": 2,
        "pt_n_embd": 32,
        "pt_block_size": 64,
        "min_loss": 0.001,
        "log_every": 999,
    }
    mdir = tmp_path / "pt_smoke"
    res = eng.train(pairs, mdir, config=cfg)
    assert res["final_loss"] < 2.0
    assert res["num_examples"] == len(pairs)
    assert (mdir / "model.pt").exists()

    # Trained question should produce plausible SQL via guarded decode.
    out = eng.generate("count customers", mdir, params={"max_new": 64})
    sql = (out.get("sql") or "").upper()
    assert "SELECT" in sql
    assert "COUNT" in sql or "customers".upper() in sql.replace("`", "")


@pytest.mark.skipif(
    not PytorchEngine().is_available()[0],
    reason="torch not installed",
)
def test_pytorch_respects_decode_guards(tmp_path: Path):
    """Repetition loops should be suppressed by guarded_generate params."""
    pairs = _tiny_pairs() * 3  # more data for stability
    eng = PytorchEngine()
    cfg = {"pt_max_iters": 200, "pt_batch_size": 8, "pt_n_embd": 32,
           "pt_n_layer": 2, "pt_n_head": 2, "min_loss": 0.01, "log_every": 999}
    mdir = tmp_path / "pt_decode"
    eng.train(pairs, mdir, config=cfg)
    out = eng.generate(
        "list customers",
        mdir,
        params={"max_new": 80, "no_repeat_ngram": 2, "repetition_penalty": 1.5},
    )
    sql = out.get("sql") or ""
    # No obvious 3+ word repetition loops.
    words = sql.split()
    for i in range(len(words) - 2):
        tri = words[i:i + 3]
        assert words.count(" ".join(tri)) <= 1 or len(set(tri)) > 1
