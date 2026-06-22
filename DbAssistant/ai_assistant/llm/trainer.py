"""
Training loop for the tiny LLM.

Mini-batch gradient descent with Adam over next-token examples. Designed to be
small and fast: a few hundred steps on a tiny dataset converge in seconds on
CPU. Returns rich metrics so callers (CLI/API/UI) can report progress.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, Optional

from ai_assistant.llm.dataset import (
    auto_context,
    build_sequences,
    build_tokenizer,
    make_examples,
)
from ai_assistant.llm.model import NeuralLM
from ai_assistant.llm.tokenizer import WordTokenizer


@dataclass
class TrainConfig:
    context: int = 0            # 0 => auto (span the longest sequence)
    max_context: int = 40       # cap for auto context
    emb_dim: int = 12
    hidden: int = 48
    epochs: int = 150
    batch_size: int = 32
    lr: float = 0.02
    seed: int = 1234
    min_loss: float = 0.05      # early-stop when avg epoch loss drops below this
    log_every: int = 10
    min_freq: int = 1
    sql_targets_only: bool = True


class Trainer:
    def __init__(self, config: TrainConfig | None = None):
        self.config = config or TrainConfig()

    def train(
        self,
        pairs: list[dict],
        *,
        progress: Optional[Callable[[dict], None]] = None,
    ) -> tuple[WordTokenizer, NeuralLM, dict]:
        if not pairs:
            raise ValueError("No training pairs provided.")
        cfg = self.config
        started = time.time()

        tok = build_tokenizer(pairs, min_freq=cfg.min_freq)
        sequences = build_sequences(pairs, tok)
        context = cfg.context if cfg.context and cfg.context > 0 else auto_context(
            sequences, cap=cfg.max_context
        )
        examples = make_examples(
            pairs, tok, context, sql_targets_only=cfg.sql_targets_only
        )
        model = NeuralLM(
            tok.vocab_size,
            context=context,
            emb_dim=cfg.emb_dim,
            hidden=cfg.hidden,
            seed=cfg.seed,
        )

        rng = random.Random(cfg.seed)
        history: list[float] = []
        final_loss = float("inf")
        epochs_run = 0
        for epoch in range(1, cfg.epochs + 1):
            rng.shuffle(examples)
            epoch_loss = 0.0
            batches = 0
            for i in range(0, len(examples), cfg.batch_size):
                batch = examples[i:i + cfg.batch_size]
                loss, grads, n = model.loss_and_grad(batch)
                model.step(grads, n, lr=cfg.lr)
                epoch_loss += loss
                batches += 1
            avg = epoch_loss / max(1, batches)
            history.append(avg)
            final_loss = avg
            epochs_run = epoch
            if progress and (epoch % cfg.log_every == 0 or epoch == 1):
                progress({"epoch": epoch, "loss": round(avg, 4)})
            if avg <= cfg.min_loss:
                break

        metrics = {
            "epochs_run": epochs_run,
            "final_loss": round(final_loss, 4),
            "vocab_size": tok.vocab_size,
            "context": context,
            "num_pairs": len(pairs),
            "num_examples": len(examples),
            "params": _param_count(model),
            "elapsed_sec": round(time.time() - started, 3),
            "loss_history": [round(h, 4) for h in history],
            "config": cfg.__dict__,
        }
        return tok, model, metrics


def _param_count(m: NeuralLM) -> int:
    return (
        m.vocab_size * m.emb_dim
        + m.context * m.emb_dim * m.hidden
        + m.hidden
        + m.hidden * m.vocab_size
        + m.vocab_size
    )
