"""Stage 1 — NumPy-vectorized MLP n-gram engine."""

from __future__ import annotations

import json
import math
import random
import time
from pathlib import Path
from typing import Any, Callable, Optional

from ai_assistant.llm.dataset import (
    auto_context,
    build_sequences,
    build_tokenizer,
    make_examples,
    question_prefix,
)
from ai_assistant.llm.engines.base import LlmEngine
from ai_assistant.llm.tokenizer import WordTokenizer


def _import_numpy():
    import numpy as np
    return np


class NumpyNeuralLM:
    """MLP n-gram LM with NumPy arrays (same architecture as NeuralLM)."""

    def __init__(
        self,
        vocab_size: int,
        *,
        context: int,
        emb_dim: int,
        hidden: int,
        seed: int = 1234,
    ):
        np = _import_numpy()
        self.vocab_size = vocab_size
        self.context = context
        self.emb_dim = emb_dim
        self.hidden = hidden
        rng = np.random.default_rng(seed)

        ind = 1.0 / math.sqrt(emb_dim)
        inh = 1.0 / math.sqrt(context * emb_dim)
        ino = 1.0 / math.sqrt(hidden)

        self.E = rng.uniform(-ind, ind, (vocab_size, emb_dim))
        self.W1 = rng.uniform(-inh, inh, (context * emb_dim, hidden))
        self.b1 = np.zeros(hidden)
        self.W2 = rng.uniform(-ino, ino, (hidden, vocab_size))
        self.b2 = np.zeros(vocab_size)

        self._init_adam()

    def _init_adam(self) -> None:
        np = _import_numpy()
        z = lambda s: np.zeros(s)
        self._mE, self._vE = z(self.E.shape), z(self.E.shape)
        self._mW1, self._vW1 = z(self.W1.shape), z(self.W1.shape)
        self._mb1, self._vb1 = z(self.b1.shape), z(self.b1.shape)
        self._mW2, self._vW2 = z(self.W2.shape), z(self.W2.shape)
        self._mb2, self._vb2 = z(self.b2.shape), z(self.b2.shape)
        self._t = 0

    def _forward(self, context_ids: list[int]):
        np = _import_numpy()
        C, D = self.context, self.emb_dim
        embs = self.E[context_ids]  # (C, D)
        x = embs.reshape(C * D)
        h = np.tanh(x @ self.W1 + self.b1)
        logits = h @ self.W2 + self.b2
        m = logits.max()
        exps = np.exp(logits - m)
        probs = exps / exps.sum()
        return x, h, probs

    def loss_and_grad(self, batch: list[tuple[list[int], int]]):
        np = _import_numpy()
        gE = np.zeros_like(self.E)
        gW1 = np.zeros_like(self.W1)
        gb1 = np.zeros_like(self.b1)
        gW2 = np.zeros_like(self.W2)
        gb2 = np.zeros_like(self.b2)
        total_loss = 0.0

        for context_ids, target in batch:
            x, h, probs = self._forward(context_ids)
            total_loss += -math.log(max(float(probs[target]), 1e-12))

            dlogits = probs.copy()
            dlogits[target] -= 1.0

            gb2 += dlogits
            gW2 += np.outer(h, dlogits)
            dh = self.W2 @ dlogits
            dh_pre = dh * (1.0 - h * h)
            gb1 += dh_pre
            gW1 += np.outer(x, dh_pre)
            dx = dh_pre @ self.W1.T
            dx_reshaped = dx.reshape(self.context, self.emb_dim)
            for i, cid in enumerate(context_ids):
                gE[cid] += dx_reshaped[i]

        n = max(1, len(batch))
        return total_loss / n, (gE, gW1, gb1, gW2, gb2), n

    def step(self, grads, n: int, *, lr: float = 0.02, b1=0.9, b2=0.999, eps=1e-8):
        np = _import_numpy()
        gE, gW1, gb1, gW2, gb2 = grads
        self._t += 1
        t = self._t
        bc1 = 1.0 - b1 ** t
        bc2 = 1.0 - b2 ** t
        inv_n = 1.0 / n

        def upd(p, m, v, g):
            g = g * inv_n
            m = b1 * m + (1 - b1) * g
            v = b2 * v + (1 - b2) * g * g
            p = p - lr * (m / bc1) / (np.sqrt(v / bc2) + eps)
            return p, m, v

        self.E, self._mE, self._vE = upd(self.E, self._mE, self._vE, gE)
        self.W1, self._mW1, self._vW1 = upd(self.W1, self._mW1, self._vW1, gW1)
        self.b1, self._mb1, self._vb1 = upd(self.b1, self._mb1, self._vb1, gb1)
        self.W2, self._mW2, self._vW2 = upd(self.W2, self._mW2, self._vW2, gW2)
        self.b2, self._mb2, self._vb2 = upd(self.b2, self._mb2, self._vb2, gb2)

    def generate(
        self,
        prefix_ids: list[int],
        *,
        pad_id: int,
        eos_id: int,
        config: "GenerationConfig | None" = None,
        decode_token: Callable[[int], str] | None = None,
    ) -> list[int]:
        from ai_assistant.llm.decode import guarded_generate

        return guarded_generate(
            prefix_ids,
            predict_proba=lambda ctx: list(self._forward(ctx)[2]),
            decode_token=decode_token or (lambda _i: ""),
            pad_id=pad_id,
            eos_id=eos_id,
            context=self.context,
            config=config,
        )

    def save_npz(self, path: Path) -> None:
        np = _import_numpy()
        np.savez(
            path,
            vocab_size=self.vocab_size,
            context=self.context,
            emb_dim=self.emb_dim,
            hidden=self.hidden,
            E=self.E,
            W1=self.W1,
            b1=self.b1,
            W2=self.W2,
            b2=self.b2,
        )

    @classmethod
    def load_npz(cls, path: Path) -> "NumpyNeuralLM":
        np = _import_numpy()
        data = np.load(path, allow_pickle=False)
        m = cls(
            int(data["vocab_size"]),
            context=int(data["context"]),
            emb_dim=int(data["emb_dim"]),
            hidden=int(data["hidden"]),
        )
        m.E = data["E"]
        m.W1 = data["W1"]
        m.b1 = data["b1"]
        m.W2 = data["W2"]
        m.b2 = data["b2"]
        m._init_adam()
        return m


class NumpyEngine(LlmEngine):
    name = "numpy"
    stage = 1
    display_name = "NumPy MLP"
    requires = ["numpy"]

    def is_available(self) -> tuple[bool, str]:
        try:
            _import_numpy()
            return True, ""
        except ImportError:
            return False, "numpy not installed (pip install numpy)"

    def train(
        self,
        pairs: list[dict],
        model_dir: Path,
        *,
        config: dict[str, Any],
        progress: Optional[Callable[[dict], None]] = None,
    ) -> dict[str, Any]:
        ok, reason = self.is_available()
        if not ok:
            raise RuntimeError(reason)

        model_dir.mkdir(parents=True, exist_ok=True)
        started = time.time()
        cfg = config
        min_freq = int(cfg.get("min_freq", 1))
        epochs = int(cfg.get("epochs", 150))
        batch_size = int(cfg.get("batch_size", 32))
        lr = float(cfg.get("lr", 0.02))
        seed = int(cfg.get("seed", 1234))
        min_loss = float(cfg.get("min_loss", 0.05))
        log_every = int(cfg.get("log_every", 10))
        max_context = int(cfg.get("max_context", 40))

        tok = build_tokenizer(pairs, min_freq=min_freq)
        sequences = build_sequences(pairs, tok)
        context = int(cfg.get("context", 0))
        if not context or context <= 0:
            context = auto_context(sequences, cap=max_context)
        examples = make_examples(pairs, tok, context, sql_targets_only=True)
        model = NumpyNeuralLM(
            tok.vocab_size,
            context=context,
            emb_dim=int(cfg.get("emb_dim", 12)),
            hidden=int(cfg.get("hidden", 48)),
            seed=seed,
        )

        rng = random.Random(seed)
        history: list[float] = []
        final_loss = float("inf")
        epochs_run = 0
        for epoch in range(1, epochs + 1):
            rng.shuffle(examples)
            epoch_loss = 0.0
            batches = 0
            for i in range(0, len(examples), batch_size):
                batch = examples[i:i + batch_size]
                loss, grads, n = model.loss_and_grad(batch)
                model.step(grads, n, lr=lr)
                epoch_loss += loss
                batches += 1
            avg = epoch_loss / max(1, batches)
            history.append(avg)
            final_loss = avg
            epochs_run = epoch
            if progress and (epoch % log_every == 0 or epoch == 1):
                progress({"epoch": epoch, "loss": round(avg, 4)})
            if avg <= min_loss:
                break

        model.save_npz(model_dir / "model.npz")
        tok.save(model_dir / "tokenizer.json")
        params = (
            tok.vocab_size * model.emb_dim
            + model.context * model.emb_dim * model.hidden
            + model.hidden
            + model.hidden * tok.vocab_size
            + tok.vocab_size
        )
        return {
            "epochs_run": epochs_run,
            "final_loss": round(final_loss, 4),
            "vocab_size": tok.vocab_size,
            "context": context,
            "num_pairs": len(pairs),
            "num_examples": len(examples),
            "params": params,
            "elapsed_sec": round(time.time() - started, 3),
            "loss_history": [round(h, 4) for h in history],
        }

    def generate(
        self,
        question: str,
        model_dir: Path,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        from ai_assistant.llm.decode import trim_sql_output

        tok = WordTokenizer.load(model_dir / "tokenizer.json")
        model = NumpyNeuralLM.load_npz(model_dir / "model.npz")
        prefix = question_prefix(question, tok)
        from ai_assistant.llm.decode import GenerationConfig

        out_ids = model.generate(
            prefix,
            pad_id=tok.pad_id,
            eos_id=tok.eos_id,
            config=GenerationConfig.from_params(params),
            decode_token=tok.decode_token,
        )
        raw = tok.decode(out_ids).strip()
        return {"sql": trim_sql_output(raw) or raw}

    def status(self, model_dir: Path) -> dict[str, Any]:
        meta_path = model_dir / "meta.json"
        if meta_path.exists():
            return json.loads(meta_path.read_text(encoding="utf-8"))
        if (model_dir / "model.npz").exists():
            return {"artifact": "model.npz", "engine": self.name}
        return {}
