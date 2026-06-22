"""
NeuralLM — a tiny, from-scratch neural language model in pure Python.

Architecture (a neural n-gram LM, à la Bengio et al. 2003):

    context tokens (C ids)
        -> embedding lookup (V x D)
        -> concat (C*D)
        -> linear W1 (C*D x H) + tanh
        -> linear W2 (H x V)
        -> softmax over vocab

Training is real mini-batch gradient descent: manual forward + backprop with an
Adam optimizer and cross-entropy loss. No numpy, no torch — just Python lists and
the ``math`` module, so it runs anywhere this tool runs and trains a small model
in seconds. Swap this class for a PyTorch model later behind the same Trainer.
"""

from __future__ import annotations

import json
import math
import random
from pathlib import Path
from typing import Callable


def _zeros(n: int) -> list[float]:
    return [0.0] * n


def _matrix(rows: int, cols: int, scale: float, rng: random.Random) -> list[list[float]]:
    return [[rng.uniform(-scale, scale) for _ in range(cols)] for _ in range(rows)]


class NeuralLM:
    def __init__(
        self,
        vocab_size: int,
        *,
        context: int = 3,
        emb_dim: int = 16,
        hidden: int = 64,
        seed: int = 1234,
    ):
        self.vocab_size = vocab_size
        self.context = context
        self.emb_dim = emb_dim
        self.hidden = hidden
        self.seed = seed
        rng = random.Random(seed)

        ind = 1.0 / math.sqrt(emb_dim)
        inh = 1.0 / math.sqrt(context * emb_dim)
        ino = 1.0 / math.sqrt(hidden)
        # Parameters
        self.E = _matrix(vocab_size, emb_dim, ind, rng)
        self.W1 = _matrix(context * emb_dim, hidden, inh, rng)
        self.b1 = _zeros(hidden)
        self.W2 = _matrix(hidden, vocab_size, ino, rng)
        self.b2 = _zeros(vocab_size)

        self._init_adam()

    # ── Adam optimizer state ────────────────────────────────────────────

    def _init_adam(self) -> None:
        self._mE = [_zeros(self.emb_dim) for _ in range(self.vocab_size)]
        self._vE = [_zeros(self.emb_dim) for _ in range(self.vocab_size)]
        self._mW1 = [_zeros(self.hidden) for _ in range(self.context * self.emb_dim)]
        self._vW1 = [_zeros(self.hidden) for _ in range(self.context * self.emb_dim)]
        self._mb1, self._vb1 = _zeros(self.hidden), _zeros(self.hidden)
        self._mW2 = [_zeros(self.vocab_size) for _ in range(self.hidden)]
        self._vW2 = [_zeros(self.vocab_size) for _ in range(self.hidden)]
        self._mb2, self._vb2 = _zeros(self.vocab_size), _zeros(self.vocab_size)
        self._t = 0

    # ── forward ─────────────────────────────────────────────────────────

    def _forward(self, context_ids: list[int]):
        C, D, H = self.context, self.emb_dim, self.hidden
        E, W1, b1, W2, b2 = self.E, self.W1, self.b1, self.W2, self.b2

        # concat embeddings of the context tokens
        x = [0.0] * (C * D)
        for i, cid in enumerate(context_ids):
            emb = E[cid]
            base = i * D
            for d in range(D):
                x[base + d] = emb[d]

        # hidden = tanh(x @ W1 + b1)
        h = [0.0] * H
        for j in range(H):
            s = b1[j]
            for i in range(C * D):
                s += x[i] * W1[i][j]
            h[j] = math.tanh(s)

        # logits = h @ W2 + b2
        V = self.vocab_size
        logits = list(b2)
        for j in range(H):
            hj = h[j]
            if hj == 0.0:
                continue
            row = W2[j]
            for k in range(V):
                logits[k] += hj * row[k]

        # softmax
        m = max(logits)
        exps = [math.exp(v - m) for v in logits]
        ssum = sum(exps)
        probs = [e / ssum for e in exps]
        return x, h, probs

    # ── loss + gradients over a batch ───────────────────────────────────

    def loss_and_grad(self, batch: list[tuple[list[int], int]]):
        C, D, H, V = self.context, self.emb_dim, self.hidden, self.vocab_size
        W1, W2 = self.W1, self.W2

        gE = {}  # sparse: token_id -> grad vector (most tokens untouched per batch)
        gW1 = [[0.0] * H for _ in range(C * D)]
        gb1 = [0.0] * H
        gW2 = [[0.0] * V for _ in range(H)]
        gb2 = [0.0] * V

        total_loss = 0.0
        for context_ids, target in batch:
            x, h, probs = self._forward(context_ids)
            total_loss += -math.log(max(probs[target], 1e-12))

            # dlogits = probs - onehot(target)
            dlogits = probs[:]
            dlogits[target] -= 1.0

            # grads for W2, b2 and dh
            dh = [0.0] * H
            for j in range(H):
                hj = h[j]
                row2 = W2[j]
                grow2 = gW2[j]
                acc = 0.0
                for k in range(V):
                    dl = dlogits[k]
                    grow2[k] += hj * dl
                    acc += row2[k] * dl
                dh[j] = acc
            for k in range(V):
                gb2[k] += dlogits[k]

            # backprop through tanh
            dh_pre = [dh[j] * (1.0 - h[j] * h[j]) for j in range(H)]

            # grads for W1, b1 and dx
            dx = [0.0] * (C * D)
            for i in range(C * D):
                xi = x[i]
                row1 = W1[i]
                grow1 = gW1[i]
                acc = 0.0
                for j in range(H):
                    dpj = dh_pre[j]
                    grow1[j] += xi * dpj
                    acc += row1[j] * dpj
                dx[i] = acc
            for j in range(H):
                gb1[j] += dh_pre[j]

            # scatter dx back into embedding gradients
            for i, cid in enumerate(context_ids):
                base = i * D
                vec = gE.get(cid)
                if vec is None:
                    vec = [0.0] * D
                    gE[cid] = vec
                for d in range(D):
                    vec[d] += dx[base + d]

        n = max(1, len(batch))
        return total_loss / n, (gE, gW1, gb1, gW2, gb2), n

    # ── Adam parameter update ───────────────────────────────────────────

    def step(self, grads, n: int, *, lr: float = 0.01, b1=0.9, b2=0.999, eps=1e-8):
        gE, gW1, gb1, gW2, gb2 = grads
        self._t += 1
        t = self._t
        bc1 = 1.0 - b1 ** t
        bc2 = 1.0 - b2 ** t
        inv_n = 1.0 / n

        def upd(p, m, v, g):
            g *= inv_n
            m = b1 * m + (1 - b1) * g
            v = b2 * v + (1 - b2) * g * g
            p -= lr * (m / bc1) / (math.sqrt(v / bc2) + eps)
            return p, m, v

        # embeddings (only touched tokens)
        for cid, gvec in gE.items():
            E, mE, vE = self.E[cid], self._mE[cid], self._vE[cid]
            for d in range(self.emb_dim):
                E[d], mE[d], vE[d] = upd(E[d], mE[d], vE[d], gvec[d])

        for i in range(self.context * self.emb_dim):
            W1, mW1, vW1, g = self.W1[i], self._mW1[i], self._vW1[i], gW1[i]
            for j in range(self.hidden):
                W1[j], mW1[j], vW1[j] = upd(W1[j], mW1[j], vW1[j], g[j])
        for j in range(self.hidden):
            self.b1[j], self._mb1[j], self._vb1[j] = upd(
                self.b1[j], self._mb1[j], self._vb1[j], gb1[j]
            )

        for j in range(self.hidden):
            W2, mW2, vW2, g = self.W2[j], self._mW2[j], self._vW2[j], gW2[j]
            for k in range(self.vocab_size):
                W2[k], mW2[k], vW2[k] = upd(W2[k], mW2[k], vW2[k], g[k])
        for k in range(self.vocab_size):
            self.b2[k], self._mb2[k], self._vb2[k] = upd(
                self.b2[k], self._mb2[k], self._vb2[k], gb2[k]
            )

    # ── generation ──────────────────────────────────────────────────────

    def predict_proba(self, context_ids: list[int]) -> list[float]:
        _, _, probs = self._forward(context_ids)
        return probs

    def generate(
        self,
        prefix_ids: list[int],
        *,
        pad_id: int,
        eos_id: int,
        config: "GenerationConfig | None" = None,
        decode_token: Callable[[int], str] | None = None,
    ) -> list[int]:
        """Continue *prefix_ids* with guarded decoding."""
        from ai_assistant.llm.decode import guarded_generate

        return guarded_generate(
            prefix_ids,
            predict_proba=self.predict_proba,
            decode_token=decode_token or (lambda _i: ""),
            pad_id=pad_id,
            eos_id=eos_id,
            context=self.context,
            config=config,
        )

    # ── persistence ─────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "vocab_size": self.vocab_size,
            "context": self.context,
            "emb_dim": self.emb_dim,
            "hidden": self.hidden,
            "seed": self.seed,
            "E": self.E,
            "W1": self.W1,
            "b1": self.b1,
            "W2": self.W2,
            "b2": self.b2,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "NeuralLM":
        m = cls(
            data["vocab_size"],
            context=data["context"],
            emb_dim=data["emb_dim"],
            hidden=data["hidden"],
            seed=data.get("seed", 1234),
        )
        m.E, m.W1, m.b1, m.W2, m.b2 = (
            data["E"], data["W1"], data["b1"], data["W2"], data["b2"]
        )
        return m

    def save(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.to_dict()), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> "NeuralLM":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
