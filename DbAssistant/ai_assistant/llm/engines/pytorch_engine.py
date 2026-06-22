"""Stage 2 — PyTorch nano-GPT transformer engine (default)."""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Callable, Optional

from ai_assistant.llm.dataset import (
    build_sequences,
    build_tokenizer,
    pair_to_sequence,
    question_prefix,
)
from ai_assistant.llm.engines.base import LlmEngine
from ai_assistant.llm.tokenizer import WordTokenizer

# Loss-mask sentinel: positions set to this in the target are ignored by
# cross-entropy (PAD padding + the question region, so we train SQL-only).
_IGNORE = -100


def _import_torch():
    import torch
    return torch


def _build_supervised_examples(
    sequences: list[list[int]],
    *,
    sep_id: int,
    pad_id: int,
    block_size: int,
) -> tuple[list[list[int]], list[list[int]]]:
    """Turn ``<bos> q <sep> sql <eos>`` sequences into padded (input, target)
    rows for teacher-forced training.

    For each sequence the target at position ``t`` is ``seq[t+1]``; we mask
    (``_IGNORE``) every target that falls in the question region (``t+1`` up to
    and including the ``<sep>``) and every PAD position, so the model is scored
    only on producing the SQL (and the closing ``<eos>``). Sequences longer than
    ``block_size+1`` are left-truncated on the question side so the SQL tail and
    ``<eos>`` are always preserved.
    """
    inputs: list[list[int]] = []
    targets: list[list[int]] = []
    for seq in sequences:
        if len(seq) < 2:
            continue
        if len(seq) > block_size + 1:
            # Keep <bos> + the tail that fits (question may be clipped, SQL kept).
            seq = [seq[0]] + seq[-(block_size):]
        try:
            sep_idx = seq.index(sep_id)
        except ValueError:
            sep_idx = 0
        inp = seq[:-1]
        tgt = seq[1:]
        # Mask question-region targets: target tgt[i] predicts seq[i+1]; it is in
        # the SQL region only when (i+1) > sep_idx.
        masked = [t if (i + 1) > sep_idx else _IGNORE for i, t in enumerate(tgt)]
        pad_n = block_size - len(inp)
        if pad_n > 0:
            inp = inp + [pad_id] * pad_n
            masked = masked + [_IGNORE] * pad_n
        inputs.append(inp)
        targets.append(masked)
    return inputs, targets


def _resolve_device(config: dict[str, Any]) -> str:
    torch = _import_torch()
    pref = (config.get("pt_device") or "auto").strip().lower()
    if pref == "cpu":
        return "cpu"
    if pref == "cuda" and torch.cuda.is_available():
        return "cuda"
    if pref == "mps" and getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
        return "mps"
    if pref == "auto":
        if torch.cuda.is_available():
            return "cuda"
        # NOTE: Deliberately avoid MPS on "auto". Training the transformer on the
        # Apple-Silicon MPS backend from a background/daemon thread (e.g. the Tk
        # AI Query Assistant training worker) can hard-crash the process with a
        # segfault. CPU is stable and still runs the full transformer. Users who
        # explicitly set pt_device="mps" still get MPS.
        return "cpu"
    return "cpu"


def _build_gpt_config(vocab_size: int, block_size: int, config: dict[str, Any]):
    n_layer = int(config.get("pt_n_layer", 2))
    n_head = int(config.get("pt_n_head", 2))
    n_embd = int(config.get("pt_n_embd", 64))
    dropout = float(config.get("pt_dropout", 0.0))

    class GPTConfig:
        pass

    cfg = GPTConfig()
    cfg.vocab_size = vocab_size
    cfg.block_size = block_size
    cfg.n_layer = n_layer
    cfg.n_head = n_head
    cfg.n_embd = n_embd
    cfg.dropout = dropout
    return cfg


def _make_model(cfg):
    torch = _import_torch()
    import torch.nn as nn
    import torch.nn.functional as F

    class CausalSelfAttention(nn.Module):
        def __init__(self, config):
            super().__init__()
            assert config.n_embd % config.n_head == 0
            self.n_head = config.n_head
            self.n_embd = config.n_embd
            self.head_dim = config.n_embd // config.n_head
            self.c_attn = nn.Linear(config.n_embd, 3 * config.n_embd)
            self.c_proj = nn.Linear(config.n_embd, config.n_embd)
            self.dropout = nn.Dropout(config.dropout)
            self.register_buffer(
                "bias",
                torch.tril(torch.ones(config.block_size, config.block_size))
                .view(1, 1, config.block_size, config.block_size),
            )

        def forward(self, x):
            B, T, C = x.size()
            qkv = self.c_attn(x)
            q, k, v = qkv.split(self.n_embd, dim=2)
            k = k.view(B, T, self.n_head, self.head_dim).transpose(1, 2)
            q = q.view(B, T, self.n_head, self.head_dim).transpose(1, 2)
            v = v.view(B, T, self.n_head, self.head_dim).transpose(1, 2)
            att = (q @ k.transpose(-2, -1)) * (1.0 / math.sqrt(self.head_dim))
            att = att.masked_fill(self.bias[:, :, :T, :T] == 0, float("-inf"))
            att = F.softmax(att, dim=-1)
            att = self.dropout(att)
            y = att @ v
            y = y.transpose(1, 2).contiguous().view(B, T, C)
            return self.dropout(self.c_proj(y))

    class MLP(nn.Module):
        def __init__(self, config):
            super().__init__()
            self.c_fc = nn.Linear(config.n_embd, 4 * config.n_embd)
            self.c_proj = nn.Linear(4 * config.n_embd, config.n_embd)
            self.dropout = nn.Dropout(config.dropout)

        def forward(self, x):
            return self.dropout(self.c_proj(F.gelu(self.c_fc(x))))

    class Block(nn.Module):
        def __init__(self, config):
            super().__init__()
            self.ln1 = nn.LayerNorm(config.n_embd)
            self.attn = CausalSelfAttention(config)
            self.ln2 = nn.LayerNorm(config.n_embd)
            self.mlp = MLP(config)

        def forward(self, x):
            x = x + self.attn(self.ln1(x))
            x = x + self.mlp(self.ln2(x))
            return x

    class GPT(nn.Module):
        def __init__(self, config):
            super().__init__()
            self.config = config
            self.tok_emb = nn.Embedding(config.vocab_size, config.n_embd)
            self.pos_emb = nn.Embedding(config.block_size, config.n_embd)
            self.drop = nn.Dropout(config.dropout)
            self.blocks = nn.ModuleList([Block(config) for _ in range(config.n_layer)])
            self.ln_f = nn.LayerNorm(config.n_embd)
            self.head = nn.Linear(config.n_embd, config.vocab_size, bias=False)

        def forward(self, idx, targets=None):
            B, T = idx.size()
            pos = torch.arange(0, T, dtype=torch.long, device=idx.device)
            x = self.drop(self.tok_emb(idx) + self.pos_emb(pos))
            for block in self.blocks:
                x = block(x)
            x = self.ln_f(x)
            logits = self.head(x)
            loss = None
            if targets is not None:
                # ignore_index=-100 skips PAD positions and the question region
                # (targets are masked there) so loss is computed on SQL tokens
                # only — focusing the model's capacity on what we generate.
                loss = F.cross_entropy(
                    logits.view(-1, logits.size(-1)),
                    targets.view(-1),
                    ignore_index=-100,
                )
            return logits, loss

    return GPT(cfg)


class PytorchEngine(LlmEngine):
    name = "pytorch"
    stage = 2
    display_name = "PyTorch Transformer"
    requires = ["torch"]

    def is_available(self) -> tuple[bool, str]:
        try:
            _import_torch()
            return True, ""
        except ImportError:
            return False, "torch not installed (see ai_query/requirements-llm.txt)"
        except OSError as exc:
            return False, f"torch installed but cannot load ({exc})"
        except Exception as exc:  # noqa: BLE001
            return False, f"torch unavailable: {exc}"

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

        torch = _import_torch()
        model_dir.mkdir(parents=True, exist_ok=True)
        started = time.time()
        device = _resolve_device(config)
        seed = int(config.get("seed", 1234))
        torch.manual_seed(seed)

        min_freq = int(config.get("min_freq", 1))
        tok = build_tokenizer(pairs, min_freq=min_freq)
        sequences = build_sequences(pairs, tok)

        # Block size spans the longest training sequence so the whole question is
        # always in view when the SQL is produced. Capped (pt_block_size or 512)
        # to bound the positional table / memory; longer sequences are
        # left-truncated on the question side (SQL tail preserved).
        longest = max((len(s) for s in sequences), default=8)
        hard_cap = int(config.get("pt_block_size", 0)) or 512
        block_size = max(8, min(longest, hard_cap))

        # Supervised, per-pair, SQL-masked examples (no cross-pair flattening).
        xs, ys = _build_supervised_examples(
            sequences, sep_id=tok.sep_id, pad_id=tok.pad_id, block_size=block_size,
        )
        if not xs:
            raise RuntimeError("No trainable sequences after preprocessing.")

        # One-time tensor build, moved to device once (no per-iter Python stacks).
        X = torch.tensor(xs, dtype=torch.long, device=device)
        Y = torch.tensor(ys, dtype=torch.long, device=device)
        num_examples = X.size(0)
        num_target_tokens = int((Y != _IGNORE).sum().item())

        batch_size = min(int(config.get("pt_batch_size", 16)), num_examples)
        max_iters = int(config.get("pt_max_iters", 500))
        lr = float(config.get("pt_lr", 3e-4))
        grad_clip = float(config.get("pt_grad_clip", 1.0))
        log_every = int(config.get("log_every", 50))

        gpt_cfg = _build_gpt_config(tok.vocab_size, block_size, config)
        model = _make_model(gpt_cfg).to(device)
        opt = torch.optim.AdamW(model.parameters(), lr=lr)

        history: list[float] = []
        final_loss = float("inf")
        iters_run = 0
        gen = torch.Generator(device="cpu").manual_seed(seed)

        model.train()
        for it in range(1, max_iters + 1):
            ix = torch.randint(num_examples, (batch_size,), generator=gen)
            xb = X[ix]
            yb = Y[ix]
            _, loss = model(xb, yb)
            opt.zero_grad(set_to_none=True)
            loss.backward()
            if grad_clip and grad_clip > 0:
                torch.nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
            opt.step()
            val = float(loss.item())
            history.append(val)
            final_loss = val
            iters_run = it
            if progress and (it % log_every == 0 or it == 1):
                progress({"epoch": it, "loss": round(val, 4)})
            if val <= float(config.get("min_loss", 0.05)):
                break

        # All-or-nothing artifact write: serialise both the model and tokenizer
        # to temp files first, then atomically rename them into place. A crash
        # mid-save therefore never leaves a half-written model.pt or a
        # model/tokenizer mismatch — the previous artifacts stay intact until
        # both new files are fully flushed.
        import os

        model_tmp = model_dir / "model.pt.tmp"
        tok_tmp = model_dir / "tokenizer.json.tmp"
        try:
            with open(model_tmp, "wb") as fh:
                torch.save(
                    {
                        "model_state": model.state_dict(),
                        "gpt_config": {
                            "vocab_size": gpt_cfg.vocab_size,
                            "block_size": gpt_cfg.block_size,
                            "n_layer": gpt_cfg.n_layer,
                            "n_head": gpt_cfg.n_head,
                            "n_embd": gpt_cfg.n_embd,
                            "dropout": gpt_cfg.dropout,
                        },
                    },
                    fh,
                )
                fh.flush()
                os.fsync(fh.fileno())
            tok.save(tok_tmp)
            os.replace(model_tmp, model_dir / "model.pt")
            os.replace(tok_tmp, model_dir / "tokenizer.json")
        finally:
            for tmp in (model_tmp, tok_tmp):
                try:
                    if tmp.exists():
                        tmp.unlink()
                except OSError:
                    pass
        params = sum(p.numel() for p in model.parameters())

        return {
            "epochs_run": iters_run,
            "final_loss": round(final_loss, 4),
            "vocab_size": tok.vocab_size,
            "context": block_size,
            "num_pairs": len(pairs),
            "num_examples": num_examples,
            "num_target_tokens": num_target_tokens,
            "params": params,
            "elapsed_sec": round(time.time() - started, 3),
            "loss_history": [round(h, 4) for h in history[-20:]],
            "device": device,
        }

    def _load_model(self, model_dir: Path, device: str):
        torch = _import_torch()
        model_path = model_dir / "model.pt"
        try:
            payload = torch.load(model_path, map_location=device, weights_only=True)
        except TypeError:
            payload = torch.load(model_path, map_location=device)
        gcfg = payload["gpt_config"]

        class _Cfg:
            vocab_size = gcfg["vocab_size"]
            block_size = gcfg["block_size"]
            n_layer = gcfg["n_layer"]
            n_head = gcfg["n_head"]
            n_embd = gcfg["n_embd"]
            dropout = gcfg.get("dropout", 0.0)

        model = _make_model(_Cfg)
        model.load_state_dict(payload["model_state"])
        model.to(device)
        model.eval()
        return model, gcfg["block_size"]

    def generate(
        self,
        question: str,
        model_dir: Path,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        from ai_assistant.llm.decode import guarded_generate, trim_sql_output

        torch = _import_torch()
        tok = WordTokenizer.load(model_dir / "tokenizer.json")
        device = _resolve_device(params)
        model, block_size = self._load_model(model_dir, device)
        pad_id = tok.pad_id

        def predict_proba(ctx_ids: list[int]) -> list[float]:
            # guarded_generate left-pads to ``context``; strip those pads so the
            # transformer sees tokens at the same positions as during training,
            # then crop to the model's block size.
            ids = [i for i in ctx_ids if i != pad_id] or [tok.bos_id]
            ids = ids[-block_size:]
            with torch.no_grad():
                x = torch.tensor([ids], dtype=torch.long, device=device)
                logits, _ = model(x)
                probs = torch.softmax(logits[0, -1, :], dim=-1)
            return probs.detach().cpu().tolist()

        prefix = question_prefix(question, tok)
        # Shared decoding guards (repetition penalty / no-repeat n-gram / top-k /
        # early stop) — same path as the numpy/python engines, so the repair pass
        # in LlmService.generate tunes the transformer too.
        from ai_assistant.llm.decode import GenerationConfig

        out_ids = guarded_generate(
            prefix,
            predict_proba=predict_proba,
            decode_token=tok.decode_token,
            pad_id=pad_id,
            eos_id=tok.eos_id,
            context=block_size,
            config=GenerationConfig.from_params({**params, "max_new": params.get("max_new", 256)}),
        )
        raw = tok.decode(out_ids).strip()
        return {"sql": trim_sql_output(raw) or raw}

    def status(self, model_dir: Path) -> dict[str, Any]:
        meta_path = model_dir / "meta.json"
        if meta_path.exists():
            return json.loads(meta_path.read_text(encoding="utf-8"))
        if (model_dir / "model.pt").exists():
            return {"artifact": "model.pt", "engine": self.name}
        return {}
