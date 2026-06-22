"""Stage 0 — pure-Python MLP n-gram engine (zero dependencies)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Optional

from ai_assistant.llm.dataset import question_prefix
from ai_assistant.llm.engines.base import LlmEngine
from ai_assistant.llm.model import NeuralLM
from ai_assistant.llm.tokenizer import WordTokenizer
from ai_assistant.llm.trainer import TrainConfig, Trainer


class PythonEngine(LlmEngine):
    name = "python"
    stage = 0
    display_name = "Pure Python MLP"
    requires: list[str] = []

    def is_available(self) -> tuple[bool, str]:
        return True, ""

    def _train_config(self, config: dict[str, Any]) -> TrainConfig:
        return TrainConfig(
            context=int(config.get("context", 0)),
            max_context=int(config.get("max_context", 40)),
            emb_dim=int(config.get("emb_dim", 12)),
            hidden=int(config.get("hidden", 48)),
            epochs=int(config.get("epochs", 150)),
            batch_size=int(config.get("batch_size", 32)),
            lr=float(config.get("lr", 0.02)),
            seed=int(config.get("seed", 1234)),
            min_loss=float(config.get("min_loss", 0.05)),
            log_every=int(config.get("log_every", 10)),
            min_freq=int(config.get("min_freq", 1)),
        )

    def train(
        self,
        pairs: list[dict],
        model_dir: Path,
        *,
        config: dict[str, Any],
        progress: Optional[Callable[[dict], None]] = None,
    ) -> dict[str, Any]:
        model_dir.mkdir(parents=True, exist_ok=True)
        trainer = Trainer(self._train_config(config))
        tok, model, metrics = trainer.train(pairs, progress=progress)
        model.save(model_dir / "model.json")
        tok.save(model_dir / "tokenizer.json")
        return metrics

    def generate(
        self,
        question: str,
        model_dir: Path,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        from ai_assistant.llm.decode import trim_sql_output

        tok = WordTokenizer.load(model_dir / "tokenizer.json")
        model = NeuralLM.load(model_dir / "model.json")
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
        if (model_dir / "model.json").exists():
            return {"artifact": "model.json", "engine": self.name}
        return {}
