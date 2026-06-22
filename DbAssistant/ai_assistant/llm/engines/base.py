"""
Abstract base for pluggable LLM training/generation engines.

Each engine implements train / generate / status against a model directory.
Engines are selected via config (``ai.llm.engine``) and recorded in
``meta.json`` so inference uses the same engine that trained the model.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Optional


class LlmEngine(ABC):
    """Pluggable NL->SQL engine (Stage 0–3)."""

    name: str = "base"
    stage: int = 0
    display_name: str = "Base"
    requires: list[str] = []  # pip package names for availability hints

    @abstractmethod
    def is_available(self) -> tuple[bool, str]:
        """Return (available, reason_if_not)."""

    @abstractmethod
    def train(
        self,
        pairs: list[dict],
        model_dir: Path,
        *,
        config: dict[str, Any],
        progress: Optional[Callable[[dict], None]] = None,
    ) -> dict[str, Any]:
        """Train on *pairs*, persist artifacts under *model_dir*, return metrics."""

    @abstractmethod
    def generate(
        self,
        question: str,
        model_dir: Path,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Generate SQL for *question* using artifacts in *model_dir*."""

    @abstractmethod
    def status(self, model_dir: Path) -> dict[str, Any]:
        """Return engine-specific status for a trained model directory."""

    def info(self) -> dict[str, Any]:
        ok, reason = self.is_available()
        return {
            "name": self.name,
            "stage": self.stage,
            "display_name": self.display_name,
            "available": ok,
            "reason": reason if not ok else "",
            "requires": list(self.requires),
        }
