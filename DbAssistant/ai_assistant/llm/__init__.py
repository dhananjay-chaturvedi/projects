# ---------------------------------------------------------------------
# description: From-scratch tiny LLM (neural language model) for NL->SQL
# initial version: 09-JUN-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------
"""
A self-contained, dependency-free neural language model you can train on your
own NL->SQL data — entirely from scratch, on CPU, in seconds.

This is intentionally *small and fast* for testing the full pipeline end to end:
tokenizer -> embeddings -> MLP -> softmax -> backprop (Adam) -> sampling. It is a
genuine trainable neural net (not a lookup table), just sized for a laptop and a
tiny dataset. The same `LlmService` interface can later be backed by a real
PyTorch/transformers + LoRA trainer for large corpora without changing callers.

Public surface:
    WordTokenizer  -- whitespace/symbol tokenizer with special tokens
    NeuralLM       -- pure-Python MLP n-gram language model (trainable)
    Trainer        -- training loop (Adam, cross-entropy, early stop)
    LlmService     -- shared train/generate/status/export logic (UI/CLI/API)
"""

from __future__ import annotations

from ai_assistant.llm.tokenizer import WordTokenizer
from ai_assistant.llm.model import NeuralLM
from ai_assistant.llm.trainer import Trainer, TrainConfig

__all__ = ["WordTokenizer", "NeuralLM", "Trainer", "TrainConfig"]
