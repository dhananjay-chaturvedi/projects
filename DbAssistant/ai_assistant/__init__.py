"""Umbrella package for the tool's AI subsystems.

Houses the three AI-related assistants and the shared, code/math-based
measurement subsystem they all rely on:

* ``ai_assistant.meters``  — deterministic accuracy/error/understanding and
  build/code quality meters (no model prompts; pure parsing, set math and
  rule checks).
* ``ai_assistant.rag`` — offline retrieval-augmented Generate SQL: schema +
  glossary/example indexing into a local vector store, hybrid retrieval.
* ``ai_assistant.llm`` — local trainable NL→SQL model (python/numpy/pytorch/
  ollama engines) powering the offline "Local LLM" backend.
* ``ai_assistant.app_builder`` — AI-driven app builder + engine.

The RAG and LLM subsystems are wired into the ``ai_query`` module (service /
CLI / API / backends) and its UI (RAG controls + Train LLM panel).
"""

from __future__ import annotations

__all__ = ["meters"]
