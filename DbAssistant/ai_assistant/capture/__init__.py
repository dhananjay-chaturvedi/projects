"""Capture pipeline — record grounded AI turns for training and auditing.

Each record is isolated by ``project_id`` + ``connection`` + ``database`` so
many projects/databases can contribute without cross-contamination.
"""

from __future__ import annotations

from ai_assistant.capture.pipeline import CapturePipeline, maybe_capture_turn

__all__ = ["CapturePipeline", "maybe_capture_turn"]
