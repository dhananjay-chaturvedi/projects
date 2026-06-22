"""Back-compat shim for moved LLM training data sources."""

from ai_assistant.llm.data_sources import *  # noqa: F401,F403
from ai_assistant.llm.data_sources import (  # noqa: F401
    _dedupe_pairs,
    _fold_question,
    _normalize_sql,
    _pairs_from_insight,
    _valid_sql,
)
