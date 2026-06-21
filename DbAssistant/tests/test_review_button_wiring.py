"""The Tk 'Review SQL' button must route through the shared AIService.review_sql."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_tk_review_routes_through_shared_service():
    tk = (ROOT / "common/ui/tk/ai/ai_query_ui.py").read_text()
    # run_sql_review must call the shared service, not duplicate the prompt or
    # hard-code the Claude CLI.
    assert "svc.review_sql(" in tk
    # The old direct CLI call inside the review thread is gone.
    review_idx = tk.index("def run_sql_review")
    end_idx = tk.index("def ", review_idx + 10)
    body = tk[review_idx:end_idx]
    assert "_call_claude_cli" not in body


def test_review_sql_present_across_surfaces():
    svc = (ROOT / "ai_query/service.py").read_text()
    assert "def review_sql" in svc
    cli = (ROOT / "ai_query/cli.py").read_text()
    assert "review_sql(" in cli
    api = (ROOT / "ai_query/api.py").read_text()
    assert "/api/ai/review" in api and "review_sql(" in api
