"""SQL editor query parsing tests."""

from common.ui.tk.sql_editor_pane import SQLEditorPane


def _make_pane():
    return SQLEditorPane.__new__(SQLEditorPane)


def test_parse_queries_ignores_semicolons_in_comment_lines():
    pane = _make_pane()
    text = (
        "-- SQL Query Editor — F5 or Ctrl+Enter; separate multiple queries with semicolons.\n\n"
        "show tables"
    )
    assert pane.parse_queries(text) == ["show tables"]


def test_parse_queries_splits_on_statement_semicolons():
    pane = _make_pane()
    text = "select 1;\nselect 2;"
    assert pane.parse_queries(text) == ["select 1", "select 2"]
