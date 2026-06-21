"""Regression tests for the ``ai`` one-shot CLI routing.

``ai_query ai --conn X "free text"`` previously crashed with an argparse
"invalid choice" error because the free-text question collided with the ``ai``
subparsers. ``inject_oneshot_ask`` now routes free text to a dedicated ``ask``
subcommand. These tests lock that behaviour in (pure parsing — no model calls).
"""
from __future__ import annotations

import argparse

import pytest

from ai_query.cli import inject_oneshot_ask, register_cli


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ai_query")
    sub = parser.add_subparsers(dest="command", metavar="<command>")
    register_cli(sub)
    return parser


@pytest.mark.parametrize(
    "argv, expected",
    [
        # free-text one-shot -> ask is injected
        (["ai", "--conn", "db", "how many tables?"],
         ["ai", "ask", "--conn", "db", "how many tables?"]),
        # question-first ordering
        (["ai", "list tables", "--conn", "db"],
         ["ai", "ask", "list tables", "--conn", "db"]),
        # known subcommands are untouched
        (["ai", "session", "list"], ["ai", "session", "list"]),
        (["ai", "explain", "--sql", "SELECT 1"], ["ai", "explain", "--sql", "SELECT 1"]),
        # --list-backends is untouched
        (["ai", "--list-backends"], ["ai", "--list-backends"]),
        # explicit ask is untouched (already routed)
        (["ai", "ask", "--conn", "db", "q"], ["ai", "ask", "--conn", "db", "q"]),
        # no question, only flags -> untouched (parser reports a clear error)
        (["ai", "--conn", "db"], ["ai", "--conn", "db"]),
        # non-ai argv is untouched
        (["query", "--conn", "db", "--sql", "SELECT 1"],
         ["query", "--conn", "db", "--sql", "SELECT 1"]),
    ],
)
def test_inject_oneshot_ask(argv, expected):
    assert inject_oneshot_ask(argv) == expected


def test_oneshot_parses_to_ask_with_question():
    parser = _build_parser()
    argv = inject_oneshot_ask(["ai", "--conn", "db", "how", "many", "tables?"])
    args = parser.parse_args(argv)
    assert args.command == "ai"
    assert args.ai_subcommand == "ask"
    assert args.conn == "db"
    assert " ".join(args.question) == "how many tables?"


def test_subcommands_still_parse():
    parser = _build_parser()
    args = parser.parse_args(["ai", "session", "list"])
    assert args.ai_subcommand == "session"
    assert args.session_cmd == "list"


def test_list_backends_still_parses():
    parser = _build_parser()
    args = parser.parse_args(["ai", "--list-backends"])
    assert args.ai_subcommand is None
    assert args.list_backends is True


def test_freetext_question_no_longer_invalid_choice():
    """The historic crash: a bare free-text question must not raise SystemExit."""
    parser = _build_parser()
    argv = inject_oneshot_ask(["ai", "--conn", "db", "explain to me the schema"])
    # Must parse cleanly (previously argparse raised SystemExit: invalid choice).
    args = parser.parse_args(argv)
    assert args.ai_subcommand == "ask"
    assert " ".join(args.question) == "explain to me the schema"
