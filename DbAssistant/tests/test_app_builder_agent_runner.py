"""Tests for App Builder agentic runner (capability matrix + commands)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from ai_assistant.app_builder.agent_runner import (
    AgentEvent,
    AgentEventType,
    AgentMode,
    AgentRunner,
    _cursor_model,
    agent_is_idle,
    agent_signaled_done,
    build_command,
    capabilities,
    detect_questions,
    is_genuine_question,
    parse_stream_line,
    supports_agentic_write,
)
from ai_query import module_config as mc
from ai_query.backends.claude_cli import ClaudeCliBackend
from ai_query.backends.codex_backend import CodexBackend
from ai_query.backends.cursor_backend import CursorBackend


def test_capabilities_matrix():
    assert capabilities("cursor").agentic_write is True
    assert capabilities("cursor").resume is True
    assert capabilities("claude").agentic_write is True
    assert capabilities("codex").agentic_write is True
    assert capabilities("codex").resume is False
    assert capabilities("local-llm").agentic_write is False
    assert supports_agentic_write("cursor") is True
    assert supports_agentic_write("local-llm") is False


def test_build_command_cursor_agent_mode(tmp_path):
    backend = CursorBackend()
    backend._cli_path = "/usr/bin/cursor"
    backend._available = True
    cmd = build_command(
        backend, prompt="build app", workspace=tmp_path, mode=AgentMode.BUILD)
    assert "agent" in cmd
    assert "--force" in cmd
    # Current Cursor CLI only accepts `plan` and `ask`; write-capable agentic
    # mode is the default when --mode is omitted.
    assert "--mode" not in cmd
    assert "build app" in cmd


def test_build_command_cursor_ask_mode(tmp_path):
    backend = CursorBackend()
    backend._cli_path = "/usr/bin/cursor"
    backend._available = True
    cmd = build_command(
        backend, prompt="answer", workspace=tmp_path, mode=AgentMode.ASK,
        resume_session_id="chat-123")
    assert "--resume" in cmd
    assert cmd[cmd.index("--resume") + 1] == "chat-123"
    assert cmd[cmd.index("--mode") + 1] == "ask"


def test_build_command_cursor_plan_mode(tmp_path):
    backend = CursorBackend()
    backend._cli_path = "/usr/bin/cursor"
    backend._available = True
    cmd = build_command(
        backend, prompt="plan it", workspace=tmp_path, mode=AgentMode.PLAN,
        resume_session_id="chat-9")
    # Session B's planning turn — read-only `plan` mode, still resumable.
    assert cmd[cmd.index("--mode") + 1] == "plan"
    assert cmd[cmd.index("--resume") + 1] == "chat-9"


def test_cursor_model_uses_ask_model_for_ask_mode(monkeypatch):
    monkeypatch.setattr(mc, "get", lambda section, key, default="": {
        ("ai.cursor", "model"): "auto",
        ("ai.cursor", "ask_model"): "composer-2",
    }.get((section, key), default))
    assert _cursor_model(AgentMode.BUILD) == "auto"
    assert _cursor_model(AgentMode.ASK) == "composer-2"


def test_cursor_model_falls_back_when_ask_model_blank(monkeypatch):
    monkeypatch.setattr(mc, "get", lambda section, key, default="": {
        ("ai.cursor", "model"): "gpt-5.3-codex",
        ("ai.cursor", "ask_model"): "",
    }.get((section, key), default))
    assert _cursor_model(AgentMode.ASK) == "gpt-5.3-codex"


def test_build_command_cursor_ask_uses_ask_model(monkeypatch, tmp_path):
    monkeypatch.setattr(mc, "get", lambda section, key, default="": {
        ("ai.cursor", "model"): "auto",
        ("ai.cursor", "ask_model"): "composer-2",
    }.get((section, key), default))
    backend = CursorBackend()
    backend._cli_path = "/usr/bin/cursor"
    backend._available = True
    cmd = build_command(
        backend, prompt="advise", workspace=tmp_path, mode=AgentMode.ASK)
    assert cmd[cmd.index("--model") + 1] == "composer-2"


def test_build_command_claude_uses_stdin_prompt(tmp_path):
    backend = ClaudeCliBackend()
    backend._cli_path = "/usr/bin/claude"
    backend._available = True
    cmd = build_command(backend, prompt="hi", workspace=tmp_path)
    assert "-p" in cmd
    assert "--permission-mode" in cmd


def test_build_command_codex_agentic(tmp_path):
    backend = CodexBackend()
    backend._cli_path = "/usr/bin/codex"
    backend._available = True
    cmd = build_command(backend, prompt="build", workspace=tmp_path)
    assert "exec" in cmd
    assert "--dangerously-bypass-approvals-and-sandbox" in cmd


def test_parse_stream_json_assistant_text():
    line = json.dumps({"type": "message", "text": "Hello builder"})
    events = parse_stream_line(line)
    assert any(e.type == AgentEventType.ASSISTANT_TEXT and e.text == "Hello builder"
               for e in events)


def test_parse_cursor_nested_message_text():
    line = json.dumps({
        "type": "assistant",
        "message": {"content": [{"type": "text", "text": "Nested text"}]},
    })
    events = parse_stream_line(line)
    assert any(e.type == AgentEventType.ASSISTANT_TEXT and e.text == "Nested text"
               for e in events)


def test_parse_stream_json_session_id():
    line = json.dumps({"type": "session", "session_id": "abc-42"})
    events = parse_stream_line(line)
    assert any(e.type == AgentEventType.SESSION_ID for e in events)


def test_parse_claude_thinking_only_suppresses_raw_json():
    line = json.dumps({
        "type": "assistant",
        "message": {
            "content": [{"type": "thinking", "thinking": "planning the build"}],
        },
        "session_id": "sess-1",
    })
    events = parse_stream_line(line)
    assert events == []


def test_parse_claude_tool_use_emits_tool_call():
    line = json.dumps({
        "type": "assistant",
        "message": {
            "content": [{
                "type": "tool_use",
                "name": "Read",
                "input": {"file_path": "/tmp/app.py"},
            }],
        },
    })
    events = parse_stream_line(line)
    assert any(
        e.type == AgentEventType.TOOL_CALL and "Read" in e.text for e in events)


def test_agent_runner_suppresses_session_id_callback():
    seen: list[str] = []

    class _Backend:
        name = "cursor"

    runner = AgentRunner(_Backend(), Path("."), on_event=lambda ev: seen.append(ev.type.value))
    runner._handle_event(AgentEvent(
        AgentEventType.SESSION_ID, text="sid-1", detail={"session_id": "sid-1"}))
    assert runner.session_id == "sid-1"
    assert seen == []


def test_detect_questions():
    text = "Should I use SQLite? Also need your input on auth."
    qs = detect_questions(text)
    assert qs
    assert any("?" in q for q in qs)


def test_detect_questions_ignores_non_question_lines():
    # No '?' → not treated as a question (narration / idle phrasing).
    text = "Plan:\n- I will set up the database\n- Awaiting your confirmation"
    assert detect_questions(text) == []


def test_is_genuine_question():
    assert is_genuine_question("Which database should I use?")
    assert is_genuine_question("Do you want auth enabled?")
    # Not genuine: no question mark, or rhetorical/narration.
    assert not is_genuine_question("I will add the routes now.")
    assert not is_genuine_question("Awaiting your confirmation.")
    assert not is_genuine_question("?")


def test_agent_signaled_done():
    assert agent_signaled_done("The app is complete and all tests pass.")
    assert agent_signaled_done("Implemented everything.\nDONE")
    assert not agent_signaled_done("Still working on the cart page.")


def test_agent_is_idle():
    assert agent_is_idle("Awaiting your next instruction.")
    assert agent_is_idle("Let me know how to proceed.")
    assert not agent_is_idle("Added the checkout route and a test.")


def test_parse_permission_request_event():
    line = json.dumps({"type": "permission_request",
                       "tool": "write", "path": "src/app.py"})
    events = parse_stream_line(line)
    assert any(e.type == AgentEventType.QUESTION
               and e.detail.get("permission") for e in events)


def test_parse_cursor_interaction_query_as_question():
    line = json.dumps({
        "type": "interaction_query",
        "subtype": "request",
        "query_type": "askQuestionInteractionQuery",
        "query": {
            "askQuestionInteractionQuery": {
                "args": {
                    "title": "Add order history",
                    "questions": [{
                        "id": "order_history",
                        "prompt": (
                            "How should order history work, given there are "
                            "no user accounts?"
                        ),
                        "options": [
                            {"id": "email", "label": "Look up orders by email"},
                            {"id": "session", "label": "Track browser session"},
                        ],
                    }],
                },
            },
        },
    })
    events = parse_stream_line(line)
    assert len(events) == 1
    assert events[0].type == AgentEventType.QUESTION
    assert events[0].detail["interaction_question"] is True
    assert "How should order history work" in events[0].text
    assert "email: Look up orders by email" in events[0].text
    assert events[0].detail["option_items"] == [
        {"id": "email", "label": "Look up orders by email"},
        {"id": "session", "label": "Track browser session"},
    ]
    assert events[0].detail["allow_multiple"] is False
    assert len(events[0].detail["questions"]) == 1
    assert events[0].detail["questions"][0]["prompt"].startswith(
        "How should order history work")


def test_parse_cursor_interaction_skip_response_is_notice():
    line = json.dumps({
        "type": "interaction_query",
        "subtype": "response",
        "query_type": "askQuestionInteractionQuery",
        "response": {
            "askQuestionInteractionResponse": {
                "result": {
                    "rejected": {
                        "reason": (
                            "Questions skipped by the user, continue with the "
                            "information you already have"
                        )
                    }
                }
            }
        },
    })
    events = parse_stream_line(line)
    assert len(events) == 1
    assert events[0].type == AgentEventType.NOTICE
    assert events[0].detail["interaction_response"] is True
    assert events[0].detail["suppressed_skip"] is True
    assert events[0].text == ""


def test_parse_generic_tool_call_has_empty_text():
    # Generic tool events still flow (so the UI can flush buffered narration),
    # but with empty text so no noisy "tool_call: tool_call" line is shown.
    events = parse_stream_line(json.dumps({"type": "tool_call"}))
    assert len(events) == 1
    assert events[0].type == AgentEventType.TOOL_CALL
    assert events[0].text == ""


def test_parse_named_tool_call_is_visible():
    events = parse_stream_line(json.dumps({
        "type": "tool_call", "tool": "search_files"}))
    assert len(events) == 1
    assert events[0].type == AgentEventType.TOOL_CALL
    assert events[0].text == "search_files"


def test_parse_file_and_shell_tools_are_specific_not_duplicate_tool_calls():
    file_events = parse_stream_line(json.dumps({
        "type": "tool_call", "tool": "write_file", "path": "src/app.py"}))
    assert [e.type for e in file_events] == [AgentEventType.FILE_WRITE]
    assert file_events[0].text == "src/app.py"

    shell_events = parse_stream_line(json.dumps({
        "type": "tool_call", "tool": "shell", "command": "pytest -q"}))
    assert [e.type for e in shell_events] == [AgentEventType.SHELL_RUN]
    assert shell_events[0].text == "pytest -q"


class _FakeProc:
    def __init__(self, lines, returncode=0):
        self._lines = lines
        self.returncode = returncode
        self.stdout = _FakeStdout(lines)
        self.stderr = None
        self.stdin = None

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        pass


class _FakeStdout:
    def __init__(self, lines):
        self._lines = iter(lines)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._lines)


def test_agent_runner_streams_events(tmp_path):
    backend = CursorBackend()
    backend._cli_path = "/usr/bin/cursor"
    backend._available = True
    lines = [
        json.dumps({"type": "message", "text": "Building app"}) + "\n",
        json.dumps({"type": "session", "session_id": "sess-1"}) + "\n",
    ]
    fake = _FakeProc(lines)

    with patch("ai_assistant.app_builder.agent_runner.subprocess.Popen",
               return_value=fake) as popen:
        runner = AgentRunner(backend, tmp_path, mode=AgentMode.BUILD, timeout=5)
        events = runner.run("build it")
    assert any(e.type == AgentEventType.ASSISTANT_TEXT for e in events)
    assert runner.session_id == "sess-1"
    assert "Building app" in runner.transcript
    # No stdin prompt for cursor → child gets DEVNULL (never blocks on input).
    import subprocess as _sp
    assert popen.call_args.kwargs.get("stdin") == _sp.DEVNULL


def test_agent_runner_empty_turn_emits_notice(tmp_path):
    """A clean turn that streams no parseable output must surface a NOTICE,
    so sessions never appear blank/frozen to the user."""
    backend = CursorBackend()
    backend._cli_path = "/usr/bin/cursor"
    backend._available = True
    fake = _FakeProc([], returncode=0)  # no stdout lines at all
    with patch("ai_assistant.app_builder.agent_runner.subprocess.Popen",
               return_value=fake):
        runner = AgentRunner(backend, tmp_path, mode=AgentMode.BUILD, timeout=5)
        events = runner.run("build it")
    assert any(e.type == AgentEventType.NOTICE for e in events)
    assert any(e.type == AgentEventType.DONE for e in events)
    # A NOTICE is non-fatal: it must not be an error event.
    assert not any(e.type == AgentEventType.ERROR for e in events)


def test_agent_runner_nonzero_exit_no_stderr_emits_notice(tmp_path):
    backend = CursorBackend()
    backend._cli_path = "/usr/bin/cursor"
    backend._available = True
    fake = _FakeProc([], returncode=2)
    with patch("ai_assistant.app_builder.agent_runner.subprocess.Popen",
               return_value=fake):
        runner = AgentRunner(backend, tmp_path, mode=AgentMode.BUILD, timeout=5)
        events = runner.run("build it")
    notices = [e for e in events if e.type == AgentEventType.NOTICE]
    assert notices and "code 2" in notices[0].text


def test_agent_runner_cancel_before_run_yields_error(tmp_path):
    import threading

    backend = CursorBackend()
    backend._cli_path = "/usr/bin/cursor"
    backend._available = True
    cancel = threading.Event()
    cancel.set()
    runner = AgentRunner(backend, tmp_path, mode=AgentMode.BUILD,
                         timeout=5, cancel_event=cancel)
    with patch("ai_assistant.app_builder.agent_runner.subprocess.Popen") as popen:
        events = runner.run("build it")
    assert popen.call_count == 0  # never spawned because already cancelled
    assert any(e.type == AgentEventType.ERROR and e.text == "cancelled"
               for e in events)
    assert runner.cancelled is True
