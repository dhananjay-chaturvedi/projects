"""Shared inter-session protocol tokens for the triple-session App Builder.

These are the single source of truth for the literal markers the sessions emit
and that the orchestrator / App Builder Assistant parse back out. Keeping the
emitted-string and the parsing-regex side by side here stops the prompt text and
the parser from drifting apart.

* ``MARK_START`` / ``MARK_DONE`` — Sessions B and C wrap the message they intend
  for another session between these so the assistant can lift the real payload
  out of any surrounding chatter (see ``build_session.extract_payload``).
* ``PHASE_DONE_*`` — Session A announces a finished component with
  ``PHASE-DONE: <component>``; ``PHASE_DONE_RE`` parses those announcements
  (see ``agent_runner.phase_done_components``).
"""

from __future__ import annotations

import re

# ── handoff markers (B/C → assistant) ──────────────────────────────────────────
MARK_START = "START!"
MARK_DONE = "DONE!"
MARK_RULE = (f"Wrap the message you want delivered between {MARK_START} and "
             f"{MARK_DONE} on their own (e.g. '{MARK_START} … {MARK_DONE}').")

# ── phase-completion marker (A → assistant) ────────────────────────────────────
PHASE_DONE_TOKEN = "PHASE-DONE"
PHASE_DONE_RE = re.compile(
    rf"(?im)^\s*{PHASE_DONE_TOKEN}\s*:\s*([a-z][a-z0-9_/-]*)",
)


def phase_done_marker(component: str = "<component>") -> str:
    """Return the exact ``PHASE-DONE: <component>`` string a session must emit."""
    return f"{PHASE_DONE_TOKEN}: {component}"
