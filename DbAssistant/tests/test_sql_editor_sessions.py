"""Phase 1(b): each SQL Editor tab owns exactly one private DB session, cloned
from the selected connection, swapped on connection change, closed on dispose.

These build a SQLEditorPane via __new__ to avoid constructing Tk widgets.
"""

from __future__ import annotations

import threading

import pytest

import common.db_manager as db_manager_mod
from common.ui.tk.sql_editor_pane import SQLEditorPane


class FakeManager:
    """Stand-in DatabaseManager that records connect/disconnect."""

    instances: list = []

    def __init__(self, db_type):
        self.db_type = db_type
        self.conn = None
        self._last_connect_params = None
        self.connect_calls = 0
        self.disconnect_calls = 0
        FakeManager.instances.append(self)

    def connect(self, **kwargs):
        self.connect_calls += 1
        self._last_connect_params = dict(kwargs)
        self.conn = object()
        return self.conn

    def disconnect(self):
        self.disconnect_calls += 1
        self.conn = None


def _primary(db_type="MariaDB", params=None):
    m = FakeManager(db_type)
    m._last_connect_params = params or {"host": "h", "username": "u", "password": "p"}
    m.conn = object()
    return m


def _make_pane(connections):
    pane = SQLEditorPane.__new__(SQLEditorPane)
    pane.get_connections_callback = lambda: connections
    pane.status_callback = lambda *a, **k: None
    pane.selected_connection_name = None
    pane._own_session = None
    pane._own_session_name = None
    pane._session_lock = threading.Lock()
    return pane


@pytest.fixture(autouse=True)
def _patch_manager(monkeypatch):
    FakeManager.instances = []
    monkeypatch.setattr(db_manager_mod, "DatabaseManager", FakeManager)
    yield


def test_tab_opens_its_own_session_lazily():
    primary = _primary()
    pane = _make_pane({"c1": primary})
    pane.selected_connection_name = "c1"

    session = pane.get_current_db_manager()
    # A new session was cloned from the primary's connect params, not the primary.
    assert session is not primary
    assert session.connect_calls == 1
    assert session._last_connect_params == primary._last_connect_params
    # The primary itself was not re-connected.
    assert primary.connect_calls == 0


def test_same_tab_reuses_its_session():
    pane = _make_pane({"c1": _primary()})
    pane.selected_connection_name = "c1"
    s1 = pane.get_current_db_manager()
    s2 = pane.get_current_db_manager()
    assert s1 is s2
    assert s1.connect_calls == 1  # not reconnected on reuse


def test_switching_connection_closes_old_session_and_opens_new():
    primary1 = _primary("MariaDB")
    primary2 = _primary("PostgreSQL")
    pane = _make_pane({"c1": primary1, "c2": primary2})

    pane.selected_connection_name = "c1"
    s1 = pane.get_current_db_manager()

    pane.selected_connection_name = "c2"
    s2 = pane.get_current_db_manager()

    assert s1 is not s2
    assert s1.disconnect_calls == 1          # old session closed
    assert s2.db_type == "PostgreSQL"
    assert s2.conn is not None


def test_dispose_closes_session():
    pane = _make_pane({"c1": _primary()})
    pane.selected_connection_name = "c1"
    s = pane.get_current_db_manager()
    pane.dispose()
    assert s.disconnect_calls == 1
    assert pane._own_session is None


def test_fallback_to_primary_when_no_connect_params():
    primary = _primary()
    primary._last_connect_params = None  # cannot clone
    pane = _make_pane({"c1": primary})
    pane.selected_connection_name = "c1"
    session = pane.get_current_db_manager()
    assert session is primary  # graceful fallback


def test_disappeared_connection_returns_none_and_releases():
    pane = _make_pane({"c1": _primary()})
    pane.selected_connection_name = "c1"
    s = pane.get_current_db_manager()
    # Connection removed from the active dict.
    pane.get_connections_callback = lambda: {}
    assert pane.get_current_db_manager() is None
    assert s.disconnect_calls == 1
    assert pane._own_session is None


def test_two_tabs_same_connection_get_independent_sessions():
    primary = _primary()
    conns = {"c1": primary}
    tab_a = _make_pane(conns)
    tab_b = _make_pane(conns)
    tab_a.selected_connection_name = "c1"
    tab_b.selected_connection_name = "c1"
    sa = tab_a.get_current_db_manager()
    sb = tab_b.get_current_db_manager()
    assert sa is not sb  # sibling tabs are isolated
    assert sa is not primary and sb is not primary
