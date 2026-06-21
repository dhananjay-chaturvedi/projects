"""stop.py PID termination tests."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

import monitoring.stop as stop


def test_pid_file_path():
    p = stop.pid_file("daemon")
    assert "daemon" in str(p)


def test_stop_unknown_pid_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    # Should not raise when pid file missing
    stop.stop("nonexistent_daemon_name_xyz")
