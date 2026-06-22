"""Proxy to common.ui.tk.monitor.server_monitor.server_monitor_ui (preserves patch targets)."""

from __future__ import annotations

import importlib
import sys

_impl = importlib.import_module("common.ui.tk.monitor.server_monitor.server_monitor_ui")
sys.modules[__name__] = _impl
