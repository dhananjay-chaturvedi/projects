# ---------------------------------------------------------------------
# description: Monitoring UI — launcher and re-exports for the module
# The main implementation lives in ``monitoring.server_monitor.server_monitor_ui``.
# ---------------------------------------------------------------------

# Allow ``python monitoring/monitoring_ui.py`` from project root.
if __name__ == "__main__":
    import sys
    from pathlib import Path

    _root = Path(__file__).resolve().parents[1]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from common.ui.tk.monitor.server_monitor import ServerMonitorUI


def launch_ui(**_context) -> None:
    """Canonical desktop UI entry for Monitoring (``--ui`` and direct script)."""
    from common.ui.tk.launcher import launch_desktop_ui

    launch_desktop_ui(feature_module="monitor")


__all__ = ["ServerMonitorUI", "launch_ui"]

if __name__ == "__main__":
    launch_ui()
