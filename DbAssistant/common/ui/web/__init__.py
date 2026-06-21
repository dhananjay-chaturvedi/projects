"""
Standalone Web UI package (optional — delete this folder without breaking
CLI/API/other UIs).

This package ships its own HTML/CSS/JS single-page app **and** its own server.
The server reads the in-process core service (``CoreDBService`` / a module
composite) directly and registers its own routes via
``common.headless.core_routes``. It never imports the public REST API
(``common.headless.app_factory``):

* Deleting the public API leaves this Web UI fully working.
* Deleting this folder leaves the REST API, CLI and the other UIs intact.

The SPA reads shared UI properties from ``common.ui.shared`` (title, tab order,
labels, colour theme) via the server's ``/ui/config`` endpoint so it mirrors
the Tk desktop UI.
"""

from common.ui.web.server import build_web_app, launch_web_ui
from common.ui.web.mount import mount_web_ui

__all__ = ["build_web_app", "launch_web_ui", "mount_web_ui"]
