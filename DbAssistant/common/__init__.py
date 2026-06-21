"""
Shared infrastructure for all DbManagementTool module builds.

Ship ``common/`` plus exactly one module package (e.g. ``schema_converter/``)
for a single-module distribution. Contains no UI, master CLI, or module-specific
logic — only database connectivity, registry, config, and core headless service.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

__all__ = ["ROOT_DIR"]
