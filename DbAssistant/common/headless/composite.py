"""Combine core + module services for CLI/API routers (no duplication)."""

from __future__ import annotations

from typing import Any


class CompositeService:
    """Lookup methods on module services first, then on the core service."""

    __slots__ = ("_core", "_modules")

    def __init__(self, core: Any, *modules: Any):
        self._core = core
        self._modules = modules

    def __getattr__(self, name: str):
        for obj in (*self._modules, self._core):
            try:
                return getattr(obj, name)
            except AttributeError:
                continue
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")


def composite_service(core: Any, *modules: Any) -> CompositeService:
    return CompositeService(core, *modules)
