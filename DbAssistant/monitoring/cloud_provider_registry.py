"""
cloud_provider_registry.py
===========================
Central registry for cloud provider specs.

Usage — build a monitor for a saved connection entry
-----------------------------------------------------
    from cloud_provider_registry import CloudProviderRegistry

    monitor, error = CloudProviderRegistry.build_monitor(entry, sso_callback=my_cb)

Usage — fetch metrics from a live monitor
-----------------------------------------
    text, graph_data, alerts = CloudProviderRegistry.fetch_metrics(
        display_name, entry, monitor, threshold_checker
    )

Adding a new cloud provider
---------------------------
1.  Create ``cloud_providers/<name>_provider.py`` with a module-level SPEC.
2.  Add an import line in the ``_bootstrap()`` function below.
3.  That is the only required change — no edits to server_monitor_ui.py needed.
"""

from __future__ import annotations

from typing import Optional

from .cloud_monitor_base import CloudProviderSpec, DiscoveryResult


class CloudProviderRegistry:
    """
    Class-level registry mapping provider name → CloudProviderSpec.

    Thread-safety: the registry is populated once at import time
    (_bootstrap) and then only read.  No locking needed.
    """

    _registry: dict[str, CloudProviderSpec] = {}

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    @classmethod
    def register(cls, spec: CloudProviderSpec) -> None:
        """Register a provider spec.  Overwrites any existing entry silently."""
        cls._registry[spec.name] = spec

    @classmethod
    def get(cls, provider: str) -> Optional[CloudProviderSpec]:
        """Return the spec for *provider*, or None if unknown."""
        if provider in cls._registry:
            return cls._registry[provider]
        provider_l = (provider or "").strip().lower()
        for name, spec in cls._registry.items():
            if name.lower() == provider_l:
                return spec
        return None

    @classmethod
    def all_providers(cls) -> list[str]:
        """Return a sorted list of registered provider names."""
        return sorted(cls._registry.keys())

    # ------------------------------------------------------------------
    # Convenience delegators
    # ------------------------------------------------------------------

    @classmethod
    def build_monitor(cls, entry: dict, sso_callback=None):
        """
        Delegate to the registered provider's build_monitor function.
        Returns (monitor, None) on success or (None, error_string) on failure.
        """
        provider = entry.get("provider", "")
        spec = cls.get(provider)
        if spec is None:
            return None, f"No monitor implementation for provider '{provider}'."
        try:
            return spec.build_monitor(entry, sso_callback=sso_callback)
        except Exception as exc:
            return None, f"{provider} monitor initialisation failed: {exc}"

    @classmethod
    def fetch_metrics(cls, display_name: str, entry: dict, monitor, threshold_checker=None):
        """
        Delegate to the registered provider's fetch_metrics function.
        Returns (text_block, graph_data, alerts).
        """
        provider = entry.get("provider", "")
        spec = cls.get(provider)
        if spec is None:
            return f"  Unknown provider '{provider}'.", {}, []
        try:
            return spec.fetch_metrics(display_name, entry, monitor, threshold_checker)
        except Exception as exc:
            return [("Error", [("Status", f"{provider} metrics failed: {exc}")])], {}, []

    @classmethod
    def login(cls, entry: dict):
        """
        Run the provider's interactive login flow (e.g. `aws login`,
        `az login`, `gcloud auth login`). Returns (ok, message).
        """
        provider = entry.get("provider", "")
        spec = cls.get(provider)
        if spec is None:
            return False, f"No implementation for provider '{provider}'."
        if not spec.login:
            return False, f"Provider '{provider}' does not support interactive login."
        try:
            return spec.login(entry)
        except Exception as exc:
            return False, f"{provider} login failed: {exc}"

    @classmethod
    def discover(cls, entry: dict, target_kind: str, sso_callback=None) -> DiscoveryResult:
        """
        Delegate to the registered provider's discover function.
        Returns a DiscoveryResult; sets ``error`` when the provider has no
        discover implementation or discovery raises.
        """
        provider = entry.get("provider", "")
        spec = cls.get(provider)
        if spec is None:
            return DiscoveryResult(
                error=f"No implementation for provider '{provider}'."
            )
        if not spec.discover:
            return DiscoveryResult(
                error=f"Provider '{provider}' does not support auto-discovery."
            )
        try:
            return spec.discover(entry, target_kind, sso_callback=sso_callback)
        except Exception as exc:
            return DiscoveryResult(error=f"{provider} discovery failed: {exc}")

    @classmethod
    def refresh_monitor(cls, entry: dict, monitor, sso_callback=None):
        """
        Refresh or validate a live monitor. Providers that support in-place
        credential refresh expose ``refresh_monitor``; older providers fall
        back to rebuilding the monitor object.

        Returns (monitor, None) on success or (monitor_or_none, error_string)
        on failure.
        """
        provider = entry.get("provider", "")
        spec = cls.get(provider)
        if spec is None:
            return None, f"No monitor implementation for provider '{provider}'."
        if spec.refresh_monitor:
            try:
                return spec.refresh_monitor(entry, monitor, sso_callback=sso_callback)
            except Exception as exc:
                return monitor, f"{provider} monitor refresh failed: {exc}"
        try:
            return spec.build_monitor(entry, sso_callback=sso_callback)
        except Exception as exc:
            return None, f"{provider} monitor rebuild failed: {exc}"


# ---------------------------------------------------------------------------
# Bootstrap — import all built-in provider modules so they self-register.
# To add a new provider: add one import line here and create the module.
# ---------------------------------------------------------------------------

def _bootstrap() -> None:
    from monitoring.cloud_providers import aws_provider     # noqa: F401
    from monitoring.cloud_providers import azure_provider   # noqa: F401
    from monitoring.cloud_providers import gcp_provider     # noqa: F401

    # Attach optional headless login callables when a provider exposes one.
    for _mod in (aws_provider, azure_provider, gcp_provider):
        _login = getattr(_mod, "cli_login", None)
        if _login is not None and getattr(_mod.SPEC, "login", None) is None:
            _mod.SPEC.login = _login

    CloudProviderRegistry.register(aws_provider.SPEC)
    CloudProviderRegistry.register(azure_provider.SPEC)
    CloudProviderRegistry.register(gcp_provider.SPEC)


_bootstrap()
