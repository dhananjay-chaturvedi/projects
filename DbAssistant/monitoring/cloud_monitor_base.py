"""
cloud_monitor_base.py
=====================
Single source-of-truth for the CloudDBMonitor abstract base class and the
CloudProviderSpec dataclass used by CloudProviderRegistry.

All cloud monitor modules (monitor_aws, monitor_azure, monitor_gcp, and any
future provider) import CloudDBMonitor from here instead of defining their own
copy.  This guarantees a single runtime contract so isinstance() checks work
correctly across providers.

Adding a new cloud provider
---------------------------
1. Create cloud_providers/<provider>_provider.py
2. Define a module-level SPEC = CloudProviderSpec(...)
3. Call CloudProviderRegistry.register(SPEC) at import time
4. Import the module in cloud_provider_registry.py

No changes to server_monitor_ui.py are needed.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Discovery result — returned by provider discover() callables
# ---------------------------------------------------------------------------

@dataclass
class DiscoveryResult:
    """
    Result of a provider's environment-credential discovery pass.

    *regions*     — region/zone codes the identity can access.
    *accounts*    — profiles / subscriptions / projects (each a dict with
                    ``id``, ``label``, and optional extra keys).
    *resources*   — discoverable resources; each item has ``label`` (display)
                    and ``fields`` (dict of form keys to apply on selection).
    *detected*    — suggested defaults, e.g. ``{"region": "us-east-1"}``.
    *warnings*    — non-fatal issues (partial lists, missing SDKs, etc.).
    *error*       — fatal error message; when set, discovery failed entirely.
    """

    regions: list[str] = field(default_factory=list)
    accounts: list[dict[str, Any]] = field(default_factory=list)
    resources: list[dict[str, Any]] = field(default_factory=list)
    detected: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    error: str | None = None


# ---------------------------------------------------------------------------
# Abstract base class — one definition shared by all providers
# ---------------------------------------------------------------------------

class CloudDBMonitor(ABC):
    """
    Minimal contract every cloud monitor must implement.

    check_health() is called on every metrics poll cycle.  Return an empty
    list when everything is healthy; return one or more human-readable error
    strings when something is wrong.
    """

    @abstractmethod
    def check_health(self) -> list[str]:
        """Return [] on success, or a list of error strings on failure."""


# ---------------------------------------------------------------------------
# Provider specification — describes a cloud provider to the registry
# ---------------------------------------------------------------------------

@dataclass
class CloudProviderSpec:
    """
    Declarative descriptor for a cloud provider.

    Fields
    ------
    name : str
        Short identifier used as the ``provider`` key in connection entries,
        e.g. ``"AWS"``, ``"Azure"``, ``"GCP"``.
    display_name : str
        Human-readable label shown in the UI, e.g. ``"Amazon Web Services"``.
    auth_modes : list[str]
        Supported authentication mode identifiers, e.g. ``["keys"]``.
    build_monitor : Callable
        ``(entry: dict, sso_callback=None) -> tuple[CloudDBMonitor | None, str | None]``

        Build and return a live monitor object from a connection entry dict.
        On success return ``(monitor, None)``.
        On failure return ``(None, error_message_string)``.

        *sso_callback* is an optional callable supplied by the UI for providers
        that need interactive (browser) authentication:
        ``sso_callback(az_cmd: list[str]) -> subprocess.CompletedProcess``

    fetch_metrics : Callable
        ``(display_name: str, entry: dict, monitor: CloudDBMonitor,
           threshold_checker) -> tuple[str, dict[str, float]]``

        Fetch current metrics from a live monitor.  Return a 2-tuple of
        (text_block, graph_data) where *graph_data* maps graph-key strings to
        float values for the MetricsVisualizer.
    """

    name: str
    display_name: str
    build_monitor: Callable
    fetch_metrics: Callable
    auth_modes: list[str] = field(default_factory=list)
    refresh_monitor: Optional[Callable] = None
    # Optional headless interactive login (e.g. `aws login`, `az login`,
    # `gcloud auth ... login`). Signature: ``(entry: dict) -> tuple[bool, str]``
    # returning (ok, message).
    login: Optional[Callable] = None
    # Optional environment-credential discovery.
    # Signature: ``(entry: dict, target_kind: str, sso_callback=None)
    #             -> DiscoveryResult``
    discover: Optional[Callable] = None
