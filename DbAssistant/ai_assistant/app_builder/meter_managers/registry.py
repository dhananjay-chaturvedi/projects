"""registry — the extensible catalogue of meter-managers.

Maps a meter's canonical name (``Measurement.meter``) to the
:class:`~ai_assistant.app_builder.meter_managers.base.MeterManager` that governs
it, and turns a set of measurements into prioritized remediation signals for
the advisor. New managers register here just like meters do.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ai_assistant.meters.base import Measurement

from ai_assistant.app_builder.meter_managers.base import (
    MeterManager,
    RemediationSignal,
    Severity,
)
from ai_assistant.app_builder.meter_managers.managers import (
    ArchitectureManager,
    ArchitectureRecoveryManager,
    ArchetypeFitManager,
    BackendLogicManager,
    BusinessIntentManager,
    CliInterfaceManager,
    CodeHygieneManager,
    ComponentCoverageManager,
    DesignCompletenessManager,
    FeatureCompletenessManager,
    FunctionalCorrectnessManager,
    MetadataCompletenessManager,
    SchemaDesignManager,
    SchemaFidelityManager,
    SolidPrinciplesManager,
)

# Severity ranking for prioritization (worst first).
_SEVERITY_RANK = {Severity.FAIL: 0, Severity.WARNING: 1, Severity.OK: 2}


class MeterManagerRegistry:
    """Central, extensible registry of meter-managers."""

    def __init__(self) -> None:
        self._managers: dict[str, MeterManager] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        for mgr in (
            FunctionalCorrectnessManager(),
            FeatureCompletenessManager(),
            BusinessIntentManager(),
            SchemaDesignManager(),
            SchemaFidelityManager(),
            BackendLogicManager(),
            CodeHygieneManager(),
            ArchitectureManager(),
            ArchitectureRecoveryManager(),
            ArchetypeFitManager(),
            ComponentCoverageManager(),
            CliInterfaceManager(),
            MetadataCompletenessManager(),
            SolidPrinciplesManager(),
            DesignCompletenessManager(),
        ):
            self.register(mgr)

    def register(self, manager: MeterManager) -> None:
        """Add or replace the manager for its meter (future managers plug in)."""
        self._managers[manager.meter_name] = manager

    def get(self, meter_name: str) -> MeterManager | None:
        return self._managers.get(meter_name)

    def names(self) -> list[str]:
        return list(self._managers)

    def manage(
        self, measurement: Measurement, *, component: str = "",
    ) -> RemediationSignal | None:
        """Run the manager governing *measurement* (default manager if none)."""
        mgr = self._managers.get(measurement.meter)
        if mgr is None:
            mgr = MeterManager()
            mgr.meter_name = measurement.meter
        return mgr.manage(measurement, component=component)

    def manage_all(
        self,
        measurements: Iterable[Measurement],
        *,
        component: str = "",
    ) -> list[RemediationSignal]:
        """Return remediation signals for every non-OK measurement, worst first.

        Sorted by severity then by deficit so the advisor (and ultimately
        Session A) tackles the biggest gaps first.
        """
        signals: list[RemediationSignal] = []
        for m in measurements:
            sig = self.manage(m, component=component)
            if sig is not None:
                signals.append(sig)
        signals.sort(key=lambda s: (_SEVERITY_RANK.get(s.severity, 3),
                                    -s.deficit))
        return signals

    def summarize(self, signals: Iterable[RemediationSignal]) -> dict[str, Any]:
        """Compact rollup of a signal list for progress events."""
        sl = list(signals)
        return {
            "count": len(sl),
            "failing": [s.meter for s in sl if s.severity is Severity.FAIL],
            "warning": [s.meter for s in sl if s.severity is Severity.WARNING],
            "signals": [s.as_dict() for s in sl],
        }
