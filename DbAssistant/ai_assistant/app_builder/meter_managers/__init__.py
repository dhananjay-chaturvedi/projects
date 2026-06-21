"""App Builder meter-managers.

A *meter* measures one dimension of the build and returns a
:class:`~ai_assistant.meters.base.Measurement`. A *meter-manager* is the policy
layer on top of a meter: when a meter is failing, warning, or otherwise not
meeting its acceptance threshold, the manager works out **how far behind** the
app/component is and **why**, then emits a :class:`RemediationSignal` describing
what is missing.

The App Builder Assistant collects these signals and routes them to Session B
(the advisor). B decides whether the gap genuinely needs handling or is already
covered, and — if needed — turns the signal into one concrete instruction for
Session A. Managers never talk to a model and never instruct A directly; they
only produce structured, factual remediation signals.

New managers plug in through :class:`MeterManagerRegistry` exactly like meters,
so the system stays extensible.
"""

from __future__ import annotations

from ai_assistant.app_builder.meter_managers.base import (
    MeterManager,
    RemediationSignal,
    Severity,
)
from ai_assistant.app_builder.meter_managers.managers import (
    ArchitectureManager,
    BackendLogicManager,
    BusinessIntentManager,
    CliInterfaceManager,
    CodeHygieneManager,
    DesignCompletenessManager,
    FeatureCompletenessManager,
    FunctionalCorrectnessManager,
    SchemaDesignManager,
    SolidPrinciplesManager,
)
from ai_assistant.app_builder.meter_managers.registry import MeterManagerRegistry

__all__ = [
    "MeterManager",
    "RemediationSignal",
    "Severity",
    "MeterManagerRegistry",
    "ArchitectureManager",
    "BackendLogicManager",
    "BusinessIntentManager",
    "CliInterfaceManager",
    "CodeHygieneManager",
    "DesignCompletenessManager",
    "FeatureCompletenessManager",
    "FunctionalCorrectnessManager",
    "SchemaDesignManager",
    "SolidPrinciplesManager",
]
