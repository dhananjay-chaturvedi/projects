"""App Builder meters — deterministic evaluators for the build lifecycle.

These meters live *inside* the app_builder package (separate from the generic
``ai_assistant.meters`` battery) because they are specific to the triple-session
build process: they score design understanding, design similarity between the
A/B/C sessions, component/feature completeness, and the industry-standard code
quality dimensions used to judge a generated application.

Every meter is fully deterministic (parsing + set/maths, never a model call)
and returns the shared :class:`ai_assistant.meters.base.Measurement` object so
the App Builder Assistant can compose, gate and track them uniformly.

The :class:`~ai_assistant.app_builder.meters.registry.AppMeterRegistry` is the
extensibility seam: register a new ``Meter`` subclass there (or via
``register``) and it is automatically available to the assistant and its
meter-managers.
"""

from __future__ import annotations

from ai_assistant.meters.base import Measurement, Meter

from ai_assistant.app_builder.meters.intent_classifier import (
    IntentClassifier,
    MessageIntent,
    classify_intent,
)
from ai_assistant.app_builder.meters.design_plan import (
    DesignPlan,
    extract_plan,
)
from ai_assistant.app_builder.meters.design_similarity_meter import (
    DesignSimilarityMeter,
)
from ai_assistant.app_builder.meters.design_completeness_meter import (
    DesignCompletenessMeter,
)
from ai_assistant.app_builder.meters.feature_completeness_meter import (
    FeatureCompletenessMeter,
)
from ai_assistant.app_builder.meters.business_intent_meter import (
    BusinessIntentMeter,
)
from ai_assistant.app_builder.meters.schema_design_meter import SchemaDesignMeter
from ai_assistant.app_builder.meters.backend_logic_meter import BackendLogicMeter
from ai_assistant.app_builder.meters.code_hygiene_meter import CodeHygieneMeter
from ai_assistant.app_builder.meters.architecture_meter import ArchitectureMeter
from ai_assistant.app_builder.meters.cli_interface_meter import CliInterfaceMeter
from ai_assistant.app_builder.meters.solid_principles_meter import (
    SolidPrinciplesMeter,
)
from ai_assistant.app_builder.meters.functional_correctness_meter import (
    FunctionalCorrectnessMeter,
)
from ai_assistant.app_builder.meters.registry import AppMeterRegistry

__all__ = [
    "Measurement",
    "Meter",
    "IntentClassifier",
    "MessageIntent",
    "classify_intent",
    "DesignPlan",
    "extract_plan",
    "DesignSimilarityMeter",
    "DesignCompletenessMeter",
    "FeatureCompletenessMeter",
    "BusinessIntentMeter",
    "SchemaDesignMeter",
    "BackendLogicMeter",
    "CodeHygieneMeter",
    "ArchitectureMeter",
    "CliInterfaceMeter",
    "SolidPrinciplesMeter",
    "FunctionalCorrectnessMeter",
    "AppMeterRegistry",
]
