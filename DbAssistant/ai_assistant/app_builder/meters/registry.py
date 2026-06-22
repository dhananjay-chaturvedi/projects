"""registry — the extensible catalogue of App Builder meters.

The :class:`AppMeterRegistry` is the single seam through which the App Builder
Assistant reaches every meter. New meters are added by registering a
:class:`~ai_assistant.meters.base.Meter` instance under a name; they then show
up in :meth:`all` and can be driven by a matching meter-manager.

The registry also offers convenience ``evaluate_*`` helpers that run the right
meter with the right arguments and return plain dicts (``Measurement.as_dict``)
so results are easy to serialize into progress events and the UI.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ai_assistant.meters.base import Measurement, Meter

from ai_assistant.app_builder.meters.design_plan import DesignPlan
from ai_assistant.app_builder.meters.design_similarity_meter import (
    DesignSimilarityMeter,
)
from ai_assistant.app_builder.meters.design_completeness_meter import (
    DesignCompletenessMeter,
)
from ai_assistant.app_builder.meters.feature_completeness_meter import (
    FeatureCompletenessMeter,
)
from ai_assistant.app_builder.meters.business_intent_meter import BusinessIntentMeter
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
from ai_assistant.app_builder.meters.metadata_completeness_meter import (
    MetadataCompletenessMeter,
)
from ai_assistant.app_builder.meters.archetype_fit_meter import ArchetypeFitMeter
from ai_assistant.app_builder.meters.schema_fidelity_meter import SchemaFidelityMeter
from ai_assistant.app_builder.meters.architecture_recovery_meter import (
    ArchitectureRecoveryMeter,
)
from ai_assistant.app_builder.meters.component_coverage_meter import (
    ComponentCoverageMeter,
)
from ai_assistant.app_builder.meters.db_semantic_meters import (
    DataSemanticsMeter,
    EntityRoleFitMeter,
    PredictionGroundingMeter,
    RelationshipFidelityMeter,
    WorkflowCoverageMeter,
)


@dataclass(frozen=True)
class QualityInput:
    """Inputs for the generic App Builder quality battery."""

    files: Mapping[str, str]
    description: str = ""
    features: Iterable[str] = field(default_factory=tuple)
    entities: Iterable[str] = field(default_factory=tuple)
    test_outcome: Mapping[str, Any] | None = None
    thresholds: Mapping[str, float] | None = None

    @classmethod
    def from_source(
        cls,
        source: "QualityInput | Mapping[str, str]",
        **legacy_fields,
    ) -> "QualityInput":
        """Coerce an existing input object or the legacy files+kwargs shape."""
        if isinstance(source, cls):
            return source
        return cls(
            files=source,
            description=legacy_fields.get("description", "") or "",
            features=legacy_fields.get("features", ()),
            entities=legacy_fields.get("entities", ()),
            test_outcome=legacy_fields.get("test_outcome"),
            thresholds=legacy_fields.get("thresholds"),
        )


class AppMeterRegistry:
    """Central, extensible registry of App Builder meters."""

    def __init__(self) -> None:
        self._meters: dict[str, Meter] = {}
        self._register_defaults()

    def _register_defaults(self) -> None:
        self.register("design_similarity", DesignSimilarityMeter())
        self.register("design_completeness", DesignCompletenessMeter())
        self.register("feature_completeness", FeatureCompletenessMeter())
        self.register("business_intent", BusinessIntentMeter())
        self.register("schema_design", SchemaDesignMeter())
        self.register("backend_logic", BackendLogicMeter())
        self.register("code_hygiene", CodeHygieneMeter())
        self.register("architecture", ArchitectureMeter())
        self.register("cli_interface", CliInterfaceMeter())
        self.register("solid_principles", SolidPrinciplesMeter())
        self.register("functional_correctness", FunctionalCorrectnessMeter())
        self.register("metadata_completeness", MetadataCompletenessMeter())
        self.register("archetype_fit", ArchetypeFitMeter())
        self.register("schema_fidelity", SchemaFidelityMeter())
        self.register("architecture_recovery", ArchitectureRecoveryMeter())
        self.register("component_coverage", ComponentCoverageMeter())
        self.register("relationship_fidelity", RelationshipFidelityMeter())
        self.register("entity_role_fit", EntityRoleFitMeter())
        self.register("data_semantics", DataSemanticsMeter())
        self.register("workflow_coverage", WorkflowCoverageMeter())
        self.register("prediction_grounding", PredictionGroundingMeter())

    # ── extensibility ────────────────────────────────────────────────────────
    def register(self, name: str, meter: Meter) -> None:
        """Add or replace a meter under *name* (future meters plug in here)."""
        self._meters[name] = meter

    def get(self, name: str) -> Meter | None:
        return self._meters.get(name)

    def names(self) -> list[str]:
        return list(self._meters)

    # ── design / understanding phase ──────────────────────────────────────────
    def evaluate_design_similarity(
        self, plans: Iterable[DesignPlan], *, threshold: float = 0.8,
    ) -> dict[str, Any]:
        m = self._meters["design_similarity"]
        return m.measure(list(plans), threshold=threshold).as_dict()

    def evaluate_design_completeness(
        self, design: DesignPlan, files: Mapping[str, str], *,
        threshold: float = 0.8,
    ) -> dict[str, Any]:
        m = self._meters["design_completeness"]
        return m.measure(design, files, threshold=threshold).as_dict()

    # ── build-quality battery ──────────────────────────────────────────────────
    def quality_measurements(
        self,
        quality: QualityInput | Mapping[str, str],
        **legacy_fields,
    ) -> dict[str, Measurement]:
        """Run the full quality battery, returning the raw Measurement objects."""
        quality = QualityInput.from_source(quality, **legacy_fields)
        files = quality.files
        description = quality.description
        features = quality.features
        entities = quality.entities
        test_outcome = quality.test_outcome
        thresholds = dict(quality.thresholds or {})
        results: dict[str, Measurement] = {}

        results["business_intent"] = self._meters["business_intent"].measure(
            description=description, files=files, entities=entities,
            threshold=thresholds.get("business_intent"))
        results["feature_completeness"] = self._meters[
            "feature_completeness"].measure(
            features=features, files=files, description=description,
            threshold=thresholds.get("feature_completeness"))
        results["schema_design"] = self._meters["schema_design"].measure(
            files, threshold=thresholds.get("schema_design"))
        results["backend_logic"] = self._meters["backend_logic"].measure(
            files, threshold=thresholds.get("backend_logic"))
        results["code_hygiene"] = self._meters["code_hygiene"].measure(
            files, threshold=thresholds.get("code_hygiene"))
        results["architecture"] = self._meters["architecture"].measure(
            files, threshold=thresholds.get("architecture"))
        results["cli_interface"] = self._meters["cli_interface"].measure(
            files, description=description, features=features,
            threshold=thresholds.get("cli_interface"))
        results["solid_principles"] = self._meters["solid_principles"].measure(
            files, threshold=thresholds.get("solid_principles"))
        results["functional_correctness"] = self._meters[
            "functional_correctness"].measure(
            test_outcome, threshold=thresholds.get("functional_correctness"))
        return results

    # Aggregate weighting — toward functional correctness + spec fidelity.
    # CLI/SOLID carry weight too, but are skipped from the aggregate when a
    # measurement reports ``applicable=False`` (see report_from_measurements),
    # so web-only or script-style apps are never penalized for them.
    _AGG_WEIGHTS = {
        "functional_correctness": 3.0,
        "feature_completeness": 2.5,
        "business_intent": 2.5,
        "backend_logic": 2.0,
        "schema_design": 1.5,
        "architecture": 1.5,
        "solid_principles": 1.5,
        "cli_interface": 1.5,
        "code_hygiene": 1.0,
        "metadata_completeness": 1.5,
        "archetype_fit": 2.0,
        "schema_fidelity": 2.0,
        "architecture_recovery": 2.0,
        "component_coverage": 2.0,
        "relationship_fidelity": 2.5,
        "entity_role_fit": 2.5,
        "data_semantics": 1.5,
        "workflow_coverage": 2.0,
        "prediction_grounding": 2.0,
    }

    @staticmethod
    def _applicable(m: Measurement) -> bool:
        """A meter opts out of scoring by setting ``evidence.applicable=False``."""
        return bool((m.evidence or {}).get("applicable", True))

    @classmethod
    def report_from_measurements(
        cls, results: Mapping[str, Measurement],
    ) -> dict[str, Any]:
        """Build the per-meter + aggregate readout from computed measurements.

        Meters that report ``applicable=False`` are excluded from both the
        aggregate and the failing list so optional dimensions (CLI for a web
        app, SOLID for a script) never drag the score around.
        """
        num = sum(results[k].score * w for k, w in cls._AGG_WEIGHTS.items()
                  if k in results and cls._applicable(results[k]))
        den = sum(w for k, w in cls._AGG_WEIGHTS.items()
                  if k in results and cls._applicable(results[k]))
        overall = num / den if den else 0.0
        failing = [k for k, m in results.items()
                   if cls._applicable(m) and not m.passed]
        return {
            "overall": round(overall, 4),
            "meters": {k: m.as_dict() for k, m in results.items()},
            "failing": failing,
            "passed": not failing,
        }

    def evaluate_db_build(
        self,
        files: Mapping[str, str],
        *,
        profile: Mapping[str, Any] | None = None,
        schema: Mapping[str, Any] | None = None,
        archetype: str = "",
        insight: Mapping[str, Any] | None = None,
        thresholds: Mapping[str, float] | None = None,
    ) -> dict[str, Any]:
        thresholds = dict(thresholds or {})
        results: dict[str, Measurement] = {}
        if profile:
            results["metadata_completeness"] = self._meters[
                "metadata_completeness"].measure(profile)
        results["archetype_fit"] = self._meters["archetype_fit"].measure(
            files, archetype=archetype,
            threshold=thresholds.get("archetype_fit"))
        results["schema_fidelity"] = self._meters["schema_fidelity"].measure(
            files, schema=schema, threshold=thresholds.get("schema_fidelity"))
        results["relationship_fidelity"] = self._meters[
            "relationship_fidelity"].measure(
            files, profile=profile,
            threshold=thresholds.get("relationship_fidelity"))
        results["entity_role_fit"] = self._meters["entity_role_fit"].measure(
            files, profile=profile, threshold=thresholds.get("entity_role_fit"))
        results["data_semantics"] = self._meters["data_semantics"].measure(
            files, profile=profile, threshold=thresholds.get("data_semantics"))
        results["workflow_coverage"] = self._meters["workflow_coverage"].measure(
            files, insight=insight, threshold=thresholds.get("workflow_coverage"))
        results["prediction_grounding"] = self._meters[
            "prediction_grounding"].measure(
            profile=profile, insight=insight,
            threshold=thresholds.get("prediction_grounding"))
        return self.report_from_measurements(results)

    def evaluate_codebase_build(
        self,
        files: Mapping[str, str],
        *,
        profile: Mapping[str, Any] | None = None,
        components: Iterable[str] = (),
        thresholds: Mapping[str, float] | None = None,
    ) -> dict[str, Any]:
        thresholds = dict(thresholds or {})
        results: dict[str, Measurement] = {}
        results["architecture_recovery"] = self._meters[
            "architecture_recovery"].measure(
            files, profile=profile,
            threshold=thresholds.get("architecture_recovery"))
        results["component_coverage"] = self._meters["component_coverage"].measure(
            files, components=components, profile=profile,
            threshold=thresholds.get("component_coverage"))
        # Codebase mode can produce either a predicted app or a structure/
        # metadata artifact. Full-app generic meters such as schema/backend/test
        # correctness are not always applicable to the latter, so reuse only the
        # generic hygiene signal here and let the codebase-specific meters carry
        # the recovery/coverage gate.
        results["code_hygiene"] = self._meters["code_hygiene"].measure(
            files, threshold=thresholds.get("code_hygiene", 0.5))
        return self.report_from_measurements(results)

    def evaluate_quality(
        self,
        quality: QualityInput | Mapping[str, str],
        **legacy_fields,
    ) -> dict[str, Any]:
        """Run the full quality battery and return per-meter + aggregate scores."""
        results = self.quality_measurements(
            QualityInput.from_source(quality, **legacy_fields))
        return self.report_from_measurements(results)
