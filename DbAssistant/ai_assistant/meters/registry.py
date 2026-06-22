"""MeterSuite — single entry point that owns every meter instance.

Provides convenience aggregate evaluations for the two main contexts:
* response quality (AI Query answers / training samples), and
* artifact quality (code + build produced by the app builder).
"""

from __future__ import annotations

from typing import Any

from ai_assistant.meters.accuracy_meter import AccuracyMeter
from ai_assistant.meters.app_quality_meter import AppQualityMeter
from ai_assistant.meters.base import weighted_score
from ai_assistant.meters.build_accuracy_meter import BuildAccuracyMeter
from ai_assistant.meters.build_design_accuracy_meter import BuildDesignAccuracyMeter
from ai_assistant.meters.code_accuracy_meter import CodeAccuracyMeter
from ai_assistant.meters.code_adaptability_meter import CodeAdaptabilityMeter
from ai_assistant.meters.code_design_management_system import CodeDesignManagementSystem
from ai_assistant.meters.code_efficiency_management_system import (
    CodeEfficiencyManagementSystem,
)
from ai_assistant.meters.data_understanding_meter import DataUnderstandingMeter
from ai_assistant.meters.error_meter import ErrorMeter
from ai_assistant.meters.input_reliability_management_system import (
    InputReliabilityManagementSystem,
)
from ai_assistant.meters.output_accuracy_management_system import (
    OutputAccuracyManagementSystem,
)
from ai_assistant.meters.process_adherence_meter import ProcessAdherenceMeter
from ai_assistant.meters.requirement_coverage_meter import RequirementCoverageMeter
from ai_assistant.meters.requirement_fidelity_meter import RequirementFidelityMeter
from ai_assistant.meters.understanding_meter import UnderstandingMeter


class MeterSuite:
    """Owns one instance of every meter / management system."""

    def __init__(self) -> None:
        self.accuracy = AccuracyMeter()
        self.error = ErrorMeter()
        self.understanding = UnderstandingMeter()
        self.output = OutputAccuracyManagementSystem()
        self.input = InputReliabilityManagementSystem()
        self.build_accuracy = BuildAccuracyMeter()
        self.build_design = BuildDesignAccuracyMeter()
        self.code_accuracy = CodeAccuracyMeter()
        self.code_adaptability = CodeAdaptabilityMeter()
        self.code_efficiency = CodeEfficiencyManagementSystem()
        self.code_design = CodeDesignManagementSystem()
        self.requirement_coverage = RequirementCoverageMeter()
        self.requirement_fidelity = RequirementFidelityMeter()
        self.data_understanding = DataUnderstandingMeter()
        self.process_adherence = ProcessAdherenceMeter()
        self.app_quality = AppQualityMeter()

    def evaluate_app_quality(
        self,
        files: dict[str, str],
        *,
        description: str = "",
        threshold: float = 0.7,
    ) -> dict[str, Any]:
        """Standard deterministic quality battery for a produced app workspace."""
        m = self.app_quality.measure(files, description=description)
        return {
            "score": round(m.score, 4),
            "accepted": m.score >= threshold,
            "checks": dict(m.evidence.get("checks", {})),
            "issues": list(m.issues),
            "measurement": m.as_dict(),
        }

    def evaluate_code_artifact(
        self, code: str, *, language: str = "python", **kwargs: Any
    ) -> dict[str, Any]:
        """Blend the four code meters into one artifact-quality verdict."""
        m = {
            self.code_accuracy.name: self.code_accuracy.measure(
                code, language=language,
                lint_issues=kwargs.get("lint_issues"), tests=kwargs.get("tests"),
            ),
            self.code_adaptability.name: self.code_adaptability.measure(code, language=language),
            self.code_efficiency.name: self.code_efficiency.measure(
                code, language=language, runtime=kwargs.get("runtime")
            ),
            self.code_design.name: self.code_design.measure(code, language=language),
        }
        weights = {
            self.code_accuracy.name: 3.0,
            self.code_adaptability.name: 1.5,
            self.code_efficiency.name: 1.5,
            self.code_design.name: 2.0,
        }
        score = weighted_score({k: v.score for k, v in m.items()}, weights)
        return {
            "score": round(score, 4),
            "accepted": score >= 0.75 and m[self.code_accuracy.name].passed,
            "measurements": {k: v.as_dict() for k, v in m.items()},
        }

    def evaluate_build(
        self,
        *,
        expected_files,
        produced_files,
        required_services=(),
        present_services=(),
        required_rules=None,
    ) -> dict[str, Any]:
        acc = self.build_accuracy.measure(
            expected_files=expected_files, produced_files=produced_files,
            required_services=required_services, present_services=present_services,
        )
        design = self.build_design.measure(
            produced_files=produced_files, required_rules=required_rules
        )
        score = weighted_score(
            {acc.meter: acc.score, design.meter: design.score},
            {acc.meter: 2.0, design.meter: 1.0},
        )
        return {
            "score": round(score, 4),
            "accepted": score >= 0.75,
            "measurements": {acc.meter: acc.as_dict(), design.meter: design.as_dict()},
        }

    def evaluate_requirements(
        self,
        *,
        entities,
        features,
        files,
        services=(),
        kind: str = "crud",
        threshold: float = 0.9,
    ) -> dict[str, Any]:
        """Requirement recall: are the requested app surfaces actually built?

        For a CRUD app that means each entity/feature has API + UI + tests; for a
        storefront it means the shopping surfaces (catalog/cart/checkout) exist.
        Returns a verdict whose ``accepted`` is the auto-build loop's *done*
        condition for completeness, plus the structured ``gaps`` the orchestrator
        feeds back to the AI so it knows exactly what is still missing.
        """
        m = self.requirement_coverage.measure(
            entities=entities, features=features, files=files,
            services=services, kind=kind,
        )
        return {
            "score": round(m.score, 4),
            "accepted": m.score >= threshold,
            "gaps": list(m.evidence.get("gaps", [])),
            "fully_covered": bool(m.evidence.get("fully_covered")),
            "measurement": m.as_dict(),
        }

    def evaluate_fidelity(
        self, *, description, files, entities=(), threshold: float = 0.7
    ) -> dict[str, Any]:
        """How faithfully the produced app reflects the user's request."""
        m = self.requirement_fidelity.measure(
            description=description, files=files, entities=entities
        )
        return {
            "score": round(m.score, 4),
            "accepted": m.score >= threshold,
            "missing": list(m.evidence.get("missing", [])),
            "issues": list(m.issues),
            "measurement": m.as_dict(),
        }

    def evaluate_data_understanding(
        self, insight, *, threshold: float = 0.7
    ) -> dict[str, Any]:
        """How completely the DB was understood before building."""
        m = self.data_understanding.measure(insight)
        return {
            "score": round(m.score, 4),
            "accepted": m.score >= threshold,
            "issues": list(m.issues),
            "measurement": m.as_dict(),
        }

    def evaluate_process(self, journal, *, threshold: float = 0.7) -> dict[str, Any]:
        """How faithfully the mandated, mode-specific build process was followed."""
        m = self.process_adherence.measure(journal)
        return {
            "score": round(m.score, 4),
            "accepted": m.score >= threshold,
            "issues": list(m.issues),
            "measurement": m.as_dict(),
        }

    def all_meters(self) -> dict[str, object]:
        return {
            self.accuracy.name: self.accuracy,
            self.error.name: self.error,
            self.understanding.name: self.understanding,
            self.output.name: self.output,
            self.input.name: self.input,
            self.build_accuracy.name: self.build_accuracy,
            self.build_design.name: self.build_design,
            self.code_accuracy.name: self.code_accuracy,
            self.code_adaptability.name: self.code_adaptability,
            self.code_efficiency.name: self.code_efficiency,
            self.code_design.name: self.code_design,
            self.requirement_coverage.name: self.requirement_coverage,
            self.requirement_fidelity.name: self.requirement_fidelity,
            self.data_understanding.name: self.data_understanding,
            self.process_adherence.name: self.process_adherence,
        }
