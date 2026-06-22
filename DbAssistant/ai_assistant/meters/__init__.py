"""Code/math-based measurement subsystem shared by all AI assistants.

Every public meter is deterministic and prompt-free. Import the suite for a
single owner of all meters::

    from ai_assistant.meters import MeterSuite
    suite = MeterSuite()
    verdict = suite.output.evaluate(question, sql, schema=schema, execution=exec_)
"""

from __future__ import annotations

from ai_assistant.meters.accuracy_meter import AccuracyMeter
from ai_assistant.meters.base import Measurement, Meter
from ai_assistant.meters.build_accuracy_meter import BuildAccuracyMeter
from ai_assistant.meters.build_design_accuracy_meter import BuildDesignAccuracyMeter
from ai_assistant.meters.code_accuracy_meter import CodeAccuracyMeter
from ai_assistant.meters.code_adaptability_meter import CodeAdaptabilityMeter
from ai_assistant.meters.code_design_management_system import CodeDesignManagementSystem
from ai_assistant.meters.code_efficiency_management_system import (
    CodeEfficiencyManagementSystem,
)
from ai_assistant.meters.error_meter import ErrorMeter
from ai_assistant.meters.input_reliability_management_system import (
    InputReliabilityManagementSystem,
    InputSignal,
)
from ai_assistant.meters.output_accuracy_management_system import (
    OutputAccuracyManagementSystem,
    OutputVerdict,
)
from ai_assistant.meters.data_understanding_meter import DataUnderstandingMeter
from ai_assistant.meters.process_adherence_meter import ProcessAdherenceMeter
from ai_assistant.meters.registry import MeterSuite
from ai_assistant.meters.requirement_coverage_meter import RequirementCoverageMeter
from ai_assistant.meters.requirement_fidelity_meter import RequirementFidelityMeter
from ai_assistant.meters.understanding_meter import UnderstandingMeter

__all__ = [
    "Measurement", "Meter", "MeterSuite",
    "AccuracyMeter", "ErrorMeter", "UnderstandingMeter",
    "OutputAccuracyManagementSystem", "OutputVerdict",
    "InputReliabilityManagementSystem", "InputSignal",
    "BuildAccuracyMeter", "BuildDesignAccuracyMeter",
    "CodeAccuracyMeter", "CodeAdaptabilityMeter",
    "CodeEfficiencyManagementSystem", "CodeDesignManagementSystem",
    "RequirementCoverageMeter", "RequirementFidelityMeter",
    "DataUnderstandingMeter", "ProcessAdherenceMeter",
]
