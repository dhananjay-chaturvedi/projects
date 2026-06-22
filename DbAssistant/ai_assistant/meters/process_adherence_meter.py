"""process_adherence_meter — was the *required build process* actually followed?

The product spec mandates not just a good result but a specific, modular
process:

* ``from_database`` — the App Builder MUST consult the **AI Query Assistant** to
  understand the data, use the **code agent** to generate code/tests, create
  **sample data**, and **run tests**; it should verify the build against the data.
* ``from_scratch`` — the App Builder MUST drive the **code agent** (chat)
  directly (it does NOT need the AI Query Assistant), create **sample data**, and
  **run tests**; if a DB connection was selected it should deploy the app schema
  to that connection.

This meter scores a recorded build *journal* against those rules so we can
measure how faithfully the mandated process was followed (not just the artifact).
Deterministic: pure rule checks over recorded booleans/flags.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai_assistant.meters.base import Meter, Measurement, weighted_score


class ProcessAdherenceMeter(Meter):
    """Score a build journal against the mandated, mode-specific process."""

    name = "process_adherence_meter"
    default_threshold = 0.7

    def measure(self, journal: Mapping[str, Any]) -> Measurement:
        mode = str(journal.get("mode") or "from_scratch")
        channels = {str(c).lower() for c in (journal.get("channels") or [])}
        used_qa = "query_assistant" in channels
        used_agent = "code_agent" in channels
        sample = bool(journal.get("sample_data_created"))
        tests_run = bool(journal.get("tests_run"))
        tests_passed = bool(journal.get("tests_passed"))
        verified = bool(journal.get("verified_with_data"))
        schema_deployed = bool(journal.get("schema_deployed"))
        connection = bool(journal.get("connection"))

        components: dict[str, float] = {
            "code_agent": 1.0 if used_agent else 0.0,
            "sample_data": 1.0 if sample else 0.0,
            "tests_run": 1.0 if tests_run else 0.0,
            "tests_passed": 1.0 if tests_passed else 0.0,
        }
        weights: dict[str, float] = {
            "code_agent": 2.0, "sample_data": 2.0,
            "tests_run": 1.5, "tests_passed": 1.5,
        }
        issues: list[str] = []
        if not used_agent:
            issues.append("the AI code agent (chat) was not used to build")
        if not sample:
            issues.append("no sample data was created for parallel testing")
        if not tests_run:
            issues.append("tests were not run during the build")
        elif not tests_passed:
            issues.append("generated tests did not pass")

        if mode == "from_database":
            components["data_understanding"] = 1.0 if used_qa else 0.0
            weights["data_understanding"] = 3.0
            components["verified_with_data"] = 1.0 if verified else 0.0
            weights["verified_with_data"] = 1.0
            if not used_qa:
                issues.append("from_database build did not consult the AI Query "
                              "Assistant to understand the data")
            if not verified:
                issues.append("build was not verified against the database's data")
        else:  # from_scratch (and any non-database mode)
            if connection:
                components["schema_deployed"] = 1.0 if schema_deployed else 0.0
                weights["schema_deployed"] = 1.5
                if not schema_deployed:
                    issues.append("a connection was selected but the app schema "
                                  "was not deployed to it")

        return Measurement(
            meter=self.name,
            score=weighted_score(components, weights),
            components=components,
            weights=weights,
            evidence={
                "mode": mode, "channels": sorted(channels),
                "sample_data_created": sample, "tests_run": tests_run,
                "tests_passed": tests_passed, "verified_with_data": verified,
                "schema_deployed": schema_deployed,
            },
            issues=issues,
            threshold=self.default_threshold,
        )
