"""build_accuracy_meter — did the produced build match what was requested?

Compares the set of files the builder produced against the expected manifest
(precision/recall/F1) and checks that every requested service (notification,
docs, hosting, CI/CD, database, monitoring, …) is actually present.
"""

from __future__ import annotations

from collections.abc import Iterable

from ai_assistant.meters.base import Meter, Measurement, coverage, prf


def _norm(paths: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for p in paths or []:
        s = str(p).strip().replace("\\", "/").lstrip("./")
        if s:
            out.add(s)
    return out


class BuildAccuracyMeter(Meter):
    name = "build_accuracy_meter"
    default_threshold = 0.8

    def measure(
        self,
        *,
        expected_files: Iterable[str],
        produced_files: Iterable[str],
        required_services: Iterable[str] = (),
        present_services: Iterable[str] = (),
    ) -> Measurement:
        expected = _norm(expected_files)
        produced = _norm(produced_files)
        precision, recall, f1 = prf(expected, produced)

        req_s = {str(s).lower() for s in required_services}
        pres_s = {str(s).lower() for s in present_services}
        svc_cov = coverage(req_s, pres_s)

        missing_files = sorted(expected - produced)
        missing_services = sorted(req_s - pres_s)
        issues: list[str] = []
        if missing_files:
            issues.append(f"missing {len(missing_files)} expected file(s)")
        if missing_services:
            issues.append(f"missing service(s): {', '.join(missing_services)}")

        components = {"file_recall": recall, "file_precision": precision,
                      "service_coverage": svc_cov}
        weights = {"file_recall": 2.0, "file_precision": 1.0, "service_coverage": 3.0}
        return self._result(
            components, weights, issues=issues,
            evidence={
                "f1": round(f1, 4),
                "missing_files": missing_files[:50],
                "extra_files": sorted(produced - expected)[:50],
                "missing_services": missing_services,
            },
        )
