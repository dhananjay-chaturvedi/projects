"""understanding_meter — did the assistant understand the request?

Scores comprehension with deterministic signals: the produced SQL's operation
matches the question's intent (count/list/filter/aggregate/mutate), the entities
named in the question are reflected in the SQL, and (for follow-ups) the new SQL
preserves continuity with the prior turn.
"""

from __future__ import annotations

from ai_assistant.meters import sqlmetrics as sm
from ai_assistant.meters.base import Meter, Measurement, jaccard

_INTENT_KEYWORDS = {
    "count": ("count", "how many", "number of", "total number"),
    "aggregate": ("sum", "average", "avg", "total", "max", "min", "most", "least", "top"),
    "filter": ("where", "with", "having", "only", "greater", "less", "between", "equal"),
    "sort": ("order", "sort", "highest", "lowest", "ascending", "descending", "top"),
    "list": ("list", "show", "all", "find", "get", "display", "which", "what"),
    "mutate": ("insert", "update", "delete", "add", "remove", "change", "set"),
}


def _question_intents(question: str) -> set[str]:
    q = (question or "").lower()
    return {intent for intent, kws in _INTENT_KEYWORDS.items() if any(k in q for k in kws)}


def _sql_intents(sql: str) -> set[str]:
    s = (sql or "").lower()
    out: set[str] = set()
    if "count(" in s:
        out.add("count")
    if any(f in s for f in ("sum(", "avg(", "max(", "min(", "group by")):
        out.add("aggregate")
    if " where " in s:
        out.add("filter")
    if "order by" in s:
        out.add("sort")
    if s.strip().startswith("select"):
        out.add("list")
    if any(s.strip().startswith(k) for k in ("insert", "update", "delete")):
        out.add("mutate")
    return out


class UnderstandingMeter(Meter):
    name = "understanding_meter"
    default_threshold = 0.7

    def measure(
        self,
        question: str,
        sql: str,
        *,
        previous_sql: str | None = None,
        is_followup: bool = False,
    ) -> Measurement:
        components: dict[str, float] = {}
        weights: dict[str, float] = {}
        evidence: dict[str, object] = {}
        issues: list[str] = []

        q_intents = _question_intents(question)
        s_intents = _sql_intents(sql)
        evidence["question_intents"] = sorted(q_intents)
        evidence["sql_intents"] = sorted(s_intents)
        # "list" is implicit in almost every SELECT, so don't punish its absence.
        core_q = q_intents - {"list"}
        if core_q:
            matched = len(core_q & s_intents) / len(core_q)
        else:
            matched = 1.0 if s_intents else 0.0
        components["intent_match"] = matched
        weights["intent_match"] = 2.0
        if core_q and not (core_q & s_intents):
            issues.append(
                f"question implies {sorted(core_q)} but SQL expresses {sorted(s_intents)}"
            )

        # Entity continuity question -> sql.
        from ai_assistant.meters.accuracy_meter import _question_terms

        qterms = _question_terms(question)
        idents = sm.referenced_identifiers(sql)
        components["entity_grounding"] = (
            len(qterms & idents) / len(qterms) if qterms else 1.0
        )
        weights["entity_grounding"] = 1.5

        if is_followup and previous_sql:
            prev_tables = sm.referenced_tables(previous_sql)
            cur_tables = sm.referenced_tables(sql)
            continuity = jaccard(prev_tables, cur_tables)
            components["followup_continuity"] = continuity
            weights["followup_continuity"] = 1.5
            evidence["prev_tables"] = sorted(prev_tables)
            if prev_tables and not (prev_tables & cur_tables):
                issues.append("follow-up SQL shares no tables with the previous turn")

        return self._result(components, weights, evidence=evidence, issues=issues)
