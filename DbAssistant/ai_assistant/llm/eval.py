"""Training-accuracy meters for local NL->SQL models.

Grounded in Spider / BIRD / Defog practice:
  * Valid-SQL parse rate
  * Dev executability (EXPLAIN / LIMIT 0)
  * Normalized exact match
  * Execution Accuracy (EX) — result-set compare vs gold SQL
  * Soft-F1 partial credit on result cells
  * Exact Set Match (ESM) — sqlglot structural compare
  * Complexity-bucket breakdown

Two depths (config ``[ai.llm.eval] depth``):
  * ``lightweight`` — random dev split from training pairs (fast)
  * ``full`` — persisted per-connection gold benchmark + history
"""

from __future__ import annotations

import json
import random
import re
import time
from pathlib import Path
from typing import Any, Callable

from ai_assistant.llm.training_policy import _complexity
from ai_assistant.llm.validation import normalize_sql, parse_sql


def _eval_config() -> dict[str, Any]:
    try:
        from ai_query import module_config as mc

        depth = (mc.get("ai.llm.eval", "depth", default="lightweight") or "lightweight").strip()
        dev_split = float(mc.get("ai.llm.eval", "dev_split", default="0.15") or 0.15)
        return {
            "depth": depth,
            "dev_split": max(0.05, min(dev_split, 0.5)),
            "execution_accuracy": mc.get_bool("ai.llm.eval", "execution_accuracy", default=True),
            "soft_f1": mc.get_bool("ai.llm.eval", "soft_f1", default=True),
            "esm": mc.get_bool("ai.llm.eval", "esm", default=True),
            "benchmark_path": (mc.get("ai.llm.eval", "benchmark_path", default="") or "").strip(),
        }
    except Exception:
        return {
            "depth": "lightweight",
            "dev_split": 0.15,
            "execution_accuracy": True,
            "soft_f1": True,
            "esm": True,
            "benchmark_path": "",
        }


def benchmarks_root() -> Path:
    from common import paths as _paths

    return _paths.session_dir() / "llm" / "benchmarks"


def benchmark_path(connection: str, *, override: str = "") -> Path:
    if override:
        return Path(override)
    safe = re.sub(r"[^\w.-]+", "_", connection or "default")
    return benchmarks_root() / f"{safe}.jsonl"


def save_benchmark(connection: str, pairs: list[dict], *, path: Path | None = None) -> Path:
    """Persist gold benchmark items for full eval."""
    p = path or benchmark_path(connection)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for item in pairs:
            gold = item.get("gold_sql") or item.get("sql") or ""
            obj = {
                "question": item.get("question", ""),
                "gold_sql": [gold] if isinstance(gold, str) else list(gold),
                "category": item.get("category") or item.get("description") or "",
                "hardness": item.get("hardness") or _complexity(gold if isinstance(gold, str) else gold[0]),
            }
            fh.write(json.dumps(obj) + "\n")
    return p


def load_benchmark(connection: str, *, path: Path | None = None) -> list[dict]:
    p = path or benchmark_path(connection)
    if not p.exists():
        return []
    out: list[dict] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if obj.get("question") and obj.get("gold_sql"):
            out.append(obj)
    return out


def split_dev(pairs: list[dict], *, dev_split: float, seed: int = 1234) -> tuple[list[dict], list[dict]]:
    if len(pairs) < 4:
        return pairs, []
    rng = random.Random(seed)
    shuffled = list(pairs)
    rng.shuffle(shuffled)
    n_dev = max(1, int(len(shuffled) * dev_split))
    dev = shuffled[:n_dev]
    train = shuffled[n_dev:]
    return train, dev


def normalized_match(a: str, b: str) -> bool:
    return normalize_sql(a).lower() == normalize_sql(b).lower()


def exact_set_match(pred: str, gold: str, *, db_type: str = "") -> bool:
    """Structural compare via sqlglot when available."""
    ok_a, norm_a, _ = parse_sql(pred, db_type=db_type or None)
    ok_b, norm_b, _ = parse_sql(gold, db_type=db_type or None)
    if not ok_a or not ok_b:
        return normalized_match(pred, gold)
    try:
        import sqlglot  # type: ignore

        dialect = None
        if db_type:
            from ai_assistant.llm.validation import _sqlglot_dialect

            dialect = _sqlglot_dialect(db_type)
        pa = sqlglot.parse_one(norm_a, read=dialect)
        pb = sqlglot.parse_one(norm_b, read=dialect)
        return pa.sql() == pb.sql()
    except Exception:
        return normalized_match(pred, gold)


def _make_executor(core: Any, connection: str, executor: Any = None) -> Any:
    if executor is not None:
        return executor
    if core is not None and connection:
        return lambda sql: core.execute(connection, sql)
    return None


def _rows_to_set(result: dict | None) -> set[tuple]:
    if not result or not isinstance(result, dict):
        return set()
    cols = result.get("columns") or []
    rows = result.get("rows") or []
    out: set[tuple] = set()
    for row in rows:
        if isinstance(row, dict):
            out.add(tuple(str(row.get(c, "")) for c in cols))
        elif isinstance(row, (list, tuple)):
            out.add(tuple(str(x) for x in row))
    return out


def _execute_rows(executor: Any, sql: str) -> tuple[dict | None, str]:
    if executor is None:
        return None, "no executor"
    try:
        if callable(executor):
            res = executor(sql)
        else:
            res = executor.execute("", sql)
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)
    if isinstance(res, tuple) and len(res) == 2:
        raw, err = res
        if err:
            return None, str(err)
        if isinstance(raw, dict) and "columns" in raw:
            return raw, ""
        if isinstance(raw, dict) and raw.get("error"):
            return None, str(raw.get("error"))
        return {"columns": [], "rows": []}, ""
    if isinstance(res, dict):
        if res.get("error"):
            return None, str(res.get("error"))
        return res, ""
    return {"columns": [], "rows": []}, ""


def execution_accuracy(
    pred_sql: str,
    gold_sqls: list[str],
    *,
    executor: Any,
) -> tuple[bool, bool, float]:
    """Return (exact_match, subset_match, soft_f1)."""
    pred_rows, err = _execute_rows(executor, pred_sql)
    if err or pred_rows is None:
        return False, False, 0.0
    pred_set = _rows_to_set(pred_rows)
    best_f1 = 0.0
    exact = False
    subset = False
    for gold in gold_sqls:
        gold_rows, gerr = _execute_rows(executor, gold)
        if gerr or gold_rows is None:
            continue
        gold_set = _rows_to_set(gold_rows)
        if not gold_set and not pred_set:
            return True, True, 1.0
        if pred_set == gold_set:
            return True, True, 1.0
        if gold_set and gold_set.issubset(pred_set):
            subset = True
        inter = len(gold_set & pred_set)
        if gold_set or pred_set:
            prec = inter / max(1, len(pred_set))
            rec = inter / max(1, len(gold_set))
            f1 = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
            best_f1 = max(best_f1, f1)
    return exact, subset, best_f1


def evaluate_generated_sql(
    question: str,
    pred_sql: str,
    gold_sql: str,
    *,
    db_type: str = "",
    executor: Any = None,
    cfg: dict | None = None,
) -> dict[str, Any]:
    cfg = cfg or _eval_config()
    parse_ok, norm_pred, _ = parse_sql(pred_sql, db_type=db_type or None)
    exec_fn = _make_executor(None, "", executor)
    item = {
        "question": question,
        "parse_ok": parse_ok,
        "normalized_match": normalized_match(pred_sql, gold_sql),
        "executable": False,
        "exact_set_match": False,
        "execution_exact": False,
        "execution_subset": False,
        "soft_f1": 0.0,
        "hardness": _complexity(gold_sql),
    }
    if cfg.get("esm"):
        item["exact_set_match"] = exact_set_match(pred_sql, gold_sql, db_type=db_type)
    if exec_fn is not None and parse_ok:
        from ai_assistant.llm.sql_check import check_sql

        chk = check_sql(norm_pred, db_type=db_type, executor=exec_fn, explain=True, limit_zero=True)
        item["executable"] = bool(chk.get("valid"))
        if cfg.get("execution_accuracy"):
            ex, sub, f1 = execution_accuracy(norm_pred, [gold_sql], executor=exec_fn)
            item["execution_exact"] = ex
            item["execution_subset"] = sub
            if cfg.get("soft_f1"):
                item["soft_f1"] = round(f1, 4)
    return item


def _aggregate(items: list[dict]) -> dict[str, Any]:
    n = max(1, len(items))
    buckets: dict[str, list[dict]] = {}
    for it in items:
        buckets.setdefault(it.get("hardness", "simple"), []).append(it)

    def _rate(key: str) -> float:
        return round(sum(1 for i in items if i.get(key)) / n, 4)

    def _avg(key: str) -> float:
        vals = [float(i.get(key) or 0) for i in items]
        return round(sum(vals) / max(1, len(vals)), 4)

    by_hardness = {}
    for h, group in buckets.items():
        gn = max(1, len(group))
        by_hardness[h] = {
            "count": len(group),
            "parse_ok_rate": round(sum(1 for i in group if i.get("parse_ok")) / gn, 4),
            "executable_rate": round(sum(1 for i in group if i.get("executable")) / gn, 4),
            "execution_exact_rate": round(sum(1 for i in group if i.get("execution_exact")) / gn, 4),
        }
    return {
        "count": len(items),
        "parse_ok_rate": _rate("parse_ok"),
        "executable_rate": _rate("executable"),
        "normalized_match_rate": _rate("normalized_match"),
        "exact_set_match_rate": _rate("exact_set_match"),
        "execution_exact_rate": _rate("execution_exact"),
        "execution_subset_rate": _rate("execution_subset"),
        "soft_f1_avg": _avg("soft_f1"),
        "by_hardness": by_hardness,
    }


def evaluate_model(
    *,
    pairs: list[dict],
    generate_fn: Callable[[str], str],
    connection: str = "",
    db_type: str = "",
    core: Any = None,
    executor: Any = None,
    train_metrics: dict | None = None,
    depth: str | None = None,
) -> dict[str, Any]:
    """Run lightweight or full eval and return meter dict."""
    cfg = _eval_config()
    depth = depth or cfg["depth"]
    started = time.time()
    exec_fn = _make_executor(core, connection, executor)
    benchmark_items: list[dict] = []

    if depth == "full":
        benchmark_items = load_benchmark(connection, path=Path(cfg["benchmark_path"]) if cfg["benchmark_path"] else None)
        if not benchmark_items and pairs:
            # Seed benchmark from validated training pairs on first full eval.
            seed_pairs = [{"question": p["question"], "gold_sql": p["sql"],
                           "description": p.get("description", "")} for p in pairs]
            save_benchmark(connection, seed_pairs)
            benchmark_items = load_benchmark(connection)

    if benchmark_items:
        eval_pairs = [
            {"question": b["question"], "sql": (b["gold_sql"][0] if b["gold_sql"] else "")}
            for b in benchmark_items
        ]
        mode = "full_benchmark"
    else:
        _train, dev = split_dev(pairs, dev_split=float(cfg["dev_split"]))
        eval_pairs = dev or pairs[: min(5, len(pairs))]
        mode = "lightweight_dev_split"

    details: list[dict] = []
    for p in eval_pairs:
        q = p.get("question", "")
        gold = p.get("sql", "")
        try:
            pred = (generate_fn(q) or "").strip()
        except Exception as exc:  # noqa: BLE001
            pred = ""
            details.append({"question": q, "error": str(exc), "parse_ok": False, "hardness": _complexity(gold)})
            continue
        details.append(evaluate_generated_sql(q, pred, gold, db_type=db_type, executor=exec_fn, cfg=cfg))

    summary = _aggregate(details)
    summary["mode"] = mode
    summary["depth"] = depth
    summary["elapsed_sec"] = round(time.time() - started, 3)
    if train_metrics:
        summary["final_loss"] = train_metrics.get("final_loss")
        summary["loss_history"] = train_metrics.get("loss_history")
    out = {"ok": True, "summary": summary, "details": details[:50], "error": None}

    if depth == "full" and connection:
        _append_history(connection, summary)
    return out


def _history_path(connection: str) -> Path:
    safe = re.sub(r"[^\w.-]+", "_", connection or "default")
    return benchmarks_root() / f"{safe}.history.jsonl"


def _append_history(connection: str, summary: dict) -> None:
    p = _history_path(connection)
    p.parent.mkdir(parents=True, exist_ok=True)
    rec = {"at": time.strftime("%Y-%m-%d %H:%M:%S"), **summary}
    with p.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec) + "\n")


def load_history(connection: str, *, limit: int = 20) -> list[dict]:
    p = _history_path(connection)
    if not p.exists():
        return []
    lines = p.read_text(encoding="utf-8").splitlines()
    out = []
    for line in lines[-limit:]:
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def format_eval_summary(summary: dict | None) -> str:
    """One-line meter for UI/CLI status bars."""
    if not summary:
        return ""
    return (
        f"Accuracy ({summary.get('mode', 'eval')}, n={summary.get('count', 0)}): "
        f"parse={summary.get('parse_ok_rate')} "
        f"exec={summary.get('executable_rate')} "
        f"match={summary.get('normalized_match_rate')} "
        f"EX={summary.get('execution_exact_rate')} "
        f"soft_f1={summary.get('soft_f1_avg')}"
    )
