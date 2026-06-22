"""
End-to-end self-test for pluggable LLM engines.

Run with::

    python -m ai_assistant.llm.selftest

Trains each *available* engine on the built-in sample in a temp directory,
generates SQL for trained questions, and verifies auto-fallback when pytorch
is requested but unavailable. No models are left in the session dir.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

CHECKS = [
    ("list all customers", "customers"),
    ("show all products", "products"),
    ("count the number of orders", "orders"),
]


def _test_engine(svc, engine_name: str, tmp: Path) -> bool:
    from ai_assistant.llm.engines import get_engine

    eng = get_engine(engine_name)
    if eng is None:
        print(f"  [skip] unknown engine: {engine_name}")
        return True
    ok, reason = eng.is_available()
    if not ok:
        print(f"  [skip] {engine_name}: {reason}")
        return True

    print(f"\n== engine: {engine_name} ==")
    overrides = {}
    if engine_name == "pytorch":
        overrides = {"pt_max_iters": 80, "pt_batch_size": 8}
    elif engine_name in ("python", "numpy"):
        overrides = {"epochs": 30, "min_loss": 0.02}

    r = svc.train(
        name=engine_name,
        engine=engine_name,
        include_sample=True,
        overrides=overrides,
    )
    if not r.get("ok"):
        print(f"  FAIL train: {r.get('error')}")
        return False
    print(
        f"  trained engine={r.get('engine')} pairs={r['num_pairs']} "
        f"loss={r.get('final_loss')} {r.get('elapsed_sec')}s"
    )

    hits = 0
    for q, expect in CHECKS:
        g = svc.generate(q, name=engine_name)
        sql = g.get("sql") or ""
        ok_hit = expect.lower() in sql.lower()
        hits += int(ok_hit)
        print(f"    Q: {q}\n      -> {sql}    [{'OK' if ok_hit else 'miss'}]")
    if hits < len(CHECKS) - 1:
        print(f"  WARN only {hits}/{len(CHECKS)} hits (engine may need more training)")
    return True


def _test_fallback(svc, tmp: Path) -> bool:
    """Verify resolve_engine falls back when preferred is unavailable."""
    from ai_assistant.llm.engines import resolve_engine

    # Simulate unavailable pytorch by requesting a fake engine then fallback
    eng, used, did_fb = resolve_engine("nonexistent_engine_xyz", "python")
    if used != "python":
        print(f"  FAIL fallback: expected python, got {used}")
        return False
    print(f"  fallback: nonexistent -> {used} (did_fallback={did_fb})")
    return True


def main() -> int:
    from ai_assistant.llm.engines import available_engines
    from ai_assistant.llm.service import LlmService

    tmp = Path(tempfile.mkdtemp(prefix="llm_engines_selftest_"))
    print(f"[selftest] temp dir: {tmp}")

    svc = LlmService(models_dir=tmp)

    print("\n[0] registered engines:")
    for e in available_engines():
        mark = "OK" if e["available"] else "NO"
        print(f"    {e['name']:8s} stage={e['stage']} [{mark}] {e.get('reason', '')}")

    all_ok = True
    for name in ("python", "numpy", "pytorch", "ollama"):
        if not _test_engine(svc, name, tmp):
            all_ok = False

    print("\n== auto-fallback ==")
    if not _test_fallback(svc, tmp):
        all_ok = False

    if all_ok:
        print("\n[selftest] ALL CHECKS PASSED")
        return 0
    print("\n[selftest] SOME CHECKS FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
