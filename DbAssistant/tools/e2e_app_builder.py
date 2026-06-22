"""End-to-end App Builder harness (collaboration pipeline).

Builds a real app via the App Builder service using a live AI backend
(cursor/claude), with the collaboration pipeline enabled, then reports the
understanding phase, the standard quality meters, and runs the generated app's
own test suite to confirm functional correctness.

Usage:
    python tools/e2e_app_builder.py "<app name>" "<description>" [backend] [rounds]
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _backend(name: str):
    from ai_query.agent import AIQueryAgent
    agent = AIQueryAgent()
    agent.set_backend(name)
    return getattr(agent, "_active_backend", None)


def main() -> int:
    name = sys.argv[1] if len(sys.argv) > 1 else "e2e_todo"
    description = (sys.argv[2] if len(sys.argv) > 2
                   else "a simple todo list app with tasks you can create, "
                        "list, complete and delete")
    backend_name = sys.argv[3] if len(sys.argv) > 3 else "cursor"
    rounds = int(sys.argv[4]) if len(sys.argv) > 4 else 4

    from ai_assistant.app_builder.service import make_service

    backend = _backend(backend_name)
    if backend is None:
        print(f"backend {backend_name!r} unavailable")
        return 2

    svc = make_service()
    body = {
        "name": name,
        "mode": "from_scratch",
        "description": description,
        "use_ai": True,
        "agentic": True,
        "collaboration": True,
        "interaction": "uninterrupted",
        "uninterrupted": True,
        "run_tests": True,
        "max_rounds": rounds,
        "validation_depth": "low_token",
    }

    # Timing instrumentation: stamp every progress event so we can see exactly
    # where wall-clock time goes (the slow part is the per-LLM-turn latency).
    t0 = time.monotonic()
    timeline: list[tuple[float, str, str]] = []
    builder_session_ids: list[str] = []

    def _stamp(label: str, text: str = "") -> None:
        timeline.append((time.monotonic() - t0, label, text[:80]))

    def on_progress(p):
        if not isinstance(p, dict):
            return
        if "agent_event" in p:
            payload = p["agent_event"]
            ev = payload.get("event", {}) or {}
            t = ev.get("type", "")
            sid = (ev.get("detail") or {}).get("session_id") if isinstance(
                ev.get("detail"), dict) else None
            if sid and payload.get("session") == "builder":
                builder_session_ids.append(str(sid))
            _stamp(f"{payload.get('session','?')}/{t}", str(ev.get("text", "")))
            if t in ("session_understanding", "assistant_quality", "kickoff",
                     "build_agreement", "test_plan", "plan_approved"):
                el = time.monotonic() - t0
                print(f"  [{el:6.1f}s] {t}: {str(ev.get('text',''))[:110]}",
                      flush=True)
        elif "index" in p:  # plain BuildRound
            el = time.monotonic() - t0
            print(f"  [{el:6.1f}s] round {p.get('index')} "
                  f"phase={p.get('phase')} score={p.get('score')} "
                  f"cov={p.get('coverage')}", flush=True)

    print(f"=== building {name!r} via {backend_name} (collaboration on) ===", flush=True)
    result = svc.auto_build(body, on_progress=on_progress, backend=backend)
    total = time.monotonic() - t0

    print("\n=== TIMING BREAKDOWN ===")
    print(f"total wall-clock: {total:.1f}s")
    # Bucket time between consecutive events by their phase label prefix.
    buckets: dict[str, float] = {}
    prev_t = 0.0
    for ts, label, _ in timeline:
        key = label.split("/")[0]
        buckets[key] = buckets.get(key, 0.0) + (ts - prev_t)
        prev_t = ts
    for key, secs in sorted(buckets.items(), key=lambda kv: -kv[1]):
        print(f"  {key:14s} {secs:6.1f}s")
    # Confirm a SINGLE incremental builder session (resume), not rebuilds.
    unique_builder = list(dict.fromkeys(builder_session_ids))
    if unique_builder:
        print(f"builder session ids: {len(unique_builder)} unique "
              f"({'incremental — same session resumed' if len(unique_builder) == 1 else 'WARNING: multiple builder sessions — possible restart'})")
    else:
        print("builder session ids: n/a (not emitted by backend)")

    print("\n=== RESULT ===")
    print("ok:", result.get("ok"), "| score:", result.get("score"),
          "| stop:", result.get("stop_reason"))
    commits = result.get("commits") or []
    print(f"build rounds committed: {len(commits)}")
    u = result.get("understanding") or {}
    if u:
        sim = u.get("similarity")
        sim_score = (sim.get("score") if isinstance(sim, dict) else sim)
        print(f"understanding ready={u.get('ready')} similarity={sim_score} "
              f"rounds={u.get('rounds')}")
    q = result.get("quality") or {}
    if q:
        print(f"quality overall: {q.get('overall')}")
        meters = q.get("meters") or {}
        if isinstance(meters, dict):
            print("quality meters:")
            for k, v in meters.items():
                if isinstance(v, dict):
                    print(f"  {k}: {v.get('score')} ({v.get('grade')})")
                else:
                    print(f"  {k}: {v}")
    print("agreement:", json.dumps(result.get("agreement", {}))[:200])

    ws = Path(result.get("workspace", ""))
    print("\nworkspace:", ws)
    if (ws / "tests").is_dir():
        print("=== running generated app tests ===")
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q"],
            cwd=str(ws), capture_output=True, text=True, timeout=300,
            env={"APP_DB_PATH": ":memory:"})
        print(proc.stdout[-2000:])
        print("tests returncode:", proc.returncode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
