"""CLI for AppBuilderAssistant."""

from __future__ import annotations

import json
from pathlib import Path

from common.core import cliutil


def register_cli(subparsers) -> None:
    from ai_query import module_config as _mc

    _def_max_rounds = _mc.get_int("ai.app_builder", "max_rounds", default=4)
    _def_target_score = _mc.get_float("ai.app_builder", "target_score", default=0.9)
    _def_port = _mc.get_int("ai.app_builder", "default_port", default=8000)

    p = subparsers.add_parser(
        "app-builder",
        help="Build apps from scratch, codebase or database(s) with AiAppEngine",
    )
    sub = p.add_subparsers(dest="app_action", required=True)

    init = sub.add_parser("init", help="Validate blueprint and write workspace metadata")
    init.add_argument("--name", required=True)
    init.add_argument("--mode", default="from_scratch",
                      choices=["from_scratch", "from_codebase", "from_database"])
    init.add_argument("--services", default="", help="Comma-separated service templates")
    init.add_argument("--connections", default="", help="Comma-separated DB connections")

    sc = sub.add_parser("scaffold", help="Scaffold minimal app infra (from_scratch)")
    sc.add_argument("--name", required=True)

    bd = sub.add_parser("build", help="Build an app (scratch | database | codebase)")
    bd.add_argument("--name", required=True)
    bd.add_argument("--mode", default="from_scratch",
                    choices=["from_scratch", "from_codebase", "from_database"])
    bd.add_argument("--description", default="")
    bd.add_argument("--services", default="", help="Comma-separated service templates")
    bd.add_argument("--connections", default="", help="Comma-separated DB connections")
    bd.add_argument("--codebase-path", default="", help="Path for from_codebase mode")
    bd.add_argument("--variant", default="application",
                    choices=["application", "explorer"],
                    help="Builder variant: real app or metadata/explorer")
    bd.add_argument("--build-profile", default="prototype",
                    choices=["prototype", "full"],
                    help="prototype = fast demo; full = production-functional")
    bd.add_argument("--db-app-variant", default="application",
                    choices=["application", "insights_admin"])
    bd.add_argument("--codebase-variant", default="predicted_app",
                    choices=["predicted_app", "structure_metadata"])
    bd.add_argument("--use-ai", action="store_true", help="Ask an AI backend to generate files")
    bd.add_argument("--backend", default="", help="AI backend name (with --use-ai)")
    bd.add_argument("--mask-pii", action="store_true", help="Mask PII in AI prompts for this build")
    bd.add_argument("--train-llm", action="append", default=[], dest="train_llm",
                    help="LLM model name to train after build (repeatable)")
    bd.add_argument("--train-new-name", default="", dest="train_new_name",
                    help="New LLM model name to create and train")
    bd.add_argument("--rich-train", action="store_true", dest="rich_train",
                    help="Train the LLM from the build's OWN data (generated "
                         "schema/queries + DB insight), execution-validated")
    bd.add_argument("--train-engine", default="", dest="train_engine",
                    help="Training engine: python|numpy|pytorch|ollama")
    bd.add_argument("--use-rag", action="store_true", default=False,
                    help="Include RAG examples when auto-training after build")
    bd.add_argument("--index-rag", action="store_true",
                    help="Index RAG before auto-training after build")
    bd.add_argument("--rag-strategy", default="index_first",
                    choices=["index_first", "parallel"],
                    help="RAG index strategy when --index-rag")

    ab = sub.add_parser(
        "auto-build",
        help="Autonomous build: iterate with the AI Query Assistant until it passes",
    )
    ab.add_argument("--name", required=True)
    ab.add_argument("--mode", default="from_scratch",
                    choices=["from_scratch", "from_codebase", "from_database"])
    ab.add_argument("--description", default="")
    ab.add_argument("--services", default="", help="Comma-separated service templates")
    ab.add_argument("--connections", default="", help="Comma-separated DB connections")
    ab.add_argument("--codebase-path", default="", help="Path for from_codebase mode")
    ab.add_argument("--variant", default="application",
                    choices=["application", "explorer"],
                    help="Builder variant: real app or metadata/explorer")
    ab.add_argument("--build-profile", default="prototype",
                    choices=["prototype", "full"],
                    help="prototype = fast demo; full = production-functional")
    ab.add_argument("--db-app-variant", default="application",
                    choices=["application", "insights_admin"])
    ab.add_argument("--codebase-variant", default="predicted_app",
                    choices=["predicted_app", "structure_metadata"])
    ab.add_argument("--use-ai", action="store_true",
                    help="Route refinement through the AI Query Assistant")
    ab.add_argument("--max-rounds", type=int, default=_def_max_rounds)
    ab.add_argument("--target-score", type=float, default=_def_target_score)
    ab.add_argument("--mask-pii", action="store_true", help="Mask PII in AI prompts for this build")
    ab.add_argument("--train-llm", action="append", default=[], dest="train_llm",
                    help="LLM model name to train after build (repeatable)")
    ab.add_argument("--train-new-name", default="", dest="train_new_name",
                    help="New LLM model name to create and train")
    ab.add_argument("--rich-train", action="store_true", dest="rich_train",
                    help="Train the LLM from the build's OWN data (generated "
                         "schema/queries + DB insight), execution-validated")
    ab.add_argument("--train-engine", default="", dest="train_engine",
                    help="Training engine: python|numpy|pytorch|ollama")
    ab.add_argument("--use-rag", action="store_true", default=False,
                    help="Include RAG examples when auto-training after build")
    ab.add_argument("--index-rag", action="store_true",
                    help="Index RAG before auto-training after build")
    ab.add_argument("--rag-strategy", default="index_first",
                    choices=["index_first", "parallel"],
                    help="RAG index strategy when --index-rag")

    pk = sub.add_parser(
        "package",
        help="Approve + package a built app into a shippable bundle "
             "(install/run scripts + archive)",
    )
    pk.add_argument("--name", required=True)
    pk.add_argument("--port", type=int, default=_def_port,
                    help="Default listen port baked into the run scripts")
    pk.add_argument("--no-archive", action="store_true",
                    help="Skip building the distributable .zip archive")

    dl = sub.add_parser(
        "delete",
        help="Erase a build's workspace and all artifacts (leaves no trace)",
    )
    dl.add_argument("--name", required=True)

    sa = sub.add_parser("start-app", help="Start a generated app with uvicorn")
    sa.add_argument("--name", required=True)
    sa.add_argument("--port", type=int, default=_def_port)

    so = sub.add_parser("stop-app", help="Stop a running generated app")
    so.add_argument("--name", required=True)

    st = sub.add_parser("app-status", help="Check whether a generated app is running")
    st.add_argument("--name", required=True)

    lm = sub.add_parser("llm-models", help="List trained LLM models and engines")
    lm.add_argument("--format", choices=["table", "json"], default="table")

    tl = sub.add_parser("train-llm", help="Train LLM(s) without building an app")
    tl.add_argument("--connection", default="", help="DB connection for training data")
    tl.add_argument("--mode", default="from_database",
                    choices=["from_scratch", "from_database", "from_codebase"])
    tl.add_argument("--name", action="append", default=[], dest="train_llm",
                    help="Existing model name to train (repeatable)")
    tl.add_argument("--new-name", default="", dest="train_new_name",
                    help="New model name to create and train")
    tl.add_argument("--engine", default="", dest="train_engine",
                    help="Training engine: python|numpy|pytorch|ollama")
    tl.add_argument("--description", default="", help="Optional context for pair generation")
    tl.add_argument("--codebase-path", default="", help="Path for from_codebase mode")
    tl.add_argument("--use-rag", action="store_true", default=False,
                    help="Include RAG examples in training data")
    tl.add_argument("--no-rag", action="store_true", help="Do not use RAG examples")
    tl.add_argument("--index-rag", action="store_true",
                    help="Build/refresh RAG index before training")
    tl.add_argument("--rag-strategy", default="index_first",
                    choices=["index_first", "parallel"],
                    help="When --index-rag: index first (default) or in parallel")
    tl.add_argument("--include-sample", action="store_true",
                    help="Force built-in sample NL->SQL pairs as fallback")
    tl.add_argument("--no-mine", action="store_true",
                    help="Disable DB query mining (from_database)")
    tl.add_argument("--sample-limit", type=int, default=0, dest="train_sample_limit",
                    help="Per-table sample row limit when mining (default 5)")
    tl.add_argument("--max-tables", type=int, default=0, dest="train_max_tables",
                    help="Max tables to mine queries from (default 40)")

    bt = sub.add_parser(
        "build-train-llm",
        help="Train LLM(s) from an existing build's OWN data (generated schema/"
             "queries + DB insight), execution-validated — even for from_scratch",
    )
    bt.add_argument("--name", required=True, help="Built app name (its workspace)")
    bt.add_argument("--connection", default="",
                    help="DB connection to validate against (from_database builds)")
    bt.add_argument("--model", action="append", default=[], dest="train_llm",
                    help="Existing model name to train (repeatable)")
    bt.add_argument("--new-name", default="", dest="train_new_name",
                    help="New model name to create and train")
    bt.add_argument("--engine", default="", dest="train_engine",
                    help="Training engine: python|numpy|pytorch|ollama")
    bt.add_argument("--train-mode", default="full", dest="train_mode",
                    choices=["full", "incremental"])
    bt.add_argument("--workspace", default="", help="Override workspace path")
    bt.add_argument("--format", choices=["table", "json"], default="table")

    mp = sub.add_parser(
        "mine-pairs",
        help="Preview validated NL->SQL training pairs mined from a DB connection",
    )
    mp.add_argument("--connection", required=True)
    mp.add_argument("--sample-limit", type=int, default=5)
    mp.add_argument("--max-tables", type=int, default=40)
    mp.add_argument("--max-pairs", type=int, default=400)
    mp.add_argument("--no-validate", action="store_true",
                    help="Skip executing queries against the DB (faster, unvetted)")
    mp.add_argument("--format", choices=["table", "json"], default="table")

    rs = sub.add_parser("rag-status", help="Show RAG index status for a connection")
    rs.add_argument("--connection", default="", help="Connection name (empty = all)")
    rs.add_argument("--format", choices=["table", "json"], default="table")

    ir = sub.add_parser("index-rag", help="Build or refresh RAG index for a connection")
    ir.add_argument("--connection", required=True)
    ir.add_argument("--rebuild", action="store_true", help="Rebuild from scratch")

    jp = sub.add_parser("jobs", help="Background agentic build jobs")
    jsub = jp.add_subparsers(dest="job_action", required=True)
    j_start = jsub.add_parser("start", help="Start an agentic build job")
    j_start.add_argument("--body-file", required=True, help="JSON build request body")
    j_status = jsub.add_parser("status", help="Job status")
    j_status.add_argument("--id", required=True, dest="job_id")
    j_events = jsub.add_parser("events", help="Poll job events")
    j_events.add_argument("--id", required=True, dest="job_id")
    j_events.add_argument("--cursor", type=int, default=0)
    j_stop = jsub.add_parser("stop", help="Stop a job")
    j_stop.add_argument("--id", required=True, dest="job_id")
    j_msg = jsub.add_parser("message", help="Send a message to a job session")
    j_msg.add_argument("--id", required=True, dest="job_id")
    j_msg.add_argument("--text", required=True)
    j_msg.add_argument("--target", default="auto")
    j_tc = jsub.add_parser("take-control", help="Take control of a job")
    j_tc.add_argument("--id", required=True, dest="job_id")
    j_ans = jsub.add_parser("answer", help="Answer a pending job decision")
    j_ans.add_argument("--id", required=True, dest="job_id")
    j_ans.add_argument("--value", required=True)


def _train_fields(args) -> dict:
    out = {
        "train_llm": list(getattr(args, "train_llm", None) or []),
        "train_new_name": getattr(args, "train_new_name", "") or "",
        "train_engine": getattr(args, "train_engine", "") or "",
    }
    if hasattr(args, "rich_train"):
        out["rich_train"] = bool(getattr(args, "rich_train", False))
    if hasattr(args, "use_rag") or hasattr(args, "no_rag"):
        out["use_rag"] = bool(getattr(args, "use_rag", False)) and not bool(
            getattr(args, "no_rag", False))
    if hasattr(args, "index_rag"):
        out["index_rag"] = bool(getattr(args, "index_rag", False))
    if hasattr(args, "rag_strategy"):
        out["rag_strategy"] = getattr(args, "rag_strategy", "index_first") or "index_first"
    if hasattr(args, "include_sample"):
        out["include_sample"] = bool(getattr(args, "include_sample", False))
    if hasattr(args, "no_mine"):
        out["mine_db"] = not bool(getattr(args, "no_mine", False))
    if getattr(args, "train_sample_limit", 0):
        out["train_sample_limit"] = int(getattr(args, "train_sample_limit"))
    if getattr(args, "train_max_tables", 0):
        out["train_max_tables"] = int(getattr(args, "train_max_tables"))
    return out


def dispatch_cli(args) -> int:
    from ai_assistant.app_builder.service import make_service

    svc = make_service()
    if args.app_action == "build":
        body = {
            "name": args.name,
            "mode": args.mode,
            "description": args.description,
            "services": [s.strip() for s in args.services.split(",") if s.strip()],
            "features": ["list", "create", "edit", "delete"],
            "connections": [c.strip() for c in args.connections.split(",") if c.strip()],
            "codebase_path": args.codebase_path,
            "variant": args.variant,
            "build_profile": args.build_profile,
            "db_app_variant": args.db_app_variant,
            "codebase_variant": args.codebase_variant,
            "use_ai": args.use_ai,
            "backend": args.backend,
            "mask_pii": bool(getattr(args, "mask_pii", False)),
            **_train_fields(args),
        }
        r = svc.build(body)
        (cliutil.ok if r.get("ok") else cliutil.err)(
            f"[{r.get('mode')}] agent={r.get('agent')} produced "
            f"{len(r.get('files', []))} file(s) in {r.get('workspace')} "
            f"(score={r.get('verdict', {}).get('score')})"
        )
        for issue in r.get("verdict", {}).get("issues", []):
            cliutil.err(f"  issue: {issue}")
        return 0 if r.get("ok") else 1
    if args.app_action == "auto-build":
        body = {
            "name": args.name,
            "mode": args.mode,
            "description": args.description,
            "services": [s.strip() for s in args.services.split(",") if s.strip()],
            "features": ["list", "create", "edit", "delete"],
            "connections": [c.strip() for c in args.connections.split(",") if c.strip()],
            "codebase_path": args.codebase_path,
            "variant": args.variant,
            "build_profile": args.build_profile,
            "db_app_variant": args.db_app_variant,
            "codebase_variant": args.codebase_variant,
            "use_ai": args.use_ai,
            "max_rounds": args.max_rounds,
            "target_score": args.target_score,
            "mask_pii": bool(getattr(args, "mask_pii", False)),
            **_train_fields(args),
        }
        r = svc.auto_build(body)
        intro = r.get("introspection_status") or {}
        if intro:
            conn = intro.get("connection") or "(none)"
            if intro.get("ok"):
                cliutil.ok(
                    f"  selected DB introspection: OK ({conn}, "
                    f"{intro.get('tables', 0)} table(s)); runtime=SQLite")
            else:
                cliutil.err(
                    f"  selected DB introspection: WARNING ({conn}) — "
                    f"{intro.get('error') or 'no schema loaded'}; runtime=SQLite")
        quality = r.get("quality") or {}
        meters = quality.get("meters") or {}
        db_meter_names = (
            "relationship_fidelity", "entity_role_fit", "data_semantics",
            "workflow_coverage", "prediction_grounding",
        )
        parts = []
        for name in db_meter_names:
            meter = meters.get(name) or {}
            if meter and (meter.get("evidence") or {}).get("applicable", True):
                parts.append(f"{name}={float(meter.get('score', 0.0)):.2f}")
        if parts:
            cliutil.info("  DB semantics meters: " + ", ".join(parts))
        for note in (r.get("insight") or {}).get("advisory_notes") or []:
            cliutil.info(f"  DB advisory: {note}")
        for rnd in r.get("rounds", []):
            cliutil.info(
                f"  round {rnd['index']} [{rnd['phase']}] score={rnd['score']} "
                f"accepted={rnd['accepted']} — {rnd['note']}"
            )
        pf = r.get("preflight") or {}
        if pf:
            if pf.get("ok"):
                cliutil.ok("  code gate: compile + import dry-run PASSED")
            else:
                cliutil.err("  code gate: compile + import dry-run FAILED "
                            "(app would crash on launch)")
                for e in (pf.get("syntax_errors") or [])[:5]:
                    cliutil.err(f"    syntax: {e}")
                if pf.get("import_error"):
                    cliutil.err(f"    import: {pf['import_error'].splitlines()[-1][:200]}"
                                if pf["import_error"].strip() else "    import: failed")
                for e in (pf.get("module_errors") or [])[:5]:
                    cliutil.err(f"    module: {e}")
        boot = r.get("boot_check") or {}
        if boot:
            if boot.get("ok"):
                cliutil.ok("  boot check: TestClient lifespan PASSED")
            else:
                cliutil.err("  boot check: TestClient lifespan FAILED")
                if boot.get("error"):
                    cliutil.err(f"    {boot['error']}")
                elif boot.get("health_status"):
                    cliutil.err(f"    GET /health HTTP {boot['health_status']}")
        smoke = r.get("http_smoke") or {}
        if smoke:
            if smoke.get("skipped"):
                cliutil.info(
                    f"  launch smoke: SKIPPED ({smoke.get('skip_reason') or 'unavailable'})")
            elif smoke.get("ok"):
                cliutil.ok("  launch smoke: uvicorn + HTTP GET PASSED")
            else:
                cliutil.err("  launch smoke: uvicorn + HTTP GET FAILED")
                for e in (smoke.get("errors") or [])[:5]:
                    cliutil.err(f"    {e}")
        (cliutil.ok if r.get("ok") else cliutil.err)(
            f"auto-build {'OK' if r.get('ok') else 'INCOMPLETE'}: "
            f"{len(r.get('files', []))} file(s) in {r.get('workspace')} "
            f"(score={r.get('score')}, used_ai={r.get('used_ai')})"
        )
        return 0 if r.get("ok") else 1
    if args.app_action == "init":
        body = {
            "name": args.name,
            "mode": args.mode,
            "services": [s.strip() for s in args.services.split(",") if s.strip()],
            "connections": [c.strip() for c in args.connections.split(",") if c.strip()],
        }
        r = svc.init_blueprint(body)
        cliutil.info(json.dumps(r, indent=2)[:1200])
        return 0 if r.get("ok") else 1
    if args.app_action == "scaffold":
        r = svc.scaffold_from_scratch(args.name)
        (cliutil.ok if r.get("ok") else cliutil.err)(
            f"Scaffolded {len(r.get('files', []))} files in {r.get('workspace')}"
        )
        return 0 if r.get("ok") else 1
    if args.app_action == "package":
        r = svc.package_app({
            "name": args.name,
            "port": args.port,
            "archive": not args.no_archive,
        })
        for issue in r.get("issues", []):
            cliutil.err(f"  issue: {issue}")
        if r.get("ok"):
            cliutil.ok(
                f"Packaged '{r.get('app_name')}': {len(r.get('created', []))} "
                f"file(s) in {r.get('workspace')}"
            )
            cliutil.info("  created: " + ", ".join(r.get("created", [])))
            if r.get("archive"):
                cliutil.ok(f"  archive: {r.get('archive')}")
            cliutil.info(
                "  install: ./install.sh (or install.bat) — then ./run.sh "
                "(or run.bat)")
        else:
            cliutil.err(f"Packaging failed for '{r.get('app_name')}'")
        return 0 if r.get("ok") else 1
    if args.app_action == "delete":
        r = svc.delete_app({"name": args.name})
        for issue in r.get("issues", []):
            cliutil.err(f"  issue: {issue}")
        if r.get("ok"):
            if r.get("deleted"):
                cliutil.ok(f"Deleted build '{args.name}': {r.get('workspace')}")
            else:
                cliutil.info(f"Nothing to delete for '{args.name}'.")
        else:
            cliutil.err(f"Delete failed for '{args.name}'")
        return 0 if r.get("ok") else 1
    if args.app_action == "start-app":
        r = svc.start_app({"name": args.name, "port": args.port})
        for issue in r.get("issues", []):
            cliutil.err(f"  issue: {issue}")
        if r.get("ok"):
            cliutil.ok(f"Started '{args.name}' at {r.get('url')} (pid={r.get('pid')})")
        return 0 if r.get("ok") else 1
    if args.app_action == "stop-app":
        r = svc.stop_app({"name": args.name})
        cliutil.ok(f"Stopped '{args.name}'" if r.get("stopped") else f"'{args.name}' was not running")
        return 0 if r.get("ok") else 1
    if args.app_action == "app-status":
        r = svc.app_status({"name": args.name})
        if r.get("running"):
            cliutil.ok(f"'{args.name}' running at {r.get('url')}")
        else:
            cliutil.info(f"'{args.name}' is not running")
        return 0
    if args.app_action == "llm-models":
        r = svc.llm_models()
        if args.format == "json":
            print(json.dumps(r, indent=2))
            return 0
        for m in r.get("models") or []:
            cliutil.info(f"  model: {m.get('name')} engine={m.get('engine')} "
                         f"trained_at={m.get('trained_at', '')}")
        for e in r.get("engines") or []:
            avail = "yes" if e.get("available") else "no"
            cliutil.info(f"  engine: {e.get('name')} available={avail}")
        return 0
    if args.app_action == "train-llm":
        body = {
            "mode": args.mode,
            "description": args.description,
            "connections": [args.connection] if args.connection else [],
            "codebase_path": getattr(args, "codebase_path", "") or "",
            **_train_fields(args),
        }
        r = svc.train_llm(body)
        if not r.get("ok"):
            cliutil.err(r.get("error") or r.get("reason") or "Training failed.")
            return 1
        cliutil.ok(
            f"Trained {len(r.get('models') or [])} model(s) on {r.get('pairs')} pairs "
            f"({r.get('source', '')})")
        cliutil.info(f"  reason: {r.get('reason', '')}")
        for m in r.get("models") or []:
            cliutil.info(f"  {m.get('name')}: ok={m.get('ok')} engine={m.get('engine')}")
        return 0
    if args.app_action == "build-train-llm":
        body = {
            "name": args.name,
            "connections": [args.connection] if args.connection else [],
            "workspace": getattr(args, "workspace", "") or "",
            "train_mode": getattr(args, "train_mode", "full"),
            **_train_fields(args),
        }
        r = svc.build_train_llm(body)
        if args.format == "json":
            print(json.dumps(r, indent=2, default=str))
            return 0 if r.get("ok") else 1
        if not r.get("ok"):
            cliutil.err(r.get("error") or r.get("reason") or "Build-data training failed.")
            return 1
        cs = r.get("corpus_stats") or {}
        cliutil.ok(
            f"Trained {len(r.get('models') or [])} model(s) on {r.get('pairs')} "
            f"build-data pair(s) (validation={cs.get('validation')}, "
            f"rejected={cs.get('rejected', 0)})")
        for m in r.get("models") or []:
            cliutil.info(f"  {m.get('name')}: ok={m.get('ok')} engine={m.get('engine')}")
        return 0
    if args.app_action == "mine-pairs":
        r = svc.mine_training_pairs({
            "connections": [args.connection],
            "train_sample_limit": args.sample_limit,
            "train_max_tables": args.max_tables,
            "train_max_pairs": args.max_pairs,
            "validate": not bool(args.no_validate),
        })
        if args.format == "json":
            print(json.dumps(r, indent=2))
            return 0 if r.get("ok") else 1
        if not r.get("ok"):
            cliutil.err(r.get("error") or "Mining failed.")
            return 1
        stats = r.get("stats") or {}
        cliutil.ok(
            f"Mined {stats.get('kept', 0)} validated pairs from "
            f"{stats.get('tables', 0)} table(s) "
            f"({stats.get('validated', 0)}/{stats.get('candidates', 0)} passed; "
            f"db_type={r.get('db_type')})")
        for cat, n in (stats.get("by_category") or {}).items():
            cliutil.info(f"  {cat}: {n}")
        for p in (r.get("pairs") or [])[:10]:
            cliutil.info(f"  Q: {p.get('question')}")
            cliutil.info(f"     {p.get('sql')}")
        return 0
    if args.app_action == "rag-status":
        r = svc.rag_status(getattr(args, "connection", "") or "")
        if args.format == "json":
            print(json.dumps(r, indent=2))
            return 0 if r.get("ok", True) else 1
        if not r.get("ok"):
            cliutil.err(r.get("error") or "RAG status failed.")
            return 1
        if args.connection:
            cliutil.info(
                f"  {args.connection}: indexed={r.get('indexed')} "
                f"docs={r.get('doc_count', 0)}")
        else:
            for conn, meta in (r.get("connections") or {}).items():
                cliutil.info(f"  {conn}: {meta}")
        return 0
    if args.app_action == "index-rag":
        r = svc.index_rag(args.connection, rebuild=bool(args.rebuild))
        if not r.get("ok"):
            cliutil.err(r.get("error") or "RAG indexing failed.")
            return 1
        cliutil.ok(f"Indexed RAG for '{args.connection}'")
        return 0
    if args.app_action == "jobs":
        from ai_assistant.app_builder.jobs import get_job_manager

        jobs = get_job_manager(svc)
        act = args.job_action
        if act == "start":
            body = json.loads(Path(args.body_file).read_text(encoding="utf-8"))
            r = jobs.start(body)
            cliutil.ok(f"Started job {r.get('job_id')} status={r.get('status')}")
            return 0
        if act == "status":
            r = jobs.status(args.job_id)
            cliutil.info(json.dumps(r, indent=2))
            return 0
        if act == "events":
            evs = jobs.events(args.job_id, args.cursor)
            print(json.dumps(evs, indent=2))
            return 0
        if act == "stop":
            r = jobs.stop(args.job_id)
            cliutil.ok(f"Stop requested: {r}")
            return 0
        if act == "message":
            r = jobs.send_message(args.job_id, args.text, target=args.target)
            cliutil.info(json.dumps(r, indent=2))
            return 0 if r.get("ok") else 1
        if act == "take-control":
            r = jobs.take_control(args.job_id)
            cliutil.info(json.dumps(r, indent=2))
            return 0
        if act == "answer":
            r = jobs.answer(args.job_id, args.value)
            cliutil.info(json.dumps(r, indent=2))
            return 0
    return 2
