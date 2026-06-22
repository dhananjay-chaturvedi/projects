"""AppBuilderAssistant service."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ai_assistant.app_builder.engine import AiAppEngine, AppBlueprint, BuildMode
from ai_assistant.app_builder.builder_types import (
    APPLICATION,
    EXPLORER,
    FULL,
    PROTOTYPE,
    policy_for,
)
from ai_assistant.app_builder.flows import BuildFlows
from ai_query import module_config as mc
from common import paths as app_paths


@dataclass(frozen=True)
class AutoBuildRequest:
    """Runtime/dependency bundle for autonomous app builds."""

    body: dict
    bridge: Any = None
    db_understanding: Any = None
    decider: Any = None
    db_manager: Any = None
    on_progress: Any = None
    backend: Any = None
    cancel_event: Any = None


class AppBuilderService:
    def __init__(self, core: Any = None) -> None:
        self._core = core
        self._engine = AiAppEngine()
        self._flows = BuildFlows(engine=self._engine)
        self._job_coordinators: dict[str, Any] = {}
        self._active_job_id: str | None = None
        #: live A/B/C coordinator from the most recent agentic build (post-build
        #: interactive chat); None until an agentic build runs.
        self.last_coordinator = None
        #: live BuildDecider from the most recent agentic build (take-control).
        self.last_decider = None

    def _workspace(self, name: str) -> Path:
        from common.security.paths import assert_safe_name, resolve_under

        safe = assert_safe_name(name, label="app name")
        return resolve_under(app_paths.app_builder_dir(), safe)

    # ── PII masking (per-build override) ───────────────────────────────────
    def get_pii_masking(self) -> dict[str, Any]:
        """Return the global AI PII masking default (does not change it)."""
        try:
            from ai_query.agent import AIQueryAgent

            agent = AIQueryAgent()
            return {
                "ok": True,
                "enabled": bool(getattr(agent, "mask_pii_enabled", True)),
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "enabled": True, "error": str(exc)}

    def _mask_pii_enabled(self, body: dict) -> bool:
        if "mask_pii" in body:
            return bool(body.get("mask_pii"))
        return bool(self.get_pii_masking().get("enabled", True))

    # ── LLM training ───────────────────────────────────────────────────────
    def _llm_training_service(self):
        from ai_assistant.llm.training_service import LlmTrainingService

        return LlmTrainingService(
            self._core,
            insight_provider=self._insight_for_training,
        )

    def llm_training_available(self) -> bool:
        try:
            self._llm_training_service()
            return True
        except Exception:
            return False

    def llm_models(self) -> dict[str, Any]:
        try:
            return self._llm_training_service().llm_models()
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "models": [], "engines": [], "error": str(exc)}

    def _insight_for_training(self, body: dict) -> Any:
        """Best-effort DB insight for manual training (no app build)."""
        if body.get("mode") != "from_database":
            return None
        conn = (body.get("connections") or [""])[0]
        if not conn:
            return None
        try:
            from ai_assistant.app_builder.db_understanding import DbUnderstandingClient
            from ai_query.agent import AIQueryAgent

            db_manager = None
            if self._core is not None:
                try:
                    db_manager = self._core.get_manager(conn)
                except Exception:
                    pass
            client = DbUnderstandingClient(
                query_assistant=AIQueryAgent(),
                db_manager=db_manager,
                connection_name=conn,
                user_description=body.get("description", ""),
                variant=body.get("db_app_variant", "application"),
                mask_pii=self._mask_pii_enabled(body),
            )
            return client.understand()
        except Exception:
            return None

    def rag_status(self, connection: str = "") -> dict[str, Any]:
        try:
            return self._llm_training_service().rag_status(connection)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def index_rag(self, connection: str, *, rebuild: bool = False) -> dict[str, Any]:
        try:
            return self._llm_training_service().index_rag(connection, rebuild=rebuild)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "connection": connection, "error": str(exc)}

    def train_llm(self, body: dict, *, on_progress: Any = None) -> dict[str, Any]:
        try:
            body = dict(body or {})
            pre_indexed = False
            if body.get("index_rag") and str(body.get("rag_strategy") or "index_first") == "index_first":
                conn = (body.get("connections") or [""])[0]
                idx_res = self.index_rag(conn, rebuild=False)
                if not idx_res.get("ok"):
                    err = idx_res.get("error") or "RAG indexing failed."
                    return {"ok": False, "error": err,
                            "reason": f"RAG index failed for '{conn}': {err}",
                            "rag_indexed": False, "connection": conn}
                pre_indexed = True
                body["index_rag"] = False
            result = self._llm_training_service().train_llm(body, on_progress=on_progress)
            if pre_indexed and result.get("ok"):
                result["rag_indexed"] = True
            return result
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc), "reason": str(exc)}

    def mine_training_pairs(self, body: dict) -> dict[str, Any]:
        try:
            return self._llm_training_service().mine_training_pairs(body)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc), "pairs": [], "stats": {}}

    # ── rich build-data training ─────────────────────────────────────────────
    def _persist_build_data(self, workspace: Path, insight: Any, result: dict) -> None:
        """Snapshot the build's insight + result so training can run after the
        build (standalone ``build-train-llm``), not just inline."""
        try:
            var = Path(workspace) / "var"
            var.mkdir(parents=True, exist_ok=True)
            if insight is not None:
                data = insight.as_dict() if hasattr(insight, "as_dict") else insight
                if isinstance(data, dict):
                    (var / "build_insight.json").write_text(
                        json.dumps(data, indent=2, default=str), encoding="utf-8")
            if isinstance(result, dict):
                slim = {k: result.get(k) for k in (
                    "mode", "transcript", "decisions", "resolved_connection",
                    "introspection_status") if k in result}
                (var / "build_result.json").write_text(
                    json.dumps(slim, indent=2, default=str), encoding="utf-8")
        except Exception:
            pass

    def _load_persisted_build_data(self, workspace: Path) -> tuple[Any, dict]:
        insight: Any = None
        result: dict = {}
        try:
            ins_path = Path(workspace) / "var" / "build_insight.json"
            if ins_path.exists():
                insight = json.loads(ins_path.read_text(encoding="utf-8"))
        except Exception:
            insight = None
        try:
            res_path = Path(workspace) / "var" / "build_result.json"
            if res_path.exists():
                result = json.loads(res_path.read_text(encoding="utf-8")) or {}
        except Exception:
            result = {}
        return insight, result

    def build_train_llm(
        self,
        body: dict,
        *,
        insight: Any = None,
        build_result: Any = None,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        """Train selected/new LLM model(s) from a build's *own* rich data.

        Distinct from the generic harvest (``Build and Train LLM``): the corpus
        is derived from the generated app's schema + queries, the DB-understanding
        insight, the build transcript, and (``from_codebase``) the codebase
        profile — then execution-validated against the selected connection
        (``from_database``) or a throwaway SQLite built from the generated schema
        (``from_scratch`` / ``from_codebase``). Works even with no connection.
        """
        try:
            from ai_assistant.llm.build_corpus import collect_build_corpus
            from ai_assistant.llm.data_sources import resolve_train_names

            names = resolve_train_names(body)
            if not names:
                return {"ok": False, "error": "No LLM model names selected for training.",
                        "reason": "Select an existing model or enter a new model name."}

            workspace = body.get("workspace") or str(self._workspace(body.get("name", "")))
            ws = Path(workspace)
            conn = (body.get("connections") or [""])[0] or str(body.get("connection") or "")
            db_type = None
            if conn and self._core is not None:
                try:
                    db_type = (self._core.get_connection_profile(conn) or {}).get(
                        "db_type") or None
                except Exception:
                    db_type = None

            if insight is None:
                insight = body.get("insight")
            if insight is None or build_result is None:
                p_insight, p_result = self._load_persisted_build_data(ws)
                insight = insight if insight is not None else p_insight
                build_result = build_result if build_result is not None else p_result
            codebase_profile = body.get("codebase_profile")

            if on_progress:
                on_progress({"type": "training_capture", "status": "collecting",
                             "source": "build_data"})

            corpus = collect_build_corpus(
                ws,
                insight=insight,
                build_result=build_result,
                codebase_profile=codebase_profile,
                connection=conn,
                core=self._core,
                db_type=db_type,
                validate=bool(body.get("validate", True)),
            )
            pairs = corpus.get("pairs") or []
            stats = corpus.get("stats") or {}
            if not pairs:
                return {"ok": False,
                        "error": "No accurate NL->SQL pairs could be derived from this build.",
                        "reason": ("The build produced no execution-validated training "
                                   "pairs. Build with a schema/connection and retry."),
                        "stats": stats, "source": "build_data"}

            if on_progress:
                on_progress({"type": "training_capture", "status": "captured",
                             "pairs": len(pairs), "source": "build_data",
                             "connection": conn, "validation": stats.get("validation")})

            result = self._llm_training_service().train_pairs(
                pairs,
                names=names,
                engine=str(body.get("train_engine") or body.get("engine") or "").strip()
                or None,
                connection=conn,
                include_sample=False,
                use_rag=bool(body.get("use_rag", False)),
                train_mode=str(body.get("train_mode") or "full"),
                on_progress=on_progress,
            )
            result["source"] = "build_data"
            result["corpus_stats"] = stats
            return result
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc), "reason": str(exc),
                    "source": "build_data"}

    def _maybe_auto_train_llm(
        self, body: dict, workspace: Path, insight: Any, *, on_progress: Any = None,
        build_result: Any = None,
    ) -> dict[str, Any] | None:
        from ai_assistant.llm.data_sources import resolve_train_names

        # Always snapshot build data so a later standalone build-train-llm works,
        # even when this build didn't request training.
        self._persist_build_data(workspace, insight, build_result or {})
        if not resolve_train_names(body):
            return None
        if not self.llm_training_available():
            return None
        # Rich (build-data) training is the accurate, build-grounded path.
        if body.get("rich_train") or body.get("build_train"):
            train_body = dict(body)
            train_body["workspace"] = str(workspace)
            return self.build_train_llm(
                train_body, insight=insight, build_result=build_result,
                on_progress=on_progress)
        train_body = dict(body)
        train_body["from_build"] = True
        train_body["workspace"] = str(workspace)
        if insight is not None:
            train_body["insight"] = insight
        return self.train_llm(train_body, on_progress=on_progress)

    # ── blueprint helpers ────────────────────────────────────────────────────
    def _build_profile(self, body: dict) -> str:
        default = mc.get(
            "ai.app_builder", "default_build_profile", default=PROTOTYPE)
        profile = str(body.get("build_profile") or default or PROTOTYPE).strip().lower()
        return FULL if profile == FULL else PROTOTYPE

    def _variant(self, body: dict, mode: BuildMode) -> str:
        explicit = str(body.get("variant") or "").strip().lower()
        if explicit:
            return policy_for(mode).normalize_variant(explicit)
        default = mc.get("ai.app_builder", "default_variant", default=APPLICATION)
        if default:
            normalized = policy_for(mode).normalize_variant(str(default))
            if normalized != APPLICATION:
                return normalized
        if mode == BuildMode.FROM_DATABASE:
            old = str(body.get("db_app_variant") or "application").strip().lower()
            return EXPLORER if old == "insights_admin" else APPLICATION
        if mode == BuildMode.FROM_CODEBASE:
            old = str(body.get("codebase_variant") or "predicted_app").strip().lower()
            return EXPLORER if old == "structure_metadata" else APPLICATION
        return APPLICATION

    def _blueprint(self, body: dict) -> AppBlueprint:
        mode = BuildMode(body.get("mode", "from_scratch"))
        variant = self._variant(body, mode)
        db_app_variant = (
            "insights_admin" if mode == BuildMode.FROM_DATABASE
            and variant == EXPLORER
            else body.get("db_app_variant", "application")
        )
        # The insights/admin variant always builds the DB-insights dashboard
        # (kind="insights") regardless of what archetype detection would pick.
        kind = body.get("kind", "")
        if mode == BuildMode.FROM_DATABASE and db_app_variant == "insights_admin":
            kind = "insights"
        return AppBlueprint(
            name=body.get("name", "myapp"),
            mode=mode,
            services=list(body.get("services") or []),
            connections=list(body.get("connections") or []),
            codebase_path=body.get("codebase_path", ""),
            language=body.get("language", "python"),
            description=body.get("description", ""),
            entities=list(body.get("entities") or []),
            features=list(body.get("features") or []),
            kind=kind,
            build_profile=self._build_profile(body),
            variant=variant,
            db_app_variant=db_app_variant,
            codebase_variant=(
                "structure_metadata" if mode == BuildMode.FROM_CODEBASE
                and variant == EXPLORER
                else body.get("codebase_variant", "predicted_app")
            ),
        )

    def _agent_for(self, body: dict):
        """Return an AI-backed agent when requested+available, else deterministic."""
        if not body.get("use_ai"):
            return None
        try:
            from ai_assistant.app_builder.agent import CliBackendAgent
            from ai_query.backends import AIBackendRegistry

            reg = AIBackendRegistry()
            name = body.get("backend") or reg.get_default_name() or ""
            backend = reg.get(name) if name else None
            if backend and reg.check_one(backend.name):
                return CliBackendAgent(backend, mask_pii=self._mask_pii_enabled(body))
        except Exception:
            pass
        return None

    def init_blueprint(self, body: dict) -> dict[str, Any]:
        mode = BuildMode(body.get("mode", "from_scratch"))
        bp = AppBlueprint(
            name=body.get("name", "myapp"),
            mode=mode,
            services=list(body.get("services") or []),
            connections=list(body.get("connections") or []),
            codebase_path=body.get("codebase_path", ""),
            language=body.get("language", "python"),
            description=body.get("description", ""),
            build_profile=self._build_profile(body),
            variant=self._variant(body, mode),
        )
        verdict = self._engine.validate_blueprint(bp)
        ws = self._workspace(bp.name)
        ws.mkdir(parents=True, exist_ok=True)
        meta = {
            "blueprint": {
                "name": bp.name, "mode": bp.mode.value,
                "services": bp.services, "connections": bp.connections,
                "codebase_path": bp.codebase_path, "language": bp.language,
                "description": bp.description,
                "build_profile": bp.build_profile, "variant": bp.variant,
            },
            "agent_packet": self._engine.agent_metadata_packet(bp),
            "validation": verdict.as_dict(),
        }
        (ws / "blueprint.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
        return {"ok": verdict.accepted, "workspace": str(ws), **meta}

    # ── build flows ──────────────────────────────────────────────────────────
    def build(self, body: dict) -> dict[str, Any]:
        """Dispatch a build by mode (from_scratch | from_database | from_codebase)."""
        mode = BuildMode(body.get("mode", "from_scratch"))
        if mode == BuildMode.FROM_DATABASE:
            out = self.build_from_database(body)
        elif mode == BuildMode.FROM_CODEBASE:
            out = self.build_from_codebase(body)
        else:
            out = self.build_from_scratch(body)
        # Snapshot build data and (optionally) train an LLM from the build's own
        # data — keeps the deterministic build path at parity with auto_build.
        try:
            workspace = self._workspace(str(body.get("name", "")))
            train_result = self._maybe_auto_train_llm(
                body, workspace, out.get("insight") if isinstance(out, dict) else None,
                build_result=out if isinstance(out, dict) else None,
            )
            if train_result is not None and isinstance(out, dict):
                out["training"] = train_result
        except Exception:
            pass
        return out

    def build_from_scratch(self, body: dict) -> dict[str, Any]:
        bp = self._blueprint(body)
        bp.mode = BuildMode.FROM_SCRATCH
        if not bp.services:
            bp.services = ["ci_cd", "document", "hosting", "database"]
        return self._flows.build_from_scratch(
            bp, self._workspace(bp.name), agent=self._agent_for(body)
        )

    def build_from_database(self, body: dict) -> dict[str, Any]:
        bp = self._blueprint(body)
        bp.mode = BuildMode.FROM_DATABASE
        schema, introspection_status = self._selected_schema(body, bp)
        result = self._flows.build_from_database(
            bp, self._workspace(bp.name), schema, agent=self._agent_for(body)
        )
        result["resolved_connection"] = (
            introspection_status.get("connection") or (bp.connections[0] if bp.connections else "")
        )
        result["introspection_status"] = introspection_status
        return result

    def build_from_codebase(self, body: dict) -> dict[str, Any]:
        bp = self._blueprint(body)
        bp.mode = BuildMode.FROM_CODEBASE
        return self._flows.build_from_codebase(
            bp, self._workspace(bp.name), agent=self._agent_for(body)
        )

    def package_app(self, body: dict) -> dict[str, Any]:
        """Approve + package a built app into a shippable bundle.

        Runs after a build (passed or failed) once the user has reviewed the
        app: writes cross-platform install/run scripts that create a venv,
        install all dependencies, and complete first-run setup, plus an
        ``INSTALL.md`` and (by default) a distributable ``.zip`` archive.
        """
        from ai_assistant.app_builder.packaging import package_app

        name = str(body.get("name") or "myapp")
        _default_port = mc.get_int("ai.app_builder", "default_port", default=8000)
        result = package_app(
            self._workspace(name),
            app_name=name,
            port=int(body.get("port", _default_port) or _default_port),
            make_archive=bool(body.get("archive", True)),
        )
        return result.as_dict()

    def delete_app(self, body: dict) -> dict[str, Any]:
        """Erase a build's workspace, leaving no on-disk trace.

        Removes the generated app and all build artifacts for ``name`` from the
        App Builder output directory. Only paths inside ``app_builder_dir()`` can
        be deleted (a safety guard), so this can never touch unrelated files.
        Returns ``{"ok", "deleted", "workspace", "issues"}``.
        """
        import shutil

        name = str(body.get("name") or "").strip()
        if not name:
            return {"ok": False, "deleted": False, "workspace": "",
                    "issues": ["no build name given"]}
        root = app_paths.app_builder_dir().resolve()
        try:
            target = self._workspace(name).resolve()
        except Exception as exc:  # noqa: BLE001
            from common.security.paths import PathEscapeError

            if isinstance(exc, PathEscapeError):
                return {"ok": False, "deleted": False, "workspace": "",
                        "issues": ["refusing to delete a path outside the App "
                                   "Builder output directory"]}
            raise
        try:
            inside = target == root or root in target.parents
        except Exception:  # noqa: BLE001
            inside = False
        if not inside or target == root:
            return {"ok": False, "deleted": False, "workspace": str(target),
                    "issues": ["refusing to delete a path outside the App "
                               "Builder output directory"]}
        if not target.exists():
            return {"ok": True, "deleted": False, "workspace": str(target),
                    "issues": ["nothing to delete (workspace not found)"]}
        try:
            shutil.rmtree(target)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "deleted": False, "workspace": str(target),
                    "issues": [str(exc)]}
        return {"ok": True, "deleted": True, "workspace": str(target),
                "issues": []}

    # ── app runtime (shared by Tk / TUI / Web / CLI / API) ───────────────────
    def start_app(self, body: dict) -> dict[str, Any]:
        """Start the generated FastAPI app with uvicorn in its workspace."""
        import subprocess
        import sys

        from ai_assistant.app_builder import preflight

        name = str(body.get("name") or "").strip()
        if not name:
            return {"ok": False, "issues": ["no build name given"]}
        workspace = self._workspace(name)
        if not (workspace / "src" / "app.py").exists():
            return {"ok": False, "issues": ["src/app.py not found — build first"]}
        _default_port = mc.get_int("ai.app_builder", "default_port", default=8000)
        port = int(body.get("port", _default_port) or _default_port)
        if port <= 0 or port > 65535:
            return {"ok": False, "issues": ["port must be between 1 and 65535"]}
        _host = mc.get("ai.app_builder", "host", default="127.0.0.1") or "127.0.0.1"
        # Stop any existing process for this app name.
        self.stop_app({"name": name})
        try:
            proc = subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "src.app:app",
                 "--host", _host, "--port", str(port)],
                cwd=str(workspace),
                env=preflight.launch_env(workspace),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "issues": [str(exc)]}
        if not hasattr(self, "_app_processes"):
            self._app_processes: dict[str, Any] = {}
        self._app_processes[name] = {"proc": proc, "port": port,
                                     "workspace": str(workspace)}
        url = f"http://127.0.0.1:{port}"
        return {"ok": True, "url": url, "port": port, "pid": proc.pid,
                "workspace": str(workspace)}

    def stop_app(self, body: dict) -> dict[str, Any]:
        """Stop a running generated app (best-effort)."""
        name = str(body.get("name") or "").strip()
        procs = getattr(self, "_app_processes", {}) or {}
        entry = procs.pop(name, None) if name else None
        if entry is None:
            return {"ok": True, "stopped": False, "issues": ["not running"]}
        proc = entry.get("proc")
        try:
            if proc is not None and proc.poll() is None:
                proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        return {"ok": True, "stopped": True, "name": name}

    def app_status(self, body: dict) -> dict[str, Any]:
        """Return whether a generated app is running."""
        name = str(body.get("name") or "").strip()
        entry = (getattr(self, "_app_processes", {}) or {}).get(name)
        if entry is None:
            return {"ok": True, "running": False}
        proc = entry.get("proc")
        running = proc is not None and proc.poll() is None
        if not running:
            (getattr(self, "_app_processes", {}) or {}).pop(name, None)
        return {"ok": True, "running": running, "port": entry.get("port"),
                "url": f"http://127.0.0.1:{entry.get('port', 8000)}"
                if running else ""}

    def run_agentic_build(
        self,
        body: dict,
        *,
        on_progress: Any = None,
        ask: Any = None,
        cancel_event: Any = None,
        backend: Any = None,
    ) -> dict[str, Any]:
        """Headless agentic build — shared path for TUI, Web jobs, and CLI.

        Encapsulates the Tk ``agent_build.work()`` setup: DB understanding for
        ``from_database``, deploy target, decider from interaction flags, and
        ``auto_build`` with progress/cancel hooks.
        """
        import threading

        body = dict(body)
        job_id = str(body.pop("_internal_job_id", "") or "").strip() or None
        self._active_job_id = job_id
        body.setdefault("use_ai", True)
        body.setdefault("run_tests", True)
        mask_pii = self._mask_pii_enabled(body)
        conn_name = (body.get("connections") or [""])[0]
        db_understanding = None
        db_manager = None
        deploy_dbm = None

        if body.get("mode") == "from_database" and body.get("use_ai"):
            from ai_assistant.app_builder.db_understanding import (
                DbUnderstandingClient,
            )
            from ai_query.agent import AIQueryAgent

            agent = AIQueryAgent()
            if self._core is not None and conn_name:
                try:
                    db_manager = self._core.get_manager(conn_name)
                except Exception:
                    db_manager = None
            db_understanding = DbUnderstandingClient(
                query_assistant=agent,
                db_manager=db_manager,
                connection_name=conn_name,
                user_description=body.get("description", ""),
                variant=body.get("db_app_variant", "application"),
                mask_pii=mask_pii,
            )
        if (body.get("mode") == "from_scratch"
                and conn_name and body.get("deploy_schema")):
            if self._core is not None:
                try:
                    deploy_dbm = self._core.get_manager(conn_name)
                except Exception:
                    deploy_dbm = None

        from ai_assistant.app_builder.interaction import decider_from_options

        interaction = str(body.get("interaction", "auto"))
        decider = decider_from_options(
            interaction=interaction,
            uninterrupted=interaction == "uninterrupted",
            ask=ask,
        )
        self.last_decider = decider
        if backend is None:
            backend = self._resolve_backend(body)

        if cancel_event is None:
            cancel_event = threading.Event()

        try:
            return self.auto_build(
                AutoBuildRequest(
                    body=body,
                    db_understanding=db_understanding,
                    decider=decider,
                    db_manager=deploy_dbm,
                    on_progress=on_progress,
                    backend=backend,
                    cancel_event=cancel_event,
                ),
            )
        finally:
            self._active_job_id = None

    def _resolve_backend(self, body: dict) -> Any:
        """Return the active AIBackend for agentic builds, or None."""
        try:
            from ai_query.backends import AIBackendRegistry

            reg = AIBackendRegistry()
            name = body.get("backend") or reg.get_default_name() or ""
            backend = reg.get(name) if name else None
            if backend and reg.check_one(backend.name):
                return backend
            for b in reg.list_all_backends():
                if reg.check_one(b.name):
                    return b
        except Exception:
            pass
        return None

    def auto_build(self, request: AutoBuildRequest | dict, **legacy) -> dict[str, Any]:
        """Autonomous, meter-driven build: iterate with AI until the app passes.

        Two modular AI channels (per product spec):

        * the **code agent** (``bridge``) — a direct AI agent (the "chat" backend)
          that generates app code/tests and handles non-DB investigations. Used
          for ``from_scratch`` and to write code in every mode.
        * the **AI Query Assistant** (``db_understanding``) — used *only* for
          ``from_database`` to understand the meaning/nature of the data and read
          real sample rows before building. The App Builder never mixes these:
          the AI Query Assistant is consumed as-is for data understanding only.

        Without AI it still produces and validates the deterministic baseline.
        """
        if not isinstance(request, AutoBuildRequest):
            request = AutoBuildRequest(body=request, **legacy)
        body = request.body
        bridge = request.bridge
        db_understanding = request.db_understanding
        decider = request.decider
        db_manager = request.db_manager
        on_progress = request.on_progress
        backend = request.backend
        cancel_event = request.cancel_event
        from ai_assistant.app_builder.orchestrator import (
            AppBuildOrchestrator,
            OrchestratorConfig,
            RunContext,
        )

        bp = self._blueprint(body)
        schema = body.get("schema") or {}
        introspection_status: dict[str, Any] = {}
        mask_pii = self._mask_pii_enabled(body)
        db_understanding = self._prepare_db_understanding(body, db_understanding)
        if bp.mode == BuildMode.FROM_DATABASE:
            schema, introspection_status = self._selected_schema(body, bp)
        if bp.mode == BuildMode.FROM_CODEBASE:
            body = self._prepare_codebase_insight(body, bp)
            bp.description = body.get("description", bp.description)
            bp.codebase_variant = (
                "structure_metadata" if bp.variant == EXPLORER else "predicted_app")
        # A directly-provided schema is a valid source even without a live
        # connection (mirrors BuildFlows.build_from_database).
        if bp.mode == BuildMode.FROM_DATABASE and schema and not bp.connections:
            bp.connections = ["(provided-schema)"]
        # Interaction control: callers may inject a decider (UI dialog-backed);
        # otherwise build one from the body flags (headless → fully silent).
        if decider is None:
            from ai_assistant.app_builder.interaction import decider_from_options

            decider = decider_from_options(
                interaction=str(body.get("interaction", "auto")),
                uninterrupted=bool(body.get("uninterrupted", True)),
            )
        from ai_assistant.app_builder.agent_runner import supports_agentic_write

        force_agentic = bool(body.get("agentic", False))
        # Only probe installed CLIs when an agentic (direct-write) build may run.
        # Database auto-builds are eligible too: AiQA supplies DB grounding while
        # an agentic CLI writes/validates the app through the A/B/C sessions.
        if (force_agentic or (bp.mode in (BuildMode.FROM_SCRATCH,
                                          BuildMode.FROM_CODEBASE,
                                          BuildMode.FROM_DATABASE)
                              and body.get("use_ai"))):
            if backend is None:
                backend = self._resolve_backend(body)
        # use_ai + an agentic-capable backend should use the three-session loop.
        # For FROM_DATABASE, AiQA remains the DB-understanding channel; the
        # agentic backend is only the file-writing/validation runtime.
        if (not force_agentic
                and bp.mode in (BuildMode.FROM_SCRATCH, BuildMode.FROM_CODEBASE,
                                BuildMode.FROM_DATABASE)
                and body.get("use_ai") and backend is not None
                and supports_agentic_write(backend)):
            force_agentic = True
        if force_agentic and backend is not None and not supports_agentic_write(backend):
            force_agentic = False
        if bridge is None and body.get("use_ai"):
            if force_agentic and backend is not None:
                class _AgenticStub:
                    def available(self):
                        return True
                bridge = _AgenticStub()
            elif bp.mode == BuildMode.FROM_SCRATCH:
                from ai_assistant.app_builder.ai_bridge import DirectChatBridge
                from ai_query.agent import AIQueryAgent

                bridge = DirectChatBridge(AIQueryAgent(), mask_pii=mask_pii)
            else:
                from ai_assistant.app_builder.ai_bridge import make_bridge

                bridge = make_bridge(
                    connection_name=bp.connections[0] if bp.connections else "",
                    mask_pii=mask_pii,
                )

        # Collaboration pipeline (parallel understanding + design-similarity gate
        # + meter-governed remediation via Session B) runs for agentic AI builds.
        # Callers can override with body["collaboration"].
        collaboration = bool(body.get("collaboration", force_agentic))
        default_rounds = mc.get_int(
            "ai.app_builder",
            "full_max_rounds" if bp.build_profile == FULL else "max_rounds",
            default=6 if bp.build_profile == FULL else 4,
        )
        default_target_score = mc.get_float(
            "ai.app_builder", "target_score", default=0.9)
        default_finalize_repairs = mc.get_int(
            "ai.app_builder", "max_finalize_repairs", default=2)
        orch = AppBuildOrchestrator(
            self._engine,
            config=OrchestratorConfig(
                max_rounds=int(body.get("max_rounds", default_rounds)),
                target_score=float(body.get("target_score", default_target_score)),
                validation_mode=str(body.get("validation_depth", "low_token")),
                collaboration=collaboration,
                max_finalize_repairs=int(
                    body.get("max_finalize_repairs", default_finalize_repairs)),
            ),
        )
        if bp.mode == BuildMode.FROM_CODEBASE:
            orch._codebase_profile = body.get("codebase_profile") or {}
            orch._codebase_components = body.get("codebase_components") or []
        orch._service_ref = self  # early coord exposure for mid-build UI routing
        workspace = self._workspace(bp.name)
        try:
            result = orch.run(
                bp,
                workspace,
                context=RunContext(
                    schema=schema or None,
                    bridge=bridge,
                    backend=backend,
                    db_understanding=db_understanding,
                    run_tests=bool(body.get("run_tests", False)),
                    decider=decider,
                    deploy_schema=bool(body.get("deploy_schema", False)),
                    db_manager=db_manager,
                    on_progress=on_progress,
                    force_agentic=force_agentic,
                    cancel_event=cancel_event,
                    mask_pii=mask_pii,
                ),
            )
        except Exception as exc:  # noqa: BLE001
            partial_workspace = False
            try:
                partial_workspace = workspace.exists() and any(workspace.iterdir())
            except OSError:
                pass
            return {
                "ok": False,
                "workspace": str(workspace),
                "final_score": 0.0,
                "files": [],
                "rounds": [],
                "mode": bp.mode.value,
                "used_ai": bool(body.get("use_ai")),
                "error": str(exc),
                "stop_reason": "orchestration error",
                "aborted": True,
                "partial_workspace": partial_workspace,
                "resolved_connection": introspection_status.get("connection", ""),
                "introspection_status": introspection_status,
            }
        # Expose the live A/B/C coordinator (agentic builds only) so the UI can
        # offer post-build interactive chat with the same sessions.
        self.last_coordinator = getattr(orch, "_coord", None)
        out = result.as_dict()
        if introspection_status:
            out["resolved_connection"] = introspection_status.get("connection", "")
            out["introspection_status"] = introspection_status
        train_result = self._maybe_auto_train_llm(
            body,
            workspace,
            getattr(orch, "_insight", None) or out.get("insight"),
            on_progress=on_progress,
            build_result=out,
        )
        if train_result is not None:
            out["training"] = train_result
        return out

    def _prepare_db_understanding(
        self, body: dict, db_understanding: Any,
    ) -> Any:
        """Wire db_app_builder_assistant when from_database and no client supplied."""
        if body.get("mode") != "from_database" or db_understanding is not None:
            return db_understanding
        from ai_assistant.app_builder.db_app_assistant import DbAppBuilderAssistant
        from ai_query.agent import AIQueryAgent

        conn = (body.get("connections") or [""])[0]
        agent = None
        db_manager = None
        if self._core is not None and conn:
            try:
                db_manager = self._core.get_manager(conn)
            except Exception:
                pass
        if body.get("use_ai"):
            try:
                agent = AIQueryAgent()
            except Exception:
                agent = None
        assistant = DbAppBuilderAssistant(
            query_assistant=agent,
            db_manager=db_manager,
            core=self._core,
            connection_name=conn,
            user_description=body.get("description", ""),
            variant=(
                "insights_admin"
                if self._variant(body, BuildMode.FROM_DATABASE) == EXPLORER
                else body.get("db_app_variant", "application")
            ),
            mask_pii=self._mask_pii_enabled(body),
        )
        return assistant.make_understanding_client()

    def _prepare_codebase_insight(self, body: dict, bp: AppBlueprint) -> dict:
        """Profile codebase and enrich body description before build."""
        from ai_assistant.app_builder.codebase_app_assistant import (
            CodebaseAppBuilderAssistant,
        )

        assistant = CodebaseAppBuilderAssistant(
            codebase_path=bp.codebase_path,
            user_description=body.get("description", ""),
            variant=(
                "structure_metadata" if bp.variant == EXPLORER
                else body.get("codebase_variant", "predicted_app")
            ),
        )
        insight = assistant.understand()
        assistant.prepare_blueprint(bp, insight)
        body = dict(body)
        body["description"] = bp.description
        body["codebase_profile"] = insight.profile.as_dict()
        body["codebase_components"] = insight.components
        return body

    # back-compat thin wrapper used by older callers/tests
    def scaffold_from_scratch(self, name: str) -> dict[str, Any]:
        return self.build_from_scratch({"name": name})

    def _selected_schema(
        self, body: dict, bp: AppBlueprint,
    ) -> tuple[dict[str, list[str]], dict[str, Any]]:
        """Return schema from the user-selected connection only.

        The generated prototype always runs on SQLite. The selected DB is used to
        understand/introspect the app shape; if that fails we record the failure
        and continue with a launchable SQLite prototype rather than trying other
        saved connections.
        """
        selected = (bp.connections or [""])[0]
        provided = body.get("schema") or {}
        if provided:
            return provided, {
                "ok": True,
                "connection": selected,
                "source": "provided_schema",
                "tables": len(provided),
                "runtime_fallback": "sqlite",
            }
        if not selected:
            return {}, {
                "ok": False,
                "connection": "",
                "source": "none",
                "error": "no database connection selected",
                "tables": 0,
                "runtime_fallback": "sqlite",
            }
        schema, error = self._introspect_schema(
            selected, max_tables=int(body.get("max_tables", 25)))
        return schema, {
            "ok": bool(schema) and not error,
            "connection": selected,
            "source": "selected_connection",
            "error": error,
            "tables": len(schema),
            "runtime_fallback": "sqlite",
        }

    def _introspect_schema(
        self, connection: str, *, max_tables: int = 25,
    ) -> tuple[dict[str, list[str]], str]:
        """Best-effort schema introspection via the core DB service."""
        out: dict[str, list[str]] = {}
        core = self._core
        if core is None:
            return out, "no core DB service available"
        try:
            tables = core.get_objects(connection, "tables") or []
        except Exception as exc:  # noqa: BLE001
            return out, str(exc)
        for table in list(tables)[:max_tables]:
            tname = table if isinstance(table, str) else str(table)
            try:
                info = core.get_table_schema(connection, tname)
                cols = _column_names(info.get("columns") or [])
            except Exception:
                cols = []
            out[tname.split(".")[-1]] = cols or ["id"]
        if not out:
            return out, "selected connection returned no tables"
        return out, ""


def _column_names(columns: list) -> list[str]:
    names: list[str] = []
    for c in columns:
        if isinstance(c, dict):
            names.append(str(c.get("name") or c.get("column") or c.get("Field") or "").strip())
        elif isinstance(c, (list, tuple)) and c:
            names.append(str(c[0]).strip())
        elif isinstance(c, str):
            names.append(c.strip())
    return [n for n in names if n]


def make_service(core: Optional[Any] = None) -> Any:
    from common.headless.composite import composite_service
    from common.headless.db_service import CoreDBService

    core = core or CoreDBService()
    return composite_service(core, AppBuilderService(core))
