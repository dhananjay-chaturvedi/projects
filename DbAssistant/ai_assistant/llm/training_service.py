"""Standalone orchestration for rich NL->SQL LLM training.

This module is intentionally independent from App Builder. It coordinates data
collection, optional RAG indexing, DB query mining, pair persistence, and calls
into :class:`ai_assistant.llm.service.LlmService`.
"""

from __future__ import annotations

import threading
from typing import Any, Callable


class LlmTrainingService:
    """High-level local NL->SQL training service.

    ``core`` is optional so scratch/codebase/current-pair training can ship
    without the DB module. Database mining and RAG indexing need it.
    """

    def __init__(
        self,
        core: Any = None,
        *,
        insight_provider: Callable[[dict], Any] | None = None,
    ) -> None:
        self._core = core
        self._insight_provider = insight_provider

    def llm_models(self) -> dict[str, Any]:
        from ai_assistant.llm.service import LlmService

        llm = LlmService()
        listed = llm.list_models()
        engines = llm.engines()
        return {
            "ok": bool(listed.get("ok", True)),
            "models": listed.get("models") or [],
            "engines": engines.get("engines") or [],
            "error": listed.get("error") or engines.get("error"),
        }

    def _rag_service(self):
        from ai_assistant.rag.service import RagService

        return RagService(self._core)

    def rag_status(self, connection: str = "") -> dict[str, Any]:
        try:
            return self._rag_service().status(connection)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def index_rag(self, connection: str, *, rebuild: bool = False) -> dict[str, Any]:
        if not connection:
            return {"ok": False, "error": "A connection name is required."}
        try:
            return self._rag_service().index(connection, rebuild=rebuild)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "connection": connection, "error": str(exc)}

    def _train_sample_limit(self, body: dict) -> int:
        raw = body.get("train_sample_limit")
        if raw in (None, ""):
            try:
                from ai_query import module_config as mc

                raw = mc.get_int("ai.app_builder", "train_sample_limit", default=5)
            except Exception:
                raw = 5
        try:
            return max(1, min(int(raw), 1000))
        except Exception:
            return 5

    def _train_overrides(self, connection: str = "") -> dict[str, Any]:
        db_type = ""
        if self._core is not None and connection:
            try:
                profile = self._core.get_connection_profile(connection) or {}
                db_type = profile.get("db_type", "") or ""
            except Exception:
                db_type = ""
        return {"core": self._core, "db_type": db_type, "connection": connection}

    def mine_training_pairs(self, body: dict) -> dict[str, Any]:
        conn = (body.get("connections") or [""])[0] or str(body.get("connection") or "")
        if not conn:
            return {"ok": False, "error": "A database connection is required.",
                    "pairs": [], "stats": {}}
        if self._core is None:
            return {"ok": False, "error": "No core DB service available.",
                    "pairs": [], "stats": {}}
        from ai_assistant.llm.db_query_miner import mine_connection_pairs

        return mine_connection_pairs(
            self._core,
            conn,
            sample_limit=self._train_sample_limit(body),
            max_tables=int(body.get("train_max_tables") or 40),
            max_pairs=int(body.get("train_max_pairs") or 400),
            validate=bool(body.get("validate", True)),
        )

    def train_from_connections(
        self, body: dict, *, on_progress: Callable[[dict], None] | None = None,
    ) -> dict[str, Any]:
        """Train one (or more) model(s) from SEVERAL connections in parallel.

        Each connection's validated NL->SQL pairs are collected (optionally
        concurrently) and staged into a per-connection shard file; the shards
        are then merged under a single per-model lock and the model is trained
        once on the union. This is the safe way to "train the same model from
        different connections at once" — see
        :meth:`LlmService.stage_shard` / :meth:`LlmService.commit_shards`.
        """
        from concurrent.futures import ThreadPoolExecutor

        from ai_assistant.llm.data_sources import resolve_train_names
        from ai_assistant.llm.service import LlmService

        names = resolve_train_names(body)
        if not names:
            return {"ok": False, "error": "No LLM model names selected for training.",
                    "reason": "Select an existing model or enter a new model name."}
        connections = [c for c in (body.get("connections") or []) if str(c).strip()]
        if len(connections) < 1:
            return {"ok": False, "error": "Select at least one connection.",
                    "reason": "train_from_connections needs one or more connections."}
        engine = str(body.get("train_engine") or body.get("engine") or "").strip() or None
        llm = LlmService()
        workers = max(1, min(int(body.get("gen_workers") or 1), 8))

        def _collect_and_stage(conn: str) -> dict:
            try:
                mined = self.mine_training_pairs({**body, "connections": [conn]})
                pairs = mined.get("pairs") or []
                pairs, _warn = self._live_validate_pairs(pairs, connection=conn)
                staged = 0
                for name in names:
                    r = llm.stage_shard(name, conn, pairs)
                    staged = max(staged, r.get("staged", 0))
                if on_progress:
                    on_progress({"type": "training_capture", "status": "staged",
                                 "connection": conn, "pairs": len(pairs)})
                return {"connection": conn, "staged": staged, "pairs": len(pairs)}
            except Exception as exc:  # noqa: BLE001
                return {"connection": conn, "error": str(exc), "staged": 0}

        if workers > 1 and len(connections) > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                stage_stats = list(pool.map(_collect_and_stage, connections))
        else:
            stage_stats = [_collect_and_stage(c) for c in connections]

        models: list[dict] = []
        for name in names:
            res = llm.commit_shards(
                name, engine=engine,
                overrides=self._train_overrides(connections[0]),
                progress=on_progress,
            )
            res["name"] = name
            models.append(res)
        ok = any(m.get("ok") for m in models)
        return {
            "ok": ok,
            "models": models,
            "connections": connections,
            "stage_stats": stage_stats,
            "source": "multi_connection",
            "reason": (f"staged {len(connections)} connection(s); "
                       f"committed {len(models)} model(s)"),
        }

    def _live_validate_pairs(
        self, pairs: list[dict], *, connection: str = ""
    ) -> tuple[list[dict], str | None]:
        """Reject pairs that fail live DB validation when a connection is available.

        Continues past individual failures and returns all valid pairs. Returns
        a warning message if any pairs were rejected (but does not abort).
        """
        if not pairs or not connection or self._core is None:
            return pairs, None
        from ai_assistant.llm.sql_check import check_sql

        db_type = self._train_overrides(connection).get("db_type") or ""
        validated: list[dict] = []
        rejected: list[str] = []
        for p in pairs:
            sql = (p.get("sql") or "").strip()
            if not sql:
                continue
            chk = check_sql(
                sql,
                db_type=db_type,
                core=self._core,
                connection=connection,
                explain=True,
                limit_zero=True,
            )
            if not chk.get("valid"):
                q = (p.get("question") or "")[:80]
                err = chk.get("error") or "SQL failed live validation"
                rejected.append(f"'{q}': {err}")
            else:
                validated.append({
                    **p,
                    "sql": chk.get("normalized") or sql,
                })
        if not validated:
            summary = "; ".join(rejected[:3])
            return [], (
                f"All {len(rejected)} training pair(s) failed live validation: {summary}. "
                "Fix the SQL in Generated SQL before training."
            )
        warning = None
        if rejected:
            warning = (
                f"{len(rejected)} training pair(s) rejected during live validation "
                f"({len(validated)} accepted)."
            )
        return validated, warning

    @staticmethod
    def _model_epoch_progress(on_progress: Any, model_name: str):
        """Adapt engine epoch callbacks into training_epoch progress events."""

        def _cb(ev: dict) -> None:
            if on_progress and isinstance(ev, dict) and "epoch" in ev:
                on_progress({
                    "type": "training_epoch",
                    "model": model_name,
                    "epoch": ev.get("epoch"),
                    "loss": ev.get("loss"),
                })

        return _cb

    def train_pairs(
        self,
        pairs: list[dict],
        *,
        names: list[str],
        engine: str | None = None,
        connection: str = "",
        include_sample: bool = False,
        use_rag: bool = False,
        train_mode: str = "full",
        on_progress: Any = None,
    ) -> dict[str, Any]:
        """Train selected models from explicit NL->SQL pairs."""
        from ai_assistant.llm.data_sources import _dedupe_pairs, persist_pairs
        from ai_assistant.llm.model_ledger import merge_incremental_pairs
        from ai_assistant.llm.service import LlmService

        names = [str(n).strip() for n in names if str(n).strip()]
        if not names:
            return {"ok": False, "error": "No LLM model names selected for training.",
                    "reason": "Select at least one model or enter a new model name."}
        incremental = str(train_mode or "full").strip().lower() == "incremental"
        db_type = self._train_overrides(connection).get("db_type") or None
        pairs = _dedupe_pairs(pairs, db_type=db_type)
        if not pairs and not include_sample:
            return {"ok": False, "error": "No valid NL->SQL pairs were found.",
                    "reason": "No valid NL->SQL pairs were found."}
        pairs, live_err = self._live_validate_pairs(pairs, connection=connection)
        if live_err and not pairs:
            return {"ok": False, "error": live_err, "reason": live_err}
        if not pairs and not include_sample:
            return {"ok": False, "error": "No valid NL->SQL pairs were found.",
                    "reason": "No valid NL->SQL pairs after live validation."}
        results: list[dict] = []
        ok = True
        llm = LlmService()
        already_trained = 0
        new_pairs = len(pairs)
        last_count = len(pairs)
        dataset_path = ""
        for name in names:
            fit_pairs = pairs
            if incremental:
                fit_pairs, already_trained, new_pairs = merge_incremental_pairs(
                    pairs, name, db_type=db_type,
                )
            dataset_path, count = (
                persist_pairs(connection, fit_pairs, core=self._core) if fit_pairs else ("", 0)
            )
            last_count = count
            if on_progress:
                on_progress({"type": "training_progress", "model": name, "status": "training"})
            r = llm.train(
                name=name,
                engine=engine,
                include_sample=include_sample,
                dataset_path=dataset_path or None,
                rag_connection=connection if use_rag else "",
                overrides=self._train_overrides(connection),
                progress=self._model_epoch_progress(on_progress, name),
            )
            results.append(r)
            ok = ok and bool(r.get("ok"))
        return {
            "ok": ok,
            "pairs": last_count,
            "connection": connection,
            "models": results,
            "dataset_path": dataset_path if names else "",
            "source": "explicit_pairs",
            "rag_used": use_rag,
            "include_sample": include_sample,
            "train_mode": train_mode,
            "already_trained": already_trained,
            "new_pairs": new_pairs,
            "reason": (
                f"source=explicit_pairs; pairs={last_count}; "
                f"train_mode={train_mode}; already_trained={already_trained}; "
                f"new_pairs={new_pairs}"
            ),
            "error": None if ok else "One or more models failed to train.",
        }

    def train_llm(self, body: dict, *, on_progress: Any = None) -> dict[str, Any]:
        """Train selected local NL->SQL models from build, DB, codebase, or scratch data."""
        from ai_assistant.llm.data_sources import (
            _dedupe_pairs,
            collect_build_pairs,
            collect_codebase_pairs,
            collect_connection_pairs,
            collect_scratch_pairs,
            persist_pairs,
            resolve_train_names,
        )
        from ai_assistant.llm.service import LlmService

        names = resolve_train_names(body)
        if not names:
            return {"ok": False, "error": "No LLM model names selected for training.",
                    "reason": "Select at least one model or enter a new model name."}

        train_mode = str(body.get("train_mode") or "full").strip().lower()
        incremental = train_mode == "incremental"

        conn = (body.get("connections") or [""])[0]
        mode = str(body.get("mode") or "from_database")
        engine = str(body.get("train_engine") or body.get("engine") or "").strip() or None
        insight = body.get("insight")
        from_build = bool(body.get("from_build"))
        workspace = body.get("workspace") or ""
        use_rag = bool(body.get("use_rag", False))
        index_rag = bool(body.get("index_rag", False))
        rag_strategy = str(body.get("rag_strategy") or "index_first").strip().lower()
        include_sample = bool(body.get("include_sample", False))
        mine_db = bool(body.get("mine_db", True))
        sample_limit = self._train_sample_limit(body)
        max_tables = int(body.get("train_max_tables") or 40)
        rag_indexed = False
        mine_stats: dict[str, Any] = {}
        source = "unknown"

        if on_progress:
            on_progress({"type": "training_capture", "status": "collecting"})

        if index_rag and conn:
            if rag_strategy == "parallel":
                if on_progress:
                    on_progress({"type": "training_rag", "status": "indexing_parallel",
                                 "connection": conn})

                def _bg_index() -> None:
                    res = self.index_rag(conn, rebuild=False)
                    if on_progress:
                        on_progress({
                            "type": "training_rag",
                            "status": "indexed" if res.get("ok") else "index_failed",
                            "connection": conn,
                            "result": res,
                        })

                threading.Thread(target=_bg_index, daemon=True).start()
            else:
                idx_res = self.index_rag(conn, rebuild=False)
                rag_indexed = bool(idx_res.get("ok"))
                if not rag_indexed:
                    err = idx_res.get("error") or "RAG indexing failed."
                    return {"ok": False, "error": err,
                            "reason": f"RAG index failed for '{conn}': {err}",
                            "rag_indexed": False, "rag_used": use_rag,
                            "connection": conn}

        conn_db_type = self._train_overrides(conn).get("db_type") or None
        if from_build and workspace:
            pairs = collect_build_pairs(workspace, conn, insight, db_type=conn_db_type)
            source = "build"
        elif mode == "from_codebase":
            pairs = collect_codebase_pairs(body.get("codebase_path") or "")
            source = "codebase" if pairs else "codebase_empty"
        elif mode == "from_scratch":
            pairs = collect_scratch_pairs(body.get("description") or "")
            source = "scratch" if pairs else "scratch_empty"
        else:
            pairs = []
            if mine_db and conn and self._core is not None:
                from ai_assistant.llm.db_query_miner import mine_connection_pairs

                mined = mine_connection_pairs(
                    self._core, conn, sample_limit=sample_limit,
                    max_tables=max_tables, validate=True, on_progress=on_progress,
                )
                mine_stats = mined.get("stats") or {}
                if mined.get("ok"):
                    pairs.extend(mined.get("pairs") or [])
            if insight is None and self._insight_provider is not None:
                insight = self._insight_provider(body)
            pairs.extend(collect_connection_pairs(
                conn, insight, use_rag=use_rag, db_type=conn_db_type
            ))
            extra = body.get("extra_pairs") or []
            if extra:
                pairs.extend(extra)
            # Dedupe/validate with the connection's dialect so already
            # DB-validated SQL (e.g. MariaDB backtick identifiers, SCHEMA())
            # is not silently dropped by a generic-dialect re-parse.
            pairs = _dedupe_pairs(pairs, db_type=conn_db_type)
            source = "db_mined" if mine_stats.get("kept") else "connection"

        if not pairs:
            include_sample = True
            source = "sample_seed"

        pairs, live_err = self._live_validate_pairs(pairs, connection=conn)
        if live_err and not pairs:
            return {"ok": False, "error": live_err, "reason": live_err,
                    "connection": conn, "source": source}

        dataset_path, count = persist_pairs(conn, pairs, core=self._core) if pairs else ("", 0)
        if on_progress:
            on_progress({"type": "training_capture", "status": "captured",
                         "pairs": count, "connection": conn, "source": source,
                         "include_sample": include_sample})

        from ai_assistant.llm.model_ledger import merge_incremental_pairs

        llm = LlmService()
        results: list[dict] = []
        all_ok = True
        already_trained = 0
        new_pairs = count
        last_count = count
        for name in names:
            fit_pairs = pairs
            if incremental:
                fit_pairs, already_trained, new_pairs = merge_incremental_pairs(
                    pairs, name, db_type=conn_db_type,
                )
                dataset_path, last_count = (
                    persist_pairs(conn, fit_pairs, core=self._core) if fit_pairs else ("", 0)
                )
            if on_progress:
                on_progress({"type": "training_progress", "model": name, "status": "training"})
            r = llm.train(
                name=name,
                engine=engine,
                include_sample=include_sample,
                dataset_path=dataset_path or None,
                rag_connection=conn if use_rag else "",
                overrides=self._train_overrides(conn),
                progress=self._model_epoch_progress(on_progress, name),
            )
            results.append(r)
            all_ok = all_ok and bool(r.get("ok"))

        reason_parts = [f"source={source}", f"pairs={last_count}", f"train_mode={train_mode}"]
        if incremental:
            reason_parts.append(f"already_trained={already_trained}")
            reason_parts.append(f"new_pairs={new_pairs}")
        if use_rag:
            reason_parts.append(f"rag_used={conn or 'none'}")
        if index_rag:
            reason_parts.append(f"rag_indexed={rag_indexed or rag_strategy == 'parallel'}")
        if include_sample:
            reason_parts.append("sample_fallback=yes")
        if mine_stats.get("kept"):
            reason_parts.append(
                f"db_mined={mine_stats.get('kept')} "
                f"(validated {mine_stats.get('validated', 0)}/"
                f"{mine_stats.get('candidates', 0)})")
        reason = "; ".join(reason_parts)
        if on_progress:
            on_progress({"type": "training_done", "ok": all_ok, "models": results,
                         "pairs": count, "source": source, "reason": reason,
                         "mine_stats": mine_stats})
        return {
            "ok": all_ok,
            "pairs": last_count,
            "connection": conn,
            "models": results,
            "dataset_path": dataset_path,
            "source": source,
            "rag_used": use_rag,
            "rag_indexed": rag_indexed or (index_rag and rag_strategy == "parallel"),
            "include_sample": include_sample,
            "mine_stats": mine_stats,
            "train_mode": train_mode,
            "already_trained": already_trained,
            "new_pairs": new_pairs,
            "reason": reason,
            "error": None if all_ok else "One or more models failed to train.",
        }
