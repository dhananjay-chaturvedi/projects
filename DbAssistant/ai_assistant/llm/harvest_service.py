"""Automated NL->SQL training-data harvesting.

Orchestrates an unattended loop that turns a live database connection into a
large, validated NL->SQL training corpus and trains the local models on it:

1. Collect seed questions from every source (curated corpus, an AI-generated
   question bank, DB mining, capture replay, and a user-supplied list).
2. Produce SQL for each question — either by rendering a curated template or by
   asking the backend AI agent (Cursor) one question at a time through the same
   AI Query Assistant path the user uses interactively.
3. Live-validate every query (parse + EXPLAIN + LIMIT 0).
4. Persist accepted pairs to RAG and train the selected local models.

This module is intentionally independent of :mod:`ai_query`: backend access is
injected as callables so the dependency direction stays ai_query -> ai_assistant
(mirroring :class:`ai_assistant.llm.training_service.LlmTrainingService`).
"""

from __future__ import annotations

import json
import re
import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Callable

# (connection, question) -> {"sql": str|None, "explanation": str, "error": str|None}
GenerateSqlFn = Callable[[str, str], dict]
# () -> a thread-local GenerateSqlFn (for parallel workers)
GenerateSqlFactory = Callable[[], GenerateSqlFn]
# (prompt) -> raw model text
GenerateTextFn = Callable[[str], str]
# (connection, base_question, followups) -> [{"question","sql","explanation"}, ...]
# index 0 is the base turn; the rest are the follow-up refinements in order.
RunThreadFn = Callable[[str, str, list], list]

_JSON_ARRAY_RE = re.compile(r"\[.*\]", re.DOTALL)
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


class LlmHarvestService:
    """Unattended NL->SQL harvesting + training orchestrator."""

    def __init__(
        self,
        core: Any = None,
        *,
        generate_sql_fn: GenerateSqlFn | None = None,
        generate_sql_factory: GenerateSqlFactory | None = None,
        generate_text_fn: GenerateTextFn | None = None,
        run_thread_fn: RunThreadFn | None = None,
    ) -> None:
        self._core = core
        self._generate_sql_fn = generate_sql_fn
        self._generate_sql_factory = generate_sql_factory
        self._generate_text_fn = generate_text_fn
        self._run_thread_fn = run_thread_fn
        self._thread_local = threading.local()

    def _resolve_gen_sql(self) -> GenerateSqlFn | None:
        """Return the SQL generator for the current thread."""
        if self._generate_sql_factory is not None:
            cached = getattr(self._thread_local, "gen_sql", None)
            if cached is None:
                cached = self._generate_sql_factory()
                self._thread_local.gen_sql = cached
            return cached
        return self._generate_sql_fn

    # ── helpers ──────────────────────────────────────────────────────────
    def _db_type(self, connection: str) -> str:
        if self._core is None or not connection:
            return ""
        try:
            profile = self._core.get_connection_profile(connection) or {}
            return profile.get("db_type", "") or ""
        except Exception:
            return ""

    def _schema_summary(self, connection: str, *, max_tables: int = 25) -> str:
        """Build a compact ``TABLE name(col type, ...)`` summary for prompting."""
        if self._core is None or not connection:
            return ""
        from ai_assistant.llm.db_query_miner import DbTrainingMiner

        miner = DbTrainingMiner(self._core, connection, max_tables=max_tables)
        lines: list[str] = []
        for t in miner._tables():  # noqa: SLF001 - same package reuse
            info = miner._table_info(t)  # noqa: SLF001
            cols = ", ".join(
                f"{c.name} {c.type}".strip() for c in info.columns[:20]
            )
            lines.append(f"- {info.name}({cols})" if cols else f"- {info.name}")
        return "\n".join(lines)

    # ── question bank ────────────────────────────────────────────────────
    def generate_question_bank(
        self,
        connection: str,
        *,
        complexity: list[str] | None = None,
        count: int = 50,
        on_progress: Any = None,
    ) -> dict[str, Any]:
        """Ask the backend AI agent to invent schema-grounded DB questions.

        Returns ``{"ok": bool, "questions": [...], "error": str|None}``. Each
        question is later fed one-by-one through the AI Query Assistant to
        produce (and validate) its SQL.
        """
        if self._generate_text_fn is None:
            return {"ok": False, "questions": [],
                    "error": "No backend text generator available for question bank."}
        if self._core is None or not connection:
            return {"ok": False, "questions": [],
                    "error": "A database connection is required."}

        levels = complexity or ["basic", "advanced", "complex"]
        count = max(1, min(int(count or 50), 500))
        schema = self._schema_summary(connection)
        if not schema:
            return {"ok": False, "questions": [],
                    "error": "Could not read the schema to generate questions."}

        db_type = self._db_type(connection) or "SQL"
        if on_progress:
            on_progress({"type": "harvest_question_bank", "status": "generating",
                         "count": count})

        prompt = self._question_bank_prompt(db_type, schema, levels, count)
        try:
            raw = self._generate_text_fn(prompt) or ""
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "questions": [], "error": f"Question generation failed: {exc}"}

        questions = self._parse_questions(raw, limit=count)
        if on_progress:
            on_progress({"type": "harvest_question_bank", "status": "generated",
                         "questions": len(questions)})
        return {
            "ok": bool(questions),
            "questions": questions,
            "error": None if questions else "Backend returned no usable questions.",
        }

    @staticmethod
    def _question_bank_prompt(
        db_type: str, schema: str, levels: list[str], count: int
    ) -> str:
        lvl = ", ".join(levels)
        return (
            f"You are generating a training set of natural-language questions for a "
            f"{db_type} database. Using ONLY the real schema below, write {count} "
            f"diverse, answerable, READ-ONLY analytical questions a data analyst "
            f"might ask. Cover these difficulty levels: {lvl} "
            f"(basic = single-table counts/filters/projections; advanced = "
            f"grouping, aggregation, distinct, ordering/top-N; complex = joins "
            f"across related tables, subqueries, window functions, date bucketing).\n\n"
            f"Rules:\n"
            f"- Reference only the real tables and columns shown.\n"
            f"- Each question must be answerable with a single SELECT query.\n"
            f"- Vary the phrasing; no two questions should be near-duplicates.\n"
            f"- Do NOT write any SQL. Output ONLY a JSON array of question strings.\n\n"
            f"SCHEMA:\n{schema}\n\n"
            f"Output (JSON array of {count} strings):"
        )

    @staticmethod
    def _parse_questions(raw: str, *, limit: int) -> list[str]:
        """Extract a clean, de-duplicated list of NL questions from model text."""
        from ai_assistant.llm.validation import (
            clean_question,
            normalize_question_for_match,
        )

        text = (raw or "").strip()
        candidates: list[str] = []

        # Prefer a JSON array (optionally inside a code fence).
        fence = _CODE_FENCE_RE.search(text)
        json_blob = fence.group(1) if fence else text
        m = _JSON_ARRAY_RE.search(json_blob)
        if m:
            try:
                arr = json.loads(m.group(0))
                if isinstance(arr, list):
                    candidates = [str(x) for x in arr if isinstance(x, (str, int, float))]
            except Exception:
                candidates = []

        # Fallback: one question per line (strip bullets/numbering/quotes).
        if not candidates:
            for line in text.splitlines():
                line = line.strip().strip(",")
                line = re.sub(r"^\s*(?:[-*\u2022]|\d+[.)])\s*", "", line)
                line = line.strip().strip('"').strip("'").strip()
                if len(line) >= 8 and "?" in line or (line and line.lower().startswith(
                    ("how ", "what ", "which ", "list ", "show ", "count ", "find ", "give "))):
                    candidates.append(line)

        out: list[str] = []
        seen: set[str] = set()
        for c in candidates:
            q = clean_question(c)
            if len(q) < 8:
                continue
            key = normalize_question_for_match(q)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(q)
            if len(out) >= limit:
                break
        return out

    # ── validation ───────────────────────────────────────────────────────
    def _validate_pairs(
        self,
        pairs: list[dict],
        *,
        connection: str,
        db_type: str,
        conn_by_dbtype: dict[str, str] | None = None,
    ) -> tuple[list[dict], int]:
        """Keep only pairs whose SQL passes validation, dropping bad ones individually.

        ``conn_by_dbtype`` maps a normalized db_type -> a live connection of
        that type. A pair whose dialect has a matching live connection is
        verified with a real dry-run (EXPLAIN / LIMIT 0); otherwise it is
        parse-validated. This is resilient (unlike the all-or-nothing
        training_service._live_validate_pairs) so harvesting never aborts on a
        single bad query.
        """
        from ai_assistant.llm.dataset import normalize_db_type
        from ai_assistant.llm.sql_check import check_sql

        # Default routing: the schema-source connection validates its own dialect.
        routing: dict[str, str] = dict(conn_by_dbtype or {})
        if not routing and connection and db_type:
            routing[normalize_db_type(db_type)] = connection

        kept: list[dict] = []
        rejected = 0
        for p in pairs:
            sql = (p.get("sql") or "").strip()
            if not sql:
                rejected += 1
                continue
            pair_db = p.get("db_type") or db_type
            pair_db_norm = normalize_db_type(pair_db)
            if pair_db_norm in ("mongodb", "documentdb"):
                mongo_conn = routing.get(pair_db_norm) or routing.get("mongodb") or ""
                if mongo_conn and self._core is not None:
                    chk = check_sql(
                        sql, db_type=pair_db, core=self._core, connection=mongo_conn,
                        explain=True, limit_zero=True,
                    )
                    if not chk.get("valid") and not chk.get("parse_ok"):
                        rejected += 1
                        continue
                kept.append({**p, "sql": sql, "db_type": pair_db})
                continue
            val_conn = routing.get(pair_db_norm, "")
            live_for_pair = bool(val_conn and self._core is not None)
            chk = check_sql(
                sql, db_type=pair_db,
                core=self._core if live_for_pair else None,
                connection=val_conn if live_for_pair else "",
                explain=live_for_pair, limit_zero=live_for_pair,
            )
            if live_for_pair:
                if not chk.get("valid"):
                    rejected += 1
                    continue
            elif not chk.get("parse_ok") and not chk.get("valid"):
                rejected += 1
                continue
            row = {**p, "sql": chk.get("normalized") or sql}
            if pair_db:
                row["db_type"] = pair_db
            kept.append(row)
        return kept, rejected

    def _try_generate_pair(
        self,
        connection: str,
        question: str,
        *,
        description: str,
        gen_timeout: int,
        gen_retries: int,
    ) -> dict[str, Any]:
        """Attempt SQL generation with bounded retries; never raises."""
        gen_fn = self._resolve_gen_sql()
        if gen_fn is None:
            return {"ok": False, "question": question, "description": description,
                    "error": "no generator"}
        attempts = max(0, int(gen_retries)) + 1
        last_err = ""
        for _ in range(attempts):
            try:
                with ThreadPoolExecutor(max_workers=1) as one:
                    fut = one.submit(gen_fn, connection, question)
                    res = fut.result(timeout=max(1, int(gen_timeout))) or {}
            except Exception as exc:  # noqa: BLE001
                res = {}
                last_err = str(exc)
                continue
            sql = (res.get("sql") or "").strip()
            if res.get("error") or not sql:
                last_err = str(res.get("error") or "empty SQL")
                continue
            return {
                "ok": True,
                "pair": {
                    "question": question,
                    "sql": sql,
                    "description": (res.get("explanation") or description or "")[:240],
                    "explanation": (res.get("explanation") or description or "")[:500],
                    "db_type": self._db_type(connection),
                },
                "retried": attempts > 1,
            }
        return {"ok": False, "question": question, "description": description,
                "error": last_err or "generation failed"}

    def _syntax_variants(
        self,
        connection: str,
        question: str,
        base_sql: str,
        *,
        count: int = 2,
    ) -> list[dict]:
        """Ask the backend for alternative valid SQL syntaxes for the same question."""
        if self._generate_text_fn is None or not base_sql:
            return []
        db_type = self._db_type(connection) or "SQL"
        prompt = (
            f"USER QUESTION: {question}\n"
            f"Database type: {db_type}\n"
            f"Existing SQL:\n{base_sql}\n\n"
            f"Provide up to {count} alternative read-only SQL queries that answer "
            f"the same question using different but valid {db_type} syntax. "
            f"Output ONLY a JSON array of SQL strings."
        )
        try:
            raw = self._generate_text_fn(prompt) or ""
        except Exception:
            return []
        variants: list[str] = []
        fence = _CODE_FENCE_RE.search(raw)
        json_blob = fence.group(1) if fence else raw
        m = _JSON_ARRAY_RE.search(json_blob)
        if m:
            try:
                arr = json.loads(m.group(0))
                if isinstance(arr, list):
                    variants = [str(x).strip() for x in arr if isinstance(x, (str, int, float))]
            except Exception:
                variants = []
        out: list[dict] = []
        for alt in variants:
            if alt.strip().upper().startswith(("SELECT", "WITH", "DB.")):
                out.append({
                    "question": question,
                    "sql": alt.strip(),
                    "description": "syntax_variant",
                    "explanation": f"Alternative {db_type} syntax for: {question}",
                    "db_type": db_type,
                })
        return out

    def _generate_pairs_for_questions(
        self,
        connection: str,
        items: list[tuple[str, str]],
        *,
        on_progress: Any = None,
        should_stop: Any = None,
        gen_workers: int = 1,
        gen_timeout: int = 120,
        gen_retries: int = 1,
        max_consecutive_failures: int = 0,
        multi_syntax: bool = False,
    ) -> dict[str, Any]:
        """Generate SQL for each (question, description) via the AI assistant.

        Returns a dict with ``pairs``, ``failed`` (backlog candidates),
        ``retried``, ``circuit_broken``, and ``skipped`` counts.
        """
        if not items or self._resolve_gen_sql() is None:
            return {"pairs": [], "failed": [], "retried": 0,
                    "circuit_broken": False, "skipped": 0}

        stop = should_stop if callable(should_stop) else (lambda: False)
        workers = max(1, int(gen_workers or 1))
        total = len(items)
        pairs: list[dict] = []
        failed: list[dict] = []
        retried = 0
        skipped = 0
        circuit_broken = False
        consecutive_failures = 0
        done_count = 0
        breaker_limit = int(max_consecutive_failures or 0)

        def _emit(status: str, **extra: Any) -> None:
            if on_progress:
                on_progress({
                    "type": "harvest_generate",
                    "status": status,
                    "done": done_count,
                    "total": total,
                    "kept": len(pairs),
                    **extra,
                })

        def _handle_result(result: dict[str, Any], question: str) -> None:
            nonlocal done_count, retried, skipped, consecutive_failures, circuit_broken
            done_count += 1
            if result.get("ok"):
                pairs.append(result["pair"])
                if multi_syntax:
                    variants = self._syntax_variants(
                        connection,
                        result["pair"]["question"],
                        result["pair"]["sql"],
                    )
                    if variants:
                        vkept, _ = self._validate_pairs(
                            variants, connection=connection, db_type=self._db_type(connection),
                        )
                        pairs.extend(vkept)
                if result.get("retried"):
                    retried += 1
                consecutive_failures = 0
                _emit("generated", question=question)
                return
            skipped += 1
            consecutive_failures += 1
            failed.append({
                "question": result.get("question") or question,
                "description": result.get("description") or "",
                "error": result.get("error") or "",
            })
            _emit("skipped", question=question)
            if breaker_limit > 0 and consecutive_failures >= breaker_limit:
                circuit_broken = True

        if workers <= 1:
            for q, desc in items:
                if stop():
                    _emit("stopped")
                    break
                if circuit_broken:
                    break
                _emit("generating", question=q)
                _handle_result(
                    self._try_generate_pair(
                        connection, q, description=desc,
                        gen_timeout=gen_timeout, gen_retries=gen_retries,
                    ),
                    q,
                )
            return {
                "pairs": pairs, "failed": failed, "retried": retried,
                "circuit_broken": circuit_broken, "skipped": skipped,
            }

        pending: dict[Future, tuple[str, str]] = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            item_iter = iter(items)
            exhausted = False

            def _submit_next() -> None:
                nonlocal exhausted
                if exhausted or circuit_broken or stop():
                    return
                try:
                    q, desc = next(item_iter)
                except StopIteration:
                    exhausted = True
                    return
                _emit("generating", question=q)
                fut = pool.submit(
                    self._try_generate_pair,
                    connection,
                    q,
                    description=desc,
                    gen_timeout=gen_timeout,
                    gen_retries=gen_retries,
                )
                pending[fut] = (q, desc)

            for _ in range(min(workers, total)):
                _submit_next()

            while pending:
                done_set, _ = wait(set(pending.keys()), return_when=FIRST_COMPLETED)
                for fut in done_set:
                    q, _desc = pending.pop(fut)
                    try:
                        result = fut.result()
                    except Exception as exc:  # noqa: BLE001
                        result = {
                            "ok": False, "question": q, "description": _desc,
                            "error": str(exc),
                        }
                    _handle_result(result, q)
                    if not (circuit_broken or stop()):
                        _submit_next()
                if stop() or circuit_broken:
                    break

            if stop() and on_progress:
                on_progress({
                    "type": "harvest_generate",
                    "status": "stopped",
                    "done": done_count,
                    "total": total,
                    "kept": len(pairs),
                })

        return {
            "pairs": pairs, "failed": failed, "retried": retried,
            "circuit_broken": circuit_broken, "skipped": skipped,
        }

    # ── orchestration ────────────────────────────────────────────────────
    def harvest(
        self,
        body: dict,
        *,
        on_progress: Any = None,
        should_stop: Any = None,
    ) -> dict[str, Any]:
        """Run the full harvest: collect seeds, generate+validate SQL, train.

        Body fields (all optional except a connection):
          connection / connections, train_llm (model names), train_new_name,
          train_engine, complexity (list), generated_questions (int),
          max_questions (int), questions (user list), use_captures (bool),
          use_curated (bool), mine_db (bool), do_train (bool, default True),
          use_rag (bool, default True).

        *should_stop* is an optional ``Callable[[], bool]`` for graceful
        cancellation. It is honoured only at safe checkpoints — between backend
        questions and at phase boundaries — and NEVER mid-training, so a model
        write always completes and the saved model is never left half-written.
        """
        from ai_assistant.llm.dataset import normalize_db_type
        from ai_assistant.llm.data_sources import _dedupe_pairs

        stop = should_stop if callable(should_stop) else (lambda: False)

        # Accept one or many connections. Advanced training spans every selected
        # connection; the first is the primary schema source.
        conns = [str(c).strip() for c in (body.get("connections") or []) if str(c).strip()]
        if not conns:
            single = str(body.get("connection") or "").strip()
            if single:
                conns = [single]
        if not conns:
            return {"ok": False, "error": "A database connection is required."}
        conn = conns[0]
        if self._core is None:
            return {"ok": False, "error": "No core DB service available."}

        # Map each selected connection's dialect -> connection so every dialect
        # with a live target can be dry-run validated (syntax-accuracy boost).
        conn_by_dbtype: dict[str, str] = {}
        for c in conns:
            dt = normalize_db_type(self._db_type(c))
            if dt and dt not in conn_by_dbtype:
                conn_by_dbtype[dt] = c

        db_type = self._db_type(conn)
        complexity = body.get("complexity") or ["basic", "advanced", "complex"]
        use_curated = bool(body.get("use_curated", True))
        use_captures = bool(body.get("use_captures", True))
        use_followups = bool(body.get("followups", True))
        mine_db = bool(body.get("mine_db", False))
        do_train = bool(body.get("do_train", True))
        use_rag = bool(body.get("use_rag", True))
        train_mode = str(body.get("train_mode") or "full").strip().lower()
        incremental = train_mode == "incremental"
        advanced_training = bool(
            body.get("advanced_training")
            or body.get("advanced_full")
            or body.get("advanced_incremental")
        )
        # Advanced ALWAYS means all DB dialects (multi-dialect template training).
        use_multi_dialect = advanced_training or bool(body.get("multi_dialect"))
        # Training depth: "offline" (templates/mining/captures only — no backend
        # AI; falls back to template training) or "online" (also asks the backend
        # AI agent). Only an EXPLICIT "offline" suppresses backend AI; when the
        # field is absent we keep the historical behaviour (AI runs if requested)
        # so existing callers are unaffected.
        depth_raw = str(body.get("training_depth") or "").strip().lower()
        online = depth_raw != "offline"
        depth = "offline" if not online else ("online" if depth_raw == "online" else "online")
        # Offline depth falls back to template training: no backend AI calls,
        # no multi-syntax AI variants. Multi-dialect templates still apply.
        multi_syntax = online and bool(body.get("multi_syntax", advanced_training))
        gen_count = int(body.get("generated_questions") or 0) if online else 0
        max_questions = int(body.get("max_questions") or 200)
        user_questions = [str(q).strip() for q in (body.get("questions") or []) if str(q).strip()]
        questions_file = str(body.get("questions_file") or "").strip()
        if questions_file:
            try:
                from ai_assistant.llm.question_import import load_questions_from_file

                user_questions.extend(load_questions_from_file(questions_file))
            except Exception as exc:  # noqa: BLE001
                return {"ok": False, "error": f"Failed to load questions file: {exc}"}
        gen_workers = max(1, int(body.get("gen_workers") or 1))
        gen_timeout = max(1, int(body.get("gen_timeout") or 120))
        gen_retries = max(0, int(body.get("gen_retries") or 1))
        retry_backlog = bool(body.get("retry_backlog", True))
        max_consecutive_failures = int(body.get("max_consecutive_failures") or 0)
        template_mode = str(body.get("template_mode") or "both").strip().lower()

        names = list(body.get("train_llm") or [])
        if body.get("train_new_name"):
            names.append(str(body["train_new_name"]).strip())
        names = [n for n in names if str(n).strip()]
        primary_model = names[0] if names else str(body.get("train_new_name") or "default")

        from ai_assistant.llm.model_ledger import (
            known_question_keys,
            load_backlog,
            save_backlog,
        )
        from ai_assistant.llm.validation import normalize_question_for_match

        backlog_items = load_backlog(primary_model) if retry_backlog else []
        known_keys: set[str] = (
            known_question_keys(names) if incremental and names else set()
        )
        skipped_known = 0

        # Backend work is opt-in AND online-only (offline depth = template
        # training, no AI). Backlog items also trigger generation when enabled.
        backend_requested = online and bool(self._resolve_gen_sql()) and (
            gen_count > 0 or bool(user_questions) or bool(backlog_items)
        )

        sources: dict[str, int] = {}
        offline_pairs: list[dict] = []
        rejected_total = 0

        # 1) Curated corpus: template pairs (pre-validated) + generate problems.
        #    Collected across EVERY selected connection so advanced training
        #    learns real objects from each database, not just the primary.
        gen_problems: list[dict] = []
        if use_curated:
            from ai_assistant.llm.seed_corpus import render_seed_pairs

            tmpl_total = 0
            for c in conns:
                rendered = render_seed_pairs(
                    self._core, c, db_type=self._db_type(c), complexity=complexity,
                    template_mode=template_mode,
                )
                tmpl_pairs = rendered.get("pairs") or []
                offline_pairs.extend(tmpl_pairs)
                tmpl_total += len(tmpl_pairs)
                # Generate-problems (for online follow-up threads) come from the
                # primary connection only to avoid combinatorial blow-up.
                if c == conn:
                    gen_problems = rendered.get("generate_problems") or []
            sources["curated_template"] = tmpl_total

        if use_multi_dialect:
            from ai_assistant.llm.dialect_corpus import collect_multi_dialect_pairs

            sample_limit = int(body.get("sample_limit") or 5)
            max_tables = int(body.get("max_tables") or 40)
            md = collect_multi_dialect_pairs(
                self._core,
                conn,
                connected_db_type=db_type,
                sample_limit=sample_limit,
                max_tables=max_tables,
                include_mongo=True,
                conn_by_dbtype=conn_by_dbtype,
                template_mode=template_mode,
            )
            md_pairs = md.get("pairs") or []
            offline_pairs.extend(md_pairs)
            sources["multi_dialect"] = len(md_pairs)

        # 2) DB mining (offline, optional) — across every selected connection.
        if mine_db:
            from ai_assistant.llm.db_query_miner import mine_connection_pairs

            mined_total = 0
            for c in conns:
                mined = mine_connection_pairs(self._core, c, validate=True)
                mp = mined.get("pairs") or []
                offline_pairs.extend(mp)
                mined_total += len(mp)
            sources["db_mined"] = mined_total

        # 3) Capture replay (offline; existing chats/followups, carrying explanation).
        if use_captures:
            from ai_assistant.llm.data_sources import _pairs_from_capture

            cap_total = 0
            for c in conns:
                cap = _pairs_from_capture(c)
                offline_pairs.extend(cap)
                cap_total += len(cap)
            sources["capture_replay"] = cap_total

        # 4) Validate and train the offline corpus first. This gives the user a
        # usable model quickly even if optional backend generation takes a while.
        offline_kept, rejected_offline = self._validate_pairs(
            offline_pairs, connection=conn, db_type=db_type,
            conn_by_dbtype=conn_by_dbtype,
        )
        rejected_total += rejected_offline
        offline_kept = _dedupe_pairs(offline_kept, db_type=db_type or None)
        if on_progress:
            on_progress({
                "type": "harvest_offline_collected",
                "pairs": len(offline_kept),
                "rejected": rejected_offline,
                "sources": dict(sources),
            })

        result: dict[str, Any] = {
            "ok": bool(offline_kept),
            "connection": conn,
            "connections": conns,
            "db_type": db_type,
            "training_depth": depth,
            "advanced": advanced_training,
            "multi_dialect": use_multi_dialect,
            "dialect_connections": dict(conn_by_dbtype),
            "pairs": len(offline_kept),
            "offline_pairs": len(offline_kept),
            "backend_pairs": 0,
            "rejected": rejected_total,
            "sources": sources,
            "trained": False,
            "offline_trained": False,
            "backend_enhanced": False,
            "stopped": False,
            "circuit_broken": False,
            "models": [],
            "train_mode": train_mode,
            "already_trained": 0,
            "new_pairs": len(offline_kept),
            "skipped_known": 0,
            "retried": 0,
            "backlog_pending": len(backlog_items),
            "error": None if offline_kept else "No valid training pairs harvested.",
        }

        from ai_assistant.llm.training_service import LlmTrainingService

        trainer = LlmTrainingService(self._core)

        def train_collected(pairs: list[dict], *, phase: str) -> dict[str, Any]:
            if not pairs or not do_train:
                return {"ok": False, "reason": "No training requested."}
            if on_progress:
                on_progress({"type": "harvest_train_start", "phase": phase,
                             "pairs": len(pairs), "models": names})
            train_res = trainer.train_pairs(
                pairs,
                names=names,
                engine=str(body.get("train_engine") or "").strip() or None,
                connection=conn,
                include_sample=False,
                use_rag=use_rag,
                train_mode=train_mode,
                on_progress=on_progress,
            )
            if on_progress:
                on_progress({"type": "harvest_train_done", "phase": phase,
                             "ok": bool(train_res.get("ok")),
                             "models": train_res.get("models") or [],
                             "reason": train_res.get("reason") or train_res.get("error")})
            return train_res

        if offline_kept and do_train:
            train_res = train_collected(offline_kept, phase="offline")
            result["trained"] = bool(train_res.get("ok"))
            result["offline_trained"] = bool(train_res.get("ok"))
            result["models"] = train_res.get("models") or []
            result["train_reason"] = train_res.get("reason") or train_res.get("error")
            result["already_trained"] = int(train_res.get("already_trained") or 0)
            result["new_pairs"] = int(train_res.get("new_pairs") or len(offline_kept))
            if not train_res.get("ok"):
                result["error"] = train_res.get("error") or "Training failed."

        # If backend work was not requested, stop here. This is the fast path
        # behind the UI's "AI questions = 0" behaviour.
        if not backend_requested:
            sources.setdefault("backend_generated", 0)
            sources.setdefault("followup_turns", 0)
            result["harvested_pairs"] = offline_kept
            return result

        # Graceful-stop checkpoint: if cancelled after offline training but
        # before any backend work, keep the offline model and return now.
        if stop():
            sources.setdefault("backend_generated", 0)
            sources.setdefault("followup_turns", 0)
            result["stopped"] = True
            result["harvested_pairs"] = offline_kept
            if on_progress:
                on_progress({"type": "harvest_stopped", "phase": "before_backend",
                             "pairs": len(offline_kept)})
            return result

        if on_progress:
            on_progress({"type": "harvest_backend_start",
                         "generated_questions": gen_count,
                         "user_questions": len(user_questions)})

        # 5) Backend question bank (AI invents schema-grounded questions).
        bank_questions: list[str] = []
        if gen_count > 0 and not stop():
            bank = self.generate_question_bank(
                conn, complexity=complexity, count=gen_count, on_progress=on_progress,
            )
            bank_questions = bank.get("questions") or []

        # 3) Generate-mode curated problems. When the problem has follow-ups and
        #    a thread runner is available, run an uninterrupted follow-up thread
        #    (base + refinements); otherwise fall back to one-shot. In both cases
        #    the base SQL is shared across all paraphrases (exact-recall payoff).
        gen_pairs: list[dict] = []
        followup_pairs = 0
        to_generate: list[tuple[str, str]] = []  # (question, description)

        # Retry backlog is attempted before fresh bank/user/curated questions.
        for item in backlog_items:
            to_generate.append((
                item["question"],
                item.get("description") or "backlog",
            ))

        active_problems = gen_problems if gen_count > 0 else []
        followup_total = sum(
            1 for prob in active_problems
            if use_followups and prob.get("followups") and prob.get("prompts")
            and self._run_thread_fn is not None
        )
        followup_done = 0
        for prob in active_problems:
            if stop():
                break
            prompts = prob.get("prompts") or []
            if not prompts:
                continue
            desc = prob.get("category") or prob.get("id") or ""
            followups = prob.get("followups") or []
            if (
                use_followups and followups
                and self._run_thread_fn is not None
            ):
                followup_done += 1
                if on_progress:
                    on_progress({
                        "type": "harvest_followup",
                        "status": "running",
                        "done": followup_done,
                        "total": followup_total,
                        "category": desc,
                        "question": prompts[0],
                    })
                try:
                    thread = self._run_thread_fn(conn, prompts[0], followups) or []
                except Exception:
                    thread = []
                if thread:
                    base_sql = (thread[0].get("sql") or "").strip()
                    if base_sql:
                        for pr in prompts:
                            gen_pairs.append({"question": pr, "sql": base_sql,
                                              "description": desc})
                    for turn in thread[1:]:
                        sql = (turn.get("sql") or "").strip()
                        if sql:
                            gen_pairs.append({
                                "question": (turn.get("question") or "").strip(),
                                "sql": sql,
                                "description": (turn.get("explanation") or "followup")[:240],
                            })
                            followup_pairs += 1
                    continue
            # Fallback: one-shot each paraphrase.
            for pr in prompts:
                to_generate.append((pr, desc))

        # Backend question bank + user-supplied questions (one-shot each).
        for q in bank_questions:
            to_generate.append((q, "ai_question_bank"))
        for q in user_questions:
            to_generate.append((q, "user_supplied"))

        # Cap backend calls.
        if max_questions and len(to_generate) > max_questions:
            to_generate = to_generate[:max_questions]

        # Incremental: skip questions already in the model ledger.
        if incremental and known_keys:
            filtered: list[tuple[str, str]] = []
            for q, desc in to_generate:
                key = normalize_question_for_match(q)
                if key and key in known_keys:
                    skipped_known += 1
                    continue
                filtered.append((q, desc))
            to_generate = filtered
        result["skipped_known"] = skipped_known

        gen_failed: list[dict] = []
        retried_total = 0
        circuit_broken = False

        if on_progress:
            on_progress({
                "type": "harvest_generate",
                "status": "planned",
                "done": 0,
                "total": len(to_generate),
                "kept": 0,
                "workers": gen_workers,
            })

        # Parallel/serial generation through the AI Query Assistant.
        if to_generate and self._resolve_gen_sql() is not None and not stop():
            gen_out = self._generate_pairs_for_questions(
                conn,
                to_generate,
                on_progress=on_progress,
                should_stop=stop,
                gen_workers=gen_workers,
                gen_timeout=gen_timeout,
                gen_retries=gen_retries,
                max_consecutive_failures=max_consecutive_failures,
                multi_syntax=multi_syntax,
            )
            gen_pairs.extend(gen_out.get("pairs") or [])
            gen_failed = list(gen_out.get("failed") or [])
            retried_total = int(gen_out.get("retried") or 0)
            circuit_broken = bool(gen_out.get("circuit_broken"))

        # Update retry backlog: drop successes, append new failures.
        if retry_backlog and primary_model:
            backlog_map = {
                normalize_question_for_match(x["question"]): x
                for x in load_backlog(primary_model)
            }
            for p in gen_pairs:
                key = normalize_question_for_match(p.get("question", ""))
                if key:
                    backlog_map.pop(key, None)
            for f in gen_failed:
                key = normalize_question_for_match(f.get("question", ""))
                if key:
                    backlog_map[key] = {
                        "question": f["question"],
                        "description": f.get("description") or "",
                    }
            save_backlog(primary_model, list(backlog_map.values()))
            result["backlog_pending"] = len(backlog_map)

        result["retried"] = retried_total
        result["circuit_broken"] = circuit_broken
        sources["backend_generated"] = len(gen_pairs)
        sources["followup_turns"] = followup_pairs
        # Record whether cancellation cut backend generation short. Whatever was
        # collected before the stop is still validated and trained below — the
        # final training write always runs to completion (never interrupted).
        was_stopped = bool(stop())

        # 6) Live-validate backend additions and optionally retrain using the
        #    enriched union. Offline training has already completed by now.
        backend_kept, rejected_backend = self._validate_pairs(
            gen_pairs, connection=conn, db_type=db_type,
            conn_by_dbtype=conn_by_dbtype,
        )
        rejected_total += rejected_backend
        kept = list(offline_kept) + list(backend_kept)
        kept = _dedupe_pairs(kept, db_type=db_type or None)

        if on_progress:
            on_progress({"type": "harvest_collected", "pairs": len(kept),
                         "rejected": rejected_total, "sources": sources})

        result.update({
            "ok": bool(kept),
            "pairs": len(kept),
            "backend_pairs": len(backend_kept),
            "rejected": rejected_total,
            "sources": sources,
            "stopped": was_stopped,
            "error": None if kept else "No valid training pairs harvested.",
        })
        if not kept or not do_train or not backend_kept:
            result["harvested_pairs"] = kept
            return result

        train_res = train_collected(kept, phase="backend_enhanced")
        result["trained"] = bool(train_res.get("ok")) or result["trained"]
        result["backend_enhanced"] = bool(train_res.get("ok"))
        result["models"] = train_res.get("models") or []
        result["train_reason"] = train_res.get("reason")
        result["already_trained"] = int(train_res.get("already_trained") or result["already_trained"])
        result["new_pairs"] = int(train_res.get("new_pairs") or result["new_pairs"])
        if not train_res.get("ok"):
            result["error"] = train_res.get("error") or "Training failed."
        return result
