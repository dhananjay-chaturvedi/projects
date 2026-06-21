"""
LlmService — the single shared code path for the tiny LLM.

UI / CLI / API all call into this class (parity rule). Responsibilities:
    * assemble a training dataset (built-in sample + JSONL + RAG examples)
    * train via a pluggable engine (python / numpy / pytorch / ollama)
    * report status of the saved model
    * generate SQL for a question with the trained model
    * export NL->SQL pairs (incl. the RAG feedback loop) to JSONL

The trained model is saved under ``<session>/llm/<name>/`` with ``meta.json``
recording which engine trained it.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Optional

from ai_query import module_config as mc
from ai_assistant.llm.dataset import (
    SAMPLE_PAIRS,
    load_jsonl,
    save_jsonl,
)
from ai_assistant.llm.engines import available_engines, get_engine, resolve_engine


def models_root() -> Path:
    from common import paths as _paths

    return _paths.session_dir() / "llm"


class LlmService:
    def __init__(self, *, models_dir: str | Path | None = None):
        self._root = Path(models_dir) if models_dir else models_root()

    # ── paths ───────────────────────────────────────────────────────────

    def _model_dir(self, name: str = "default") -> Path:
        from common.security.paths import assert_safe_name, resolve_under

        safe = assert_safe_name(name or "default", label="model name")
        return resolve_under(self._root, safe)

    # Artifacts that make up a trained model (any engine).
    _MODEL_ARTIFACTS = (
        "model.pt", "model.json", "model.npz", "tokenizer.json",
        "dataset.jsonl", "meta.json", "ollama_meta.json",
    )

    def _versions_dir(self, mdir: Path) -> Path:
        return mdir / ".versions"

    def _snapshot_model(self, mdir: Path, *, reason: str = "manual") -> str | None:
        """Copy the current model artifacts into ``.versions/<timestamp>/``.

        Returns the snapshot directory path, or ``None`` when there is nothing
        to snapshot. Old snapshots beyond the retention count are pruned.
        """
        import shutil

        try:
            if not mdir.exists():
                return None
            present = [a for a in self._MODEL_ARTIFACTS if (mdir / a).exists()]
            if not present:
                return None
            ts = time.strftime("%Y%m%d-%H%M%S")
            vdir = self._versions_dir(mdir) / ts
            # Avoid clobbering a snapshot taken in the same second.
            suffix = 0
            while vdir.exists():
                suffix += 1
                vdir = self._versions_dir(mdir) / f"{ts}-{suffix}"
            vdir.mkdir(parents=True, exist_ok=True)
            for a in present:
                shutil.copy2(mdir / a, vdir / a)
            (vdir / "snapshot.json").write_text(
                json.dumps({
                    "reason": reason,
                    "created": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "artifacts": present,
                }, indent=2),
                encoding="utf-8",
            )
            self._prune_versions(mdir)
            return str(vdir)
        except Exception:  # noqa: BLE001
            return None

    def _prune_versions(self, mdir: Path) -> None:
        import shutil

        keep = max(0, mc.get_int("ai.llm.versions", "keep", default=5))
        vroot = self._versions_dir(mdir)
        if not vroot.exists():
            return
        snaps = sorted(
            (p for p in vroot.iterdir() if p.is_dir()),
            key=lambda p: p.stat().st_mtime,
        )
        for old in snaps[:-keep] if keep else snaps:
            try:
                shutil.rmtree(old, ignore_errors=True)
            except OSError:
                pass

    def _restore_snapshot(self, mdir: Path, snapshot: str | Path) -> bool:
        """Copy artifacts from a snapshot directory back into the model dir."""
        import shutil

        try:
            vdir = Path(snapshot)
            if not vdir.is_absolute():
                vdir = self._versions_dir(mdir) / vdir.name
            if not vdir.exists():
                return False
            for a in self._MODEL_ARTIFACTS:
                src = vdir / a
                if src.exists():
                    tmp = mdir / (a + ".restore.tmp")
                    shutil.copy2(src, tmp)
                    import os as _os
                    _os.replace(tmp, mdir / a)
            return True
        except Exception:  # noqa: BLE001
            return False

    def list_versions(self, name: str = "default") -> list[dict[str, Any]]:
        """List saved snapshots for *name*, newest first."""
        mdir = self._model_dir(name)
        vroot = self._versions_dir(mdir)
        out: list[dict[str, Any]] = []
        if not vroot.exists():
            return out
        for p in sorted(vroot.iterdir(), key=lambda d: d.name, reverse=True):
            if not p.is_dir():
                continue
            info: dict[str, Any] = {"version": p.name}
            meta = p / "snapshot.json"
            if meta.exists():
                try:
                    info.update(json.loads(meta.read_text(encoding="utf-8")))
                except Exception:  # noqa: BLE001
                    pass
            out.append(info)
        return out

    def restore_version(self, name: str, version: str) -> dict[str, Any]:
        """Roll *name* back to a saved snapshot (snapshots the current state first)."""
        from common.concurrency import file_lock

        mdir = self._model_dir(name)
        vdir = self._versions_dir(mdir) / version
        if not vdir.exists():
            return {"ok": False, "error": f"No version '{version}' for model '{name}'."}
        with file_lock(mdir / ".train"):
            # Snapshot current state so a restore is itself reversible.
            self._snapshot_model(mdir, reason="pre-restore")
            ok = self._restore_snapshot(mdir, vdir)
        if not ok:
            return {"ok": False, "error": "Restore failed."}
        return {"ok": True, "name": name, "restored": version}

    def _read_meta(self, name: str) -> dict[str, Any] | None:
        p = self._model_dir(name) / "meta.json"
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return None

    def _engine_config(self, overrides: dict | None = None) -> dict[str, Any]:
        cfg = {
            "context": mc.get_int("ai.llm", "context", default=0),
            "max_context": mc.get_int("ai.llm", "max_context", default=40),
            "emb_dim": mc.get_int("ai.llm", "emb_dim", default=12),
            "hidden": mc.get_int("ai.llm", "hidden", default=48),
            "epochs": mc.get_int("ai.llm", "epochs", default=150),
            "batch_size": mc.get_int("ai.llm", "batch_size", default=32),
            "lr": mc.get_float("ai.llm", "lr", default=0.02),
            "seed": mc.get_int("ai.llm", "seed", default=1234),
            "min_loss": mc.get_float("ai.llm", "min_loss", default=0.05),
            "log_every": mc.get_int("ai.llm", "log_every", default=10),
            "min_freq": mc.get_int("ai.llm", "min_freq", default=1),
            "no_repeat_ngram": mc.get_int("ai.llm", "no_repeat_ngram", default=3),
            "repetition_penalty": mc.get_float("ai.llm", "repetition_penalty", default=1.2),
            "top_k": mc.get_int("ai.llm", "top_k", default=0),
            # Stage 2 (pytorch)
            "pt_block_size": mc.get_int("ai.llm", "pt_block_size", default=0),
            "pt_n_layer": mc.get_int("ai.llm", "pt_n_layer", default=2),
            "pt_n_head": mc.get_int("ai.llm", "pt_n_head", default=2),
            "pt_n_embd": mc.get_int("ai.llm", "pt_n_embd", default=64),
            "pt_dropout": mc.get_float("ai.llm", "pt_dropout", default=0.0),
            "pt_batch_size": mc.get_int("ai.llm", "pt_batch_size", default=16),
            "pt_lr": mc.get_float("ai.llm", "pt_lr", default=3e-4),
            "pt_max_iters": mc.get_int("ai.llm", "pt_max_iters", default=500),
            "pt_grad_clip": mc.get_float("ai.llm", "pt_grad_clip", default=1.0),
            "pt_device": mc.get("ai.llm", "pt_device", default="auto"),
            # Stage 3 (ollama)
            "ollama_host": mc.get("ai.llm", "ollama_host", default="http://localhost:11434"),
            "ollama_model": mc.get("ai.llm", "ollama_model", default="qwen2.5-coder:1.5b"),
            "ollama_timeout": mc.get_int("ai.llm", "ollama_timeout", default=120),
        }
        for k, v in (overrides or {}).items():
            if v is not None:
                cfg[k] = v
        return cfg

    @staticmethod
    def _pick_head(n_embd: int, max_head: int) -> int:
        """Largest head count (<= max_head) that divides n_embd evenly."""
        for h in (8, 6, 4, 2, 1):
            if h <= max_head and n_embd % h == 0:
                return h
        return 1

    def _apply_adaptive_capacity(
        self, cfg: dict[str, Any], *, num_pairs: int, overrides: dict | None,
    ) -> dict[str, Any]:
        """Scale PyTorch capacity with corpus size between the pt_* floor and
        the ``[ai.llm.capacity]`` ceilings.

        A tiny corpus keeps the small (fast, memorizing) floor; a large corpus
        grows toward the ceilings so the model has capacity to generalize.
        Explicit ``pt_*`` overrides are always respected (never downsized).
        Returns a dict describing the chosen capacity (also stored in meta).
        """
        chosen = {
            "adaptive": False,
            "num_pairs": int(num_pairs),
            "pt_n_layer": cfg.get("pt_n_layer"),
            "pt_n_head": cfg.get("pt_n_head"),
            "pt_n_embd": cfg.get("pt_n_embd"),
            "pt_block_size": cfg.get("pt_block_size"),
            "pt_max_iters": cfg.get("pt_max_iters"),
        }
        try:
            if not mc.get_bool("ai.llm.capacity", "adaptive", default=True):
                return chosen
        except Exception:
            return chosen
        ov = overrides or {}
        small = mc.get_int("ai.llm.capacity", "small_pairs", default=200)
        medium = mc.get_int("ai.llm.capacity", "medium_pairs", default=1000)
        large = mc.get_int("ai.llm.capacity", "large_pairs", default=4000)
        # Fraction of the floor->ceiling range to use for this corpus size.
        if num_pairs < small:
            frac = 0.0
        elif num_pairs < medium:
            frac = 1.0 / 3.0
        elif num_pairs < large:
            frac = 2.0 / 3.0
        else:
            frac = 1.0
        if frac <= 0.0:
            return chosen

        def _scale(floor_val: int, ceil_val: int) -> int:
            if ceil_val <= floor_val:
                return floor_val
            return int(round(floor_val + frac * (ceil_val - floor_val)))

        c_layer = mc.get_int("ai.llm.capacity", "max_n_layer", default=6)
        c_head = mc.get_int("ai.llm.capacity", "max_n_head", default=8)
        c_embd = mc.get_int("ai.llm.capacity", "max_n_embd", default=384)
        c_block = mc.get_int("ai.llm.capacity", "max_block_size", default=512)
        c_iters = mc.get_int("ai.llm.capacity", "max_iters_cap", default=4000)

        if ov.get("pt_n_layer") is None:
            cfg["pt_n_layer"] = max(int(cfg.get("pt_n_layer", 2)),
                                    _scale(int(cfg.get("pt_n_layer", 2)), c_layer))
        if ov.get("pt_n_embd") is None:
            n_embd = max(int(cfg.get("pt_n_embd", 64)),
                         _scale(int(cfg.get("pt_n_embd", 64)), c_embd))
            # Round to a multiple of 8 so head-splitting stays clean.
            n_embd = max(8, (n_embd // 8) * 8)
            cfg["pt_n_embd"] = n_embd
        if ov.get("pt_n_head") is None:
            max_head = min(c_head, _scale(int(cfg.get("pt_n_head", 2)), c_head) or c_head)
            cfg["pt_n_head"] = self._pick_head(int(cfg["pt_n_embd"]), max(1, max_head))
        if ov.get("pt_block_size") is None and int(cfg.get("pt_block_size", 0)) > 0:
            cfg["pt_block_size"] = max(int(cfg.get("pt_block_size", 0)),
                                       _scale(int(cfg.get("pt_block_size", 0)), c_block))
        if ov.get("pt_max_iters") is None:
            cfg["pt_max_iters"] = max(int(cfg.get("pt_max_iters", 500)),
                                      _scale(int(cfg.get("pt_max_iters", 500)), c_iters))
        chosen.update({
            "adaptive": True,
            "fraction": round(frac, 3),
            "pt_n_layer": cfg.get("pt_n_layer"),
            "pt_n_head": cfg.get("pt_n_head"),
            "pt_n_embd": cfg.get("pt_n_embd"),
            "pt_block_size": cfg.get("pt_block_size"),
            "pt_max_iters": cfg.get("pt_max_iters"),
        })
        return chosen

    def _resolve_for_train(
        self, engine: str | None
    ) -> tuple[Any, str, bool]:
        preferred = engine or mc.get("ai.llm", "engine", default="pytorch")
        fallback = mc.get("ai.llm", "engine_fallback", default="python")
        return resolve_engine(preferred, fallback)

    def _resolve_for_model(self, name: str, engine: str | None) -> tuple[Any, str]:
        if engine:
            eng = get_engine(engine)
            if eng is None:
                raise ValueError(f"Unknown engine: {engine}")
            ok, reason = eng.is_available()
            if not ok:
                raise RuntimeError(reason)
            return eng, engine
        meta = self._read_meta(name)
        if meta and meta.get("engine"):
            eng_name = str(meta["engine"]).strip().lower()
            eng = get_engine(eng_name)
            if eng is None:
                raise ValueError(
                    f"Unknown engine '{eng_name}' recorded for model '{name}'."
                )
            ok, reason = eng.is_available()
            if not ok:
                raise RuntimeError(
                    f"Model '{name}' was trained with the '{eng_name}' engine, "
                    f"which is unavailable: {reason}"
                )
            return eng, eng_name
        mdir = self._model_dir(name)
        for eng_name in ("pytorch", "numpy", "ollama", "python"):
            if not self._is_trained(mdir, eng_name):
                continue
            eng = get_engine(eng_name)
            if eng is None:
                raise ValueError(
                    f"Unknown engine '{eng_name}' detected for model '{name}'."
                )
            ok, reason = eng.is_available()
            if not ok:
                raise RuntimeError(
                    f"Detected '{eng_name}' artifacts for model '{name}', "
                    f"but that engine is unavailable: {reason}"
                )
            return eng, eng_name
        return self._resolve_for_train(None)[:2]  # type: ignore[return-value]

    def _trained_engine_name(self, name: str, engine: str | None) -> str:
        if engine:
            return str(engine).strip().lower()
        meta = self._read_meta(name)
        if meta and meta.get("engine"):
            return str(meta["engine"]).strip().lower()
        mdir = self._model_dir(name)
        for eng_name in ("pytorch", "numpy", "ollama", "python"):
            if self._is_trained(mdir, eng_name):
                return eng_name
        return "python"

    def _is_trained(self, model_dir: Path, engine_name: str) -> bool:
        if (model_dir / "meta.json").exists():
            return True
        artifacts = {
            "python": ["model.json"],
            "numpy": ["model.npz"],
            "pytorch": ["model.pt"],
            "ollama": ["ollama_meta.json"],
        }
        for fname in artifacts.get(engine_name, ["model.json"]):
            if (model_dir / fname).exists():
                return True
        return False

    # ── dataset assembly ────────────────────────────────────────────────

    def collect_pairs(
        self,
        *,
        include_sample: bool = True,
        dataset_path: str | None = None,
        rag_connection: str = "",
        db_type: str = "",
    ) -> list[dict]:
        from ai_assistant.llm.validation import validate_pairs

        pairs: list[dict] = []
        if include_sample:
            pairs.extend(SAMPLE_PAIRS)
        if dataset_path:
            pairs.extend(load_jsonl(dataset_path))
        if rag_connection:
            pairs.extend(self._rag_examples(rag_connection))
        kept, _stats = validate_pairs(pairs, db_type=db_type or None)
        return kept

    @staticmethod
    def _rag_examples(connection: str) -> list[dict]:
        """Pull NL->SQL pairs a RAG scope can teach the model.

        Includes both user-authored ``example`` docs and the generic
        ``analytical`` patterns seeded from the analytics library, so a freshly
        seeded scope can train the model on common analytical idioms.
        """
        try:
            from ai_assistant.rag.documents import TRAINABLE_KINDS
            from ai_assistant.rag.service import default_index_path
            from ai_assistant.rag.vector_store import SqliteVectorStore

            store = SqliteVectorStore(default_index_path())
            out = []
            for doc in store.load_documents(connection):
                if doc.kind not in TRAINABLE_KINDS:
                    continue
                meta = doc.metadata or {}
                q, s = meta.get("question"), meta.get("sql")
                if q and s:
                    out.append({"question": q, "sql": s})
            return out
        except Exception:
            return []

    # ── operations ──────────────────────────────────────────────────────

    def engines(self) -> dict[str, Any]:
        return {"ok": True, "engines": available_engines(), "error": None}

    def train(
        self,
        *,
        name: str = "default",
        engine: str | None = None,
        include_sample: bool = True,
        dataset_path: str | None = None,
        rag_connection: str = "",
        overrides: dict | None = None,
        progress: Optional[Callable[[dict], None]] = None,
    ) -> dict[str, Any]:
        try:
            pairs = self.collect_pairs(
                include_sample=include_sample,
                dataset_path=dataset_path,
                rag_connection=rag_connection,
                db_type=str((overrides or {}).get("db_type") or ""),
            )
            if not pairs:
                return {"ok": False, "error": "No training data found."}

            from ai_assistant.llm.dataset import normalize_db_type, tag_question

            train_db_type = str((overrides or {}).get("db_type") or "")
            prepared: list[dict] = []
            for p in pairs:
                row = dict(p)
                dt = row.get("db_type") or train_db_type
                if dt:
                    row["question"] = tag_question(row["question"], dt)
                    row["db_type"] = normalize_db_type(dt)
                prepared.append(row)
            pairs = prepared

            eng, engine_used, did_fallback = self._resolve_for_train(engine)
            cfg = self._engine_config(overrides)
            cfg["model_name"] = name
            # Scale model capacity with corpus size (pytorch only; harmless to
            # others which ignore pt_* keys).
            capacity = self._apply_adaptive_capacity(
                cfg, num_pairs=len(pairs), overrides=overrides,
            )
            mdir = self._model_dir(name)
            mdir.mkdir(parents=True, exist_ok=True)

            # Serialise concurrent training of the SAME model (e.g. parallel
            # trainings from different connections). Without this, two writers
            # interleave model artifacts / dataset.jsonl / meta.json and leave a
            # corrupt mix. The lock makes them run one-after-another; combine
            # data across connections with stage_shard()/commit_shards().
            from common.concurrency import atomic_write_text, file_lock

            train_progress = progress
            if progress:
                def train_progress(ev: dict) -> None:  # type: ignore[no-redef]
                    if isinstance(ev, dict) and "epoch" in ev:
                        progress({
                            "type": "training_epoch",
                            "model": name,
                            "epoch": ev.get("epoch"),
                            "loss": ev.get("loss"),
                        })
                    else:
                        progress(ev)
                progress({"type": "training_progress", "model": name, "status": "training"})

            with file_lock(mdir / ".train"):
                # Pre-train snapshot: capture the current artifacts so a failed
                # or interrupted retrain can be rolled back, and so users can
                # restore an earlier model version on demand.
                snapshot = self._snapshot_model(mdir, reason="pre-train")
                try:
                    metrics = eng.train(pairs, mdir, config=cfg, progress=train_progress)
                except Exception:
                    # Training blew up mid-write — restore the last good model.
                    if snapshot:
                        self._restore_snapshot(mdir, snapshot)
                    raise
                # Persist the exact validated pairs the model was trained on so
                # the user can verify what's "in" the model (see :meth:`dataset`).
                try:
                    lines = "".join(
                        json.dumps({
                            "question": p.get("question", ""),
                            "sql": p.get("sql", ""),
                            "description": p.get("description", "")
                            or p.get("category", ""),
                            **({"explanation": p["explanation"]} if p.get("explanation") else {}),
                            **({"db_type": p["db_type"]} if p.get("db_type") else {}),
                        }) + "\n"
                        for p in pairs
                    )
                    atomic_write_text(mdir / "dataset.jsonl", lines, lock=False)
                except Exception:
                    pass
                meta = {
                    "name": name,
                    "engine": engine_used,
                    "engine_requested": engine or mc.get("ai.llm", "engine", default="pytorch"),
                    "engine_fallback": did_fallback,
                    "trained_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "capacity": capacity,
                    **metrics,
                }
                eval_metrics = self._run_post_train_eval(
                    name=name,
                    pairs=pairs,
                    train_metrics=metrics,
                    connection=rag_connection or str((overrides or {}).get("connection") or ""),
                    db_type=str((overrides or {}).get("db_type") or ""),
                    core=(overrides or {}).get("core"),
                    executor=(overrides or {}).get("executor"),
                )
                if eval_metrics:
                    meta["eval"] = eval_metrics
                atomic_write_text(
                    mdir / "meta.json", json.dumps(meta, indent=2), lock=False)
            return {
                "ok": True,
                "name": name,
                "path": str(mdir),
                "engine": engine_used,
                "engine_requested": meta["engine_requested"],
                "engine_fallback": did_fallback,
                "eval": eval_metrics,
                **metrics,
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    # ── parallel training: per-shard staging + locked merge-on-commit ─────────
    def _shard_dir(self, name: str) -> Path:
        return self._model_dir(name) / "shards"

    def stage_shard(self, name: str, shard_id: str, pairs: list[dict]) -> dict[str, Any]:
        """Append validated pairs to a per-shard file for model *name*.

        Each parallel producer (e.g. one per DB connection) writes its OWN
        ``shards/<shard_id>.jsonl`` — fully parallel-safe, no clobbering — then
        :meth:`commit_shards` merges them into the model under one lock. This is
        how the same model can be trained from several connections at once.
        """
        from common.concurrency import append_jsonl_locked

        shard_id = "".join(c if c.isalnum() or c in "-_." else "_"
                           for c in str(shard_id or "shard"))[:80] or "shard"
        rows = [{
            "question": p.get("question", ""),
            "sql": p.get("sql", ""),
            "description": p.get("description", "") or p.get("category", ""),
        } for p in (pairs or []) if p.get("sql")]
        if not rows:
            return {"ok": False, "error": "No pairs to stage.", "staged": 0}
        path = self._shard_dir(name) / f"{shard_id}.jsonl"
        n = append_jsonl_locked(path, rows)
        return {"ok": True, "staged": n, "shard": shard_id, "path": str(path)}

    def list_shards(self, name: str) -> list[str]:
        d = self._shard_dir(name)
        return sorted(p.stem for p in d.glob("*.jsonl")) if d.exists() else []

    def commit_shards(
        self,
        name: str,
        *,
        engine: str | None = None,
        include_existing: bool = True,
        include_sample: bool = False,
        overrides: dict | None = None,
        progress: Optional[Callable[[dict], None]] = None,
        clear: bool = True,
    ) -> dict[str, Any]:
        """Merge all staged shards (+ existing dataset) and retrain *name* once.

        Runs under the per-model lock so it is safe even while producers are
        still staging. Returns the train result augmented with shard stats.
        """
        from ai_assistant.llm.data_sources import _dedupe_pairs
        from ai_assistant.llm.dataset import save_jsonl
        from common.concurrency import file_lock

        mdir = self._model_dir(name)
        mdir.mkdir(parents=True, exist_ok=True)
        db_type = str((overrides or {}).get("db_type") or "") or None
        # Serialise concurrent commits on a SEPARATE lock from ".train" — the
        # inner train() takes ".train" itself, so reusing it here would deadlock.
        with file_lock(mdir / ".commit"):
            merged: list[dict] = []
            shard_dir = self._shard_dir(name)
            shard_files = sorted(shard_dir.glob("*.jsonl")) if shard_dir.exists() else []
            for sf in shard_files:
                for line in sf.read_text(encoding="utf-8", errors="replace").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        merged.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
            if include_existing and (mdir / "dataset.jsonl").exists():
                for line in (mdir / "dataset.jsonl").read_text(
                        encoding="utf-8", errors="replace").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        merged.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
            merged = _dedupe_pairs(merged, db_type=db_type)
            if not merged and not include_sample:
                return {"ok": False, "error": "No staged pairs to commit.",
                        "shards": [p.stem for p in shard_files]}
            # Persist merged corpus to a temp dataset and train from it.
            import tempfile
            fd, tmp = tempfile.mkstemp(suffix=".jsonl", prefix="shard_merge_")
            import os as _os
            _os.close(fd)
            save_jsonl(Path(tmp), merged)
            result = self.train(
                name=name, engine=engine, include_sample=include_sample,
                dataset_path=tmp, overrides=overrides, progress=progress,
            )
            try:
                _os.unlink(tmp)
            except OSError:
                pass
            if result.get("ok") and clear:
                for sf in shard_files:
                    try:
                        sf.unlink()
                    except OSError:
                        pass
            result["merged_pairs"] = len(merged)
            result["committed_shards"] = [p.stem for p in shard_files]
            return result

    def _run_post_train_eval(
        self,
        *,
        name: str,
        pairs: list[dict],
        train_metrics: dict,
        connection: str = "",
        db_type: str = "",
        core: Any = None,
        executor: Any = None,
    ) -> dict | None:
        try:
            from ai_assistant.llm.eval import evaluate_model

            def _gen(q: str) -> str:
                r = self.generate(
                    q, name=name, connection=connection, db_type=db_type,
                    core=core, live={"executor": executor},
                )
                return (r.get("sql") or "") if r.get("ok") else ""

            result = evaluate_model(
                pairs=pairs,
                generate_fn=_gen,
                connection=connection,
                db_type=db_type,
                executor=executor,
                train_metrics=train_metrics,
            )
            return result.get("summary")
        except Exception:
            return None

    def evaluate(
        self,
        *,
        name: str = "default",
        connection: str = "",
        db_type: str = "",
        core: Any = None,
        executor: Any = None,
        depth: str | None = None,
        dataset_path: str | None = None,
        include_sample: bool = False,
        rag_connection: str = "",
    ) -> dict[str, Any]:
        """Run accuracy meters against a trained model."""
        try:
            from ai_assistant.llm.eval import evaluate_model, load_history

            pairs = self.collect_pairs(
                include_sample=include_sample,
                dataset_path=dataset_path,
                rag_connection=rag_connection or connection,
                db_type=db_type,
            )

            def _gen(q: str) -> str:
                r = self.generate(
                    q, name=name, connection=connection, db_type=db_type,
                    core=core, live={"executor": executor},
                )
                return (r.get("sql") or "") if r.get("ok") else ""

            result = evaluate_model(
                pairs=pairs,
                generate_fn=_gen,
                connection=connection,
                db_type=db_type,
                executor=executor,
                depth=depth,
            )
            result["history"] = load_history(connection) if connection else []
            return result
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def status(self, name: str = "default") -> dict[str, Any]:
        mdir = self._model_dir(name)
        meta_path = mdir / "meta.json"
        if not meta_path.exists():
            return {"ok": True, "name": name, "trained": False, "meta": None}
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            eng_name = meta.get("engine", "python")
            eng = get_engine(eng_name)
            extra = eng.status(mdir) if eng else {}
            merged = {**meta, **{k: v for k, v in extra.items() if k not in meta}}
            return {
                "ok": True,
                "name": name,
                "trained": True,
                "engine": eng_name,
                "path": str(mdir),
                "meta": merged,
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def dataset(
        self,
        name: str = "default",
        *,
        query: str = "",
        limit: int = 0,
    ) -> dict[str, Any]:
        """Return the validated NL->SQL pairs a model was actually trained on.

        Lets the user verify a specific question/SQL is "in" the model. When
        ``query`` is given, only pairs whose question / SQL / description contain
        it (case-insensitive) are returned, along with a ``matched`` flag.
        """
        mdir = self._model_dir(name)
        ds_path = mdir / "dataset.jsonl"
        if not ds_path.exists():
            return {
                "ok": True,
                "name": name,
                "available": False,
                "total": 0,
                "pairs": [],
                "matched": False,
                "error": None,
                "reason": (
                    "No saved training dataset for this model. Retrain it so the "
                    "exact pairs are recorded (older models predate this feature)."
                ),
            }
        try:
            pairs: list[dict] = []
            for line in ds_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if obj.get("question") and obj.get("sql"):
                    pairs.append({
                        "question": obj.get("question", ""),
                        "sql": obj.get("sql", ""),
                        "description": obj.get("description", ""),
                    })
            total = len(pairs)
            q = (query or "").strip().lower()
            if q:
                pairs = [
                    p for p in pairs
                    if q in p["question"].lower()
                    or q in p["sql"].lower()
                    or q in (p.get("description") or "").lower()
                ]
            matched = bool(q) and bool(pairs)
            if limit and limit > 0:
                pairs = pairs[:limit]
            return {
                "ok": True,
                "name": name,
                "available": True,
                "total": total,
                "shown": len(pairs),
                "matched": matched,
                "pairs": pairs,
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "name": name, "error": str(exc), "pairs": []}

    def _exact_recall_pair(self, name: str, question: str) -> dict[str, str] | None:
        """Return saved SQL + explanation when *question* matches a trained pair."""
        from ai_assistant.llm.validation import normalize_question_for_match

        norm = normalize_question_for_match(question)
        if not norm:
            return None
        ds_path = self._model_dir(name) / "dataset.jsonl"
        if not ds_path.exists():
            return None
        try:
            for line in ds_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                q = obj.get("question") or ""
                sql = obj.get("sql") or ""
                if not q or not sql:
                    continue
                if normalize_question_for_match(q) == norm:
                    out = {"sql": str(sql).strip()}
                    expl = (obj.get("explanation") or obj.get("description") or "").strip()
                    if expl:
                        out["explanation"] = expl
                    return out
        except Exception:
            return None
        return None

    def _exact_recall_sql(self, name: str, question: str) -> str | None:
        """Return saved SQL when *question* exactly matches a trained pair."""
        recalled = self._exact_recall_pair(name, question)
        return recalled.get("sql") if recalled else None

    def _recall_alternatives(self, name: str, question: str) -> list[dict[str, str]]:
        """Return every saved pair whose question matches (deduped by SQL).

        One natural-language question can map to several valid SQL syntaxes
        (e.g. across dialects, or different phrasings of the same query). The
        trainer keeps all such ``(question, sql)`` pairs; this surfaces them as
        alternatives at query time (opt-in via ``generate(alternatives=True)``).
        """
        from ai_assistant.llm.validation import normalize_question_for_match

        norm = normalize_question_for_match(question)
        if not norm:
            return []
        ds_path = self._model_dir(name) / "dataset.jsonl"
        if not ds_path.exists():
            return []
        out: list[dict[str, str]] = []
        seen: set[str] = set()
        try:
            for line in ds_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                q = (obj.get("question") or "").strip()
                sql = (obj.get("sql") or "").strip()
                if not q or not sql:
                    continue
                if normalize_question_for_match(q) != norm:
                    continue
                if sql in seen:
                    continue
                seen.add(sql)
                row = {"sql": sql}
                expl = (obj.get("explanation") or obj.get("description") or "").strip()
                if expl:
                    row["explanation"] = expl
                if obj.get("db_type"):
                    row["db_type"] = str(obj["db_type"])
                out.append(row)
        except Exception:  # noqa: BLE001
            return out
        return out

    def _template_fallback_pair(self, question: str, db_type: str) -> dict[str, str] | None:
        """Best-effort catalog template match when neural generation fails."""
        from ai_assistant.llm.dataset import extract_db_type_tag, normalize_db_type, tag_question
        from ai_assistant.llm.query_templates import all_catalog_pairs, template_explanation

        tagged = tag_question(question, db_type)
        _, bare = extract_db_type_tag(tagged)
        bare_l = (bare or question or "").strip().lower()
        if not bare_l:
            return None
        target = normalize_db_type(db_type)
        best_sql = ""
        best_q = ""
        best_cat = ""
        for cat in all_catalog_pairs():
            if normalize_db_type(cat.get("db_type", "")) != target:
                continue
            cq = (cat.get("question") or "").lower()
            if bare_l == cq or bare_l in cq or cq in bare_l:
                best_sql = cat.get("sql") or ""
                best_q = cat.get("question") or bare
                best_cat = cat.get("category", "catalog")
                break
        if not best_sql:
            return None
        return {
            "sql": best_sql,
            "explanation": template_explanation(
                best_q, best_sql, db_type=db_type, category=best_cat,
            ),
        }

    def list_models(self) -> dict[str, Any]:
        try:
            models = []
            if self._root.exists():
                for d in sorted(self._root.iterdir()):
                    if not d.is_dir():
                        continue
                    meta_path = d / "meta.json"
                    if meta_path.exists():
                        meta = json.loads(meta_path.read_text(encoding="utf-8"))
                        models.append({
                            "name": d.name,
                            "engine": meta.get("engine", "python"),
                            "trained_at": meta.get("trained_at", ""),
                        })
                    elif any(
                        (d / f).exists()
                        for f in ("model.json", "model.npz", "model.pt", "ollama_meta.json")
                    ):
                        eng = "python"
                        if (d / "model.npz").exists():
                            eng = "numpy"
                        elif (d / "model.pt").exists():
                            eng = "pytorch"
                        elif (d / "ollama_meta.json").exists():
                            eng = "ollama"
                        models.append({"name": d.name, "engine": eng, "trained_at": ""})
            return {"ok": True, "models": models, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "models": [], "error": str(exc)}

    def _apply_placeholder_resolution(
        self,
        sql: str,
        question: str,
        *,
        connection: str,
        db_type: str,
        core: Any,
        executor: Any,
        db_manager: Any = None,
        ai_pick_fn: Any = None,
    ) -> tuple[str, dict[str, Any]]:
        """Substitute PH_* tokens using live schema when enabled."""
        from ai_assistant.llm.placeholder_resolver import has_placeholders, resolve

        mode = (
            mc.get("ai.llm", "placeholder_resolution", default="deterministic") or "deterministic"
        ).strip().lower()
        if mode == "off" or not has_placeholders(sql):
            return sql, {}
        pick = ai_pick_fn if mode == "ai_fallback" else None
        res = resolve(
            sql, question,
            core=core, connection=connection, db_type=db_type,
            executor=executor, db_manager=db_manager, ai_pick_fn=pick,
        )
        extra: dict[str, Any] = {
            "mappings": res.get("mappings") or {},
            "resolved": bool(res.get("resolved")),
            "ambiguous": bool(res.get("ambiguous")),
            "resolution_confidence": res.get("confidence", 0.0),
            "resolution": res.get("resolution", ""),
        }
        if res.get("candidates"):
            extra["resolution_candidates"] = res["candidates"]
        if res.get("ok") and res.get("sql"):
            return res["sql"], extra
        if res.get("error"):
            extra["resolution_error"] = res["error"]
        return sql, extra

    def generate(
        self,
        question: str,
        *,
        name: str = "default",
        engine: str | None = None,
        max_new: int = 0,
        temperature: float = 0.0,
        connection: str = "",
        db_type: str = "",
        core: Any = None,
        alternatives: bool = False,
        live: dict | None = None,
    ) -> dict[str, Any]:
        # Live execution context (kept as one bag to stay within the param
        # budget): ``executor`` for dry-runs, ``db_manager`` for live schema,
        # ``ai_pick_fn`` for AI placeholder disambiguation.
        live = live or {}
        executor = live.get("executor")
        db_manager = live.get("db_manager")
        ai_pick_fn = live.get("ai_pick_fn")
        if not (question or "").strip():
            return {"ok": False, "error": "Empty question.", "sql": None, "valid": False}
        mdir = self._model_dir(name)
        eng_name = self._trained_engine_name(name, engine)
        if not (mdir / "meta.json").exists() and not self._is_trained(mdir, eng_name):
            return {
                "ok": False,
                "sql": None,
                "valid": False,
                "error": f"Model '{name}' not trained yet. Train it first.",
            }
        try:
            from ai_assistant.llm.dataset import tag_question
            from ai_assistant.llm.sql_check import check_sql

            eng, engine_used = self._resolve_for_model(name, engine)
            tagged_question = tag_question(question, db_type) if db_type else question

            recalled_pair = self._exact_recall_pair(name, tagged_question)
            if recalled_pair and recalled_pair.get("sql"):
                recalled = recalled_pair["sql"]
                recalled, ph_extra = self._apply_placeholder_resolution(
                    recalled, question,
                    connection=connection, db_type=db_type, core=core,
                    executor=executor, db_manager=db_manager,
                    ai_pick_fn=ai_pick_fn,
                )
                chk = check_sql(
                    recalled,
                    db_type=db_type,
                    core=core,
                    connection=connection,
                    executor=executor,
                    explain=bool((connection and core) or executor),
                    limit_zero=bool((connection and core) or executor),
                )
                sql = chk.get("normalized") or recalled
                out = {
                    "ok": True,
                    "sql": sql,
                    "valid": bool(chk.get("valid")),
                    "parse_ok": bool(chk.get("parse_ok")),
                    "reason": chk.get("error") or "",
                    "explanation": recalled_pair.get("explanation") or "",
                    "attempts": [{
                        "attempt": 0,
                        "sql": sql,
                        "parse_ok": chk.get("parse_ok"),
                        "valid": chk.get("valid"),
                        "error": chk.get("error"),
                        "source": "exact_recall",
                    }],
                    "name": name,
                    "engine": engine_used,
                    "recalled": True,
                    "error": None,
                    **ph_extra,
                }
                if alternatives:
                    alts = [
                        a for a in self._recall_alternatives(name, tagged_question)
                        if (a.get("sql") or "").strip() != (sql or "").strip()
                    ]
                    out["alternatives"] = alts
                return out

            params = self._engine_config()
            params["max_new"] = max_new or mc.get_int("ai.llm", "max_new_tokens", default=512)
            params["temperature"] = temperature if temperature is not None else mc.get_float(
                "ai.llm", "temperature", default=0.0
            )
            attempts: list[dict] = []
            best_sql = ""
            best_valid = False
            best_reason = ""
            best_explanation = ""

            for attempt_idx in range(2):
                attempt_params = dict(params)
                if attempt_idx == 1:
                    # Repair pass: tighten the repetition guard. A *smaller*
                    # no_repeat_ngram blocks short (2-token) loops like
                    # `" . " . "`; a larger one would only catch longer repeats.
                    attempt_params["no_repeat_ngram"] = 2
                    attempt_params["repetition_penalty"] = max(
                        float(params.get("repetition_penalty", 1.3)), 1.6
                    )
                    attempt_params["top_k"] = max(int(params.get("top_k", 0)), 8)
                    if float(attempt_params.get("temperature", 0.0)) <= 0.0:
                        attempt_params["temperature"] = 0.3
                out = eng.generate(tagged_question, mdir, params=attempt_params)
                sql = (out.get("sql") or "").strip()
                sql, ph_extra = self._apply_placeholder_resolution(
                    sql, question,
                    connection=connection, db_type=db_type, core=core,
                    executor=executor, db_manager=db_manager,
                    ai_pick_fn=ai_pick_fn,
                )
                chk = check_sql(
                    sql,
                    db_type=db_type,
                    core=core,
                    connection=connection,
                    executor=executor,
                    explain=bool((connection and core) or executor),
                    limit_zero=bool((connection and core) or executor),
                )
                attempts.append({
                    "attempt": attempt_idx + 1,
                    "sql": chk.get("normalized") or sql,
                    "parse_ok": chk.get("parse_ok"),
                    "valid": chk.get("valid"),
                    "error": chk.get("error"),
                    **ph_extra,
                })
                if chk.get("valid"):
                    best_sql = chk.get("normalized") or sql
                    best_valid = True
                    best_reason = ""
                    break
                if not best_sql:
                    best_sql = chk.get("normalized") or sql
                    best_reason = chk.get("error") or "Invalid SQL"
                elif chk.get("parse_ok"):
                    best_sql = chk.get("normalized") or sql
                    best_reason = chk.get("error") or best_reason

            if not best_valid and db_type:
                fallback = self._template_fallback_pair(question, db_type)
                if fallback and fallback.get("sql"):
                    chk = check_sql(
                        fallback["sql"],
                        db_type=db_type,
                        core=core,
                        connection=connection,
                        executor=executor,
                        explain=bool((connection and core) or executor),
                        limit_zero=bool((connection and core) or executor),
                    )
                    if chk.get("valid") or chk.get("parse_ok"):
                        best_sql = chk.get("normalized") or fallback["sql"]
                        best_valid = bool(chk.get("valid"))
                        best_reason = chk.get("error") or ""
                        best_explanation = fallback.get("explanation") or ""
                        attempts.append({
                            "attempt": len(attempts) + 1,
                            "sql": best_sql,
                            "parse_ok": chk.get("parse_ok"),
                            "valid": chk.get("valid"),
                            "error": chk.get("error"),
                            "source": "template_fallback",
                        })

            result = {
                "ok": bool(best_sql),
                "sql": best_sql or None,
                "valid": best_valid,
                "parse_ok": attempts[-1].get("parse_ok") if attempts else False,
                "reason": best_reason,
                "explanation": best_explanation,
                "attempts": attempts,
                "name": name,
                "engine": engine_used,
                "recalled": False,
                "error": None if best_sql else "Local LLM produced no SQL.",
            }
            if attempts:
                for k in ("mappings", "resolved", "ambiguous", "resolution_confidence",
                          "resolution", "resolution_candidates", "resolution_error"):
                    if k in attempts[-1]:
                        result[k] = attempts[-1][k]
            return result
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "sql": None, "valid": False, "error": str(exc)}

    def export_dataset(
        self,
        path: str,
        *,
        include_sample: bool = True,
        rag_connection: str = "",
    ) -> dict[str, Any]:
        try:
            pairs = self.collect_pairs(
                include_sample=include_sample, rag_connection=rag_connection
            )
            n = save_jsonl(path, pairs)
            return {"ok": True, "path": path, "count": n, "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    def export_dataset_content(
        self,
        *,
        include_sample: bool = True,
        rag_connection: str = "",
    ) -> dict[str, Any]:
        """Build the NL->SQL dataset in memory and return it as JSONL text.

        Unlike :meth:`export_dataset`, this writes nothing to the server
        filesystem — the caller (e.g. the Web UI) receives the content over
        HTTP and saves it client-side. This is the remote-access-safe path.
        """
        try:
            pairs = self.collect_pairs(
                include_sample=include_sample, rag_connection=rag_connection
            )
            content = "".join(
                json.dumps({"question": p["question"], "sql": p["sql"]}) + "\n"
                for p in pairs
            )
            return {"ok": True, "content": content, "count": len(pairs), "error": None}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "content": "", "count": 0, "error": str(exc)}
