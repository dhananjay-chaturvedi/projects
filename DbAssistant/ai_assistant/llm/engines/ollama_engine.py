"""Stage 3 — Ollama HTTP engine (serve pretrained + Modelfile customization)."""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Optional

from ai_assistant.llm.engines.base import LlmEngine


def _http_json(method: str, url: str, payload: dict | None = None, timeout: int = 120) -> dict:
    data = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
        if not body.strip():
            return {}
        return json.loads(body)


class OllamaEngine(LlmEngine):
    name = "ollama"
    stage = 3
    display_name = "Ollama (pretrained)"
    requires: list[str] = []

    def _host(self, config: dict[str, Any]) -> str:
        return (config.get("ollama_host") or "http://localhost:11434").rstrip("/")

    def _base_model(self, config: dict[str, Any]) -> str:
        return config.get("ollama_model") or "qwen2.5-coder:1.5b"

    def _timeout(self, config: dict[str, Any]) -> int:
        return int(config.get("ollama_timeout", 120))

    def is_available(self) -> tuple[bool, str]:
        try:
            from ai_query import module_config as mc
            host = mc.get("ai.llm", "ollama_host", default="http://localhost:11434").rstrip("/")
            _http_json("GET", f"{host}/api/tags",
                       timeout=mc.get_int("ai.llm", "ollama_health_timeout", default=5))
            return True, ""
        except urllib.error.URLError as exc:
            return False, f"Ollama not reachable ({exc.reason})"
        except Exception as exc:  # noqa: BLE001
            return False, f"Ollama check failed: {exc}"

    def _build_modelfile(self, base: str, pairs: list[dict], model_name: str) -> str:
        lines = [
            f"FROM {base}",
            "",
            "SYSTEM You are a SQL assistant. Given a natural-language question, "
            "respond with ONLY the SQL query, no explanation.",
            "",
        ]
        from ai_query import module_config as mc
        _max_pairs = mc.get_int("ai.llm", "ollama_modelfile_max_pairs", default=20)
        for p in pairs[:_max_pairs]:
            q = (p.get("question") or "").replace('"', '\\"')
            s = (p.get("sql") or "").replace('"', '\\"')
            lines.append(f'MESSAGE user "{q}"')
            lines.append(f'MESSAGE assistant "{s}"')
            lines.append("")
        lines.append(f"# customized model: {model_name}")
        return "\n".join(lines)

    def train(
        self,
        pairs: list[dict],
        model_dir: Path,
        *,
        config: dict[str, Any],
        progress: Optional[Callable[[dict], None]] = None,
    ) -> dict[str, Any]:
        """Create a customized Ollama model via Modelfile (not gradient training)."""
        ok, reason = self.is_available()
        if not ok:
            raise RuntimeError(reason)

        model_dir.mkdir(parents=True, exist_ok=True)
        started = time.time()
        host = self._host(config)
        base = self._base_model(config)
        custom_name = config.get("model_name") or model_dir.name
        from ai_query import module_config as mc
        _prefix = mc.get("ai.llm", "ollama_model_prefix", default="dbtool-")
        ollama_model = f"{_prefix}{custom_name}"

        modelfile = self._build_modelfile(base, pairs, custom_name)
        (model_dir / "Modelfile").write_text(modelfile, encoding="utf-8")

        if progress:
            progress({"epoch": 1, "loss": 0.0})

        # Use ollama CLI if available; otherwise HTTP create API
        try:
            result = subprocess.run(
                ["ollama", "create", ollama_model, "-f", str(model_dir / "Modelfile")],
                capture_output=True,
                text=True,
                timeout=self._timeout(config),
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or "ollama create failed")
        except FileNotFoundError:
            # HTTP fallback: POST /api/create with modelfile content
            _http_json(
                "POST",
                f"{host}/api/create",
                {"name": ollama_model, "modelfile": modelfile},
                timeout=self._timeout(config),
            )

        meta = {
            "ollama_model": ollama_model,
            "ollama_base": base,
            "num_pairs": len(pairs),
        }
        (model_dir / "ollama_meta.json").write_text(
            json.dumps(meta, indent=2), encoding="utf-8"
        )
        return {
            "epochs_run": 1,
            "final_loss": 0.0,
            "vocab_size": 0,
            "num_pairs": len(pairs),
            "num_examples": len(pairs),
            "params": 0,
            "elapsed_sec": round(time.time() - started, 3),
            "ollama_model": ollama_model,
            "note": "Ollama customization (Modelfile), not gradient training",
        }

    def _ollama_model_name(self, model_dir: Path) -> str:
        p = model_dir / "ollama_meta.json"
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8")).get("ollama_model", "")
        meta = model_dir / "meta.json"
        if meta.exists():
            m = json.loads(meta.read_text(encoding="utf-8"))
            return m.get("ollama_model", "")
        return ""

    def generate(
        self,
        question: str,
        model_dir: Path,
        *,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        ok, reason = self.is_available()
        if not ok:
            raise RuntimeError(reason)

        host = self._host(params)
        model = self._ollama_model_name(model_dir) or self._base_model(params)
        prompt = (
            "Convert this question to SQL. Reply with SQL only.\n\n"
            f"Question: {question}\nSQL:"
        )
        resp = _http_json(
            "POST",
            f"{host}/api/generate",
            {
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": float(params.get("temperature", 0.0))},
            },
            timeout=self._timeout(params),
        )
        sql = (resp.get("response") or "").strip()
        # strip markdown fences if present
        if sql.startswith("```"):
            lines = sql.splitlines()
            sql = "\n".join(
                ln for ln in lines if not ln.strip().startswith("```")
            ).strip()
        return {"sql": sql, "ollama_model": model}

    def status(self, model_dir: Path) -> dict[str, Any]:
        meta_path = model_dir / "meta.json"
        out: dict[str, Any] = {}
        if meta_path.exists():
            out = json.loads(meta_path.read_text(encoding="utf-8"))
        ollama_meta = model_dir / "ollama_meta.json"
        if ollama_meta.exists():
            out.update(json.loads(ollama_meta.read_text(encoding="utf-8")))
        return out
