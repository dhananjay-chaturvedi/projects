"""Tests for multi-dialect local LLM conditioning and corpus seeding."""

from __future__ import annotations


def test_tag_question_adds_dialect_prefix():
    from ai_assistant.llm.dataset import extract_db_type_tag, tag_question

    q = tag_question("list all tables", "MariaDB")
    assert q == "[mariadb] list all tables"
    tag, bare = extract_db_type_tag(q)
    assert tag == "mariadb"
    assert bare == "list all tables"


def test_normalize_question_for_match_preserves_dialect_tag():
    from ai_assistant.llm.validation import normalize_question_for_match

    a = normalize_question_for_match("[postgresql] List All Tables?")
    b = normalize_question_for_match("[mariadb] List All Tables?")
    assert a != b
    assert a.startswith("[postgresql]")
    assert b.startswith("[mariadb]")


def test_all_catalog_pairs_include_every_sql_dialect():
    from ai_assistant.llm.query_templates import all_catalog_pairs, supported_sql_db_types

    pairs = all_catalog_pairs()
    dialects = {p["db_type"] for p in pairs}
    for db in supported_sql_db_types():
        assert db.lower() in dialects


def test_question_import_from_lines(tmp_path):
    from ai_assistant.llm.question_import import load_questions_from_file

    p = tmp_path / "qs.txt"
    p.write_text("list all tables\nshow all columns\n", encoding="utf-8")
    qs = load_questions_from_file(p)
    assert qs == ["list all tables", "show all columns"]


def test_template_fallback_pair_matches_catalog():
    from ai_assistant.llm.service import LlmService

    svc = LlmService()
    fb = svc._template_fallback_pair("list all tables in the public schema", "PostgreSQL")
    assert fb is not None
    assert "information_schema" in (fb.get("sql") or "").lower()
    assert fb.get("explanation")


def test_generate_tags_question_before_recall(monkeypatch, tmp_path):
    from ai_assistant.llm.service import LlmService

    monkeypatch.setenv("DBASSISTANT_HOME", str(tmp_path / "home"))
    svc = LlmService()
    name = "mdl"
    mdir = svc._model_dir(name)
    mdir.mkdir(parents=True, exist_ok=True)
    (mdir / "meta.json").write_text('{"engine":"python","name":"mdl"}', encoding="utf-8")
    (mdir / "dataset.jsonl").write_text(
        '{"question":"[mariadb] list all tables","sql":"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()","explanation":"MariaDB catalog"}\n',
        encoding="utf-8",
    )

    class _Eng:
        def generate(self, question, mdir, params=None):
            return {"sql": "SELECT bad"}

        def is_available(self):
            return True

    monkeypatch.setattr(svc, "_resolve_for_model", lambda *_a, **_kw: (_Eng(), "python"))

    out = svc.generate(
        "list all tables",
        name=name,
        db_type="MariaDB",
    )
    assert out.get("recalled") is True
    assert "TABLE_NAME" in (out.get("sql") or "")
    assert out.get("explanation")


def test_mongo_catalog_pairs_present():
    from ai_assistant.llm.query_templates import mongo_catalog_pairs

    pairs = mongo_catalog_pairs()
    assert pairs
    assert any("collection" in p["question"].lower() for p in pairs)


def test_check_sql_accepts_mongo_query():
    from ai_assistant.llm.sql_check import check_sql

    chk = check_sql("db.orders.find().limit(5)", db_type="MongoDB")
    assert chk.get("parse_ok")
    assert chk.get("valid")
