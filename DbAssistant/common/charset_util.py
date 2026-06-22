"""Shared character-set helpers for DB drivers and migration."""

from __future__ import annotations


def mysql_charset_name(charset: str) -> str:
    c = (charset or "utf-8").strip().lower().replace("-", "")
    if c in ("utf8", "utf"):
        return "utf8mb4"
    return charset


def postgres_encoding_name(charset: str) -> str:
    c = (charset or "utf-8").strip().lower()
    if c in ("utf8", "utf-8"):
        return "UTF8"
    return charset.upper()
