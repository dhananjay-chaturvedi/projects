"""Shared target-table naming helpers for data migration.

Used by the UI form, the CLI ``migrator`` command, and the REST migrator API so
all three qualify target table names identically (``source_schema.table`` ->
``target_db.table``) instead of each surface re-implementing the rule.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


def base_table_name(source_table: str) -> str:
    """Return the bare table identifier without a schema/database prefix.

    Handles ``public.t``, ``"public"."t"``, ```db`.`t``` and ``[dbo].[t]``.
    """
    if not source_table:
        return source_table
    table = str(source_table).strip()
    if "." in table:
        table = table.split(".")[-1].strip()
    for left, right in (('"', '"'), ("`", "`"), ("[", "]")):
        if table.startswith(left) and table.endswith(right):
            table = table[1:-1]
    return table


def qualify_target_table(
    source_table: str,
    target_db: str = "",
    prefix: str = "",
    suffix: str = "",
) -> str:
    """Build the target table name from a (possibly qualified) *source_table*.

    Strips the source schema/db, applies *prefix*/*suffix*, and prepends
    *target_db* when supplied so the target SQL is qualified independently of the
    source (``source_schema.orders`` -> ``target_db.mig_orders_copy``).
    """
    base = base_table_name(source_table)
    name = f"{(prefix or '').strip()}{base}{(suffix or '').strip()}"
    target_db = (target_db or "").strip()
    return f"{target_db}.{name}" if target_db else name


@dataclass(frozen=True)
class TargetNaming:
    """Target table naming options (``target_db`` + ``prefix``/``suffix``).

    Groups the recurring ``target_db``/``prefix``/``suffix`` triple into a single
    value object so schema-convert/validate calls accept one ``naming=`` argument
    instead of three loose strings. All fields default to ``""`` (no
    qualification / no affixes), so an empty :class:`TargetNaming` reproduces the
    previous "no target database, no prefix/suffix" behaviour exactly.
    """

    target_db: str = ""
    prefix: str = ""
    suffix: str = ""

    @classmethod
    def from_source(
        cls, source: "TargetNaming | Mapping[str, Any] | None"
    ) -> "TargetNaming":
        """Coerce an existing :class:`TargetNaming`, a mapping, or ``None``."""
        if isinstance(source, cls):
            return source
        src: Mapping[str, Any] = source or {}
        return cls(
            target_db=src.get("target_db", "") or "",
            prefix=src.get("prefix", "") or "",
            suffix=src.get("suffix", "") or "",
        )

    def qualify(self, source_table: str) -> str:
        """Qualify *source_table* using these naming options."""
        return qualify_target_table(
            source_table, self.target_db, self.prefix, self.suffix
        )
