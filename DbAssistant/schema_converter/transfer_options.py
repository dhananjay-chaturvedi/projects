"""Data-transfer options and per-value policies for the Data Migration module.

Centralizes the optional behaviours layered on top of the basic
``DataConverter.transfer_table_data`` path:

* G1 row filtering (WHERE / LIMIT)
* G2 column subset + rename mapping
* G3 continue-on-error (per-row fallback)
* G4 truncation / numeric-overflow policy
* G6 NULL / empty-string / boolean normalization
* G7 timezone handling for datetime values

The options object is intentionally plain so it can be built from the UI,
the CLI, the API, or saved config defaults and threaded unchanged through the
service/bridge/adapter layers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone as _dt_timezone
from decimal import Decimal
from threading import Event
from typing import Any, Callable, Mapping

from schema_converter.table_naming import TargetNaming


class RowSkip(Exception):
    """Raised internally to skip the current row (overflow_policy == 'skip')."""


class ValueOverflow(Exception):
    """Raised when a value exceeds the target column and policy == 'fail'."""


OVERFLOW_POLICIES = ("fail", "truncate", "skip")
NULL_POLICIES = ("keep", "empty_to_null", "null_to_empty")
BOOL_POLICIES = ("auto", "int", "true_false")
TIMEZONE_POLICIES = ("preserve", "naive", "utc", "target")

ManagerFactory = Callable[[str], object]
ProgressCallback = Callable[[str, int, int | None], None]
RowProgressCallback = Callable[[int, int | None], None]


@dataclass(frozen=True)
class TransferRequest:
    """Source/target routing for a single-table data transfer."""

    source_conn: str
    target_conn: str
    table: str
    target_table: str | None = None
    batch_size: int | None = None
    naming: TargetNaming = field(default_factory=TargetNaming)


@dataclass(frozen=True)
class TransferMultiRequest:
    """Source/target routing for a multi-table data transfer."""

    source_conn: str
    target_conn: str
    tables: list[str]
    batch_size: int | None = None
    naming: TargetNaming = field(default_factory=TargetNaming)
    parallel: bool = False
    workers: int | None = None


@dataclass(frozen=True)
class ParallelTransferContext:
    """Runtime dependencies for the parallel transfer worker pool."""

    source_conn: str
    target_conn: str
    source_manager_factory: ManagerFactory
    target_manager_factory: ManagerFactory
    batch_size: int | None = None
    workers: int = 1
    progress_callback: ProgressCallback | None = None
    stop_event: Event | None = None
    checkpoint_store: object | None = None


@dataclass
class TransferOptions:
    """Optional per-transfer behaviours. All default to current behaviour."""

    where: str = ""
    limit: int | None = None
    columns: tuple[str, ...] = ()  # source column subset (empty = all)
    column_map: dict = field(default_factory=dict)  # source -> target rename
    continue_on_error: bool = False
    overflow_policy: str = "fail"
    null_policy: str = "keep"
    bool_policy: str = "auto"
    timezone_policy: str = "preserve"
    target_timezone: str = ""
    reset_sequences: bool = False
    checkpoint: bool = False
    report_path: str = ""

    def __post_init__(self):
        if self.overflow_policy not in OVERFLOW_POLICIES:
            self.overflow_policy = "fail"
        if self.null_policy not in NULL_POLICIES:
            self.null_policy = "keep"
        if self.bool_policy not in BOOL_POLICIES:
            self.bool_policy = "auto"
        if self.timezone_policy not in TIMEZONE_POLICIES:
            self.timezone_policy = "preserve"
        if isinstance(self.columns, list):
            self.columns = tuple(self.columns)

    @property
    def has_value_policies(self) -> bool:
        return (
            self.overflow_policy != "fail"
            or self.null_policy != "keep"
            or self.bool_policy != "auto"
            or self.timezone_policy != "preserve"
        )


@dataclass
class TransferRuntime:
    """Per-run execution controls for table/collection transfer helpers."""

    batch_size: int | None = None
    progress_callback: RowProgressCallback | None = None
    stop_event: Event | None = None
    options: TransferOptions | None = None
    checkpoint_store: object | None = None
    stats_out: dict | None = None
    keep_ids: bool = True

    @classmethod
    def from_source(
        cls, source: "TransferRuntime | Mapping[str, Any] | None" = None
    ) -> "TransferRuntime":
        """Coerce an existing runtime object, a legacy kwargs mapping, or ``None``."""
        if isinstance(source, cls):
            return source
        src: Mapping[str, Any] = source or {}
        return cls(
            batch_size=src.get("batch_size"),
            progress_callback=src.get("progress_callback"),
            stop_event=src.get("stop_event"),
            options=src.get("options"),
            checkpoint_store=src.get("checkpoint_store"),
            stats_out=src.get("stats_out"),
            keep_ids=bool(src.get("keep_ids", True)),
        )


def parse_columns(text: str) -> tuple[str, ...]:
    """Parse ``"a, b, c"`` into ``("a", "b", "c")``."""
    if not text:
        return ()
    return tuple(c.strip() for c in str(text).split(",") if c.strip())


def parse_column_map(text: str) -> dict:
    """Parse ``"src1:tgt1, src2:tgt2"`` into a rename dict."""
    out: dict = {}
    if not text or not str(text).strip():
        return out
    raw = str(text).strip()
    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        raw = raw[1:-1]
    for part in raw.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        src, tgt = part.split(":", 1)
        if src.strip() and tgt.strip():
            out[src.strip()] = tgt.strip()
    return out


def options_from_mapping(values: Mapping[str, Any] | None) -> TransferOptions:
    """Build per-call options from a flat API/CLI/UI field mapping."""
    values = values or {}
    return TransferOptions(
        where=values.get("where") or "",
        limit=values.get("limit"),
        columns=parse_columns(values.get("columns") or ""),
        column_map=parse_column_map(values.get("column_map") or ""),
        continue_on_error=bool(values.get("continue_on_error", False)),
        overflow_policy=values.get("overflow_policy") or "fail",
        null_policy=values.get("null_policy") or "keep",
        bool_policy=values.get("bool_policy") or "auto",
        timezone_policy=values.get("timezone_policy") or "preserve",
        target_timezone=values.get("target_timezone") or "",
        reset_sequences=bool(values.get("reset_sequences", False)),
        checkpoint=bool(values.get("checkpoint", False)),
        report_path=values.get("report_path") or "",
    )


def options_from_config() -> TransferOptions:
    """Build a TransferOptions seeded from saved migrator config defaults."""
    from schema_converter import module_config as mc

    return TransferOptions(
        overflow_policy=mc.get("schema.conversion", "overflow_policy", default="fail").strip() or "fail",
        null_policy=mc.get("schema.conversion", "null_policy", default="keep").strip() or "keep",
        bool_policy=mc.get("schema.conversion", "bool_policy", default="auto").strip() or "auto",
        timezone_policy=mc.get("schema.conversion", "timezone_policy", default="preserve").strip() or "preserve",
        target_timezone=mc.get("schema.conversion", "target_timezone", default="").strip(),
        continue_on_error=mc.get_bool("schema.conversion", "continue_on_error", default=False),
        reset_sequences=mc.get_bool("schema.conversion", "reset_sequences", default=False),
    )


def merge_options(base: TransferOptions | None, override: TransferOptions | None) -> TransferOptions:
    """Layer *override* (per-run) onto *base* (config defaults)."""
    if base is None:
        return override or TransferOptions()
    if override is None:
        return base
    merged = TransferOptions(**base.__dict__)
    for key, value in override.__dict__.items():
        default = getattr(TransferOptions(), key)
        if value != default:
            setattr(merged, key, value)
    merged.__post_init__()
    return merged


# --------------------------------------------------------------------------- #
# SQL builders (G1)
# --------------------------------------------------------------------------- #
def build_select_sql(
    source_table: str,
    columns: tuple[str, ...] | list[str] | None,
    where: str,
    limit: int | None,
    db_type: str,
    order_by: list[str] | None = None,
) -> str:
    col_list = ", ".join(columns) if columns else "*"
    sql = f"SELECT {col_list} FROM {source_table}"
    if where:
        sql += f" WHERE {where}"
    if order_by:
        sql += " ORDER BY " + ", ".join(order_by)
    if limit is not None:
        if db_type == "Oracle":
            sql += f" FETCH FIRST {int(limit)} ROWS ONLY"
        else:
            sql += f" LIMIT {int(limit)}"
    return sql


# --------------------------------------------------------------------------- #
# Per-value policy engine (G4, G6, G7)
# --------------------------------------------------------------------------- #
def _apply_timezone(value: datetime, options: TransferOptions) -> datetime:
    policy = options.timezone_policy
    if policy == "preserve":
        return value
    if policy == "naive":
        return value.replace(tzinfo=None)
    if policy == "utc":
        if value.tzinfo is not None:
            return value.astimezone(_dt_timezone.utc)
        return value
    if policy == "target" and options.target_timezone:
        try:
            from zoneinfo import ZoneInfo

            tz = ZoneInfo(options.target_timezone)
            if value.tzinfo is None:
                value = value.replace(tzinfo=_dt_timezone.utc)
            return value.astimezone(tz)
        except Exception:
            return value
    return value


def _check_numeric_overflow(value, col_limit, options, column_name):
    precision = col_limit.get("num_precision") if col_limit else None
    scale = col_limit.get("num_scale") if col_limit else 0
    if not precision:
        return value
    try:
        int_digits = precision - (scale or 0)
        limit = Decimal(10) ** int_digits
        if abs(Decimal(str(value))) >= limit:
            if options.overflow_policy == "skip" or options.overflow_policy == "truncate":
                # Numbers can't be meaningfully truncated; skip the row.
                raise RowSkip(
                    f"Numeric overflow in column '{column_name}' "
                    f"(value {value} exceeds precision {precision})"
                )
            raise ValueOverflow(
                f"Numeric overflow in column '{column_name}': value {value} "
                f"exceeds target precision {precision}"
            )
    except (ValueError, ArithmeticError):
        return value
    return value


def transform_value(
    value,
    *,
    col_limit: dict | None,
    options: TransferOptions,
    target_db_type: str,
    charset: str,
    is_binary: bool,
    column_name: str = "",
):
    """Apply charset + G4/G6/G7 policies to a single cell value.

    Raises :class:`RowSkip` to drop the row, or :class:`ValueOverflow` to fail.
    """
    if isinstance(value, memoryview):
        value = value.tobytes()

    if isinstance(value, (bytes, bytearray)):
        if is_binary:
            return bytes(value)
        try:
            value = bytes(value).decode(charset, errors="replace")
        except Exception:
            return bytes(value)

    if value is None:
        if options.null_policy == "null_to_empty" and col_limit and col_limit.get("is_text"):
            return ""
        return None

    if isinstance(value, bool):
        if options.bool_policy == "int":
            return 1 if value else 0
        if options.bool_policy == "true_false":
            return "true" if value else "false"
        if target_db_type == "Oracle":
            return 1 if value else 0
        return value

    if isinstance(value, str):
        if options.null_policy == "empty_to_null" and value == "":
            return None
        cmax = col_limit.get("char_max") if col_limit else None
        if cmax and len(value) > cmax:
            if options.overflow_policy == "truncate":
                return value[:cmax]
            if options.overflow_policy == "skip":
                raise RowSkip(
                    f"Value too long for column '{column_name}' "
                    f"({len(value)} > {cmax})"
                )
            raise ValueOverflow(
                f"Value too long for column '{column_name}': "
                f"{len(value)} chars exceeds target max {cmax}"
            )
        return value

    if isinstance(value, datetime):
        return _apply_timezone(value, options)

    if isinstance(value, (int, Decimal, float)):
        return _check_numeric_overflow(value, col_limit, options, column_name)

    return value
