"""Loader for the Data Migration module's config.ini (independently shippable)."""

from __future__ import annotations

from pathlib import Path

from common.config.module_ini import ModuleIniConfig

_DIR = Path(__file__).resolve().parent

DEFAULTS: dict[str, dict[str, str]] = {
    "schema.conversion": {
        "compare_sample_size": "10",
        "zero_date_strategy": "quote",
        "parallel_workers": "1",
        "type_overrides": "",
        "conversion_charset": "utf-8",
        "overflow_policy": "fail",
        "null_policy": "keep",
        "bool_policy": "auto",
        "timezone_policy": "preserve",
        "target_timezone": "",
        "continue_on_error": "false",
        "reset_sequences": "false",
        # Max per-table skip/insert errors retained in memory during a transfer
        "transfer_error_limit": "1000",
        # Max row mismatches reported by the data-compare step
        "max_compare_mismatches": "20",
    },
    "schema.runtime": {
        # Directory name (under the system temp dir) for migration checkpoints
        "checkpoint_dir": "dbtool_migrate_checkpoints",
    },
}

_cfg = ModuleIniConfig(_DIR, defaults=DEFAULTS)

get = _cfg.get
get_int = _cfg.get_int
get_float = _cfg.get_float
get_bool = _cfg.get_bool
set_value = _cfg.set_value
restore_defaults = _cfg.restore_defaults
reload = _cfg.reload
config_path = _cfg.config_path
live_path = _cfg.live_path
