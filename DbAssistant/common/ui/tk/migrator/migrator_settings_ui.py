"""Data Migration tab — settings for schema_converter/config.ini."""

from __future__ import annotations

from common.ui.tk.module_config_dialog import FieldSpec, open_module_config_dialog
from schema_converter import module_config

_FIELDS: tuple[FieldSpec, ...] = (
    ("schema.conversion", "compare_sample_size", "Compare sample size (rows)", "int", ()),
    ("schema.conversion", "zero_date_strategy", "Zero-date strategy", "enum", ("quote", "null", "omit")),
    ("schema.conversion", "parallel_workers", "Parallel transfer workers", "int", ()),
    ("schema.conversion", "type_overrides", "Default type mapping rules", "text", ()),
    ("schema.conversion", "conversion_charset", "Data transfer charset", "text", ()),
    ("schema.conversion", "continue_on_error", "Continue on error (report bad rows)", "bool", ()),
    ("schema.conversion", "overflow_policy", "Overflow policy", "enum", ("fail", "truncate", "skip")),
    ("schema.conversion", "null_policy", "NULL/empty policy", "enum", ("keep", "empty_to_null", "null_to_empty")),
    ("schema.conversion", "bool_policy", "Boolean policy", "enum", ("auto", "int", "true_false")),
    ("schema.conversion", "timezone_policy", "Timezone policy", "enum", ("preserve", "naive", "utc", "target")),
    ("schema.conversion", "target_timezone", "Target timezone (when policy=target)", "text", ()),
    ("schema.conversion", "reset_sequences", "Reset target sequences after load", "bool", ()),
)


def open_migrator_settings(root, *, on_change=None):
    def _saved():
        module_config.reload()
        if on_change:
            on_change()

    open_module_config_dialog(
        root,
        title="Data Migration Settings — schema_converter/config.ini",
        config_module=module_config,
        fields=_FIELDS,
        on_saved=_saved,
    )
