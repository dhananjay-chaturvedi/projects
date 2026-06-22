"""Data migration screen.

Functional parity with the desktop Data Migration tab: source/target selectors,
source-table loading + multi-select, target DB/prefix/suffix, the full G1-G10
option set, and the Validate / Row-counts / Convert / Apply / Transfer / Compare
actions. Requires the migrator module.
"""

from __future__ import annotations

from typing import Any

from textual.containers import Horizontal
from textual.widgets import (
    Button,
    Checkbox,
    Collapsible,
    Input,
    Label,
    Select,
    SelectionList,
    Static,
    TextArea,
)

from common.ui.textual.screens.base import BaseScreen
from schema_converter.table_naming import TargetNaming
from schema_converter.transfer_options import (
    TransferMultiRequest,
    TransferRequest,
    options_from_mapping,
)

_OVERFLOW = ["(default)", "fail", "truncate", "skip"]
_NULL = ["(default)", "keep", "empty_to_null", "null_to_empty"]
_BOOL = ["(default)", "auto", "int", "true_false"]
_TZ = ["(default)", "preserve", "naive", "utc", "target"]


class MigrationScreen(BaseScreen):
    """Schema validation, conversion, and data transfer."""

    NAV_ID = "migration"

    def __init__(self, svc: Any, **kwargs) -> None:
        super().__init__(svc, **kwargs)
        self._tables: list[str] = []
        self._cancel_op = False

    def screen_title(self) -> str:
        return "Data Migration"

    def compose_body(self):
        names = [c["name"] for c in self.svc.list_connections()]
        opts = [(n, n) for n in names] or [("(none)", "")]
        with Horizontal(classes="actions-row"):
            yield Label("Data conversion and migration services")
            yield Button("⚙ Migration Settings", id="mig-settings")
        with Horizontal(classes="actions-row"):
            yield Label("Source ")
            yield Select(opts, id="mig-source", allow_blank=True)
            yield Button("Load tables", id="mig-load", classes="mini")
            yield Label(" Target ")
            yield Select(opts, id="mig-target", allow_blank=True)
        with Horizontal(classes="actions-row"):
            yield Label("Source DB/schema ")
            yield Input(id="mig-source-db", placeholder="(optional)")
            yield Label("Target DB ")
            yield Input(id="mig-target-db", placeholder="(optional)")
            yield Label(" Prefix ")
            yield Input(id="mig-prefix")
            yield Label(" Suffix ")
            yield Input(id="mig-suffix")

        with Collapsible(title="Source tables", collapsed=False):
            yield SelectionList(id="mig-tables-list")
            yield Label("…or type tables (comma-separated)")
            yield Input(id="mig-tables", placeholder="public.users, public.orders")

        with Collapsible(title="Migration options (G1-G10)", collapsed=True):
            yield Checkbox("Create Indexes (with schema)", value=True, id="mig-create-indexes")
            yield Checkbox("Drop Table If Exists (before schema conversion)", id="mig-drop-if-exists")
            yield Label("Type map (src:tgt,…)")
            yield Input(id="mig-type-map", placeholder="varchar2:text,int:decimal")
            with Horizontal(classes="actions-row"):
                yield Label("Batch ")
                yield Input(id="mig-batch", placeholder="1000")
                yield Label(" Limit ")
                yield Input(id="mig-limit")
            yield Label("WHERE (G1, single table)")
            yield Input(id="mig-where")
            yield Label("Columns subset (G2, single)")
            yield Input(id="mig-columns", placeholder="id,name,email")
            yield Label("Column rename map (G2)")
            yield Input(id="mig-column-map", placeholder="name:full_name")
            with Horizontal(classes="actions-row"):
                yield Label("Overflow ")
                yield Select([(o, o) for o in _OVERFLOW], id="mig-overflow",
                             value="(default)", allow_blank=False)
                yield Label(" Null ")
                yield Select([(o, o) for o in _NULL], id="mig-null",
                             value="(default)", allow_blank=False)
            with Horizontal(classes="actions-row"):
                yield Label("Bool ")
                yield Select([(o, o) for o in _BOOL], id="mig-bool",
                             value="(default)", allow_blank=False)
                yield Label(" Timezone ")
                yield Select([(o, o) for o in _TZ], id="mig-tz",
                             value="(default)", allow_blank=False)
            yield Label("Target timezone (G7)")
            yield Input(id="mig-target-tz", placeholder="Asia/Kolkata")
            with Horizontal(classes="actions-row"):
                yield Label("Workers ")
                yield Input(id="mig-workers", placeholder="4")
                yield Label(" Report path ")
                yield Input(id="mig-report", placeholder="/tmp/report.json")
            with Horizontal(classes="actions-row"):
                yield Checkbox("Parallel", id="mig-parallel")
                yield Checkbox("Continue on error", id="mig-continue")
                yield Checkbox("Reset sequences", id="mig-reset-seq")
                yield Checkbox("Checkpoint", id="mig-checkpoint")

        with Horizontal(classes="actions-row"):
            yield Button("Preview Schema", id="mig-preview", variant="primary")
            yield Button("Row counts", id="mig-rowcounts")
            yield Button("Sample Data", id="mig-sample")
            yield Button("Validate", id="mig-validate")
            yield Button("Convert", id="mig-convert")
            yield Button("Apply DDL", id="mig-apply")
            yield Button("Transfer", id="mig-transfer", variant="success")
            yield Button("Compare Schema", id="mig-compare-schema")
            yield Button("Compare Data", id="mig-compare")
            yield Select(
                [("sample", "sample"), ("full", "full")],
                value="sample", allow_blank=False, id="mig-compare-mode",
            )
            yield Button("Dump Schema", id="mig-dump")
            yield Button("Clear Preview", id="mig-clear")
            yield Button("Stop", id="mig-stop", variant="error", disabled=True)
        yield Label("Generated / editable DDL (for Apply)")
        yield TextArea("", id="mig-ddl", language="sql")
        yield TextArea("", id="mig-output", read_only=True)
        yield Static("", id="mig-status", classes="status")

    # ------------------------------------------------------------------ #
    def _src(self) -> str:
        return str(self.query_one("#mig-source", Select).value or "")

    def _tgt(self) -> str:
        return str(self.query_one("#mig-target", Select).value or "")

    def _target_type(self) -> str:
        for c in self.svc.list_connections():
            if c.get("name") == self._tgt():
                return c.get("db_type", c.get("type", ""))
        return ""

    def _selected_tables(self) -> list[str]:
        sel = list(self.query_one("#mig-tables-list", SelectionList).selected)
        if sel:
            return [str(s) for s in sel]
        raw = self.query_one("#mig-tables", Input).value
        return [t.strip() for t in raw.split(",") if t.strip()]

    def _status(self, msg: str) -> None:
        self.query_one("#mig-status", Static).update(msg)

    def _out(self, obj: Any) -> None:
        import json
        try:
            self.query_one("#mig-output", TextArea).text = json.dumps(obj, indent=2, default=str)
        except Exception:
            self.query_one("#mig-output", TextArea).text = str(obj)

    def _preview_text(self, text: str) -> None:
        self.query_one("#mig-output", TextArea).text = text

    def _set_stop_enabled(self, enabled: bool) -> None:
        self.query_one("#mig-stop", Button).disabled = not enabled

    def _begin_op(self) -> None:
        self._cancel_op = False
        self._set_stop_enabled(True)

    def _end_op(self) -> None:
        self._set_stop_enabled(False)

    def _ensure_open(self, *conns: str) -> None:
        for c in conns:
            if c and hasattr(self.svc, "open_connection"):
                try:
                    self.svc.open_connection(c)
                except Exception:
                    pass

    def _sel(self, wid: str) -> str:
        v = str(self.query_one(wid, Select).value or "")
        return "" if v == "(default)" else v

    def _int(self, wid: str):
        v = self.query_one(wid, Input).value.strip()
        return int(v) if v.isdigit() else None

    def _common(self) -> dict:
        return {
            "target_db": self.query_one("#mig-target-db", Input).value.strip(),
            "prefix": self.query_one("#mig-prefix", Input).value.strip(),
            "suffix": self.query_one("#mig-suffix", Input).value.strip(),
            "batch_size": self._int("#mig-batch"),
            "limit": self._int("#mig-limit"),
            "column_map": self.query_one("#mig-column-map", Input).value.strip(),
            "continue_on_error": self.query_one("#mig-continue", Checkbox).value,
            "overflow_policy": self._sel("#mig-overflow"),
            "null_policy": self._sel("#mig-null"),
            "bool_policy": self._sel("#mig-bool"),
            "timezone_policy": self._sel("#mig-tz"),
            "target_timezone": self.query_one("#mig-target-tz", Input).value.strip(),
            "reset_sequences": self.query_one("#mig-reset-seq", Checkbox).value,
            "checkpoint": self.query_one("#mig-checkpoint", Checkbox).value,
            "report_path": self.query_one("#mig-report", Input).value.strip(),
        }

    # ------------------------------------------------------------------ #
    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "mig-load":
            self._load_tables()
            return
        if bid == "mig-settings":
            try:
                from schema_converter import module_config as mc
                self._out({
                    "ok": True,
                    "config": {s: {k: mc.get(s, k) for k in keys}
                               for s, keys in mc.DEFAULTS.items()},
                    "path": str(mc.config_path() or mc.live_path()),
                })
                self._status("Migration settings loaded.")
            except Exception as exc:
                self._status(str(exc))
            return
        if bid == "mig-dump":
            self._dump_schema()
            return
        if bid == "mig-clear":
            self.query_one("#mig-output", TextArea).text = ""
            self._status("Preview cleared.")
            return
        if bid == "mig-stop":
            self._cancel_op = True
            self._status("Stopping operation…")
            return
        if bid == "mig-preview":
            src, tgt = self._src(), self._tgt()
            tables = self._selected_tables()
            c = self._common()
            type_map = self.query_one("#mig-type-map", Input).value.strip()
            self.run_worker(
                lambda: self._preview_schema(src, tgt, tables, c, type_map),
                thread=True, exclusive=True, group="mig-op",
            )
            return
        if bid == "mig-sample":
            src = self._src()
            tables = self._selected_tables()
            self.run_worker(
                lambda: self._sample_data(src, tables),
                thread=True, exclusive=True, group="mig-op",
            )
            return
        if not hasattr(self.svc, "validate_migration"):
            self._status("Migrator module not available on this service.")
            return
        src, tgt = self._src(), self._tgt()
        if not src or not tgt:
            self._status("Select source and target connections.")
            return
        self._ensure_open(src, tgt)
        tables = self._selected_tables()
        try:
            if bid == "mig-validate":
                if not tables:
                    self._status("Select at least one table."); return
                r = self.svc.validate_migration(
                    src, tgt, tables,
                    naming=TargetNaming.from_source(self._common()),
                    type_map=self.query_one("#mig-type-map", Input).value.strip())
                self._out(r)
                self._status("Validation complete." if not r.get("error") else r["error"])
            elif bid == "mig-rowcounts":
                if not tables:
                    self._status("Select at least one table."); return
                r = self.svc.count_rows_multi(src, tables)
                self._out(r); self._status("Row counts retrieved.")
            elif bid == "mig-convert":
                if not tables:
                    self._status("Select at least one table."); return
                c = self._common()
                r = self.svc.convert_schema_multi(
                    src, self._target_type(), tables,
                    naming=TargetNaming.from_source(c),
                    type_map=self.query_one("#mig-type-map", Input).value.strip())
                self._out(r)
                ddl = r.get("joined_ddl") or ""
                if ddl:
                    self.query_one("#mig-ddl", TextArea).text = ddl
                self._status("Schema converted — DDL ready to Apply."
                             if not r.get("error") else r["error"])
            elif bid == "mig-apply":
                ddl = self.query_one("#mig-ddl", TextArea).text.strip()
                if not ddl:
                    self._status("No DDL to apply (run Convert first)."); return
                r = self.svc.apply_ddl_to_target(
                    tgt, ddl, stop_on_error=True,
                    create_indexes=self.query_one("#mig-create-indexes", Checkbox).value,
                    drop_if_exists=self.query_one("#mig-drop-if-exists", Checkbox).value)
                self._out(r)
                self._status(f"DDL applied ({r.get('executed')} statement(s))."
                             if not r.get("error") else f"Apply failed: {r['error']}")
            elif bid == "mig-transfer":
                if not tables:
                    self._status("Select at least one table."); return
                c = self._common()
                options = options_from_mapping({
                    **c,
                    "where": self.query_one("#mig-where", Input).value.strip(),
                    "columns": self.query_one("#mig-columns", Input).value.strip(),
                })
                if len(tables) == 1:
                    request = TransferRequest(
                        source_conn=src,
                        target_conn=tgt,
                        table=tables[0],
                        naming=TargetNaming.from_source(c),
                        batch_size=c["batch_size"],
                    )
                    r = self.svc.transfer_data(
                        request,
                        options,
                    )
                else:
                    request = TransferMultiRequest(
                        source_conn=src,
                        target_conn=tgt,
                        tables=tables,
                        naming=TargetNaming.from_source(c),
                        batch_size=c["batch_size"],
                        parallel=self.query_one("#mig-parallel", Checkbox).value,
                        workers=self._int("#mig-workers"),
                    )
                    r = self.svc.transfer_data_multi(
                        request,
                        options,
                    )
                self._out(r)
                self._status(r.get("message") or ("Transfer complete." if r.get("ok") else "Transfer failed."))
            elif bid == "mig-compare-schema":
                if not tables:
                    self._status("Select one table to compare."); return
                r = self.svc.compare_schema(src, tgt, tables[0])
                self._out(r)
                self._status("Schema compare complete." if not r.get("error") else r["error"])
            elif bid == "mig-compare":
                if not tables:
                    self._status("Select one table to compare."); return
                from schema_converter.compare_options import DataCompareOptions

                mode = str(self.query_one("#mig-compare-mode", Select).value or "sample")
                r = self.svc.compare_data(
                    src, tgt, tables[0], options=DataCompareOptions(mode=mode)
                )
                self._out(r)
                self._status(
                    f"Data compare ({mode}) complete." if not r.get("error") else r["error"]
                )
        except Exception as exc:  # surface service errors in the output area
            self._out({"error": str(exc)})
            self._status(str(exc))

    def _preview_schema(
        self,
        src: str,
        tgt: str,
        tables: list[str],
        common: dict,
        type_map: str,
    ) -> None:
        from common.ui.shared.migration_preview import format_schema_preview

        if not src or not tgt:
            self.call_from_thread(self._status, "Select source and target connections.")
            return
        if not tables:
            self.call_from_thread(self._status, "Select at least one table.")
            return
        if not hasattr(self.svc, "convert_schema_multi"):
            self.call_from_thread(self._status, "Schema preview not available on this service.")
            return
        target_type = ""
        for c in self.svc.list_connections():
            if c.get("name") == tgt:
                target_type = c.get("db_type", c.get("type", ""))
                break
        self.call_from_thread(self._begin_op)
        self.call_from_thread(self._preview_text, "")
        try:
            self._ensure_open(src, tgt)
            r = self.svc.convert_schema_multi(
                src, target_type, tables,
                naming=TargetNaming.from_source(common),
                type_map=type_map,
            )
            if self._cancel_op:
                self.call_from_thread(self._preview_text, "  (Stopped by user)\n")
                self.call_from_thread(self._status, "Preview stopped.")
                return
            self.call_from_thread(self._preview_text, format_schema_preview(r))
            self.call_from_thread(
                self._status,
                "Schema preview complete." if not r.get("error") else r["error"],
            )
        except Exception as exc:  # noqa: BLE001
            self.call_from_thread(self._out, {"error": str(exc)})
            self.call_from_thread(self._status, str(exc))
        finally:
            self.call_from_thread(self._end_op)

    def _sample_data(self, src: str, tables: list[str]) -> None:
        from common.ui.shared.migration_preview import format_sample_data

        if not src:
            self.call_from_thread(self._status, "Select a source connection.")
            return
        if not tables:
            self.call_from_thread(self._status, "Select at least one table.")
            return
        if not hasattr(self.svc, "sample_rows_multi"):
            self.call_from_thread(self._status, "Sample data not available on this service.")
            return
        self.call_from_thread(self._begin_op)
        self.call_from_thread(self._preview_text, "")
        try:
            self._ensure_open(src)
            r = self.svc.sample_rows_multi(src, tables, limit=1)
            if self._cancel_op:
                self.call_from_thread(self._preview_text, "  (Stopped by user)\n")
                self.call_from_thread(self._status, "Sample data stopped.")
                return
            self.call_from_thread(self._preview_text, format_sample_data(r))
            self.call_from_thread(
                self._status,
                "Sample data retrieved." if not r.get("error") else r["error"],
            )
        except Exception as exc:  # noqa: BLE001
            self.call_from_thread(self._out, {"error": str(exc)})
            self.call_from_thread(self._status, str(exc))
        finally:
            self.call_from_thread(self._end_op)

    def _dump_schema(self) -> None:
        """Dump native CREATE TABLE/INDEX DDL for the selected source tables
        (or all tables when none are selected), mirroring the Tk "Dump Schema"
        button and the CLI ``migrator dump`` / API ``GET .../dump`` surfaces.
        """
        if not hasattr(self.svc, "dump_schema"):
            self._status("Schema dump not available on this service.")
            return
        src = self._src()
        if not src:
            self._status("Select a source connection.")
            return
        self._ensure_open(src)
        tables = self._selected_tables()
        try:
            statements: list[str] = []
            for tbl in (tables or [None]):
                r = self.svc.dump_schema(src, table=tbl)
                if r.get("error"):
                    self._out(r)
                    self._status(r["error"])
                    return
                ddl = (r.get("ddl") or "").strip()
                if ddl:
                    statements.append(ddl)
            ddl_text = "\n\n".join(statements)
            self.query_one("#mig-ddl", TextArea).text = ddl_text
            scope = f"{len(tables)} table(s)" if tables else "all tables"
            self._out({"ok": True, "scope": scope, "ddl": ddl_text})
            self._status(
                f"Schema dump complete ({scope}). DDL shown above."
                if ddl_text else "Schema dump produced no DDL."
            )
        except Exception as exc:  # surface service errors
            self._out({"error": str(exc)})
            self._status(str(exc))

    def _load_tables(self) -> None:
        src = self._src()
        if not src:
            self._status("Select a source connection.")
            return
        self._ensure_open(src)
        items = self.svc.get_objects(src, "Tables")
        if items and isinstance(items[0], dict) and items[0].get("error"):
            self._status(items[0]["error"])
            return
        self._tables = [i if isinstance(i, str) else i.get("name", str(i)) for i in items]
        shown = self._filter_by_source_db(self._tables)
        sl = self.query_one("#mig-tables-list", SelectionList)
        sl.clear_options()
        for t in shown:
            sl.add_option((t, t))
        self._status(f"Loaded {len(shown)} source tables.")

    def _filter_by_source_db(self, tables: list[str]) -> list[str]:
        """Mirror Tk: filter schema-qualified tables by the source DB/schema."""
        sel = self.query_one("#mig-source-db", Input).value.strip()
        if not sel:
            return tables
        qualified = [t for t in tables if "." in str(t)]
        if not qualified:
            return tables
        matched = [t for t in qualified if str(t).split(".")[0].strip() == sel]
        return matched or tables
