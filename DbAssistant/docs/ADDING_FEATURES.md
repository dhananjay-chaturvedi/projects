# Adding a New Feature to DbManagementTool

This guide explains where to add code so that a new feature is automatically
available across the UI, the CLI, and the REST API, with no duplicate logic.

---

## 1. Project Layers (Read First)

The tool is split into a shared **core** plus optional **modules**
(`schema`, `ai`, `monitor`). See `common/core/modules.py`:

```37:41:common/core/modules.py
KNOWN_MODULES: dict[str, tuple[str, str]] = {
    "migrator": ("schema_converter",          "Data Migration"),
    "ai":      ("ai_query",                 "AI Query Assistant"),
    "monitor": ("monitoring",               "Monitoring"),
}
```

Every module declares a `ModuleManifest` so the master CLI, API and UI can
discover its surfaces dynamically. Example for monitoring:

```43:58:monitoring/manifest.py
MANIFEST = ModuleManifest(
    name="monitor",
    title="Monitoring",
    description="Local + cloud database/host monitoring, threshold alerts, "
                "notifications and a background daemon.",
    register_cli=_register_cli,
    dispatch_cli=_dispatch_cli,
    cli_commands=["monitor", "monitor-connections", "daemon", "thresholds",
                  "os", "cloud", "notify", "alerts"],
    build_router=_build_router,
    launch_ui=_launch_ui,
    build_tab=_build_tab,
    tab_label="Monitor",
    config_files=["monitoring/monitor_thresholds.ini",
                  "monitoring/monitor_config.ini", "config.ini"],
    check_requirements=_check_requirements,
)
```

The standard layering is:

```
storage / managers / configs
        ↓
service / bridge layer        ← real business logic lives here
        ↓
UI    CLI    API              ← thin adapters; they only translate I/O
        ↓
shell menu                    ← bash wrapper around CLI/API
```

Golden rule: **business logic always goes in the service layer**, never in
CLI or API handlers, never in UI callbacks.

---

## 2. The Service / Bridge Layer

Pick the layer that matches the feature scope:

| Scope                                | Where to add the method                                  |
|--------------------------------------|----------------------------------------------------------|
| Core DB (connections, query, schema) | `common/headless/db_service.py` (`CoreDBService`)        |
| Monitoring                           | `monitoring/service.py` (`MonitorService`)               |
| AI Query                             | `ai_query/service.py` (`AIService`)                      |
| Data Migration                       | `schema_converter/bridge.py` (`SchemaBridge`)            |
| App-level (caches, dashboard, etc.)  | `common/headless/app_service.py` (`AppService`)          |

These services are layered into one composite at runtime via
`_composite_full_service` in `common/headless/app_factory.py`, so any API
route can call any module method on a single `svc` object.

---

## 3. Add the Method

A service method should:

- Take primitives or dicts (no Tkinter / FastAPI / argparse types).
- Return a JSON-serialisable dict / list.
- Use `{"ok": bool, "message": str, ...}` for success/failure shapes when
  the caller needs to act on the result.
- Catch and convert exceptions into the same `{"ok": False, ...}` shape so
  every consumer can render errors uniformly.

Example shape:

```python
def add_widget(self, name: str, color: str = "blue") -> dict:
    try:
        ok, msg = self._mgr().add(name=name, color=color)
        return {"ok": bool(ok), "message": msg}
    except Exception as exc:
        return {"ok": False, "message": str(exc)}
```

---

## 4. Expose via CLI

Each module owns its CLI in `<module>/cli.py` with two entry points:

- `register_cli(subparsers)` — registers `argparse` subcommands.
- `dispatch_cli(args)` — routes the parsed namespace to a handler.

To add a new field or subcommand:

1. Add the argument inside `register_cli`:
   ```python
   p = subparsers.add_parser("widget", help="Widget operations")
   sub = p.add_subparsers(dest="widget_action")
   add_p = sub.add_parser("add")
   add_p.add_argument("--name", required=True)
   add_p.add_argument("--color", default="blue")
   ```
2. Route it in `dispatch_cli`:
   ```python
   if cmd == "widget":
       return _widget(args)
   ```
3. Call the service from the handler — no business logic here:
   ```python
   def _widget(args) -> int:
       svc = _service()
       if args.widget_action == "add":
           r = svc.add_widget(args.name, args.color)
           if not r.get("ok"):
               cliutil.err(r.get("message"))
               return 1
           cliutil.ok(r.get("message"))
           return 0
   ```
4. If the command is a brand-new top-level command, register it in
   `common/core/modules.py::MODULE_CLI_COMMANDS` and in the module's
   `manifest.py::cli_commands`. Without this `dbtool.py widget …` will not
   dispatch.

The root `dbtool.py` (and `app/dbtool.py`) automatically forward unknown
top-level commands to whichever module's `dispatch_cli` claims them via the
manifest. You do not edit the root shim for new features.

---

## 5. Expose via REST API

Each module owns its router in `<module>/api.py` with one entry point:

- `build_router(svc=None)` — returns a `fastapi.APIRouter`.

To add a new endpoint:

1. Define a Pydantic model for inputs (if any), e.g.:
   ```python
   class WidgetCreate(BaseModel):
       name: str
       color: str = "blue"
   ```
2. Add the route inside `build_router`:
   ```python
   @router.post("/api/widgets", tags=["Widgets"])
   def add_widget(req: WidgetCreate):
       r = svc.add_widget(req.name, req.color)
       if not r.get("ok"):
           raise HTTPException(400, r.get("message"))
       return r
   ```
3. Nothing else is needed — the master FastAPI app discovers modules via
   their manifest:

```432:445:common/headless/app_factory.py
def mount_module_routers(
    app: FastAPI,
    svc: Any,
    *,
    module_key: Optional[str] = None,
) -> list[str]:
    """Mount one module (standalone) or all installed modules (full tool)."""
    mounted: list[str] = []
    if module_key is not None:
        manifest = _modules.get(module_key)
        if manifest is None or manifest.build_router is None:
            return mounted
        try:
            app.include_router(manifest.build_router(svc))
```

The root `api.py` shim re-exports `app.headless.api.app` so
`uvicorn api:app` works without any change for new endpoints.

---

## 6. Expose via UI

Tkinter UIs are explicit, so a new field usually needs:

- A new widget in the relevant `*_ui.py` (`monitoring/monitoring_ui.py`,
  `common/ui/connection_dialog.py`, etc.).
- A button/handler that calls the same service method:
  ```python
  result = self._svc.add_widget(name_var.get(), color_var.get())
  ```
- Never put business logic in the callback; only collect input, call the
  service, render the response.

---

## 7. Expose via Shell Menu

The monitoring shell menu (`monitoring/run_monitor.sh`,
`monitoring/shell_menu.sh`) is just a bash wrapper that calls the same
Python CLI. To surface a new feature there:

1. Add a menu item near the related ones.
2. Read inputs via `_dbmt_read`.
3. Invoke the CLI: `python3 -m monitoring widget add --name "$NAME"`.
4. Reuse pickers like `_mon_pick_any_connections_multi` for multi-select.

---

## 8. Persistence / Configs

- Per-module configuration → declare in the manifest's `config_files`
  so packaging copies them.
- Persistent state across CLI invocations → write a JSON file under
  `~/.dbtool/` (see `ai_query/service.py::_state_path` for an example).
- Encrypted credentials → use the existing helpers in
  `common/connection_manager.py` /
  `monitoring/monitor_connection_manager.py`.
- Per-module rules (e.g. thresholds) → an INI under the module's folder,
  loaded by a checker (see `monitoring/threshold_checker.py`).

---

## 9. Tests

Drop tests under `tests/`, following the existing patterns:

- Unit tests for service methods (most valuable).
- CLI tests: build a parser, invoke `dispatch_cli`, capture stdout.
- API tests: use `fastapi.testclient.TestClient`.

Always test the service layer directly first; CLI/API tests then just
confirm wiring.

---

## 10. Checklist for Every New Feature

1. [ ] Service method added in the right `service.py` / `bridge.py`.
2. [ ] Method returns JSON-friendly data; errors are structured.
3. [ ] CLI argument(s) added in `<module>/cli.py::register_cli`.
4. [ ] CLI handler routes through `dispatch_cli` and calls the service.
5. [ ] If new top-level command → registered in
       `common/core/modules.py::MODULE_CLI_COMMANDS` and
       `<module>/manifest.py::cli_commands`.
6. [ ] Pydantic model + FastAPI route in `<module>/api.py::build_router`.
7. [ ] UI widget/handler added if user-visible.
8. [ ] Shell menu option added if relevant.
9. [ ] Config / state files declared in manifest if any.
10. [ ] Tests cover the service method (and at least one wire test).
11. [ ] Docs / `HOW_TO_USE.md` updated if behaviour is user-facing.

---

## 11. Anti-Patterns to Avoid

- Duplicating logic between CLI and API handlers — both must call the
  service.
- Importing UI / FastAPI / argparse modules from service code.
- Catching `Exception` and re-raising different exceptions across layers —
  convert to a structured `{"ok": False, "message": ...}` once, at the
  service boundary.
- Editing root `dbtool.py` or `api.py` for module-owned features — they
  are tiny shims and auto-discover modules via manifests.
- Adding new top-level CLI commands without registering them in both
  `MODULE_CLI_COMMANDS` and the module's `manifest.cli_commands` — the
  command will silently fall through to "unknown".

---

## 12. Worked Example

Feature: add a "Widget" CRUD to the monitoring module.

1. `monitoring/service.py`
   - Implement `add_widget`, `list_widgets`, `remove_widget` on
     `MonitorService`. Wrap a `WidgetManager` for persistence.

2. `monitoring/cli.py`
   - In `register_cli`, add:
     ```python
     w = subparsers.add_parser("widget")
     ws = w.add_subparsers(dest="widget_action"); ws.required = True
     a = ws.add_parser("add");  a.add_argument("--name", required=True)
     l = ws.add_parser("list")
     r = ws.add_parser("remove"); r.add_argument("--name", required=True)
     ```
   - In `dispatch_cli`, route `cmd == "widget"` to `_widget(args)`.

3. `monitoring/api.py`
   - Add `WidgetCreate` model.
   - Add routes:
     - `POST /api/monitor/widgets`
     - `GET  /api/monitor/widgets`
     - `DELETE /api/monitor/widgets/{name}`
   - Each route calls the corresponding service method.

4. `common/core/modules.py` and `monitoring/manifest.py`
   - Add `"widget"` to the monitor module's `cli_commands` (and
     `MODULE_CLI_COMMANDS["monitor"]`).

5. `monitoring/shell_menu.sh`
   - Add menu entries that invoke `python3 -m monitoring widget …`.

6. UI
   - Add a "Widgets" tab/panel in `monitoring/monitoring_ui.py` that calls
     the same service methods.

7. Tests
   - `tests/test_monitor_widgets.py` covers all three flows via the
     service, the CLI parser, and the FastAPI `TestClient`.

That's it — one service implementation, three adapters, one dispatch
table entry, and the feature is uniformly available everywhere.
