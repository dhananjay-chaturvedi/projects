# Multi-UI architecture

All three UIs live under `common/ui/` and are each optional and independently
deletable / shippable:

| Package | Technology | Launch |
|---------|------------|--------|
| `common/ui/tk/` | Tkinter desktop (visual/functional source of truth) | `dbtool ui`, `python -m <module> --ui` |
| `common/ui/textual/` | Textual TUI (+ web) | `dbtool tui`, `python -m <module> --tui` |
| `common/ui/web/` | Standalone HTML/CSS/JS SPA + its own server | `dbtool webui`, `python -m <module> --web-ui` |
| `common/ui/shared/` | Shared UI properties (title, tab order/labels, theme, host/port) read by all three UIs | — |

Core logic lives in `common/headless/`, module `service.py` / `bridge.py`, CLI, and REST API. **Deleting any UI folder does not break CLI, API, the other UIs, or headless services.** Because all three read `common/ui/shared/`, a change to a tab label, the title, or the colour theme propagates to every UI while each still renders natively.

## Shared-vs-native rule (no triple maintenance)

Anything that is the *same* across UIs lives **once** in `common/ui/shared/` and every
UI reads it; only the *rendering* (toolkit widgets, geometry, CSS) lives in each UI folder.

| Concern | Single source of truth | UIs render natively from it |
|---------|------------------------|------------------------------|
| Tab order / labels / visibility | `common/ui/shared/tabs.py` | Tk notebook, Textual tab bar, Web tabs |
| Title, theme/colours, fonts, window size, web host/port | `common/ui/shared/properties.py` | all three |
| Object specs: fields, actions, option lists, **section order**, **collapsed-by-default** | `common/ui/shared/specs.py` | all three |

**Practical guideline:** if you find yourself making the *same* layout edit in two or
three UIs, stop — put the shared part in `common/ui/shared/` and have each UI derive it.

Worked example — the Connections tab. `specs.CONNECTION_SECTIONS` is the single source
of truth for which sections exist, their order, and their collapsed-by-default state
(only *Active connections* is expanded). Each UI just maps section ids to a native body
builder and reads order + collapse from the spec:

- Tk: `create_connections_tab_ui` loops over `_connection_sections()`; each frame uses
  `_conn_section_expanded(id)`.
- Textual: `ConnectionsScreen.compose_body` loops over `CONNECTION_SECTIONS`, wrapping each
  `_body_<id>()` builder in a `Collapsible(collapsed=...)` from the spec.
- Web: the SPA serves static section markup, then `applyConnectionLayout()` reorders the
  `<details>` blocks and sets their `open` state from `/ui/config` (which serves the spec).

Worked example — the Welcome tab. `specs.welcome_payload()` (tagline, overview, per-tab
guide, CLI/API access, keyboard shortcuts, platforms, tips, footer) is the single source
of the Welcome copy. Tk renders it natively in `create_welcome_tab_ui`; the Textual home
screen renders it in `HomeScreen._compose_welcome`; the Web SPA renders it via
`renderWelcome()` from `/ui/config`'s `specs.welcome`. Editing the copy once updates all three.

Worked example — the Database Objects tab. `specs.objects_payload()` is the single source
for the Tk-like pane labels, toolbar actions (Refresh, Import Data), Object types pane actions
(Clear results), table-card actions (Schema, Load Sample Data, Row count, Export Data), and the
server-side Export / Import fields. The set of object **types** stays engine-driven (service
metadata via `list_db_ops` / `supported_object_types`), so it is not hard-coded. Each UI shares
the same layout: a *Browse objects* header, a left *Object types* pane with one button per
engine operation, a right *Results* pane with title/count/filter, expandable table/collection
cards, and an Export / Import section. Tk renders it in `DatabaseObjectsPanel`; the Textual
`ObjectsScreen` renders `#obj-type-buttons` and per-table `#obj-card-*` controls; the Web SPA
renders the same pane/card model via `executeObjOperation()` and `renderObjCard()`.

Worked example — the SQL Editor. `specs.sql_editor_payload()` is the single source for the
connection action (Refresh connections), the auto-commit label (Auto-commit), the editor
toolbar (run-at-cursor/selected/all, stop, clear, load, save, format, autocomplete on/off,
commit, rollback), the result actions (copy/sort/filter/clear/export) and the multi-tab "+"
affordance. Stable widget ids stay in each UI; only labels are stamped from the spec, so the
old drift (TUI "Refresh"/"Autocommit" vs Tk/Web "Refresh connections"/"Auto-commit") is gone.
All three UIs share the same layout: a tab strip with a "+" new-tab button, a connection row,
the editor toolbar, a results area with the result-action toolbar, and query history. The
Textual `SqlEditorScreen` reads labels from the spec and now keeps per-tab SQL buffers (Tabs
widget); the Web SPA stamps labels via `applySqlLabels()` from `/ui/config`'s `specs.sqlEditor`.

Worked example — the AI Query Assistant. `specs.ai_payload()` is the single source for the
action buttons (Generate SQL / Execute / Stop / Explain / Optimize / Run Review / Clear), the
SQL toolbar (Copy / Edit / Send to SQL Editor / SQL execution rules), the SQL modes, the Chat
follow-up actions (Send Follow-up / Clear Chat) and — most importantly — the five-tab
"Results & AI insights" notebook (`AI_RESULT_TABS`: Query results, Explanation, Optimization,
Chat, Review). The Tk tab is the reference: Explain lands in the Explanation tab, Optimize in
Optimization, Execute in Query results, Run Review in Review and follow-ups in Chat. The
Textual `AiQueryScreen` renders these as a `TabbedContent` with one `TabPane` per tab and
routes each action to its pane; the Web SPA builds the same tab bar via `buildAiResultTabs()`
and stamps labels with `applyAiLabels()` from `/ui/config`'s `specs.ai`. Previously TUI/Web
dumped explanation, optimization and review all into one block with no Chat pane — now all
three UIs share the notebook layout.

Worked example — the Monitoring tab. `specs.monitoring_payload()` is the single source for
the top actions (Monitor Settings / Alert Thresholds), the three sections (`MONITOR_SECTIONS`:
Server / Database / Cloud — each with a title, metrics title and Add/Select/Remove target
actions), the shared metrics view toolbar (`MONITOR_VIEW_ACTIONS`) and the threshold actions.
The Tk tab is the reference and uses the *three-list concurrent* model: each section owns its
own saved-targets list, "Select" starts monitoring a target (adds it to that section's active
set — many run at once), "Remove" stops an active target (or deletes an idle saved one), and a
timer polls every active target across the three sections into three live metrics panels.
`ServerMonitorUI.create_ui` reads its section titles and button labels from the spec. The
Textual `MonitoringScreen` mirrors this exactly: three `Collapsible` sections, each with its
own `OptionList` of saved targets plus a metrics `DataTable`; `Select` adds to `self._active`,
and `_refresh_now()` polls all active targets concurrently via a thread worker calling
`monitor_any`. The Web SPA renders three `fieldset` sections, each with a `<select>` listbox
(`mon-server-list`/`-database-list`/`-cloud-list`) and a metrics grid; `monStartMonitoring`
fills `monActive`, and `refreshMonNow()` polls every active target with `Promise.all` and
renders each section's grid. Labels in both TUI and Web come from `applyMonitoringLabels()` /
the shared spec, and Remove routes by target source, matching Tk.

Governance tests in `tests/test_connections_three_ui_sync.py`,
`tests/test_welcome_three_ui_sync.py`, `tests/test_objects_parity.py`,
`tests/test_sql_editor_parity.py`, `tests/test_ai_query_parity.py` and
`tests/test_monitoring_parity.py` assert all three UIs match the shared spec (structure and
behaviour, e.g. Explain → Explanation pane; Remove → source-routed delete), so a future
layout/content change is made **once** in `specs.py` and drift is caught automatically.

## Launch paths

```bash
# Tk desktop (full or single module)
dbtool ui
dbtool ui --module migrator

# Textual terminal UI
dbtool tui
dbtool tui --module ai

# Textual in browser (same app, xterm.js terminal)
dbtool tui --web --host 0.0.0.0 --port 8080

# HTML/JS web UI (REST API + SPA on FastAPI)
dbtool webui --host 0.0.0.0 --port 8090
dbtool webui --module monitor

# Per-module standalone
python -m schema_converter --ui
python -m schema_converter --tui
python -m schema_converter --web-ui
python -m schema_converter --shell-ui   # bash menu, no UI deps
```

## Dependencies

- **Tk**: system `python3-tk` (not pip-installable)
- **Textual**: `pip install -r setup/requirements-ui.txt`
- **Web UI**: `fastapi` + `uvicorn` (already in `setup/requirements-api.txt`); no extra frontend build — the SPA is dependency-free vanilla JS.

## `common/ui/web/` — standalone HTML/JS web UI

`common/ui/web/` is self-contained: it ships a dependency-free single-page app
(`common/ui/web/static/`) **and its own server**. It reads the in-process core
service directly and does **not** import the public REST API
(`common.headless.app_factory`):

- `common/ui/web/backend.py` builds the Web UI's own FastAPI app: it constructs the service (`CoreDBService` / a module composite), registers routes via the neutral `common/headless/core_routes.py` glue (the same handlers the public API uses, so they can't drift), mounts the SPA at `/ui`, and exposes `/ui/config`.
- `common/ui/web/server.py` runs that app via uvicorn. Visit `http://host:port/ui/`. Host/port default to the shared `[ui.web]` properties.
- `/ui/config` returns the shared UI properties (title, tab order/labels, colour theme from `common/ui/shared/`) so the SPA mirrors the Tk desktop UI and picks up label/theme changes automatically.
- Because it never builds `create_app()`, **deleting the public REST API leaves the Web UI working**, and deleting `common/ui/web/` leaves the API and other UIs intact.
- Module tabs (Data Migration / AI / Monitoring) appear automatically when the corresponding module is installed (reflected in `/ui/config` and `/api/modules`).
- If an `X-API-Key` is configured (`DBTOOL_API_KEY`), enter it in the top-right field; it is sent on every request and stored in `localStorage`.

## Registry

All UI launches go through `common/core/ui_registry.py`, which lazy-imports the target package and prints a clear message if the folder is missing.
