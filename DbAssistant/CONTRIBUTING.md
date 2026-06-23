# Contributing to DbAssistant

Thank you for helping improve DbAssistant. This project treats **service-first
layering** and **surface parity** as requirements: logic lives once in
`common/headless/` or a module `service.py`, then is wired into the Desktop UI,
TUI, Web UI, CLI, and REST API together.

## Before you start

1. Read [`docs/ADDING_FEATURES.md`](docs/ADDING_FEATURES.md) for the module
   manifest pattern and where to touch CLI / API / UI code.
2. Read [`docs/UI_ARCHITECTURE.md`](docs/UI_ARCHITECTURE.md) if your change
   affects tabs, labels, or layout — shared specs live in `common/ui/shared/`.
3. Skim [`MODULES.md`](MODULES.md) if you are working inside an optional module
   (`schema_converter`, `ai_query`, `monitoring`, `ai_assistant`).

## Development setup

```bash
git clone https://github.com/dhananjay-chaturvedi/dbassistant.git
cd dbassistant
bash install.sh
source .venv/bin/activate
python dbtool.py modules
python setup/install.py --verify-only --module full --skip-venv
```

Editable install (optional):

```bash
pip install -e ".[all]"
```

## Making a change

1. **Service layer first** — add or change behavior in `CoreDBService`, a module
   `service.py`, or `bridge.py`. Avoid duplicating business rules in UI files.
2. **Wire all surfaces** — same feature should work in CLI, API, and every UI
   that exposes that module (see `common/core/ui_registry.py`).
3. **Shared UI specs** — if a label, tab order, or panel layout is the same
   across UIs, update `common/ui/shared/specs.py` or `tabs.py`, not three
   copies.
4. **Config** — module-owned settings belong in that module's `config.ini.example`,
   not in core `config.ini`, unless truly global.
5. **Security** — never commit credentials, `.env`, or filled-in `config.ini`
   files. AI execution paths must respect `common/sql_guard.py` read-only rules.

## Pull request checklist

- [ ] Logic is in the service layer, not only in Tk / Textual / Web widgets
- [ ] CLI command and/or API route added or updated when behavior is user-facing
- [ ] Shared UI specs updated when labels or layout change across surfaces
- [ ] `*.ini.example` updated when new config keys are introduced
- [ ] `python setup/install.py --verify-only --module full --skip-venv` passes
- [ ] No secrets, `node_modules/`, `website/dist/`, or `.venv/` in the diff

## Documentation

- User guide: [`HOW_TO_USE.md`](HOW_TO_USE.md)
- Public docs site source: [`website/`](website/) (Astro + Starlight)
- After changing CLI or API behavior, update the matching page under
  `website/src/content/docs/cli/` or `website/src/content/docs/api/`.

## Reporting issues

Open a GitHub issue with:

- OS and Python version
- Install method (`install.sh`, pip, module-only)
- `python dbtool.py modules` output
- Steps to reproduce and expected vs actual behavior
- Redact connection strings, passwords, and API keys

## Security

See [`SECURITY.md`](SECURITY.md) for vulnerability reporting — please do not
open public issues for security-sensitive findings.
