# First public commit — file checklist

Use this when publishing **DbAssistant** as a public Git repository. The
`.gitignore` keeps local config, virtualenvs, and website build artifacts out
of version control.

## What to commit

Track **source only**:

| Area | Include |
|------|---------|
| Core product | `common/`, `app/`, `schema_converter/`, `ai_query/`, `monitoring/`, `ai_assistant/` |
| Entry points | `dbtool.py`, `conDbUi.py`, `api.py`, `run.sh`, `run.bat`, `install.sh`, `shipper.sh` |
| Setup | `setup/` (installer, requirements, shipper) |
| Config templates | `**/*.ini.example` (not live `config.ini`) |
| Docs | `README.md`, `HOW_TO_USE.md`, `MODULES.md`, `docs/`, `website/src/`, `website/public/`, `website/package.json`, `website/package-lock.json`, `website/astro.config.mjs`, `website/tsconfig.json`, `website/DEPLOY.md`, `website/README.md` |
| Packaging | `pyproject.toml`, `VERSION`, `LICENSE`, `.gitignore`, `CHANGELOG.md` |
| Planning (optional) | `LAUNCH_PLAN.md`, `FIRST_COMMIT.md` |

## What must NOT be committed

| Path | Reason |
|------|--------|
| `.venv/`, `venv/` | Local Python environment |
| `website/node_modules/` | npm install output |
| `website/dist/`, `website/.astro/` | Astro build cache/output |
| `config.ini`, `properties.ini` | Created by installer; may hold local paths |
| `**/config.ini` (module live files) | Same — use `*.ini.example` only |
| `releases/`, `*.zip` | Shipper bundles (attach to GitHub Releases instead) |
| `__pycache__/`, `*.pyc` | Bytecode |
| `.env` | Secrets |

## Publishing to the public `dbassistant` repository

DbAssistant is developed in this monorepo under `DbAssistant/` but published from
**https://github.com/dhananjay-chaturvedi/dbassistant** (repo root = product root).

```bash
# 1) Create the public repo on GitHub (empty, no README), then clone it:
git clone https://github.com/dhananjay-chaturvedi/dbassistant.git /tmp/dbassistant

# 2) Sync from this folder:
./scripts/sync_public_repo.sh /tmp/dbassistant

# 3) Commit and push from the public clone:
cd /tmp/dbassistant
git add -A
git commit -m "Release v1.0.0"
git push -u origin main

# 4) Enable GitHub Pages: Settings → Pages → Source = GitHub Actions
# 5) (Optional) Create GitHub Release v1.0.0 to publish to PyPI
```


```bash
cd DbAssistant

# Preview what will be added (should NOT list node_modules, dist, .venv, config.ini)
git init
git add -n . 2>&1 | head -80

# Stage everything respecting .gitignore
git add .

# Confirm ignored paths stay out
git status
git check-ignore -v config.ini website/node_modules website/dist .venv 2>/dev/null || true

# First commit
git commit -m "$(cat <<'EOF'
Initial public release of DbAssistant v1.0.0.

Multi-DB management tool with Desktop, TUI, Web, CLI, and REST API surfaces.
EOF
)"

# Optional tag
git tag -a v1.0.0 -m "DbAssistant 1.0.0"
```

## Commands (DbAssistant inside an existing monorepo)

If the repo root is the parent of `DbAssistant/`:

```bash
cd /path/to/parent-repo
git add DbAssistant/.gitignore DbAssistant/LICENSE DbAssistant/FIRST_COMMIT.md
git add DbAssistant/
git status DbAssistant/
```

## Post-commit verification (fresh clone simulation)

```bash
cd /tmp
git clone <your-repo-url> dbassistant-test
cd dbassistant-test
bash install.sh
source .venv/bin/activate
python dbtool.py modules
python dbtool.py --help
```

Expected: all modules **installed / ready**, `config.ini` and `properties.ini`
created from examples, no missing imports.

## Documentation site (separate from product commit)

The product does not require `website/dist/` in Git. Deploy docs via CI:

```bash
cd website
npm ci
npm run build
# publish website/dist/ to GitHub Pages / Cloudflare Pages (see website/DEPLOY.md)
```

Replace placeholder URLs in `website/astro.config.mjs` before going live — they
are configured for `dhananjay-chaturvedi/dbassistant` and GitHub Pages at
`https://dhananjay-chaturvedi.github.io/dbassistant/`.
