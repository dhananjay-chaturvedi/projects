# Launch Plan — PyPI + GitHub Pages

This document is the ready-to-execute plan for publishing the tool to **PyPI**
(so anyone can `pip install` it) and the documentation site to **GitHub Pages**.
Nothing here has been executed — every artifact below is copy-paste ready for
when you say "go".

> Proposed distribution name: **`dbassistant`** (CLI/commands), with `dbassist`
> reserved as an alias if available. Check availability first:
> `pip index versions dbassistant` / visit `https://pypi.org/project/dbassistant/`.

---

## Part A — What `pip install` will give users

After publishing, a user runs:

```bash
pipx install dbassistant            # isolated, recommended for an app
# or
pip install dbassistant             # into the current environment
```

…and gets three console commands on their `PATH`:

| Command | Launches | Backed by |
|---------|----------|-----------|
| `dbassistant` | Desktop UI (Tkinter) | `common.ui.launcher:launch_desktop_ui` |
| `dbassistant-cli` | CLI | `app.dbtool:main` |
| `dbassistant-api` | REST API server | `api:main` (add a `main()` wrapper) |

Optional driver/feature sets install only what's needed (extras):

```bash
pip install "dbassistant[postgres]"           # psycopg2
pip install "dbassistant[mysql,oracle]"        # multiple engines
pip install "dbassistant[api]"                 # FastAPI/uvicorn
pip install "dbassistant[cloud]"               # boto3/azure/gcp
pip install "dbassistant[all]"                 # everything
```

**Important nuances to communicate to users:**

- The Tkinter UI needs system Tk (`python3-tk` on Debian/Ubuntu; bundled on
  macOS python.org builds). This is documented in install docs.
- **Remote connections** (SSH tunnel) use the system `ssh` client. Password
  auth additionally needs `sshpass`. These are system binaries, not pip
  packages — call this out in the install guide.
- Per-engine native drivers (e.g. Oracle `oracledb`, `psycopg2`) are pulled in
  by the matching extra so the base install stays lean.

---

## Part B — Files to add to the repo (drop-in ready)

### B1. `pyproject.toml` (root)

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "dbassistant"
dynamic = ["version"]
description = "Multi-engine database management tool: UI, CLI and REST API with monitoring, migration, and an AI query assistant."
readme = "README.md"
requires-python = ">=3.10"
license = { file = "LICENSE" }
authors = [{ name = "Dhananjay Chaturvedi" }]
keywords = ["database", "mysql", "postgresql", "oracle", "monitoring", "migration", "cli", "ssh-tunnel"]
classifiers = [
  "Development Status :: 4 - Beta",
  "Environment :: Console",
  "Intended Audience :: Developers",
  "Intended Audience :: System Administrators",
  "License :: OSI Approved :: MIT License",
  "Programming Language :: Python :: 3",
  "Topic :: Database",
]
dependencies = [
  "cryptography>=41",
]

[project.optional-dependencies]
postgres = ["psycopg2-binary>=2.9"]
mysql    = ["mysql-connector-python>=8.0"]
oracle   = ["oracledb>=1.4"]
mssql    = ["pyodbc>=4.0"]
mongo    = ["pymongo>=4.5"]
api      = ["fastapi>=0.110", "uvicorn[standard]>=0.27", "python-dotenv>=1.0"]
cloud    = ["boto3>=1.34", "azure-identity>=1.15", "google-auth>=2.28"]
all      = [
  "psycopg2-binary>=2.9", "mysql-connector-python>=8.0", "oracledb>=1.4",
  "pyodbc>=4.0", "pymongo>=4.5", "fastapi>=0.110", "uvicorn[standard]>=0.27",
  "python-dotenv>=1.0", "boto3>=1.34", "azure-identity>=1.15", "google-auth>=2.28",
]

[project.urls]
Homepage      = "https://github.com/<owner>/dbassistant"
Documentation = "https://<owner>.github.io/dbassistant/"
Issues        = "https://github.com/<owner>/dbassistant/issues"

[project.scripts]
dbassistant      = "common.ui.launcher:launch_desktop_ui"
dbassistant-cli  = "app.dbtool:main"
dbassistant-api  = "api:main"

[tool.setuptools.dynamic]
version = { file = "VERSION" }

[tool.setuptools.packages.find]
include = ["common*", "app*", "monitoring*", "ai_query*", "schema_converter*"]

[tool.setuptools.package-data]
"*" = ["*.ini", "*.ini.example"]
```

> Reconcile the dependency pins above against `setup/requirements-*.txt`
> (those files are the source of truth for exact versions used today). The
> extras map 1:1 to the existing `requirements-drivers.txt`, `-api.txt`,
> `-cloud.txt`, `-core.txt`.

**Pre-req code change:** add a `main()` to `api.py` so the `dbassistant-api`
entry point works:

```python
def main():
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()
```

### B2. `LICENSE`
Use **MIT** (see [`LICENSE`](LICENSE)). Keep `pyproject.toml` `license` and the
PyPI classifier in sync.

### B3. `.gitignore` (root) — minimum
```gitignore
__pycache__/
*.py[cod]
.venv/
venv/
env/
*.egg-info/
build/
dist/
.pytest_cache/
logs/
*.log
.env
# never publish real secrets / local state
.dbassistant/
```

### B4. `CONTRIBUTING.md`, `SECURITY.md`, `CHANGELOG.md`
- `CONTRIBUTING.md`: dev setup (`pip install -e ".[all]" -r setup/requirements-dev.txt`),
  how to run tests (`pytest -q`), code style, PR checklist.
- `SECURITY.md`: how to report vulnerabilities privately; note that secrets are
  Fernet-encrypted under `~/.dbassistant/keys/`.
- `CHANGELOG.md`: start at the current `VERSION` (1.0.0). Add the **Remote
  database connections (SSH tunnel)** feature as the headline entry.

---

## Part C — GitHub repository readiness

### C1. CI workflow — `.github/workflows/ci.yml`
```yaml
name: CI
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "${{ matrix.python-version }}" }
      - run: |
          python -m pip install -U pip
          pip install -e ".[all]"
          pip install -r setup/requirements-dev.txt
      - run: pytest -q -k "not integration and not comprehensive"
```

### C2. PyPI publish workflow — `.github/workflows/publish.yml`
Uses PyPI **Trusted Publishing** (OIDC) — no API token stored in GitHub.
```yaml
name: Publish to PyPI
on:
  release:
    types: [published]
jobs:
  build-and-publish:
    runs-on: ubuntu-latest
    permissions:
      id-token: write          # required for trusted publishing
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: |
          python -m pip install -U pip build
          python -m build
      - uses: pypa/gh-action-pypi-publish@release/v1
```
One-time setup: on PyPI, add a "trusted publisher" for the repo +
`publish.yml` workflow.

### C3. README badges + quickstart
Add install + quickstart to `README.md` (badge row: PyPI version, CI status,
docs site, license).

---

## Part D — GitHub Pages (documentation site)

The site already exists under `website/` (Astro Starlight).

### D1. Configure base path
In `website/astro.config.mjs` set:
```js
export default defineConfig({
  site: "https://<owner>.github.io",
  base: "/dbassistant",     // omit if using a custom domain at the root
  // ...
});
```

### D2. Pages deploy workflow — `.github/workflows/pages.yml`
```yaml
name: Deploy docs
on:
  push:
    branches: [main]
    paths: ["website/**", ".github/workflows/pages.yml"]
permissions:
  contents: read
  pages: write
  id-token: write
jobs:
  build:
    runs-on: ubuntu-latest
    defaults: { run: { working-directory: website } }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with: { node-version: 20, cache: npm, cache-dependency-path: website/package-lock.json }
      - run: npm ci
      - run: npm run build
      - uses: actions/upload-pages-artifact@v3
        with: { path: website/dist }
  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment: { name: github-pages, url: "${{ steps.deployment.outputs.page_url }}" }
    steps:
      - id: deployment
        uses: actions/deploy-pages@v4
```
One-time setup: repo **Settings → Pages → Source: GitHub Actions**.

### D3. Custom domain (optional)
GitHub Pages works fine at `https://<owner>.github.io/dbassistant/` with **no
domain purchase**. A `.com` is purely cosmetic/branding:
- Add a `CNAME` file in `website/public/` with your domain.
- Point DNS (`CNAME` → `<owner>.github.io`, or apex `ALIAS`/`A` records to
  GitHub's IPs) and enable "Enforce HTTPS".

---

## Part E — Execution checklist (run in order when you say go)

1. [ ] Decide & confirm name (`dbassistant`), verify free on PyPI.
2. [ ] Add `LICENSE`, `.gitignore`, `pyproject.toml`, `CONTRIBUTING.md`,
       `SECURITY.md`, `CHANGELOG.md`.
3. [ ] Add `main()` to `api.py`.
4. [ ] Reconcile extras in `pyproject.toml` with `setup/requirements-*.txt`.
5. [ ] Local build smoke test:
       ```bash
       python -m pip install -U build twine
       python -m build
       twine check dist/*
       pip install dist/*.whl    # in a clean venv; verify the 3 console scripts
       ```
6. [ ] Push repo to GitHub; add CI / publish / pages workflows.
7. [ ] Enable GitHub Pages (Source: Actions); confirm site builds.
8. [ ] Configure PyPI Trusted Publishing for the repo.
9. [ ] (Optional) Upload to **TestPyPI** first:
       `twine upload --repository testpypi dist/*` and
       `pip install -i https://test.pypi.org/simple/ dbassistant`.
10. [ ] Create a GitHub **Release** (tag `v1.0.0`) → triggers PyPI publish.
11. [ ] Verify: `pipx install dbassistant` on a clean machine, run
        `dbassistant-cli connections list`.
12. [ ] Announce: README badges, docs link, changelog.

---

## Part F — Why publish publicly? (recap)

- **Discoverability & install in one line** — `pipx install dbassistant`
  instead of cloning + manual setup.
- **Versioned, reproducible installs** — users pin a release; upgrades are
  `pipx upgrade`.
- **Credibility & contributions** — public issues/PRs, CI badges, docs site.
- **No server to run** — PyPI + GitHub Pages are free static/registry hosting.

PyPI does **not** require the tool to be a "library" — console-script apps are
fully supported. After install, users invoke the `dbassistant*` commands; they
don't need to `import` anything.
