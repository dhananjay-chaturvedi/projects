---
title: FAQ
description: Frequently asked questions about DbAssistant.
sidebar:
  order: 2
---

### What Python version do I need?

**Python 3.10 or newer.** 3.12 is recommended.

### Does it work without an internet connection?

Yes — use the offline bundle. The receiver does not need internet
during install. Cloud monitoring obviously still needs network
connectivity to AWS / Azure / GCP at runtime.

### Where are my credentials stored?

`~/.dbassistant/connections/` (encrypted with Fernet). Keys are at
`~/.dbassistant/keys/` (chmod 600). See
[File layout](/architecture/file-layout/).

### Can I store multiple users / environments separately?

Set `DBASSISTANT_HOME` per environment:

```bash
DBASSISTANT_HOME=~/.dbassistant-dev   python dbtool.py ...
DBASSISTANT_HOME=~/.dbassistant-prod  python dbtool.py ...
```

### Can I run the API and the desktop UI at the same time?

Yes. They share the same `~/.dbassistant/` data; each is just a
different surface over the same logic.

### Why are there both `db.json` and `cloud.json`?

DB connections (host/port/credentials) and cloud profiles (provider /
region / resource) are separate concerns and use different encryption
keys. Same pattern for `monitor.json` (SSH targets).

### How do I rotate the encryption key?

Today: export connections, delete the key, re-add. Automated rotation
is on the roadmap. See [Security model](/architecture/security/).

### Does it support proxies?

For the REST API itself, run behind nginx / Caddy / Cloudflare.
DbAssistant respects the standard `HTTP_PROXY` / `HTTPS_PROXY` /
`NO_PROXY` env vars when fetching from cloud SDKs.

### Can I use it in CI?

Yes — the CLI is fully scriptable. Set `DBASSISTANT_HOME` to a temp
path to keep CI runs isolated:

```yaml
- name: Run dbtool checks
  env:
    DBASSISTANT_HOME: ${{ runner.temp }}/dba
    DBTOOL_API_KEY: ${{ secrets.DBTOOL_API_KEY }}
  run: |
    python dbtool.py connections add --name ci --type PostgreSQL ...
    python dbtool.py query --conn ci --sql "SELECT 1"
```

### Does it work on Windows?

Yes. Use `install.bat` and `run.bat`. The desktop UI, CLI, and REST
API all work. The daemon runs via Task Scheduler — see
[Daemon & systemd](/operations/daemon/).

### Is there a Docker image?

There's no official image yet. The Dockerfile example in
[Daemon & systemd](/operations/daemon/) is a good starting point.

### Can I disable a module?

You don't need to disable — modules are loaded lazily. To stop a
module's tab from appearing in the UI, remove its folder before
launching. CLI / API commands for that module will then return
"module not installed".

### How big is the install on disk?

| Component | Size |
|-----------|------|
| Project source | < 5 MB |
| `.venv` (full tool) | ~380 MB |
| Offline bundle (full) | ~1.0–1.3 GB |
| Offline bundle (core+drivers) | ~550 MB |

### Why does the offline bundle include 4 platforms × 3 Python versions?

So one bundle works on any of: macOS arm64, macOS x86_64, Linux x86_64,
Windows x86_64, and any of Python 3.10 / 3.11 / 3.12. Pip auto-selects
the matching wheel.

### Where does AI query history live?

`~/.dbassistant/session/ai/sessions.json`. `max_stored_sessions` in
`[ai.limits]` (`ai_query/config.ini`) caps how many are kept on save
(`0` = keep all, the default).

### Does it call out to any external services besides cloud SDKs and AI CLIs?

No. Only:

- The cloud SDKs you opt into by adding a cloud connection
- The AI CLI you pick (Claude / Cursor / Codex), which makes its own
  calls
- Teams webhook (only if `ALERT_TEAMS_WEBHOOK_URL` is set)

### What's the license?

MIT.

### Can I contribute?

Yes. Open a PR or issue on GitHub. See `docs/ADDING_FEATURES.md` in the
repo for architectural guidelines.
