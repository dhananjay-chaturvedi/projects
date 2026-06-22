---
title: Common issues
description: Solutions to the most frequent installation, connection, and runtime errors.
sidebar:
  order: 1
---

## Installation

### `python3: command not found`

Install Python 3.10+ from:

- macOS: `brew install python@3.12`
- Linux (Debian/Ubuntu): `sudo apt-get install python3.12 python3.12-venv`
- Windows: <https://python.org/downloads/>

### `tkinter not found`

The desktop UI needs Tkinter. CLI/API still work.

```bash
# Debian / Ubuntu
sudo apt-get install python3-tk

# Fedora / RHEL
sudo dnf install python3-tkinter

# macOS — already included with python.org installer
```

### Offline install fails — `No matching distribution found`

The receiver's Python version doesn't match any wheel in the bundle.
Bundled wheels cover Python 3.10, 3.11, and 3.12. Either upgrade Python
or rebuild the bundle for the target version.

### `Permission denied` on `~/.dbassistant`

```bash
# Fix mode
chmod 700 ~/.dbassistant
chmod 600 ~/.dbassistant/keys/*
chmod 600 ~/.dbassistant/connections/*
```

## Database connections

### Oracle: `DPI-1047: Cannot locate a 64-bit Oracle Client library`

Either:

- Set `oracle_client_path` in `config.ini` to your Instant Client dir
- Or leave it blank to use thin mode (Oracle 12.1+ only)

On macOS / Linux, also export the library path:

```bash
# macOS
export DYLD_LIBRARY_PATH=/path/to/instantclient_19_8
# Linux
export LD_LIBRARY_PATH=/path/to/instantclient_19_8
```

### PostgreSQL: `pg_config not found` during install

```bash
# Debian / Ubuntu
sudo apt-get install libpq-dev

# Fedora
sudo dnf install libpq-devel

# macOS
brew install libpq && brew link --force libpq
```

### SQL Server: `pymssql` install fails on macOS arm64

```bash
brew install freetds
pip install --no-binary :all: pymssql
```

### MongoDB / DocumentDB: `TLS handshake failed`

DocumentDB requires the AWS RDS combined CA bundle:

```bash
curl -o ~/.dbassistant/global-bundle.pem \
    https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

Then set `tls_ca` on the connection:

```bash
python dbtool.py connections add \
    --name docdb --type DocumentDB \
    --host docdb-cluster.cluster-xxx.region.docdb.amazonaws.com \
    --port 27017 --user app --db appdb \
    --tls-ca ~/.dbassistant/global-bundle.pem
```

## REST API

### `401 Unauthorized`

`DBTOOL_API_KEY` is set but the request didn't include `X-API-Key:`.

```bash
echo $DBTOOL_API_KEY      # confirm value
curl -H "X-API-Key: $DBTOOL_API_KEY" http://localhost:8000/api/health
```

### `413 Payload too large`

SQL body exceeds the configured cap. Increase
`[api] max_body_bytes` in `config.ini` or split the request.

### `CORS error` in browser

Set `[api] cors_origins` in `config.ini` to a comma-separated list of allowed
origins. The default `*` is for development only.

## Monitoring

### Threshold breach not firing

1. Confirm the rule is enabled: `dbtool thresholds list --all`
2. Check `window` — it requires N consecutive breaches
3. Verify the metric is being collected: `dbtool monitor --once`
4. Look in `~/.dbassistant/runtime/daemon.log` for the metric value

### Teams notification: `HTTP 403`

The webhook URL is wrong, expired, or the channel removed the
connector. Generate a new Incoming Webhook in Teams and update
`ALERT_TEAMS_WEBHOOK_URL` in `.env`.

### Cloud metrics: all `null`

- Verify IAM permissions (see [AWS](/cloud/aws/), [Azure](/cloud/azure/), [GCP](/cloud/gcp/))
- Check the metric lookback window (`metrics_lookback_minutes`) — 15
  min default
- Ensure the resource has recent activity; idle DBs may not publish

### Daemon won't start

```bash
python dbtool.py daemon status     # check existing PID
rm ~/.dbassistant/runtime/daemon.pid
python dbtool.py daemon start --connections prod --interval 60
```

## AI Query Assistant

### `AI backend not found`

```bash
python dbtool.py ai --list-backends
```

Install at least one CLI:

- Claude: <https://claude.ai/download>
- Cursor: <https://cursor.com>
- Codex: <https://github.com/openai/codex>

Ensure the binary is on `$PATH` and run `claude --version` (or
equivalent) manually.

#### Backend "not installed" only when launched from the desktop app

GUI launches (Finder/Dock/launchd) often start with a minimal `PATH` that
omits user-install dirs like `~/.local/bin`, so a CLI that works in your
terminal can look "not installed" in-app. The `claude` and `codex` backends
already search `~/.local/bin`, `~/bin`, `~/.claude/local`, Homebrew, and
`/usr/local/bin`. If your CLI lives elsewhere, set an explicit path in
`ai_query/config.ini`:

```ini
[ai.claude]
cli_path = /full/path/to/claude

[ai.codex]
cli_path = /full/path/to/codex
```

### Session save failed

```bash
ls -la ~/.dbassistant/session/ai/
# should be writable by your user
```

If the directory does not exist, the next save command will create it.

## UI

### Mouse-wheel scrolling not working

Restart the UI. If it persists, post an issue with your OS and Python
versions.

### "Connections" tab shows old form values after switching profile

Fixed in 1.0.0. If you see this, check that you have the latest build
of `common/ui/cloud_db_connection_panel.py`.

### Dashboard cards have white instead of blue header

Re-run `./install.sh` to pick up the latest UI assets, then reopen the
app.

## Migration

### `~/.dbmanager` still exists after upgrade

That's expected — the migrator renames it to `~/.dbmanager.legacy` for
safekeeping. Delete it manually once you have verified everything works:

```bash
rm -rf ~/.dbmanager.legacy ~/.dbtool.legacy
```

### `~/.dbassistant/.migrate.lock` left behind

The previous process crashed mid-migration. If no other dbtool process
is running, remove the lock:

```bash
rm ~/.dbassistant/.migrate.lock
```

The next start re-runs the migration safely (it's idempotent).

## Still stuck?

- Check the [FAQ](/troubleshooting/faq/)
- Search the GitHub issues
- Open a new issue with: OS, Python version, command run, full
  traceback (`DBTOOL_DEBUG=1`).
