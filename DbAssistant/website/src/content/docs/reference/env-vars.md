---
title: Environment variables
description: All environment variables DbAssistant reads and what they affect.
sidebar:
  order: 4
---

## Application

| Variable | Default | Effect |
|----------|---------|--------|
| `DBASSISTANT_HOME` | `~/.dbassistant` | Override base data directory. Critical for tests and containers. |
| `DBTOOL_API_KEY` | (unset) | If set, required as `X-API-Key:` on every REST request |
| `DBTOOL_DEBUG` | `0` | `1` prints full tracebacks on CLI errors |
| `DBTOOL_CONFIG_INI` | `./config.ini` | Override path to `config.ini` |
| `DBTOOL_PROPERTIES_INI` | `./properties.ini` | Override path to `properties.ini` |
| `DBTOOL_THRESHOLDS_INI` | `monitoring/monitor_thresholds.ini` | Override path to threshold rules |

## Notifications

| Variable | Effect |
|----------|--------|
| `ALERT_TEAMS_WEBHOOK_URL` | MS Teams Incoming Webhook for `notify send` and threshold breach alerts |

## Database driver overrides

| Variable | Used by | Effect |
|----------|---------|--------|
| `ORACLE_HOME` | `oracledb` | Thick-mode Oracle Instant Client directory (alternative to `[paths] oracle_client_path`) |
| `LD_LIBRARY_PATH` *(Linux)* | `oracledb` | Path containing Oracle Instant Client libs |
| `DYLD_LIBRARY_PATH` *(macOS)* | `oracledb` | Same on macOS |

## Cloud SDKs

DbAssistant uses each SDK's default credential chain — no DbAssistant-specific env vars beyond what the SDK reads.

| Cloud | Common variables |
|-------|------------------|
| AWS | `AWS_PROFILE`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` |
| Azure | `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` (service principal); `AZURE_SUBSCRIPTION_ID` |
| GCP | `GOOGLE_APPLICATION_CREDENTIALS` (path to service-account JSON), `GOOGLE_CLOUD_PROJECT` |

## `.env` loading

`python-dotenv` is used at process start. `.env` in the project root is
loaded automatically. Variables already set in the process environment
take precedence.

## In Python code

```python
import os
home = os.environ.get("DBASSISTANT_HOME") or os.path.expanduser("~/.dbassistant")
```

Or via `common.paths`:

```python
from common.paths import bootstrap, base_dir
bootstrap()
print(base_dir())
```

## Testing tip

To run pytest hermetically:

```bash
export DBASSISTANT_HOME=/tmp/pytest_dba_home
pytest -q
```

`tests/conftest.py` sets this automatically — but exporting it manually
makes ad-hoc CLI / API checks completely isolated from the real user
data.
