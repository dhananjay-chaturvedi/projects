---
title: Daemon & systemd
description: Run DbAssistant monitoring as a long-lived service on Linux.
sidebar:
  order: 1
---

For production monitoring, run the daemon under a service manager so it
restarts on failure and starts at boot.

## Linux — user systemd

Recommended: install the unit at the user level so no root is needed.

Create `~/.config/systemd/user/dbtool-monitor.service`:

```ini
[Unit]
Description=DbAssistant monitoring daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=%h/apps/DbManagementTool
Environment=DBASSISTANT_HOME=%h/.dbassistant
Environment=ALERT_TEAMS_WEBHOOK_URL=https://outlook.office.com/webhook/...
ExecStart=%h/apps/DbManagementTool/.venv/bin/python \
          %h/apps/DbManagementTool/dbtool.py daemon start \
          --foreground --connections prod,stage --interval 60
Restart=on-failure
RestartSec=10

[Install]
WantedBy=default.target
```

Enable + start:

```bash
systemctl --user daemon-reload
systemctl --user enable --now dbtool-monitor.service
systemctl --user status dbtool-monitor.service
journalctl --user -u dbtool-monitor.service -f
```

Linger (keep service running when user is logged out):

```bash
sudo loginctl enable-linger $USER
```

## Linux — system systemd (root)

For machine-wide installs. Create
`/etc/systemd/system/dbtool-monitor.service`:

```ini
[Unit]
Description=DbAssistant monitoring daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=dbassist
Group=dbassist
WorkingDirectory=/opt/DbManagementTool
Environment=DBASSISTANT_HOME=/var/lib/dbassistant
EnvironmentFile=/etc/dbassistant/dbassistant.env
ExecStart=/opt/DbManagementTool/.venv/bin/python \
          /opt/DbManagementTool/dbtool.py daemon start \
          --foreground --connections prod,stage --interval 60
Restart=on-failure
RestartSec=10
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/var/lib/dbassistant

[Install]
WantedBy=multi-user.target
```

`/etc/dbassistant/dbassistant.env`:

```dotenv
DBTOOL_API_KEY=...
ALERT_TEAMS_WEBHOOK_URL=https://...
```

Enable + start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now dbtool-monitor.service
sudo systemctl status dbtool-monitor.service
sudo journalctl -u dbtool-monitor.service -f
```

## macOS — launchd

Create `~/Library/LaunchAgents/com.dbassistant.monitor.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>com.dbassistant.monitor</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/me/apps/DbManagementTool/.venv/bin/python</string>
    <string>/Users/me/apps/DbManagementTool/dbtool.py</string>
    <string>daemon</string>
    <string>start</string>
    <string>--foreground</string>
    <string>--connections</string><string>prod,stage</string>
    <string>--interval</string><string>60</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/Users/me/apps/DbManagementTool</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>DBASSISTANT_HOME</key><string>/Users/me/.dbassistant</string>
    <key>ALERT_TEAMS_WEBHOOK_URL</key><string>https://...</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key>
  <string>/Users/me/.dbassistant/runtime/launchd.out</string>
  <key>StandardErrorPath</key>
  <string>/Users/me/.dbassistant/runtime/launchd.err</string>
</dict>
</plist>
```

Load:

```bash
launchctl load ~/Library/LaunchAgents/com.dbassistant.monitor.plist
launchctl start com.dbassistant.monitor
```

## Windows — Task Scheduler

Use Task Scheduler with the trigger **At log on of any user** (or
**At system startup** for system-wide install) and the action:

```text
Program/script:  C:\path\to\DbManagementTool\.venv\Scripts\python.exe
Arguments:       dbtool.py daemon start --foreground --connections prod --interval 60
Start in:        C:\path\to\DbManagementTool
```

Set "If task fails, restart every: 1 minute, up to 3 times".

## Docker

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY . /app
RUN python -m venv .venv && \
    .venv/bin/pip install -r setup/requirements-full.txt && \
    mkdir -p /data/.dbassistant

ENV DBASSISTANT_HOME=/data/.dbassistant
VOLUME ["/data/.dbassistant"]

EXPOSE 8000

CMD [".venv/bin/python", "dbtool.py", "daemon", "start", \
     "--foreground", "--connections", "prod,stage", "--interval", "60"]
```

```bash
docker build -t dbassistant:1.0.0 .
docker run -d --name dbtool-monitor \
    -v dbassistant_data:/data/.dbassistant \
    -e ALERT_TEAMS_WEBHOOK_URL='...' \
    dbassistant:1.0.0
```

## Health checks

```bash
# CLI
python dbtool.py daemon status

# REST
curl -H "X-API-Key: $DBTOOL_API_KEY" \
     http://localhost:8000/api/daemon/status

# systemd
systemctl --user is-active dbtool-monitor.service
```

Use the REST endpoint in your uptime monitor.

## Logs

| Path | Format |
|------|--------|
| `~/.dbassistant/runtime/daemon.log` | Structured key=value lines |
| `journalctl -u dbtool-monitor.service` | systemd-managed |
| `~/.dbassistant/runtime/launchd.out` | launchd captured stdout |

Rotate the daemon log with `logrotate` if needed; the daemon does not
rotate internally.
