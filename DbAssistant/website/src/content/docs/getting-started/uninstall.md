---
title: Uninstall
description: Cleanly remove DbAssistant from your system.
sidebar:
  order: 5
---

DbAssistant ships with a cross-platform uninstaller that removes the
virtual environment, generated runtime files, user data under
`~/.dbassistant/`, and optionally the entire project directory.

## Quick uninstall

### macOS / Linux

```bash
./uninstall.sh
```

Or double-click `uninstall.command` in Finder.

### Windows

Double-click `uninstall.bat` or run from a command prompt:

```batch
uninstall.bat
```

### Any OS, Python-only

```bash
python setup/uninstall.py
```

## What gets removed

By default the uninstaller removes:

- `~/.dbassistant/` and any legacy `~/.dbmanager/` / `~/.dbtool/` folders
- `.venv/` inside the project
- `__pycache__/` (recursively)
- `logs/`, `runtime/`, daemon PID files
- Linux `systemd` user unit (if previously installed)

It **keeps**:

- The project source code
- `config.ini`, `properties.ini`, `.env`
- `monitor_thresholds.ini`

## Full purge

To remove **everything**, including the project directory and `.ini`
files:

```bash
./uninstall.sh --purge      # macOS / Linux
uninstall.bat --purge       # Windows
python setup/uninstall.py --purge
```

On Windows, `--purge` spawns a detached batch script so the project
folder can be removed even while the launcher is still running.

## Skipping confirmation

For scripted environments:

```bash
./uninstall.sh --yes
./uninstall.sh --purge --yes
```

## systemd permissions

If you installed the user-level systemd service and it can't be removed
because of permissions, the uninstaller prints exact commands to copy
and run manually as root, for example:

```bash
sudo systemctl stop dbtool-monitor.service
sudo systemctl disable dbtool-monitor.service
sudo rm /etc/systemd/system/dbtool-monitor.service
sudo systemctl daemon-reload
```

## Verifying

After uninstall, none of these should exist:

```bash
ls -la ~/.dbassistant 2>&1     # No such file or directory
ls -la .venv 2>&1              # No such file or directory
which dbtool 2>&1              # nothing
```
