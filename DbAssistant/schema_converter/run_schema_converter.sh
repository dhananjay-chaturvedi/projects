#!/usr/bin/env bash
# Data Migration — bash menu UI (no tkinter; works on Linux/macOS with bash only).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/shell_menu.sh" "$@"
