#!/usr/bin/env bash
# Monitoring — bash menu UI (no tkinter).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/shell_menu.sh" "$@"
