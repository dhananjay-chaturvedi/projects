#!/usr/bin/env bash
# AI Query Assistant — bash menu UI (no tkinter).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/shell_menu.sh" "$@"
