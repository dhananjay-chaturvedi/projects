#!/usr/bin/env bash
# DbManagementTool — macOS Finder uninstaller (double-click).
#
# This is a thin wrapper around uninstall.sh that keeps the Terminal
# window open after completion so the user can read the summary.

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

cd "$SCRIPT_DIR"

# Run the real uninstaller. We deliberately don't pass --yes so the user
# sees the single PURGE confirmation prompt.
if [ -x "./uninstall.sh" ]; then
    bash "./uninstall.sh"
else
    # Fallback if executable bit is missing (some unzip tools strip it).
    bash "${SCRIPT_DIR}/uninstall.sh"
fi

rc=$?

echo
echo "Press Return to close this window..."
read -r _ || true

exit "$rc"
