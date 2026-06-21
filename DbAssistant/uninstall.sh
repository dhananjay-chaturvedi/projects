#!/usr/bin/env bash
# DbManagementTool — Linux/macOS uninstaller launcher.
#
# Usage:
#   ./uninstall.sh              # interactive, single prompt (purge yes/no)
#   ./uninstall.sh --purge      # non-interactive, delete project root too
#   ./uninstall.sh --no-purge   # non-interactive, keep project source
#   ./uninstall.sh -y           # same as --no-purge

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"

# Locate a working Python 3 (>= 3.9).
PY=""
for c in python3 python; do
    if command -v "$c" >/dev/null 2>&1; then
        ver="$("$c" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || true)"
        case "$ver" in
            3.9*|3.1[0-9]*|3.[2-9][0-9]*|[4-9].*)
                PY="$c"
                break
                ;;
        esac
    fi
done

if [ -z "$PY" ]; then
    echo "DbManagementTool uninstaller requires Python >= 3.9." >&2
    echo "Install Python 3 and re-run this script." >&2
    exit 1
fi

exec "$PY" "$SCRIPT_DIR/setup/uninstall.py" --project-root "$SCRIPT_DIR" "$@"
