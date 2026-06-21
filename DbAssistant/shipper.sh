#!/usr/bin/env bash
# DbManagementTool — release shipper launcher.
#
# Interactive when launched with no arguments. Forwards every flag to
# setup/shipper.py when arguments are provided.
#
# Examples:
#   bash shipper.sh                      # interactive
#   bash shipper.sh --module ai          # non-interactive, lean ai bundle
#   bash shipper.sh --offline            # full + bundled wheels for mac/lin/win
#   bash shipper.sh --module monitor --offline --output ./releases

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
cd "$SCRIPT_DIR"

# Locate a working Python 3 (shipper itself needs >= 3.9).
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
    echo "DbManagementTool shipper requires Python >= 3.9 to build bundles." >&2
    exit 1
fi

# Non-interactive path: pass everything straight through.
if [ "$#" -gt 0 ]; then
    exec "$PY" "$SCRIPT_DIR/setup/shipper.py" "$@"
fi

# ── Interactive mode ──────────────────────────────────────────────────────
echo
echo "DbManagementTool — Shipper"
echo
echo "Pick which bundle to ship:"
echo "  1) full      All modules + master CLI/UI/API (default)"
echo "  2) core      Connections + SQL editor only"
echo "  3) ai        AI Query Assistant (UI + CLI + API)"
echo "  4) monitor   Monitoring (UI + CLI + API + daemon)"
echo "  5) migrator  Data Migration (schema convert + data transfer + validation)"
echo

read -r -p "Module [1-5, default 1]: " choice
case "${choice:-1}" in
    1|"") MODULE="full" ;;
    2)   MODULE="core" ;;
    3)   MODULE="ai" ;;
    4)   MODULE="monitor" ;;
    5)   MODULE="migrator" ;;
    *)
        echo "Invalid choice."
        exit 1
        ;;
esac

echo
echo "Bundle mode:"
echo "  1) lean      Source only — receiver runs 'pip install' (default, ~5 MB)"
echo "  2) offline   Bundle wheels for macOS+Linux+Windows (larger, no internet needed on receiver)"
echo
read -r -p "Mode [1-2, default 1]: " mode
case "${mode:-1}" in
    1|"") OFFLINE_FLAG="" ;;
    2)    OFFLINE_FLAG="--offline" ;;
    *)
        echo "Invalid choice."
        exit 1
        ;;
esac

echo
read -r -p "Output directory [./dist]: " out
OUT_DIR="${out:-./dist}"

echo
echo "Running: setup/shipper.py --module $MODULE ${OFFLINE_FLAG} --output $OUT_DIR"
echo

exec "$PY" "$SCRIPT_DIR/setup/shipper.py" --module "$MODULE" ${OFFLINE_FLAG} --output "$OUT_DIR"
