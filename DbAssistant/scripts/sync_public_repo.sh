#!/usr/bin/env bash
# Sync DbAssistant/ from the monorepo into a standalone public-repo checkout.
#
# Usage:
#   ./scripts/sync_public_repo.sh /path/to/dbassistant-repo
#
# The target directory should be a git clone of:
#   https://github.com/dhananjay-chaturvedi/dbassistant.git
#
# This copies source files (respecting .gitignore patterns) into the repo root,
# then you review, commit, and push from the target directory.

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 /path/to/dbassistant-repo" >&2
  exit 1
fi

TARGET="$(cd "$1" && pwd)"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -d "$TARGET/.git" ]]; then
  echo "Error: $TARGET is not a git repository." >&2
  echo "Clone first: git clone https://github.com/dhananjay-chaturvedi/dbassistant.git $TARGET" >&2
  exit 1
fi

echo "Syncing $ROOT -> $TARGET"

if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete \
    --exclude '.git' \
    --exclude '.venv' \
    --exclude 'venv' \
    --exclude '__pycache__' \
    --exclude 'website/node_modules' \
    --exclude 'website/dist' \
    --exclude 'website/.astro' \
    --exclude 'config.ini' \
    --exclude 'properties.ini' \
    --exclude 'schema_converter/config.ini' \
    --exclude 'ai_query/config.ini' \
    --exclude 'monitoring/monitor_config.ini' \
    --exclude 'monitoring/monitor_thresholds.ini' \
    --exclude '.env' \
    --exclude 'releases' \
    --exclude '*.zip' \
    --exclude '*.tar.gz' \
    --exclude 'build' \
    --exclude 'dist' \
    --exclude '*.egg-info' \
    "$ROOT/" "$TARGET/"
else
  echo "rsync not found; using tar"
  (cd "$ROOT" && tar cf - \
    --exclude='.git' \
    --exclude='.venv' \
    --exclude='venv' \
    --exclude='__pycache__' \
    --exclude='website/node_modules' \
    --exclude='website/dist' \
    --exclude='website/.astro' \
    --exclude='config.ini' \
    --exclude='properties.ini' \
    --exclude='schema_converter/config.ini' \
    --exclude='ai_query/config.ini' \
    --exclude='monitoring/monitor_config.ini' \
    --exclude='monitoring/monitor_thresholds.ini' \
    --exclude='.env' \
    --exclude='releases' \
    --exclude='*.zip' \
    --exclude='*.tar.gz' \
    --exclude='build' \
    --exclude='dist' \
    --exclude='*.egg-info' \
    .) | (cd "$TARGET" && tar xf -)
fi

echo "Done. Next steps:"
echo "  cd $TARGET"
echo "  git status"
echo "  git add -A && git commit -m 'Sync from monorepo'"
echo "  git push origin main"
