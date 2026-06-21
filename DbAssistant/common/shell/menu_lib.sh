#!/usr/bin/env bash
# Shared bash menu helpers for module run_*.sh launchers (no tkinter).
# Source from module shell_menu.sh scripts.

_dbmt_lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_dbmt_common_dir="$(cd "$_dbmt_lib_dir/.." && pwd)"

# Project root: parent of common/ (works in full tree and module-only ship).
DBMT_ROOT="${DBMT_ROOT:-$(cd "$_dbmt_common_dir/.." && pwd)}"

_dbmt_resolve_python() {
  if [[ -n "${DBMT_PYTHON:-}" && -x "$DBMT_PYTHON" ]]; then
    echo "$DBMT_PYTHON"
    return
  fi
  local v="$DBMT_ROOT/.venv/bin/python"
  if [[ -x "$v" ]]; then
    echo "$v"
    return
  fi
  command -v python3.12 >/dev/null 2>&1 && { command -v python3.12; return; }
  command -v python3 >/dev/null 2>&1 && { command -v python3; return; }
  command -v python >/dev/null 2>&1 && { command -v python; return; }
  echo "python3"
}

DBMT_PYTHON="$(_dbmt_resolve_python)"

_dbmt_header() {
  printf '\n%s\n%s\n\n' "=== $1 ===" "$(printf '%.0s-' {1..60})"
}

_dbmt_pause() {
  read -r -p "Press Enter to continue..." _
}

_dbmt_read() {
  # _dbmt_read VARNAME "prompt"
  local __v="$1" __p="$2" __d="${3:-}"
  if [[ -n "$__d" ]]; then
    read -r -p "$__p [$__d]: " "$__v"
    if [[ -z "${!__v}" ]]; then
      printf -v "$__v" '%s' "$__d"
    fi
  else
    read -r -p "$__p: " "$__v"
  fi
}

_dbmt_read_secret() {
  local __v="$1" __p="$2"
  read -r -s -p "$__p: " "$__v"
  echo
}

_dbmt_run() {
  # Requires MODULE to be set (e.g. schema_converter).
  (cd "$DBMT_ROOT" && "$DBMT_PYTHON" -m "$MODULE" "$@")
}

_dbmt_connection_names() {
  (cd "$DBMT_ROOT" && "$DBMT_PYTHON" - <<'PY'
from common.headless.db_service import CoreDBService
for c in CoreDBService().list_connections():
    print(c.get("name", ""))
PY
) 2>/dev/null | grep -v '^$' || true
}

_dbmt_pick_connection() {
  # Sets REPLY to connection name or empty on cancel.
  REPLY=""
  local -a names=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && names+=("$line")
  done < <(_dbmt_connection_names)

  if ((${#names[@]} == 0)); then
    echo "No saved connections. Add one via: $DBMT_PYTHON -m $MODULE connections add ..."
    return 1
  fi

  if ((${#names[@]} == 1)); then
    REPLY="${names[0]}"
    echo "Using connection: $REPLY"
    return 0
  fi

  PS3="Select connection (#): "
  select c in "${names[@]}" "Cancel"; do
    if [[ "$REPLY" -eq ${#names[@]}+1 || "$c" == "Cancel" ]]; then
      return 1
    fi
    if [[ -n "$c" ]]; then
      REPLY="$c"
      return 0
    fi
  done
}

_dbmt_menu_loop() {
  # Usage: define MENU_ITEMS and MENU_ACTIONS arrays, then call _dbmt_menu_loop "Title"
  local title="$1"
  while true; do
    _dbmt_header "$title"
    PS3="Choice: "
    select _choice in "${MENU_ITEMS[@]}"; do
      if [[ -z "$_choice" ]]; then
        continue
      fi
      local idx=$((REPLY - 1))
      if [[ "$idx" -eq $((${#MENU_ITEMS[@]} - 1)) ]]; then
        echo "Bye."
        exit 0
      fi
      "${MENU_ACTIONS[$idx]}"
      _dbmt_pause
      break
    done
  done
}

_dbmt_list_connections() {
  _dbmt_run connections list
}

_dbmt_add_connection() {
  local name dbtype host port user pass svcdb
  _dbmt_read name "Connection name"
  PS3="Database type: "
  select dbtype in Oracle MySQL MariaDB PostgreSQL SQLite Cancel; do
    [[ "$dbtype" == "Cancel" || -z "$dbtype" ]] && return
    break
  done
  _dbmt_read host "Host" "localhost"
  case "$dbtype" in
    Oracle) _dbmt_read port "Port" "1521"; _dbmt_read svcdb "Service name" ;;
    SQLite) port=""; svcdb="" ;;
    *) _dbmt_read port "Port" "3306"; _dbmt_read svcdb "Database name" ;;
  esac
  _dbmt_read user "Username"
  _dbmt_read_secret pass "Password"
  local -a args=(connections add --name "$name" --type "$dbtype" --host "$host" --user "$user" --password "$pass")
  [[ -n "${port:-}" ]] && args+=(--port "$port")
  if [[ "$dbtype" == "Oracle" ]]; then
    args+=(--service "$svcdb")
  elif [[ "$dbtype" != "SQLite" ]]; then
    args+=(--db "$svcdb")
  fi
  _dbmt_run "${args[@]}"
}

_dbmt_test_connection() {
  _dbmt_pick_connection || return
  _dbmt_run connections test "$REPLY"
}

_dbmt_open_connection() {
  _dbmt_pick_connection || return
  _dbmt_run connections open "$REPLY"
}

_dbmt_close_connection() {
  _dbmt_pick_connection || return
  _dbmt_run connections close "$REPLY"
}

_dbmt_close_all_connections() {
  _dbmt_run connections close-all
}

_dbmt_list_active_connections() {
  _dbmt_run connections active
}

_dbmt_run_sql() {
  _dbmt_pick_connection || return
  local sql
  _dbmt_read sql "SQL"
  _dbmt_run query --conn "$REPLY" --sql "$sql"
}

_dbmt_start_api() {
  echo "Starting API on http://127.0.0.1:8000/docs (Ctrl+C to stop)..."
  _dbmt_run api --host 127.0.0.1 --port 8000
}

_dbmt_try_full_ui() {
  if (cd "$DBMT_ROOT" && "$DBMT_PYTHON" -m "$MODULE" --ui 2>/dev/null); then
    return 0
  fi
  echo "Full desktop UI requires tkinter (python3-tk on Linux)."
  echo "  Or run: $DBMT_PYTHON -m $MODULE --ui"
  return 1
}
