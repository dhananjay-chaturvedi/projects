#!/usr/bin/env bash
# Data Migration — interactive bash menu (no tkinter).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBMT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MODULE=schema_converter
MODULE_KEY=migrator

source "$DBMT_ROOT/common/shell/menu_lib.sh"

_sc_list_tables() {
  _dbmt_pick_connection || return
  _dbmt_run objects --conn "$REPLY" --type tables
}

_sc_show_table() {
  _dbmt_pick_connection || return
  local tbl
  _dbmt_read tbl "Table name"
  _dbmt_run migrator show --conn "$REPLY" --table "$tbl"
}

_sc_convert() {
  _dbmt_pick_connection || return
  local src="$REPLY" tbl target out
  _dbmt_read tbl "Table name"
  PS3="Target database type: "
  select target in MySQL MariaDB PostgreSQL Oracle Cancel; do
    [[ "$target" == "Cancel" || -z "$target" ]] && return
    break
  done
  _dbmt_read out "Output file (blank = stdout)" ""
  local -a args=(migrator convert --source-conn "$src" --target-type "$target" --table "$tbl")
  [[ -n "$out" ]] && args+=(--output "$out")
  _dbmt_run "${args[@]}"
}

_sc_dump() {
  _dbmt_pick_connection || return
  local tbl out
  _dbmt_read tbl "Table name (blank = all tables)" ""
  _dbmt_read out "Output file (blank = stdout)" ""
  local -a args=(migrator dump --conn "$REPLY")
  [[ -n "$tbl" ]] && args+=(--table "$tbl")
  [[ -n "$out" ]] && args+=(--output "$out")
  _dbmt_run "${args[@]}"
}

MENU_ITEMS=(
  "List saved connections"
  "Add connection"
  "Test connection"
  "List tables"
  "Show table schema"
  "Convert schema to another DB type"
  "Dump DDL"
  "Run SQL query"
  "Start REST API (foreground)"
  "Open full desktop UI (needs tkinter)"
  "Exit"
)
MENU_ACTIONS=(
  _dbmt_list_connections
  _dbmt_add_connection
  _dbmt_test_connection
  _sc_list_tables
  _sc_show_table
  _sc_convert
  _sc_dump
  _dbmt_run_sql
  _dbmt_start_api
  _dbmt_try_full_ui
)

_dbmt_menu_loop "Data Migration"
