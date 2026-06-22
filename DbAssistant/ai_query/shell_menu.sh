#!/usr/bin/env bash
# AI Query Assistant — interactive bash menu (no tkinter).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBMT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MODULE=ai_query
MODULE_KEY=ai

source "$DBMT_ROOT/common/shell/menu_lib.sh"

_ai_list_backends() { _dbmt_run ai --list-backends; }

_ai_ask_once() {
  _dbmt_pick_connection || return
  local conn="$REPLY" backend question
  _dbmt_read backend "AI backend (blank = auto)" ""
  _dbmt_read question "Question"
  local -a args=(ai --conn "$conn")
  [[ -n "$backend" ]] && args+=(--backend "$backend")
  args+=("$question")
  _dbmt_run "${args[@]}"
}

_ai_session_list() { _dbmt_run ai session list; }

_ai_session_new() {
  local conn backend
  _dbmt_read conn "Connection name (blank = none)" ""
  _dbmt_read backend "Backend (blank = auto)" ""
  local -a args=(ai session new)
  [[ -n "$conn" ]] && args+=(--conn "$conn")
  [[ -n "$backend" ]] && args+=(--backend "$backend")
  _dbmt_run "${args[@]}"
}

_ai_session_ask() {
  local sid question
  _dbmt_read sid "Session (tabN or id prefix)"
  _dbmt_read question "Question"
  _dbmt_run ai session ask --session "$sid" "$question"
}

_ai_session_followup() {
  local sid msg
  _dbmt_read sid "Session (tabN or id prefix)"
  _dbmt_read msg "Follow-up message"
  _dbmt_run ai session follow-up --session "$sid" "$msg"
}

_ai_session_exec_sql() {
  local sid sql
  _dbmt_read sid "Session (tabN or id prefix)"
  _dbmt_read sql "SQL to execute"
  _dbmt_run ai session execute-sql --session "$sid" --sql "$sql"
}

MENU_ITEMS=(
  "List saved connections"
  "Add connection"
  "Test connection"
  "List AI backends"
  "Ask question (one-shot)"
  "List AI sessions"
  "New AI session"
  "Ask in session"
  "Follow-up in session"
  "Execute SQL in session (with rules)"
  "Run SQL query (direct)"
  "Start REST API (foreground)"
  "Open full desktop UI (needs tkinter)"
  "Exit"
)
MENU_ACTIONS=(
  _dbmt_list_connections
  _dbmt_add_connection
  _dbmt_test_connection
  _ai_list_backends
  _ai_ask_once
  _ai_session_list
  _ai_session_new
  _ai_session_ask
  _ai_session_followup
  _ai_session_exec_sql
  _dbmt_run_sql
  _dbmt_start_api
  _dbmt_try_full_ui
)

_dbmt_menu_loop "AI Query Assistant"
