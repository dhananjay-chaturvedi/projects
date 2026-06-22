#!/usr/bin/env bash
# Monitoring — interactive bash menu (no tkinter).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBMT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MODULE=monitoring
MODULE_KEY=monitor

source "$DBMT_ROOT/common/shell/menu_lib.sh"

_mon_list_connections() {
  # Unified view: Connections-tab DB profiles + Monitor-tab DB profiles +
  # Monitor-tab SSH/host targets + saved Cloud DB profiles. Each row carries
  # a `source` column.
  _dbmt_run monitor-connections list --source all
}

# Internal — populates MON_PAIRS=( "<source>\t<name>" ... ) by calling the
# unified CLI lister. Sets MON_PAIR_COUNT. Optional arg restricts source.
_mon_collect_pairs() {
  local src="${1:-all}"
  local cli_src="$src"
  if [[ "$src" == "db-monitor" ]]; then
    cli_src="all"
  fi
  MON_PAIRS=()
  while IFS= read -r line; do
    if [[ "$src" == "db-monitor" ]]; then
      local line_src="${line%%$'\t'*}"
      [[ "$line_src" != "db" && "$line_src" != "monitor-db" ]] && continue
    fi
    [[ -n "$line" ]] && MON_PAIRS+=("$line")
  done < <( (cd "$DBMT_ROOT" && "$DBMT_PYTHON" -m "$MODULE" \
              monitor-connections names --source "$cli_src" \
              2>/dev/null) | grep -v '^$' )
  MON_PAIR_COUNT=${#MON_PAIRS[@]}
}

_mon_label_for() {
  # Build human label "[source] name"
  local pair="$1"
  printf '[%s] %s' "${pair%%$'\t'*}" "${pair##*$'\t'}"
}

_mon_pick_any_connection() {
  # Single-select from the unified store.
  #   $1 (optional): source filter (all | db | monitor-db | db-monitor |
  #      monitor | cloud), default=all.
  # On success sets:
  #   REPLY        -> chosen connection NAME (for backward compat with _dbmt_run)
  #   MON_SOURCE   -> source store of the chosen entry
  # Returns 1 if the user cancels or there is nothing to pick.
  local src="${1:-all}"
  REPLY=""
  MON_SOURCE=""

  _mon_collect_pairs "$src"
  if ((MON_PAIR_COUNT == 0)); then
    echo "No saved connections (source=$src)."
    return 1
  fi

  local -a labels=()
  local p
  for p in "${MON_PAIRS[@]}"; do
    labels+=("$(_mon_label_for "$p")")
  done

  local original_PS3="$PS3"
  PS3="Select connection (#): "
  local _c idx
  select _c in "${labels[@]}" "Cancel"; do
    idx=$((REPLY - 1))
    PS3="$original_PS3"
    if [[ "$_c" == "Cancel" || $idx -lt 0 || $idx -ge ${#MON_PAIRS[@]} ]]; then
      REPLY=""
      return 1
    fi
    MON_SOURCE="${MON_PAIRS[$idx]%%$'\t'*}"
    REPLY="${MON_PAIRS[$idx]##*$'\t'}"
    return 0
  done
  PS3="$original_PS3"
  return 1
}

_mon_pick_any_connections_multi() {
  # Multi-select from the unified store.
  #   $1 (optional): source filter (all | db | monitor-db | db-monitor |
  #      monitor | cloud), default=all.
  # On success sets:
  #   MON_SEL_PAIRS  -> array of "source<TAB>name" for each selected entry
  #   MON_SEL_NAMES  -> comma-joined "name1,name2,..."
  #   MON_SEL_COUNT  -> number of selections
  # Returns 1 if the user cancels or picks nothing.
  local src="${1:-all}"
  MON_SEL_PAIRS=()
  MON_SEL_NAMES=""
  MON_SEL_COUNT=0

  _mon_collect_pairs "$src"
  if ((MON_PAIR_COUNT == 0)); then
    echo "No saved connections (source=$src)."
    return 1
  fi

  echo "Available connections:"
  local i=1 p
  for p in "${MON_PAIRS[@]}"; do
    printf "  %2d) %s\n" "$i" "$(_mon_label_for "$p")"
    ((i++))
  done
  echo "Enter numbers separated by spaces or commas (or 'all', empty = cancel)."
  local raw
  read -r -p "Selection: " raw
  raw="${raw// /,}"

  if [[ -z "$raw" ]]; then
    return 1
  fi

  # bash 3.2 (macOS default) doesn't support associative arrays; use a
  # parallel "picked_idx" indexed array and dedupe via a small helper.
  local -a picked_idx=()
  _mon_already_picked() {
    local needle="$1" v
    for v in "${picked_idx[@]:-}"; do
      [[ "$v" == "$needle" ]] && return 0
    done
    return 1
  }

  if [[ "$raw" == "all" || "$raw" == "ALL" ]]; then
    local i
    for ((i = 1; i <= MON_PAIR_COUNT; i++)); do picked_idx+=("$i"); done
  else
    local old_ifs="$IFS"
    IFS=','
    local tok
    for tok in $raw; do
      tok="${tok// /}"
      [[ -z "$tok" ]] && continue
      if ! [[ "$tok" =~ ^[0-9]+$ ]]; then
        echo "Ignoring non-numeric token: $tok" >&2
        continue
      fi
      if ((tok < 1 || tok > MON_PAIR_COUNT)); then
        echo "Out of range: $tok" >&2
        continue
      fi
      _mon_already_picked "$tok" && continue
      picked_idx+=("$tok")
    done
    IFS="$old_ifs"
  fi

  local idx
  for idx in $(printf '%s\n' "${picked_idx[@]:-}" | sort -n | uniq); do
    [[ -z "$idx" ]] && continue
    MON_SEL_PAIRS+=("${MON_PAIRS[$((idx - 1))]}")
  done
  MON_SEL_COUNT=${#MON_SEL_PAIRS[@]}
  if ((MON_SEL_COUNT == 0)); then
    return 1
  fi

  local names=()
  for p in "${MON_SEL_PAIRS[@]}"; do
    names+=("${p##*$'\t'}")
  done
  MON_SEL_NAMES="$(IFS=','; echo "${names[*]}")"
  return 0
}

_mon_test_any_connection() {
  _mon_pick_any_connection || return
  _dbmt_run monitor-connections test --source "$MON_SOURCE" "$REPLY"
}

# Save a Monitor-tab SSH/host target (OS metrics over SSH), mirroring the
# Monitor tab "Add" form in the desktop UI.
_mon_add_ssh_target() {
  local name host user pass ttype
  _dbmt_read name "Connection name"
  [[ -z "$name" ]] && { echo "Name is required."; return 1; }
  _dbmt_read host "Host / IP"
  [[ -z "$host" ]] && { echo "Host is required."; return 1; }
  _dbmt_read user "SSH username"
  [[ -z "$user" ]] && { echo "Username is required."; return 1; }
  _dbmt_read_secret pass "SSH password (blank = use SSH keys)"
  PS3="Target type: "
  select ttype in vm db_server service Cancel; do
    [[ "$ttype" == "Cancel" || -z "$ttype" ]] && return
    break
  done
  local -a args=(monitor-connections add --name "$name" --host "$host"
                 --username "$user" --target-type "$ttype")
  [[ -n "$pass" ]] && args+=(--password "$pass")
  _dbmt_run "${args[@]}"
}

# Save a Monitor-tab-only DB connection. This deliberately targets
# `monitor-db add` (monitor_db.json), not the core `connections add` command
# that writes to the Connections-tab store.
_mon_add_db_connection() {
  local name dbtype host port user pass svcdb
  _dbmt_read name "Connection name"
  [[ -z "$name" ]] && { echo "Name is required."; return 1; }
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
  local -a args=(monitor-db add --name "$name" --db-type "$dbtype"
                 --host "$host" --username "$user" --password "$pass")
  [[ -n "${port:-}" ]] && args+=(--port "$port")
  if [[ "$dbtype" == "Oracle" ]]; then
    args+=(--service "$svcdb")
  elif [[ "$dbtype" != "SQLite" ]]; then
    args+=(--database "$svcdb")
  fi
  _dbmt_run "${args[@]}"
}

# Unified "Add connection" for Monitoring. DB connections added here are
# Monitor-tab-only; existing Connections-tab DB profiles remain selectable.
_mon_add_connection() {
  PS3="What do you want to add? "
  local kind
  select kind in \
    "Database connection (Monitor tab only)" \
    "Cloud database / resource (AWS / Azure / GCP)" \
    "OS / SSH host (Monitor tab)" \
    "Cancel"; do
    case "$kind" in
      "Database connection"*)  _mon_add_db_connection; break ;;
      "Cloud database"*)       _dbmt_run cloud connections add; break ;;
      "OS / SSH host"*)        _mon_add_ssh_target; break ;;
      "Cancel"|*)              break ;;
    esac
  done
}

# Generic source-aware poller. `monitor --conn NAME --once|--interval` routes
# through MonitorService.monitor_any, so the same wrapper drives DB, cloud,
# and Monitor-tab SSH targets.
_mon_poll_once_for() {
  local src="$1"
  _mon_pick_any_connections_multi "$src" || return
  echo "Polling selected $src connection(s) once: $MON_SEL_NAMES"
  _dbmt_run monitor --conn "$MON_SEL_NAMES" --once
}

_mon_poll_loop_for() {
  local src="$1" label="$2"
  _mon_pick_any_connections_multi "$src" || return
  local interval
  _dbmt_read interval "Poll interval (seconds)" "30"
  echo "Polling selected $label connection(s) every ${interval}s (Ctrl+C to stop): $MON_SEL_NAMES"
  _dbmt_run monitor --conn "$MON_SEL_NAMES" --interval "$interval"
}

_mon_poll_db_once()    { _mon_poll_once_for db-monitor; }
_mon_poll_db_loop()    { _mon_poll_loop_for db-monitor "DB"; }
_mon_poll_cloud_once() { _mon_poll_once_for cloud; }
_mon_poll_cloud_loop() { _mon_poll_loop_for cloud "cloud"; }

# OS metrics is split between two CLI subcommands ("os metrics" for the local
# host, "os remote --name N" for a Monitor-tab SSH target), so the picker is
# slightly richer than the generic one above.
#
# On success sets: OS_MODE=local|remote, OS_NAMES (remote only), OS_DISK.
_mon_pick_os_target() {
  OS_MODE=""; OS_NAMES=""; OS_DISK=""
  local original_PS3="$PS3"
  PS3="OS metrics target: "
  local choice
  select choice in "Local host" "Remote host (Monitor SSH target)" "Cancel"; do
    PS3="$original_PS3"
    case "$choice" in
      "Local host")
        OS_MODE=local
        break
        ;;
      "Remote host (Monitor SSH target)")
        _mon_pick_any_connections_multi monitor || return 1
        OS_MODE=remote
        OS_NAMES="$MON_SEL_NAMES"
        break
        ;;
      "Cancel"|*)
        return 1
        ;;
    esac
  done
  _dbmt_read OS_DISK "Disk mount to inspect" "/"
  return 0
}

_mon_run_os_once() {
  if [[ "$OS_MODE" == "local" ]]; then
    _dbmt_run os metrics --disk "$OS_DISK"
  else
    local IFS=','
    local name
    for name in $OS_NAMES; do
      [[ -z "$name" ]] && continue
      echo
      echo "Remote OS metrics: $name"
      _dbmt_run os remote --name "$name" --disk "$OS_DISK"
    done
  fi
}

_mon_poll_os_once() {
  _mon_pick_os_target || return
  _mon_run_os_once
}

_mon_poll_os_loop() {
  _mon_pick_os_target || return
  local interval
  _dbmt_read interval "Poll interval (seconds)" "30"
  local who
  if [[ "$OS_MODE" == "local" ]]; then who="local host"
  else who="remote host(s) $OS_NAMES"
  fi
  echo "Polling OS metrics on $who every ${interval}s (Ctrl+C to stop)..."
  # `os metrics` / `os remote` are single-shot subcommands; loop them in bash.
  while true; do
    echo
    echo "----- $(date '+%Y-%m-%d %H:%M:%S') -----"
    _mon_run_os_once
    sleep "$interval" || break
  done
}

_mon_thresholds_list() { _dbmt_run thresholds list; }

_mon_cloud_list() { _dbmt_run cloud connections list; }

_mon_daemon_status() { _dbmt_run daemon status; }

_mon_daemon_start() {
  # The background daemon polls DB-style metrics, so it can target both
  # Connections-tab DB profiles and Monitor-tab-only DB profiles. Ask the user
  # which ones to include, pre-flight each, and pass only the survivors to
  # `daemon start`.
  echo "Pick the connections the daemon should monitor."
  echo "(Daemon polls DB metrics; cloud connections are listed but not yet polled by the daemon — use 'Fetch cloud metrics once' for those.)"

  _mon_pick_any_connections_multi db-monitor || {
    echo "No selection — aborting."
    return 1
  }

  local interval
  _dbmt_read interval "Poll interval (seconds)" "30"

  echo
  echo "Pre-flight: connecting to each selected database..."
  local -a ok_names=()
  local -a fail_names=()
  local pair src name
  for pair in "${MON_SEL_PAIRS[@]}"; do
    src="${pair%%$'\t'*}"
    name="${pair##*$'\t'}"
    printf "  - [%s] %s ... " "$src" "$name"
    if _dbmt_run monitor-connections test --source "$src" "$name" \
         >/dev/null 2>&1; then
      echo "ok"
      ok_names+=("$name")
    else
      echo "FAILED"
      fail_names+=("$name")
    fi
  done

  if ((${#fail_names[@]} > 0)); then
    echo
    echo "These connections failed pre-flight and will be skipped:"
    printf '  - %s\n' "${fail_names[@]}"
    if ((${#ok_names[@]} == 0)); then
      echo "Nothing reachable — aborting daemon start."
      return 1
    fi
    local cont
    read -r -p "Continue with the reachable ones? [Y/n]: " cont
    if [[ "$cont" =~ ^[Nn] ]]; then
      echo "Cancelled."
      return 1
    fi
  fi

  local joined
  joined="$(IFS=','; echo "${ok_names[*]}")"
  echo
  echo "Starting daemon (foreground) for: $joined"
  echo "Interval: ${interval}s. Press Ctrl+C to stop."
  _dbmt_run daemon start --foreground \
            --interval "$interval" \
            --connections "$joined"
}

_mon_daemon_stop() { _dbmt_run daemon stop; }

_mon_notify() {
  local sev msg
  PS3="Severity: "
  select sev in INFO WARNING CRITICAL Cancel; do
    [[ "$sev" == "Cancel" || -z "$sev" ]] && return
    break
  done
  _dbmt_read msg "Message"
  _dbmt_run notify send --severity "$sev" --message "$msg"
}

_mon_config_show() { _dbmt_run monitor-config show; }

_mon_config_set() {
  local sec key val
  _dbmt_read sec "Section (e.g. monitoring, cloud.lookback, notifications)"
  _dbmt_read key "Key"
  _dbmt_read val "Value"
  _dbmt_run monitor-config set "$sec" "$key" "$val"
}

_mon_notify_config_show() { _dbmt_run notify config show; }

_mon_config_restore() { _dbmt_run monitor-config restore; }

MENU_ITEMS=(
  "List saved connections (DB + monitor DB + monitor SSH + cloud)"
  "Add connection (monitor DB / cloud / OS-SSH)"
  "Test connection (DB + monitor + cloud)"
  "Poll DB metrics once"
  "Poll DB metrics (loop)"
  "Poll cloud metrics once"
  "Poll cloud metrics (loop)"
  "Poll OS metrics once"
  "Poll OS metrics (loop)"
  "List cloud connections"
  "List threshold rules"
  "Daemon status"
  "Start daemon (foreground)"
  "Stop daemon"
  "Send test notification"
  "Show monitor config (monitor_config.ini)"
  "Set monitor config value"
  "Show notification config"
  "Restore monitor config defaults"
  "Start REST API (foreground)"
  "Open full desktop UI (needs tkinter)"
  "Exit"
)
MENU_ACTIONS=(
  _mon_list_connections
  _mon_add_connection
  _mon_test_any_connection
  _mon_poll_db_once
  _mon_poll_db_loop
  _mon_poll_cloud_once
  _mon_poll_cloud_loop
  _mon_poll_os_once
  _mon_poll_os_loop
  _mon_cloud_list
  _mon_thresholds_list
  _mon_daemon_status
  _mon_daemon_start
  _mon_daemon_stop
  _mon_notify
  _mon_config_show
  _mon_config_set
  _mon_notify_config_show
  _mon_config_restore
  _dbmt_start_api
  _dbmt_try_full_ui
)

_dbmt_menu_loop "Monitoring"
