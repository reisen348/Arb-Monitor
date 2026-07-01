#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${PID_FILE:-/tmp/perp_arb_dashboard.pid}"
LOG_FILE="${LOG_FILE:-/tmp/perp_arb_dashboard.log}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8765}"
DB_PATH="${DB_PATH:-arb_state.db}"

DASHBOARD_CMD=(
  python3 -u -m perp_arb dashboard
  --source all_live
  --transport "${TRANSPORT:-rest}"
  --top "${TOP:-10000}"
  --min-label blocked
  --refresh "${REFRESH:-10}"
  --interval "${INTERVAL:-10}"
  --host "$HOST"
  --port "$PORT"
  --top-book-markets "${TOP_BOOK_MARKETS:-25}"
  --grvt-top-book-markets "${GRVT_TOP_BOOK_MARKETS:-100}"
  --lighter-book-request-workers "${LIGHTER_BOOK_REQUEST_WORKERS:-1}"
  --db-path "$DB_PATH"
)

is_running() {
  if [[ ! -f "$PID_FILE" ]]; then
    return 1
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

start_dashboard() {
  if is_running; then
    echo "dashboard already running: pid=$(cat "$PID_FILE")"
    return 0
  fi

  cd "$ROOT_DIR"
  if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
  fi

  if command -v setsid >/dev/null 2>&1 && setsid true >/dev/null 2>&1; then
    setsid "${DASHBOARD_CMD[@]}" >>"$LOG_FILE" 2>&1 < /dev/null &
  else
    nohup "${DASHBOARD_CMD[@]}" >>"$LOG_FILE" 2>&1 < /dev/null &
  fi
  echo $! > "$PID_FILE"
  sleep 2

  if is_running; then
    echo "dashboard started: pid=$(cat "$PID_FILE") log=$LOG_FILE port=$PORT"
    return 0
  fi

  echo "dashboard failed to start; recent log:"
  tail -n 40 "$LOG_FILE" 2>/dev/null || true
  rm -f "$PID_FILE"
  return 1
}

stop_dashboard() {
  if ! is_running; then
    rm -f "$PID_FILE"
    echo "dashboard not running"
    return 0
  fi

  local pid
  pid="$(cat "$PID_FILE")"
  kill "$pid"
  rm -f "$PID_FILE"
  echo "dashboard stopped: pid=$pid"
}

status_dashboard() {
  if is_running; then
    echo "dashboard running: pid=$(cat "$PID_FILE") log=$LOG_FILE port=$PORT"
  else
    echo "dashboard not running"
    return 1
  fi
}

show_logs() {
  tail -f "$LOG_FILE"
}

case "${1:-}" in
  start)
    start_dashboard
    ;;
  stop)
    stop_dashboard
    ;;
  restart)
    stop_dashboard || true
    start_dashboard
    ;;
  status)
    status_dashboard
    ;;
  logs)
    show_logs
    ;;
  *)
    echo "usage: $0 {start|stop|restart|status|logs}"
    exit 1
    ;;
esac
