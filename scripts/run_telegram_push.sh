#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

mkdir -p "$ROOT_DIR/outputs" "$ROOT_DIR/state" "$ROOT_DIR/logs"

exec "$ROOT_DIR/.venv/bin/python" \
  "$ROOT_DIR/macd_time_signal_telegram_push.py" \
  --output "$ROOT_DIR/outputs/latest_scan.csv" \
  --archive-dir "$ROOT_DIR/outputs/archive" \
  --report-dir "$ROOT_DIR/reports" \
  --state-file "$ROOT_DIR/state/telegram_push_state.json" \
  --lock-file "$ROOT_DIR/state/telegram_push.lock" \
  "$@"
