#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 YYYY-MM-DD [archive_csv]"
  exit 1
fi

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
DATE_VALUE="$1"
ARCHIVE_FILE="${2:-$ROOT_DIR/outputs/latest_scan.csv}"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

exec "$ROOT_DIR/.venv/bin/python" \
  "$ROOT_DIR/macd_time_signal_telegram_push.py" \
  --mode replay \
  --profile manual \
  --replay-file "$ARCHIVE_FILE" \
  --replay-date "$DATE_VALUE" \
  --no-dedup \
  --label "replay-$DATE_VALUE"
