#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

if [[ -x "$ROOT_DIR/scripts/run_daily_market_sync.sh" ]]; then
  "$ROOT_DIR/scripts/run_daily_market_sync.sh"
fi

exec "$ROOT_DIR/scripts/run_telegram_push.sh" \
  --profile summary \
  --data-source market-db \
  --label "收盘汇总" \
  --filter-date today \
  --recent-bars 1 \
  --all-signals \
  --notify-empty \
  --max-per-group 50 \
  "$@"
