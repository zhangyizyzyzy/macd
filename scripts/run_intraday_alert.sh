#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

exec "$ROOT_DIR/scripts/run_telegram_push.sh" \
  --profile alert \
  --label "盘中预警" \
  --filter-date today \
  --recent-bars 1 \
  --max-per-group 12 \
  "$@"
