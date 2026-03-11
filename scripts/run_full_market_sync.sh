#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${DB_PATH:-$ROOT_DIR/data/market_data.sqlite}"
LEVELS="${LEVELS:-15m,daily}"
BATCH_SIZE="${BATCH_SIZE:-30}"
START_BATCH="${START_BATCH:-1}"
UNIVERSE_CACHE="${UNIVERSE_CACHE:-$ROOT_DIR/data/universe_latest.csv}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs}"
LOG_PATH="${LOG_PATH:-$LOG_DIR/full_market_sync.log}"

mkdir -p "$LOG_DIR"

if [ ! -f "$UNIVERSE_CACHE" ]; then
  python3 "$ROOT_DIR/scripts/sync_market_data.py" \
    --levels "$LEVELS" \
    --batch-size "$BATCH_SIZE" \
    --batch-index 1 \
    --db-path "$DB_PATH" \
    --skip-bootstrap \
    --universe-retries 3 \
    | tee -a "$LOG_PATH"
  START_BATCH=2
fi

TOTAL_SYMBOLS="$(python3 - <<PY
import pandas as pd
from pathlib import Path
path = Path(r'''$UNIVERSE_CACHE''')
df = pd.read_csv(path, dtype={'symbol': str})
print(len(df))
PY
)"

TOTAL_BATCHES="$(( (TOTAL_SYMBOLS + BATCH_SIZE - 1) / BATCH_SIZE ))"

for (( batch=START_BATCH; batch<=TOTAL_BATCHES; batch++ )); do
  printf '[%s] running batch %d/%d\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$batch" "$TOTAL_BATCHES" | tee -a "$LOG_PATH"
  python3 -u "$ROOT_DIR/scripts/sync_market_data.py" \
    --levels "$LEVELS" \
    --batch-size "$BATCH_SIZE" \
    --batch-index "$batch" \
    --db-path "$DB_PATH" \
    --skip-bootstrap \
    --prefer-cached-universe \
    --universe-cache "$UNIVERSE_CACHE" \
    | tee -a "$LOG_PATH"
done

printf '[%s] full market sync finished\n' "$(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_PATH"
