#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
DB_PATH="${DB_PATH:-$ROOT_DIR/data/market_data.sqlite}"
UNIVERSE_CACHE="${UNIVERSE_CACHE:-$ROOT_DIR/data/universe_latest.csv}"
LEVELS="${LEVELS:-15m,60m,120m,240m,daily}"
BATCH_SIZE="${BATCH_SIZE:-50}"
LOOKBACK_DAYS="${LOOKBACK_DAYS:-14}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/logs}"
LOG_PATH="${LOG_PATH:-$LOG_DIR/daily_market_sync.log}"
LOCK_DIR="${LOCK_DIR:-$ROOT_DIR/state/market_data_sync.lock}"
WAIT_FOR_SYNC_SECONDS="${WAIT_FOR_SYNC_SECONDS:-0}"
LOCK_FAILURE_EXIT_CODE="${LOCK_FAILURE_EXIT_CODE:-0}"

mkdir -p "$LOG_DIR" "$(dirname "$DB_PATH")" "$(dirname "$UNIVERSE_CACHE")" "$(dirname "$LOCK_DIR")"

if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="python3"
fi

read -r START_DATE END_DATE <<EOF
$("$PYTHON_BIN" - <<PY
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

lookback_days = int(${LOOKBACK_DAYS})
now = datetime.now(ZoneInfo("Asia/Shanghai"))
print((now - timedelta(days=lookback_days)).strftime("%Y%m%d"), now.strftime("%Y%m%d"))
PY
)
EOF

acquire_lock() {
  local started_at now elapsed
  started_at="$(date +%s)"
  while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    if (( WAIT_FOR_SYNC_SECONDS <= 0 )); then
      printf '[%s] daily market sync already running, lock=%s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$LOCK_DIR" | tee -a "$LOG_PATH"
      return 1
    fi
    now="$(date +%s)"
    elapsed="$(( now - started_at ))"
    if (( elapsed >= WAIT_FOR_SYNC_SECONDS )); then
      printf '[%s] daily market sync wait timeout after %ss, lock=%s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$elapsed" "$LOCK_DIR" | tee -a "$LOG_PATH"
      return 1
    fi
    sleep 10
  done
  printf '%s\n' "$$" > "$LOCK_DIR/pid"
  trap 'rm -rf "$LOCK_DIR"' EXIT
  return 0
}

if ! acquire_lock; then
  exit "$LOCK_FAILURE_EXIT_CODE"
fi

"$PYTHON_BIN" - <<PY
from pathlib import Path
from macd_time_signal_scanner import AKShareProvider, ScanConfig

cache_path = Path(r'''$UNIVERSE_CACHE''')
cache_path.parent.mkdir(parents=True, exist_ok=True)
provider = AKShareProvider()
cfg = ScanConfig(
    start_date="$START_DATE",
    end_date="$END_DATE",
    period="daily",
    workers=1,
    recent_bars=9999,
    latest_only=False,
)
try:
    universe = provider.get_universe(cfg)
    universe.to_csv(cache_path, index=False, encoding="utf-8-sig")
    print(f"refreshed universe -> {cache_path} rows={len(universe)}")
except Exception as exc:
    if not cache_path.exists():
        raise
    print(f"refresh universe failed, fallback to cache: {exc}")
PY

TOTAL_SYMBOLS="$("$PYTHON_BIN" - <<PY
import pandas as pd
from pathlib import Path

path = Path(r'''$UNIVERSE_CACHE''')
df = pd.read_csv(path, dtype={"symbol": str})
print(len(df))
PY
)"

TOTAL_BATCHES="$(( (TOTAL_SYMBOLS + BATCH_SIZE - 1) / BATCH_SIZE ))"

printf '[%s] daily market sync start start_date=%s end_date=%s levels=%s total_symbols=%s batches=%s\n' \
  "$(date '+%Y-%m-%d %H:%M:%S')" "$START_DATE" "$END_DATE" "$LEVELS" "$TOTAL_SYMBOLS" "$TOTAL_BATCHES" | tee -a "$LOG_PATH"

for (( batch=1; batch<=TOTAL_BATCHES; batch++ )); do
  printf '[%s] running batch %d/%d\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$batch" "$TOTAL_BATCHES" | tee -a "$LOG_PATH"
  "$PYTHON_BIN" -u "$ROOT_DIR/scripts/sync_market_data.py" \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --levels "$LEVELS" \
    --batch-size "$BATCH_SIZE" \
    --batch-index "$batch" \
    --db-path "$DB_PATH" \
    --skip-bootstrap \
    --prefer-cached-universe \
    --universe-cache "$UNIVERSE_CACHE" \
    | tee -a "$LOG_PATH"
done

printf '[%s] daily market sync finished\n' "$(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_PATH"
