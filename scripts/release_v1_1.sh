#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
VERSION=$(cat "$ROOT_DIR/VERSION")
RELEASE_DIR="$ROOT_DIR/dist/macd_time_signal_v${VERSION}"
ARCHIVE_PATH="$ROOT_DIR/dist/macd_time_signal_v${VERSION}.tar.gz"

mkdir -p "$ROOT_DIR/dist"
rm -rf "$RELEASE_DIR"
mkdir -p "$RELEASE_DIR"

cp "$ROOT_DIR/README.md" "$RELEASE_DIR/"
cp "$ROOT_DIR/VERSION" "$RELEASE_DIR/"
cp "$ROOT_DIR/requirements.txt" "$RELEASE_DIR/"
cp "$ROOT_DIR/backtest_store.py" "$RELEASE_DIR/"
cp "$ROOT_DIR/macd_time_signal_scanner.py" "$RELEASE_DIR/"
cp "$ROOT_DIR/macd_timeframe_backtest.py" "$RELEASE_DIR/"
cp "$ROOT_DIR/macd_time_signal_telegram_push.py" "$RELEASE_DIR/"
cp "$ROOT_DIR/macd_time_signal_indicator.pine" "$RELEASE_DIR/"
cp "$ROOT_DIR/.env.example" "$RELEASE_DIR/"
mkdir -p "$RELEASE_DIR/scripts"
cp "$ROOT_DIR/scripts/run_telegram_push.sh" "$RELEASE_DIR/scripts/"
cp "$ROOT_DIR/scripts/run_intraday_alert.sh" "$RELEASE_DIR/scripts/"
cp "$ROOT_DIR/scripts/run_close_summary.sh" "$RELEASE_DIR/scripts/"
cp "$ROOT_DIR/scripts/replay_by_date.sh" "$RELEASE_DIR/scripts/"
cp "$ROOT_DIR/scripts/push_status.py" "$RELEASE_DIR/scripts/"
cp "$ROOT_DIR/scripts/install_systemd.sh" "$RELEASE_DIR/scripts/"

tar -czf "$ARCHIVE_PATH" -C "$ROOT_DIR/dist" "macd_time_signal_v${VERSION}"

printf 'release ready: %s\n' "$RELEASE_DIR"
printf 'archive ready: %s\n' "$ARCHIVE_PATH"
