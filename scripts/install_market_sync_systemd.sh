#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SYSTEMD_DIR="${HOME}/.config/systemd/user"

mkdir -p "$SYSTEMD_DIR"

cat > "$SYSTEMD_DIR/macd-market-sync.service" <<EOF
[Unit]
Description=MACD Market Data Daily Sync

[Service]
Type=oneshot
WorkingDirectory=$ROOT_DIR
ExecStart=$ROOT_DIR/scripts/run_daily_market_sync.sh
EOF

cat > "$SYSTEMD_DIR/macd-market-sync.timer" <<EOF
[Unit]
Description=Run MACD Market Data Daily Sync

[Timer]
OnCalendar=Mon..Fri *-*-* 08:40:00
Persistent=true
Unit=macd-market-sync.service

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now macd-market-sync.timer
systemctl --user list-timers 'macd-market-sync*'
