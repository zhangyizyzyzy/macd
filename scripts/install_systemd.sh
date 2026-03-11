#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SYSTEMD_DIR="${HOME}/.config/systemd/user"

mkdir -p "$SYSTEMD_DIR"

cat > "$SYSTEMD_DIR/macd-time-signal-alert.service" <<EOF
[Unit]
Description=MACD Time Signal Intraday Alert

[Service]
Type=oneshot
WorkingDirectory=$ROOT_DIR
ExecStart=$ROOT_DIR/scripts/run_intraday_alert.sh
EOF

cat > "$SYSTEMD_DIR/macd-time-signal-alert.timer" <<EOF
[Unit]
Description=Run MACD Time Signal Intraday Alert

[Timer]
OnCalendar=Mon..Fri *-*-* 02:00:00
OnCalendar=Mon..Fri *-*-* 02:30:00
OnCalendar=Mon..Fri *-*-* 03:00:00
OnCalendar=Mon..Fri *-*-* 03:30:00
OnCalendar=Mon..Fri *-*-* 05:30:00
OnCalendar=Mon..Fri *-*-* 06:00:00
OnCalendar=Mon..Fri *-*-* 06:30:00
OnCalendar=Mon..Fri *-*-* 07:00:00
Persistent=true
Unit=macd-time-signal-alert.service

[Install]
WantedBy=timers.target
EOF

cat > "$SYSTEMD_DIR/macd-time-signal-summary.service" <<EOF
[Unit]
Description=MACD Time Signal Close Summary

[Service]
Type=oneshot
WorkingDirectory=$ROOT_DIR
ExecStart=$ROOT_DIR/scripts/run_close_summary.sh
EOF

cat > "$SYSTEMD_DIR/macd-time-signal-summary.timer" <<EOF
[Unit]
Description=Run MACD Time Signal Close Summary

[Timer]
OnCalendar=Mon..Fri *-*-* 09:00:00
Persistent=true
Unit=macd-time-signal-summary.service

[Install]
WantedBy=timers.target
EOF

systemctl --user daemon-reload
systemctl --user enable --now macd-time-signal-alert.timer
systemctl --user enable --now macd-time-signal-summary.timer
systemctl --user list-timers 'macd-time-signal-*'
