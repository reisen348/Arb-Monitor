#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_NAME="perp-arb-dashboard.service"
SRC_UNIT="$ROOT_DIR/scripts/$UNIT_NAME"
USER_UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
DST_UNIT="$USER_UNIT_DIR/$UNIT_NAME"

mkdir -p "$USER_UNIT_DIR"
sed "s|__ARB_MONITOR_ROOT__|$ROOT_DIR|g" "$SRC_UNIT" > "$DST_UNIT"

echo "installed user unit: $DST_UNIT"
echo
echo "next commands:"
echo "  systemctl --user daemon-reload"
echo "  systemctl --user enable --now $UNIT_NAME"
echo "  systemctl --user status $UNIT_NAME"
echo "  journalctl --user -u $UNIT_NAME -f"
echo
echo "if you need it to survive logout:"
echo "  loginctl enable-linger $USER"
