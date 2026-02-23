#!/bin/bash
#
# Install or uninstall the local pipeline cron job (macOS launchd).
#
# Usage:
#   ./scripts/install-cron.sh           # Install
#   ./scripts/install-cron.sh --uninstall  # Uninstall
#   ./scripts/install-cron.sh --status     # Check status
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PLIST_NAME="com.skyting.pipeline"
PLIST_SRC="$SCRIPT_DIR/$PLIST_NAME.plist"
PLIST_DST="$HOME/Library/LaunchAgents/$PLIST_NAME.plist"
LOG_DIR="$PROJECT_DIR/data/logs"

case "${1:-install}" in
  --uninstall)
    echo "[cron] Uninstalling $PLIST_NAME..."
    launchctl unload "$PLIST_DST" 2>/dev/null || true
    rm -f "$PLIST_DST"
    echo "[cron] Uninstalled. Cron job removed."
    ;;

  --status)
    echo "[cron] Checking status..."
    if launchctl list | grep -q "$PLIST_NAME"; then
      echo "[cron] Status: INSTALLED and LOADED"
      launchctl list "$PLIST_NAME" 2>/dev/null || true
    elif [ -f "$PLIST_DST" ]; then
      echo "[cron] Status: INSTALLED but NOT LOADED"
      echo "  Run: launchctl load $PLIST_DST"
    else
      echo "[cron] Status: NOT INSTALLED"
      echo "  Run: ./scripts/install-cron.sh"
    fi

    # Show recent logs
    if [ -f "$LOG_DIR/pipeline-cron.log" ]; then
      echo ""
      echo "[cron] Last 10 lines of pipeline-cron.log:"
      tail -10 "$LOG_DIR/pipeline-cron.log"
    fi
    ;;

  install|"")
    echo "[cron] Installing $PLIST_NAME..."

    # Verify plist exists
    if [ ! -f "$PLIST_SRC" ]; then
      echo "ERROR: Plist not found at $PLIST_SRC"
      exit 1
    fi

    # Create log directory
    mkdir -p "$LOG_DIR"

    # Unload if already loaded
    launchctl unload "$PLIST_DST" 2>/dev/null || true

    # Copy plist to LaunchAgents
    cp "$PLIST_SRC" "$PLIST_DST"
    echo "[cron] Copied plist to $PLIST_DST"

    # Load the job
    launchctl load "$PLIST_DST"
    echo "[cron] Loaded into launchd"

    echo ""
    echo "[cron] Pipeline cron installed successfully!"
    echo "  Schedule: Daily at 6:00 AM"
    echo "  Logs:     $LOG_DIR/pipeline-cron.log"
    echo "  Errors:   $LOG_DIR/pipeline-cron-error.log"
    echo ""
    echo "  To test manually:  npm run pipeline:local"
    echo "  To check status:   ./scripts/install-cron.sh --status"
    echo "  To uninstall:      ./scripts/install-cron.sh --uninstall"
    ;;

  *)
    echo "Usage: $0 [install|--uninstall|--status]"
    exit 1
    ;;
esac
