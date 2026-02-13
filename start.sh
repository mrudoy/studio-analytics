#!/bin/bash
set -e

echo "=== Studio Analytics Container Starting ==="
echo "PORT=${PORT:-3000}"
echo "NODE_ENV=${NODE_ENV}"
echo "Starting Xvfb..."

# Start Xvfb in background
Xvfb :99 -screen 0 1280x720x24 -nolisten tcp &
XVFB_PID=$!
export DISPLAY=:99

# Give Xvfb a moment to start
sleep 2

if kill -0 $XVFB_PID 2>/dev/null; then
  echo "Xvfb started successfully (PID: $XVFB_PID)"
else
  echo "ERROR: Xvfb failed to start"
  exit 1
fi

echo "Starting Next.js on 0.0.0.0:${PORT:-3000}..."
exec npx next start -H 0.0.0.0 -p ${PORT:-3000}
