#!/bin/bash
#
# Run the pipeline locally but write data to the Railway (prod) database.
#
# Usage:
#   ./scripts/run-pipeline-prod.sh
#
# Prerequisites:
#   1. Create .env.production with the Railway DATABASE_URL:
#      DATABASE_URL=postgresql://postgres:PASSWORD@HOST:PORT/railway
#
#   2. The local machine needs Redis running (or set REDIS_URL)
#
# The script starts the Next.js dev server temporarily, triggers the pipeline,
# waits for completion, and shuts down.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# Check .env.production exists
if [ ! -f .env.production.local ]; then
  echo "ERROR: .env.production.local not found."
  echo ""
  echo "Create it with your Railway DATABASE_URL:"
  echo "  cp .env .env.production.local"
  echo "  # Then edit DATABASE_URL to point to Railway Postgres"
  echo ""
  exit 1
fi

echo "[pipeline-prod] Loading env from .env.production.local"

# Export env vars from .env.production.local (override .env)
set -a
source .env.production.local
set +a

echo "[pipeline-prod] DATABASE_URL points to: $(echo $DATABASE_URL | sed 's/:[^:@]*@/:***@/')"

# ── Staleness guard ──
# Skip if last pipeline run was less than 18 hours ago (avoids duplicate runs
# when launchd fires at 6am AND on login/wake the same day).
STALE_HOURS=18
LAST_RUN_AGE=$(psql "$DATABASE_URL" -t -A -c \
  "SELECT EXTRACT(EPOCH FROM (NOW() - MAX(completed_at))) / 3600 FROM pipeline_runs WHERE status = 'complete'" \
  2>/dev/null || echo "999")

# Trim whitespace and handle empty/null
LAST_RUN_AGE=$(echo "$LAST_RUN_AGE" | tr -d '[:space:]')
if [ -z "$LAST_RUN_AGE" ] || [ "$LAST_RUN_AGE" = "" ]; then
  LAST_RUN_AGE="999"
fi

# Use awk for float comparison (bash can't do decimals)
SHOULD_SKIP=$(echo "$LAST_RUN_AGE $STALE_HOURS" | awk '{print ($1 < $2) ? "yes" : "no"}')

if [ "$SHOULD_SKIP" = "yes" ]; then
  echo "[pipeline-prod] Data is ${LAST_RUN_AGE}h old (< ${STALE_HOURS}h threshold). Skipping."
  exit 0
fi

echo "[pipeline-prod] Data is ${LAST_RUN_AGE}h old. Running pipeline..."

# Trigger pipeline via API
echo "[pipeline-prod] Starting Next.js dev server..."
npx next dev --port 3099 &
NEXT_PID=$!

# Wait for server to be ready
echo "[pipeline-prod] Waiting for server..."
for i in $(seq 1 30); do
  if curl -s http://localhost:3099/api/health > /dev/null 2>&1; then
    echo "[pipeline-prod] Server ready!"
    break
  fi
  sleep 2
done

# Trigger pipeline
echo "[pipeline-prod] Triggering pipeline..."
RESPONSE=$(curl -s -X POST http://localhost:3099/api/pipeline)
echo "[pipeline-prod] Pipeline response: $RESPONSE"

# Extract jobId from response
JOB_ID=$(echo "$RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('jobId',''))" 2>/dev/null)
if [ -z "$JOB_ID" ]; then
  echo "[pipeline-prod] ERROR: No jobId in response. Aborting."
  kill $NEXT_PID 2>/dev/null; wait $NEXT_PID 2>/dev/null
  exit 1
fi
echo "[pipeline-prod] Job ID: $JOB_ID"

# Listen to SSE status stream (the /api/status endpoint returns Server-Sent Events)
echo "[pipeline-prod] Monitoring pipeline via SSE..."
TIMEOUT=1800  # 30 minutes max
SSE_LOG=$(mktemp)

# Stream SSE events into a temp file, with a timeout
curl -s -N --max-time $TIMEOUT \
  "http://localhost:3099/api/status?jobId=$JOB_ID" > "$SSE_LOG" 2>/dev/null &
CURL_PID=$!

# Parse SSE events in real-time
PIPELINE_RESULT="unknown"
ELAPSED=0
while kill -0 $CURL_PID 2>/dev/null; do
  # Read last event from the log
  LAST_EVENT=$(grep "^event:" "$SSE_LOG" | tail -1 | sed 's/^event: //')
  LAST_DATA=$(grep "^data:" "$SSE_LOG" | tail -1 | sed 's/^data: //')

  if [ "$LAST_EVENT" = "complete" ]; then
    DURATION=$(echo "$LAST_DATA" | python3 -c "import json,sys; print(json.load(sys.stdin).get('duration',0))" 2>/dev/null || echo "?")
    echo "[pipeline-prod] Pipeline complete! Duration: ${DURATION}s"
    PIPELINE_RESULT="complete"
    kill $CURL_PID 2>/dev/null
    break
  elif [ "$LAST_EVENT" = "error" ]; then
    MSG=$(echo "$LAST_DATA" | python3 -c "import json,sys; print(json.load(sys.stdin).get('message','Unknown error'))" 2>/dev/null || echo "Unknown error")
    echo "[pipeline-prod] Pipeline FAILED: $MSG"
    PIPELINE_RESULT="error"
    kill $CURL_PID 2>/dev/null
    break
  elif [ "$LAST_EVENT" = "progress" ]; then
    STEP=$(echo "$LAST_DATA" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'{d.get(\"step\",\"...\")}: {d.get(\"percent\",0)}%')" 2>/dev/null || echo "...")
    echo "[pipeline-prod] $STEP"
  fi

  sleep 3
  ELAPSED=$((ELAPSED + 3))
  if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "[pipeline-prod] Timed out after ${TIMEOUT}s"
    PIPELINE_RESULT="timeout"
    kill $CURL_PID 2>/dev/null
    break
  fi
done

wait $CURL_PID 2>/dev/null
rm -f "$SSE_LOG"

if [ "$PIPELINE_RESULT" != "complete" ]; then
  echo "[pipeline-prod] Pipeline did not complete successfully (result: $PIPELINE_RESULT)"
fi

# Cleanup
echo "[pipeline-prod] Shutting down server..."
kill $NEXT_PID 2>/dev/null
wait $NEXT_PID 2>/dev/null

echo "[pipeline-prod] Done."
