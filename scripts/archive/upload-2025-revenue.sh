#!/bin/bash
# Upload 2025 revenue categories CSV to the production upload API.
#
# Usage:
#   PROD_URL=https://your-app.up.railway.app ./scripts/upload-2025-revenue.sh
#
# The CSV covers 1/1/2025 – 12/31/2025 (full year).

set -euo pipefail

if [ -z "${PROD_URL:-}" ]; then
  echo "Error: PROD_URL not set. Example:"
  echo "  PROD_URL=https://your-app.up.railway.app ./scripts/upload-2025-revenue.sh"
  exit 1
fi

CSV_FILE="/Users/mike.rudoy_old/Downloads/union-revenue-categories-sky-ting-20260217-1703.csv"

if [ ! -f "$CSV_FILE" ]; then
  echo "Error: CSV file not found at $CSV_FILE"
  exit 1
fi

echo "Uploading 2025 revenue categories to $PROD_URL/api/upload..."
curl -X POST "$PROD_URL/api/upload" \
  -F "file=@$CSV_FILE" \
  -F "type=revenue_categories" \
  -F "periodStart=2025-01-01" \
  -F "periodEnd=2025-12-31"

echo ""
echo "Done! Check the dashboard Revenue section for 2025 data."
