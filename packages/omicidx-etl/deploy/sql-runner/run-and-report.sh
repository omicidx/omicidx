#!/usr/bin/env bash
set -euo pipefail

ETL_DIR=/home/davsean/Documents/git/omicidx-etl
OMICIDX_DIR=/home/davsean/Documents/git/omicidx

REPO="omicidx/omicidx-etl"
WORKFLOW="sql_runner_status.yaml"

start_time=$(date +%s)
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

exit_code=0

# Step 1: Consolidate raw data to parquet on R2
echo "=== Step 1: Parquet consolidation (omicidx-etl) ==="
cd "$ETL_DIR"
uv run oidx sql run || exit_code=$?

# Step 2: Build DuckDB views and upload (only if step 1 succeeded)
#if [ "$exit_code" -eq 0 ]; then
#    echo "=== Step 2: DuckDB build + upload (omicidx) ==="
#    cd "$OMICIDX_DIR"
#    uv run build_db.py --upload || exit_code=$?
#fi

end_time=$(date +%s)
duration=$(( end_time - start_time ))

# Determine status
if [ "$exit_code" -eq 0 ]; then
    status="success"
else
    status="failure"
fi

# Report status to GitHub Actions
gh workflow run "$WORKFLOW" \
    --repo "$REPO" \
    -f status="$status" \
    -f duration="${duration}s" \
    -f timestamp="$timestamp" \
    -f details="Exit code: $exit_code" \
    || echo "Warning: failed to report status to GitHub Actions"

exit "$exit_code"
