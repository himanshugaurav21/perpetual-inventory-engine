#!/usr/bin/env bash
# Phase 3: DLT Pipeline Deployment
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
source "${ROOT_DIR}/config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE}"
: "${DATABRICKS_USER:?Set DATABRICKS_USER}"
: "${PI_CATALOG:=perpetual_inventory_engine}"
: "${PIPELINE_NAME:=perpetual-inventory-pipeline}"
: "${PIPELINE_TARGET_SCHEMA:=gold}"

P="--profile ${DATABRICKS_PROFILE}"
NOTEBOOK_PATH="/Users/${DATABRICKS_USER}/perpetual-inventory-engine/pipeline_notebook"

echo "=== Phase 3: Pipeline Deployment ==="
echo "  Notebook: ${NOTEBOOK_PATH}"
echo "  Pipeline: ${PIPELINE_NAME}"

# Upload notebook
echo "--- Uploading pipeline notebook ---"
databricks workspace import "${NOTEBOOK_PATH}" \
    --file "${ROOT_DIR}/pipeline_notebook.py" \
    --format SOURCE --language PYTHON --overwrite ${P}
echo "  Uploaded."

# Create pipeline
echo "--- Creating DLT pipeline ---"
PIPELINE_JSON=$(cat <<EOF
{
    "name": "${PIPELINE_NAME}",
    "catalog": "${PI_CATALOG}",
    "target": "${PIPELINE_TARGET_SCHEMA}",
    "serverless": true,
    "continuous": false,
    "channel": "CURRENT",
    "libraries": [
        {"notebook": {"path": "${NOTEBOOK_PATH}"}}
    ],
    "configuration": {
        "pipelines.enableTrackHistory": "true",
        "pipeline.volume_base": "/Volumes/${PI_CATALOG}/bronze/source_files"
    }
}
EOF
)

RESULT=$(databricks api post /api/2.0/pipelines ${P} --json "${PIPELINE_JSON}" 2>&1)
PIPELINE_ID=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pipeline_id',''))" 2>/dev/null || echo "")

if [ -z "$PIPELINE_ID" ]; then
    echo "  Pipeline may already exist. Checking..."
    PIPELINE_ID=$(databricks pipelines list ${P} -o json 2>/dev/null | \
        python3 -c "import sys,json; pipelines=json.load(sys.stdin); print(next((p['pipeline_id'] for p in pipelines if p.get('name')=='${PIPELINE_NAME}'), ''))" 2>/dev/null || echo "")
fi

if [ -n "$PIPELINE_ID" ]; then
    echo "  Pipeline ID: ${PIPELINE_ID}"
    echo "--- Starting pipeline (full refresh) ---"
    databricks pipelines start-update "${PIPELINE_ID}" --full-refresh ${P} || true
    echo "  Pipeline update triggered."
else
    echo "  ERROR: Could not create or find pipeline."
fi

echo ""
echo "=== Phase 3 Complete ==="
echo "  Pipeline ID: ${PIPELINE_ID:-UNKNOWN}"
