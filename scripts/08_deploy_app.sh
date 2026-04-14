#!/usr/bin/env bash
# Phase 8: Databricks App Deployment
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
source "${ROOT_DIR}/config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE}"
: "${DATABRICKS_USER:?Set DATABRICKS_USER}"
: "${DATABRICKS_WAREHOUSE_ID:?Set DATABRICKS_WAREHOUSE_ID}"
: "${APP_NAME:=perpetual-inventory-app}"
: "${APP_WORKSPACE_PATH:=/Workspace/Users/${DATABRICKS_USER}/${APP_NAME}}"
: "${PI_CATALOG:=perpetual_inventory_engine}"
: "${SERVING_ENDPOINT:=databricks-claude-sonnet-4-5}"
: "${GENIE_SPACE_ID:=}"

P="--profile ${DATABRICKS_PROFILE}"
APP_DIR="${ROOT_DIR}/perpetual-inventory-app"

echo "=== Phase 8: App Deployment ==="
echo "  App:  ${APP_NAME}"
echo "  Path: ${APP_WORKSPACE_PATH}"

# Build frontend
echo "--- Building frontend ---"
cd "${APP_DIR}/frontend"
npm install
npm run build
echo "  Frontend built to dist/"

# Generate app.yaml with values from config.env
echo "--- Generating app.yaml ---"
cat > "${APP_DIR}/app.yaml" <<YAML
command:
  - "python"
  - "-m"
  - "uvicorn"
  - "app:app"
  - "--host"
  - "0.0.0.0"
  - "--port"
  - "8000"
env:
  - name: SERVING_ENDPOINT
    value: ${SERVING_ENDPOINT}
  - name: PI_CATALOG
    value: ${PI_CATALOG}
  - name: DATABRICKS_WAREHOUSE_ID
    value: ${DATABRICKS_WAREHOUSE_ID}
  - name: GENIE_SPACE_ID
    value: "${GENIE_SPACE_ID:-}"
YAML
echo "  app.yaml generated"

# Create app
echo "--- Creating Databricks App ---"
databricks apps create "${APP_NAME}" \
    --description "FreshMart Inventory Intelligence - PI Accuracy Engine" \
    ${P} 2>&1 || echo "  App may already exist"

# Sync files
echo "--- Syncing files ---"
cd "${APP_DIR}"
databricks sync . "${APP_WORKSPACE_PATH}" \
    --exclude node_modules \
    --exclude .venv \
    --exclude __pycache__ \
    --exclude .git \
    --exclude "frontend/src" \
    --exclude "frontend/public" \
    --exclude "frontend/node_modules" \
    --exclude ".databricks" \
    ${P}

# Deploy
echo "--- Deploying ---"
databricks apps deploy "${APP_NAME}" \
    --source-code-path "${APP_WORKSPACE_PATH}" \
    ${P}

# Get URL
APP_URL=$(databricks apps get "${APP_NAME}" ${P} -o json 2>/dev/null | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('url','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")

echo ""
echo "=== Phase 8 Complete ==="
echo "  App URL: ${APP_URL}"
echo ""
echo "  Post-deployment steps:"
echo "    1. Add SQL Warehouse resource (ID: ${DATABRICKS_WAREHOUSE_ID:-YOUR_ID})"
echo "    2. Add Serving Endpoint resource (${SERVING_ENDPOINT})"
echo "    3. Grant SP access to ${PI_CATALOG} catalog (bronze, gold, serving schemas)"
echo "    4. Redeploy to pick up environment variables"
