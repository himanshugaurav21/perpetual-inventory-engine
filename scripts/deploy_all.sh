#!/usr/bin/env bash
# =============================================================================
# Perpetual Inventory Engine - Full Deployment
# =============================================================================
# Orchestrates all 8 deployment phases sequentially.
#
# Usage:
#   cp config.env.template config.env  # Edit with your workspace details
#   source config.env
#   databricks auth login --host $DATABRICKS_HOST --profile $DATABRICKS_PROFILE
#   bash scripts/deploy_all.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config.env" 2>/dev/null || true

echo "============================================="
echo "  Perpetual Inventory Engine - Full Deploy"
echo "============================================="
echo "  Host:    ${DATABRICKS_HOST:-NOT SET}"
echo "  Profile: ${DATABRICKS_PROFILE:-NOT SET}"
echo "  Catalog: ${PI_CATALOG:=perpetual_inventory_engine}"
echo "============================================="
echo ""

# Phase 1
bash "${SCRIPT_DIR}/01_setup_catalog.sh"
echo ""

# Phase 2
bash "${SCRIPT_DIR}/02_generate_data.sh"
echo ""

# Phase 3
bash "${SCRIPT_DIR}/03_deploy_pipeline.sh"
echo ""
echo "Waiting 60s for pipeline to initialize..."
sleep 60

# Phase 4
bash "${SCRIPT_DIR}/04_setup_security.sh"
echo ""

# Phase 5
source "${SCRIPT_DIR}/../config.env" 2>/dev/null || true
uv run --with "databricks-connect>=16.4,<17.0" "${SCRIPT_DIR}/05_setup_serving.py"
echo ""

# Phase 6
bash "${SCRIPT_DIR}/06_setup_vector_search.sh"
echo ""

# Phase 7
python3 "${SCRIPT_DIR}/07_setup_genie.py"
echo ""

# Phase 8
bash "${SCRIPT_DIR}/08_deploy_app.sh"

echo ""
echo "============================================="
echo "  DEPLOYMENT COMPLETE"
echo "============================================="
echo "  Catalog: ${PI_CATALOG}"
echo "  Pipeline: ${PIPELINE_NAME:-perpetual-inventory-pipeline}"
echo "  App: ${APP_NAME:-perpetual-inventory-app}"
echo "============================================="
