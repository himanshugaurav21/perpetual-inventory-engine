#!/usr/bin/env bash
# Phase 2: Data Generation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."
source "${ROOT_DIR}/config.env" 2>/dev/null || true

echo "=== Phase 2: Data Generation ==="
echo "  Catalog: ${PI_CATALOG:=perpetual_inventory_engine}"
echo "  Profile: ${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE}"

cd "${ROOT_DIR}"
uv run --with polars --with numpy --with mimesis \
       --with "databricks-connect>=16.4,<17.0" \
       generate_data.py

echo ""
echo "=== Phase 2 Complete ==="
