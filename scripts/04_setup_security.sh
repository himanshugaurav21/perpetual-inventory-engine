#!/usr/bin/env bash
# Phase 4: UC Tags & Column Masks
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE}"
: "${DATABRICKS_WAREHOUSE_ID:?Set DATABRICKS_WAREHOUSE_ID}"
: "${PI_CATALOG:=perpetual_inventory_engine}"

P="--profile ${DATABRICKS_PROFILE}"

echo "=== Phase 4: Security Setup ==="

run_sql() {
    local sql="$1"
    echo "  SQL: ${sql:0:80}..."
    databricks api post /api/2.0/sql/statements ${P} --json "{
        \"warehouse_id\": \"${DATABRICKS_WAREHOUSE_ID}\",
        \"statement\": $(python3 -c "import json; print(json.dumps('$sql'))"),
        \"wait_timeout\": \"30s\"
    }" | python3 -c "
import sys, json
d = json.load(sys.stdin)
status = d.get('status', {}).get('state', 'UNKNOWN')
if status == 'FAILED':
    print(f'  WARN: {d.get(\"status\", {}).get(\"error\", {}).get(\"message\", \"\")}')
else:
    print(f'  OK')
" 2>/dev/null || echo "  SKIPPED"
}

# Column mask function
run_sql "CREATE OR REPLACE FUNCTION ${PI_CATALOG}.bronze.mask_employee_id(emp_id STRING) RETURNS STRING RETURN CONCAT('EMP-***', RIGHT(emp_id, 3))"

# Apply masks
run_sql "ALTER TABLE ${PI_CATALOG}.bronze.store_adjustments ALTER COLUMN adjusted_by SET MASK ${PI_CATALOG}.bronze.mask_employee_id"
run_sql "ALTER TABLE ${PI_CATALOG}.bronze.sales_transactions ALTER COLUMN cashier_id SET MASK ${PI_CATALOG}.bronze.mask_employee_id"

# UC Tags
run_sql "ALTER TABLE ${PI_CATALOG}.bronze.store_adjustments SET TAGS ('sensitivity' = 'high')"
run_sql "ALTER TABLE ${PI_CATALOG}.bronze.store_adjustments ALTER COLUMN adjusted_by SET TAGS ('pii_type' = 'employee_id')"
run_sql "ALTER TABLE ${PI_CATALOG}.bronze.sales_transactions ALTER COLUMN cashier_id SET TAGS ('pii_type' = 'employee_id')"

echo ""
echo "=== Phase 4 Complete ==="
