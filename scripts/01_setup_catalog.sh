#!/usr/bin/env bash
# Phase 1: Catalog & Schema Setup
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE in config.env}"
: "${DATABRICKS_WAREHOUSE_ID:?Set DATABRICKS_WAREHOUSE_ID in config.env}"
: "${PI_CATALOG:=perpetual_inventory_engine}"

P="--profile ${DATABRICKS_PROFILE}"

echo "=== Phase 1: Catalog & Schema Setup ==="
echo "  Catalog: ${PI_CATALOG}"

run_sql() {
    local sql="$1"
    echo "  SQL: ${sql}"
    databricks api post /api/2.0/sql/statements ${P} --json "{
        \"warehouse_id\": \"${DATABRICKS_WAREHOUSE_ID}\",
        \"statement\": \"${sql}\",
        \"wait_timeout\": \"30s\"
    }" | python3 -c "
import sys, json
d = json.load(sys.stdin)
status = d.get('status', {}).get('state', 'UNKNOWN')
if status == 'FAILED':
    err = d.get('status', {}).get('error', {}).get('message', 'Unknown')
    print(f'  FAILED: {err}')
    sys.exit(1)
else:
    print(f'  OK ({status})')
"
}

run_sql "CREATE CATALOG IF NOT EXISTS ${PI_CATALOG}"
for schema in bronze silver gold serving; do
    run_sql "CREATE SCHEMA IF NOT EXISTS ${PI_CATALOG}.${schema}"
done
run_sql "CREATE VOLUME IF NOT EXISTS ${PI_CATALOG}.bronze.source_files"

echo ""
echo "=== Phase 1 Complete ==="
