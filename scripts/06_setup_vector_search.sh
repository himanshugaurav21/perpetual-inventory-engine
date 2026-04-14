#!/usr/bin/env bash
# Phase 6: Vector Search Setup
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/../config.env" 2>/dev/null || true

: "${DATABRICKS_PROFILE:?Set DATABRICKS_PROFILE}"
: "${DATABRICKS_WAREHOUSE_ID:?Set DATABRICKS_WAREHOUSE_ID}"
: "${PI_CATALOG:=perpetual_inventory_engine}"
: "${VS_ENDPOINT_NAME:=perpetual-inventory-vs-endpoint}"
: "${VS_INDEX_NAME:=${PI_CATALOG}.gold.anomaly_summary_vs_index}"
: "${VS_EMBEDDING_MODEL:=databricks-gte-large-en}"

P="--profile ${DATABRICKS_PROFILE}"

echo "=== Phase 6: Vector Search Setup ==="
echo "  Endpoint: ${VS_ENDPOINT_NAME}"
echo "  Index:    ${VS_INDEX_NAME}"

# Enable CDF on source table
echo "--- Enabling Change Data Feed ---"
databricks api post /api/2.0/sql/statements ${P} --json "{
    \"warehouse_id\": \"${DATABRICKS_WAREHOUSE_ID}\",
    \"statement\": \"ALTER TABLE ${PI_CATALOG}.gold.gold_anomaly_summary SET TBLPROPERTIES (delta.enableChangeDataFeed = true)\",
    \"wait_timeout\": \"30s\"
}" > /dev/null 2>&1 || echo "  CDF may already be enabled"
echo "  CDF enabled"

# Create VS endpoint
echo "--- Creating Vector Search endpoint ---"
databricks api post /api/2.0/vector-search/endpoints ${P} --json "{
    \"name\": \"${VS_ENDPOINT_NAME}\",
    \"endpoint_type\": \"STANDARD\"
}" 2>&1 | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f'  Endpoint: {d.get(\"name\", \"created\")} ({d.get(\"endpoint_status\", {}).get(\"state\", \"PENDING\")})')
except: print('  Endpoint may already exist')
" || echo "  Endpoint may already exist"

# Wait for endpoint to come online
echo "  Waiting for endpoint to be ONLINE (up to 5 min)..."
for i in $(seq 1 30); do
    STATUS=$(databricks api get /api/2.0/vector-search/endpoints/${VS_ENDPOINT_NAME} ${P} 2>/dev/null | \
        python3 -c "import sys,json; print(json.load(sys.stdin).get('endpoint_status',{}).get('state',''))" 2>/dev/null || echo "PENDING")
    if [ "$STATUS" = "ONLINE" ]; then
        echo "  Endpoint is ONLINE"
        break
    fi
    echo "  Status: ${STATUS} (attempt ${i}/30)"
    sleep 10
done

# Create VS index
echo "--- Creating Vector Search index ---"
databricks api post /api/2.0/vector-search/indexes ${P} --json "{
    \"name\": \"${VS_INDEX_NAME}\",
    \"endpoint_name\": \"${VS_ENDPOINT_NAME}\",
    \"primary_key\": \"anomaly_id\",
    \"index_type\": \"DELTA_SYNC\",
    \"delta_sync_index_spec\": {
        \"source_table\": \"${PI_CATALOG}.gold.gold_anomaly_summary\",
        \"pipeline_type\": \"TRIGGERED\",
        \"embedding_source_columns\": [{
            \"name\": \"search_text\",
            \"model_endpoint_name\": \"${VS_EMBEDDING_MODEL}\"
        }],
        \"columns_to_sync\": [
            \"anomaly_id\", \"store_id\", \"store_name\", \"sku_id\", \"sku_name\",
            \"category\", \"department\", \"region\", \"risk_tier\",
            \"primary_anomaly_type\", \"composite_risk_score\",
            \"financial_impact\", \"explanation_text\"
        ]
    }
}" 2>&1 | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f'  Index: {d.get(\"name\", \"created\")} ({d.get(\"status\", {}).get(\"ready\", False)})')
except: print('  Index may already exist')
" || echo "  Index may already exist"

echo ""
echo "=== Phase 6 Complete ==="
echo "  Monitor index sync in the Databricks UI."
