#!/usr/bin/env python3
"""
Phase 7: Genie Space Setup
Creates a Genie Space for natural language inventory analytics.

Run:
  source config.env
  python3 scripts/07_setup_genie.py
"""

import json
import os
import subprocess

CATALOG = os.environ.get("PI_CATALOG", "perpetual_inventory_engine")
PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

print("=== Phase 7: Genie Space Setup ===")
print(f"  Catalog: {CATALOG}")

# Get auth token
token = subprocess.check_output(
    ["databricks", "auth", "token", "--profile", PROFILE],
    text=True
).strip()

# Get host from profile
host_result = subprocess.check_output(
    ["databricks", "auth", "describe", "--profile", PROFILE, "-o", "json"],
    text=True
)
host_info = json.loads(host_result)
host = host_info.get("host", "").rstrip("/")

import urllib.request

def api_call(method, path, data=None):
    url = f"{host}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  API Error {e.code}: {body[:200]}")
        return {}

# Create Genie Space
print("\n--- Creating Genie Space ---")

tables = [
    f"{CATALOG}.gold.gold_sku_risk_scores",
    f"{CATALOG}.gold.gold_store_health",
    f"{CATALOG}.gold.gold_anomaly_summary",
]

instructions = [
    "Composite risk score (composite_risk_score) is a weighted average of five scores: velocity (25%), stock consistency (25%), adjustments (20%), shrinkage (20%), shipment gap (10%). Range 0-1, higher = more risk.",
    "Ghost inventory: items where system_quantity > 0 but zero sales for 30+ days (days_since_last_sale > 30). Flagged with velocity_score >= 0.8 and primary_anomaly_type = 'ghost_inventory'.",
    "Risk tiers: CRITICAL (>= 0.75), HIGH (>= 0.50), MEDIUM (>= 0.30), LOW (< 0.30). Critical and high require investigation.",
    "PI accuracy (pi_accuracy_pct) = percentage of SKU-store combos NOT flagged as high/critical risk. Healthy stores should have 90%+.",
    "Financial impact: for ghost inventory = system_quantity * retail_price. For shrinkage = unexplained_loss * retail_price. Use estimated_shrinkage_dollars in gold_store_health for store totals.",
]

example_sqls = [
    {
        "question": "Which SKUs are likely out-of-stock but show inventory?",
        "sql": f"SELECT sku_id, sku_name, store_name, category, system_quantity, days_since_last_sale, composite_risk_score, explanation_text FROM {CATALOG}.gold.gold_anomaly_summary WHERE primary_anomaly_type = 'ghost_inventory' AND risk_tier = 'CRITICAL' ORDER BY composite_risk_score DESC LIMIT 50"
    },
    {
        "question": "Which stores have highest PI distortion?",
        "sql": f"SELECT store_id, store_name, region, store_type, pi_accuracy_pct, critical_risk_skus, high_risk_skus, total_ghost_inventory_value, estimated_shrinkage_dollars FROM {CATALOG}.gold.gold_store_health ORDER BY pi_accuracy_pct ASC LIMIT 20"
    },
    {
        "question": "Show me all critical anomalies in Electronics",
        "sql": f"SELECT sku_id, sku_name, store_name, primary_anomaly_type, composite_risk_score, financial_impact, recommended_action, explanation_text FROM {CATALOG}.gold.gold_anomaly_summary WHERE category = 'Electronics' AND risk_tier = 'CRITICAL' ORDER BY financial_impact DESC"
    },
]

genie_payload = {
    "display_name": "Inventory Accuracy Intelligence",
    "description": "Ask questions about inventory anomalies, ghost inventory, store health, and PI accuracy.",
    "warehouse_id": WAREHOUSE_ID,
    "table_identifiers": tables,
    "text_instructions": instructions,
    "curated_questions": [ex["question"] for ex in example_sqls],
}

result = api_call("POST", "/api/2.0/genie/spaces", genie_payload)
space_id = result.get("space_id", result.get("id", ""))

if space_id:
    print(f"  Genie Space created: {space_id}")
    print(f"  URL: {host}/genie/rooms/{space_id}")
else:
    print("  Could not create Genie Space. Create manually in the UI.")
    print(f"  Tables: {', '.join(tables)}")

print(f"\n=== Phase 7 Complete ===")
