#!/usr/bin/env python3
"""
Phase 5: Serving Layer Setup
Creates serving tables for the PI engine app with liquid clustering.

Run:
  source config.env
  uv run --with "databricks-connect>=16.4,<17.0" scripts/05_setup_serving.py
"""

import os

CATALOG = os.environ.get("PI_CATALOG", "perpetual_inventory_engine")
PROFILE = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
os.environ["DATABRICKS_CONFIG_PROFILE"] = PROFILE

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().getOrCreate()

print(f"=== Phase 5: Serving Layer Setup ===")
print(f"  Catalog: {CATALOG}")

# 1. at_risk_skus - flagged SKUs with operational state
print("\n--- Creating serving.at_risk_skus ---")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.serving.at_risk_skus (
        anomaly_id STRING,
        store_id STRING,
        store_name STRING,
        sku_id STRING,
        sku_name STRING,
        category STRING,
        department STRING,
        region STRING,
        risk_tier STRING,
        primary_anomaly_type STRING,
        composite_risk_score DOUBLE,
        velocity_score DOUBLE,
        adjustment_score DOUBLE,
        stock_consistency_score DOUBLE,
        shipment_gap_score DOUBLE,
        shrinkage_score DOUBLE,
        system_quantity INT,
        calculated_on_hand INT,
        stock_discrepancy INT,
        financial_impact DOUBLE,
        recommended_action STRING,
        explanation_text STRING,
        priority_rank INT,
        detected_date DATE,
        status STRING,
        assigned_to STRING,
        updated_ts TIMESTAMP
    )
    CLUSTER BY (store_id, risk_tier)
""")

spark.sql(f"TRUNCATE TABLE {CATALOG}.serving.at_risk_skus")
spark.sql(f"""
    INSERT INTO {CATALOG}.serving.at_risk_skus
    SELECT
        anomaly_id, store_id, store_name, sku_id, sku_name,
        category, department, region,
        risk_tier, primary_anomaly_type,
        composite_risk_score, velocity_score, adjustment_score,
        stock_consistency_score, shipment_gap_score, shrinkage_score,
        CAST(system_quantity AS INT), CAST(calculated_on_hand AS INT),
        CAST(stock_discrepancy AS INT),
        financial_impact, recommended_action, explanation_text,
        CAST(priority_rank AS INT), detected_date,
        'open' as status,
        NULL as assigned_to,
        CURRENT_TIMESTAMP() as updated_ts
    FROM {CATALOG}.gold.gold_anomaly_summary
""")
count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.serving.at_risk_skus").collect()[0]["cnt"]
print(f"  {count:,} at-risk SKUs loaded")

# 2. store_validations - store team feedback
print("\n--- Creating serving.store_validations ---")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.serving.store_validations (
        validation_id STRING,
        store_id STRING,
        sku_id STRING,
        validation_type STRING,
        physical_count INT,
        variance_from_system INT,
        notes STRING,
        validated_by STRING,
        validated_at TIMESTAMP
    )
    CLUSTER BY (store_id)
""")
print("  Table created (empty - populated by app)")

# 3. correction_history - approved corrections
print("\n--- Creating serving.correction_history ---")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.serving.correction_history (
        correction_id STRING,
        store_id STRING,
        sku_id STRING,
        correction_type STRING,
        old_system_quantity INT,
        new_system_quantity INT,
        quantity_corrected INT,
        financial_impact DOUBLE,
        approved_by STRING,
        approved_at TIMESTAMP,
        outcome STRING
    )
    CLUSTER BY (store_id)
""")
print("  Table created (empty - populated by app)")

spark.stop()

print(f"\n=== Phase 5 Complete ===")
print(f"  {CATALOG}.serving.at_risk_skus")
print(f"  {CATALOG}.serving.store_validations")
print(f"  {CATALOG}.serving.correction_history")
