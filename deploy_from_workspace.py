# Databricks notebook source
# MAGIC %md
# MAGIC # Perpetual Inventory Engine — Workspace Deployment
# MAGIC
# MAGIC **Deploy the entire PI Accuracy Engine directly from a Databricks workspace.**
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. Clone this repo via **Git Folders**: Workspace > Git Folders > Add Git Folder
# MAGIC 2. A Serverless SQL Warehouse (get its ID from SQL Warehouses page)
# MAGIC 3. Foundation Model endpoint (e.g., `databricks-claude-sonnet-4-5`)
# MAGIC
# MAGIC ## Phases
# MAGIC | Phase | Description |
# MAGIC |-------|-------------|
# MAGIC | 1 | Catalog & Schema Setup |
# MAGIC | 2 | Mock Data Generation (PySpark) |
# MAGIC | 3 | DLT Pipeline Deployment |
# MAGIC | 4 | Security (Tags & Masks) |
# MAGIC | 5 | Serving Tables |
# MAGIC | 6 | Vector Search |
# MAGIC | 7 | Genie Space |
# MAGIC | 8 | Databricks App |
# MAGIC | 9 | Post-Deployment (permissions, MLflow) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "perpetual_inventory_engine", "Catalog Name")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (required)")
dbutils.widgets.text("serving_endpoint", "databricks-claude-sonnet-4-5", "Foundation Model Endpoint")
dbutils.widgets.text("app_name", "perpetual-inventory-app", "App Name")

# COMMAND ----------

import os, json, time, requests, random, base64
from datetime import date, timedelta

CATALOG = dbutils.widgets.get("catalog")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
SERVING_ENDPOINT = dbutils.widgets.get("serving_endpoint")
APP_NAME = dbutils.widgets.get("app_name")

CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
HOST = spark.conf.get("spark.databricks.workspaceUrl", "")
if HOST and not HOST.startswith("https://"):
    HOST = f"https://{HOST}"

PIPELINE_NAME = "perpetual-inventory-pipeline"
APP_WORKSPACE_PATH = f"/Workspace/Users/{CURRENT_USER}/{APP_NAME}"

if not WAREHOUSE_ID:
    raise ValueError("WAREHOUSE_ID is required. Set it in the widget above.")

print(f"""
{'='*60}
  Perpetual Inventory Engine - Workspace Deployment
{'='*60}
  Host:      {HOST}
  User:      {CURRENT_USER}
  Catalog:   {CATALOG}
  Warehouse: {WAREHOUSE_ID}
  Endpoint:  {SERVING_ENDPOINT}
  App:       {APP_NAME}
{'='*60}
""")

# COMMAND ----------

# Helper functions
def api_call(method, path, data=None):
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    url = f"{HOST}{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.request(method, url, headers=headers, json=data, timeout=120)
    try:
        return resp.json()
    except:
        return {"status_code": resp.status_code, "text": resp.text}

def run_sql(sql):
    result = api_call("POST", "/api/2.0/sql/statements", {
        "warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "30s"
    })
    status = result.get("status", {}).get("state", "UNKNOWN")
    if status == "FAILED":
        err = result.get("status", {}).get("error", {}).get("message", "?")
        print(f"  X FAILED: {err}")
    else:
        print(f"  OK: {sql[:60]}...")
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Catalog & Schema Setup

# COMMAND ----------

print("=== Phase 1: Catalog & Schema Setup ===")
run_sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ["bronze", "silver", "gold", "serving"]:
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
run_sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.source_files")
print("Phase 1 complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Data Generation (PySpark)

# COMMAND ----------

print("=== Phase 2: Data Generation ===")
random.seed(42)
TODAY = date(2026, 4, 9)

# Categories and config
CATEGORIES = ["Grocery", "Electronics", "Apparel", "Home", "Health & Beauty", "Toys", "Sports"]
CAT_WEIGHTS = [0.30, 0.15, 0.15, 0.15, 0.10, 0.08, 0.07]
DEPARTMENTS = {"Grocery": "Food & Beverage", "Electronics": "Hard Goods", "Home": "Hard Goods",
               "Toys": "Hard Goods", "Sports": "Hard Goods", "Apparel": "Soft Goods", "Health & Beauty": "Soft Goods"}
BRANDS = ["FreshMart Essentials", "GreenLeaf", "TechEdge", "ComfortPlus", "ActivePeak",
          "HomeNest", "PureGlow", "SmartChoice", "KidZone", "TrailBlazer"]
SUPPLIERS = ["Global Supply Co", "PremiumGoods Inc", "DirectSource LLC", "ValueChain Dist", "FreshLine Logistics"]
REGIONS = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]
PRICE_RANGES = {"Grocery": (1.5, 25), "Electronics": (10, 500), "Apparel": (13, 150),
                "Home": (6, 200), "Health & Beauty": (4, 80), "Toys": (8, 90), "Sports": (10, 200)}
CAT_VELOCITY = {"Grocery": 3.0, "Electronics": 0.3, "Apparel": 0.5, "Home": 0.4,
                "Health & Beauty": 0.8, "Toys": 0.4, "Sports": 0.3}

from pyspark.sql import Row

# ── SKU Master (5,000 rows) ──
print("Generating sku_master...")
sku_rows = []
for i in range(5000):
    cat = random.choices(CATEGORIES, weights=CAT_WEIGHTS)[0]
    uc = round(random.uniform(*PRICE_RANGES[cat]), 2)
    sku_rows.append(Row(
        sku_id=f"SKU-{10001+i}", name=f"{random.choice(BRANDS)} Item-{i}",
        category=cat, subcategory=f"{cat}-Sub-{random.randint(1,8)}",
        department=DEPARTMENTS[cat], unit_cost=uc,
        retail_price=round(uc * random.uniform(1.5, 2.5), 2),
        supplier=random.choice(SUPPLIERS), upc=str(random.randint(100000000000, 999999999999))
    ))
spark.createDataFrame(sku_rows).write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.sku_master")
print(f"  sku_master: {len(sku_rows)} rows")

# ── Store Master (50 rows) ──
print("Generating store_master...")
store_rows = []
types_list = ["superstore"]*15 + ["neighborhood"]*20 + ["express"]*15
for i in range(50):
    stype = types_list[i]
    store_rows.append(Row(
        store_id=f"STR-{i+1:03d}", store_name=f"FreshMart Store {i+1}",
        region=random.choice(REGIONS), city=f"City-{i+1}", state=f"ST",
        store_type=stype,
        shrinkage_profile=random.choices(["high","medium","low"], weights=[0.2, 0.5, 0.3])[0],
        square_footage=random.randint(5000, 120000), manager_name=f"Manager {i+1}"
    ))
spark.createDataFrame(store_rows).write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.store_master")
print(f"  store_master: {len(store_rows)} rows")

# Build lookups
sku_data = {r.sku_id: r for r in sku_rows}
store_data = {r.store_id: r for r in store_rows}
sku_ids = [r.sku_id for r in sku_rows]
store_ids = [r.store_id for r in store_rows]

# Select anomaly pairs (~10%)
all_pairs = [(sid, skid) for sid in store_ids for skid in sku_ids]
n_anomalies = int(len(all_pairs) * 0.10)
anomaly_indices = random.sample(range(len(all_pairs)), n_anomalies)
ANOMALY_TYPES = ["ghost_inventory", "systematic_inflation", "pos_scanning_error", "shrinkage_spike", "velocity_mismatch"]
ANOMALY_PCTS = [0.35, 0.20, 0.15, 0.15, 0.15]
anomaly_map = {}
for idx in anomaly_indices:
    pair = all_pairs[idx]
    anomaly_map[pair] = random.choices(ANOMALY_TYPES, weights=ANOMALY_PCTS)[0]
print(f"  Anomalies: {len(anomaly_map)} pairs")

ghost_pairs = {k for k, v in anomaly_map.items() if v == "ghost_inventory"}
pos_error_pairs = {k for k, v in anomaly_map.items() if v == "pos_scanning_error"}

# ── Sales Transactions (500,000 rows) ──
print("Generating sales_transactions...")
sales_rows = []
for i in range(500000):
    sid = random.choice(store_ids)
    skid = random.choice(sku_ids)
    pair = (sid, skid)
    days_ago = random.randint(35, 90) if pair in ghost_pairs else random.randint(0, 90)
    sale_date = (TODAY - timedelta(days=days_ago)).isoformat()
    scanned = skid
    cashier = f"CSH-{random.randint(100,799)}"
    if pair in pos_error_pairs and days_ago < 30 and random.random() < 0.3:
        scanned = random.choice([s for s in sku_ids[:100] if sku_data[s].category == "Grocery"])
        cashier = f"CSH-{random.randint(800,899)}"
    sales_rows.append(Row(
        txn_id=f"TXN-{i+1:08d}", store_id=sid, sku_id=skid, scanned_sku_id=scanned,
        quantity_sold=random.randint(1, 5), sale_date=sale_date,
        channel=random.choices(["pos","ecomm"], weights=[0.8, 0.2])[0],
        unit_price=round(sku_data[skid].retail_price * random.uniform(0.85, 1.0), 2),
        discount_pct=round(random.uniform(0, 0.15), 2), cashier_id=cashier
    ))
spark.createDataFrame(sales_rows).write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.sales_transactions")
print(f"  sales_transactions: {len(sales_rows)} rows")

# ── Shipment Events (50,000 rows) ──
print("Generating shipment_events...")
ship_rows = []
for i in range(50000):
    days_ago = random.randint(0, 90)
    rd = (TODAY - timedelta(days=days_ago)).isoformat()
    sd = (TODAY - timedelta(days=days_ago + random.randint(1, 5))).isoformat()
    qs = random.randint(10, 100)
    qr = qs - random.randint(1, 3) if random.random() < 0.05 else qs
    ship_rows.append(Row(
        shipment_id=f"SHP-{i+1:06d}", store_id=random.choice(store_ids),
        sku_id=random.choice(sku_ids), quantity_shipped=qs, quantity_received=max(0, qr),
        received_date=rd, shipped_date=sd,
        shipment_type=random.choices(["vendor","transfer","return"], weights=[0.85,0.10,0.05])[0],
        dc_id=f"DC-{random.randint(1,5):02d}", po_number=f"PO-{random.randint(100000,999999)}"
    ))
spark.createDataFrame(ship_rows).write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.shipment_events")
print(f"  shipment_events: {len(ship_rows)} rows")

# ── Store Adjustments (30,000 rows) ──
print("Generating store_adjustments...")
inflation_pairs = [(k, v) for k, v in anomaly_map.items() if v == "systematic_inflation"]
shrinkage_pairs = [(k, v) for k, v in anomaly_map.items() if v == "shrinkage_spike"]
adj_rows = []
c = 0
# Inject inflation
for (sid, skid), _ in inflation_pairs:
    for _ in range(random.randint(3, 8)):
        c += 1
        adj_rows.append(Row(
            adjustment_id=f"ADJ-{c:06d}", store_id=sid, sku_id=skid,
            adjustment_type="correction", quantity_change=random.randint(5, 20),
            adjustment_date=(TODAY - timedelta(days=random.randint(0, 90))).isoformat(),
            adjusted_by=f"EMP-{random.randint(900,999)}", reason_code="MANUAL_RECOUNT",
            supervisor_approved=False
        ))
# Inject shrinkage spikes
for (sid, skid), _ in shrinkage_pairs:
    spike_start = random.randint(5, 30)
    for d in range(random.randint(1, 3)):
        c += 1
        adj_rows.append(Row(
            adjustment_id=f"ADJ-{c:06d}", store_id=sid, sku_id=skid,
            adjustment_type=random.choice(["correction","cycle_count"]),
            quantity_change=-random.randint(20, 100),
            adjustment_date=(TODAY - timedelta(days=spike_start+d)).isoformat(),
            adjusted_by=f"EMP-{random.randint(100,500)}", reason_code="SCHEDULED_AUDIT",
            supervisor_approved=random.random() < 0.5
        ))
# Fill normal adjustments
ADJ_TYPES = ["cycle_count","damage","theft","correction","receiving_error"]
ADJ_WEIGHTS = [0.4, 0.2, 0.15, 0.15, 0.10]
while len(adj_rows) < 30000:
    c += 1
    at = random.choices(ADJ_TYPES, weights=ADJ_WEIGHTS)[0]
    qc = -random.randint(1,5) if at in ("damage","theft") else random.randint(-5, 5)
    adj_rows.append(Row(
        adjustment_id=f"ADJ-{c:06d}", store_id=random.choice(store_ids), sku_id=random.choice(sku_ids),
        adjustment_type=at, quantity_change=qc,
        adjustment_date=(TODAY - timedelta(days=random.randint(0,90))).isoformat(),
        adjusted_by=f"EMP-{random.randint(100,500)}", reason_code="SYSTEM_CHECK",
        supervisor_approved=random.random() < 0.7
    ))
spark.createDataFrame(adj_rows[:30000]).write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.store_adjustments")
print(f"  store_adjustments: {min(len(adj_rows), 30000)} rows")

# ── Inventory Ledger (250,000 rows) ──
print("Generating inventory_ledger...")
ledger_rows = []
for sid in store_ids:
    stype = store_data[sid].store_type
    type_mult = {"superstore": 3.0, "neighborhood": 1.5, "express": 0.5}[stype]
    for skid in sku_ids:
        cat = sku_data[skid].category
        vel = CAT_VELOCITY[cat] * type_mult
        base_qty = max(0, int(vel * random.uniform(15, 45)))
        sys_qty = base_qty
        atype = anomaly_map.get((sid, skid))
        if atype == "ghost_inventory":
            sys_qty = random.randint(50, 200)
        elif atype == "systematic_inflation":
            sys_qty = base_qty + random.randint(30, 100)
        elif atype == "velocity_mismatch":
            sys_qty = max(0, base_qty - random.randint(20, 60))
        elif atype == "shrinkage_spike":
            sys_qty = max(0, base_qty - random.randint(15, 50))
        lcd = (TODAY - timedelta(days=random.randint(30, 365))).isoformat()
        lcq = max(0, base_qty + random.randint(-5, 5))
        ledger_rows.append(Row(
            store_id=sid, sku_id=skid, system_quantity=sys_qty,
            last_counted_quantity=lcq, last_count_date=lcd,
            reorder_point=max(1, int(vel * 7)),
            max_shelf_capacity=int(vel * 60),
            on_hand_value=round(sys_qty * sku_data[skid].unit_cost, 2),
            pi_variance=sys_qty - lcq
        ))
spark.createDataFrame(ledger_rows).write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.inventory_ledger")
print(f"  inventory_ledger: {len(ledger_rows)} rows")

# Also write CSVs to Volume for DLT
print("Writing CSVs to Volume...")
volume_path = f"/Volumes/{CATALOG}/bronze/source_files"
for tbl in ["sku_master", "store_master", "inventory_ledger", "shipment_events", "sales_transactions", "store_adjustments"]:
    spark.table(f"{CATALOG}.bronze.{tbl}").write.mode("overwrite").option("header", "true").csv(f"{volume_path}/{tbl}")
    print(f"  {tbl} -> Volume")

print("Phase 2 complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: DLT Pipeline

# COMMAND ----------

print("=== Phase 3: DLT Pipeline ===")
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
git_root = "/".join(notebook_path.split("/")[:-1])
pipeline_nb_path = f"{git_root}/pipeline_notebook"

pipeline_config = {
    "name": PIPELINE_NAME, "catalog": CATALOG, "target": "gold",
    "serverless": True, "continuous": False, "channel": "CURRENT",
    "libraries": [{"notebook": {"path": pipeline_nb_path}}],
    "configuration": {"pipelines.enableTrackHistory": "true",
                      "pipeline.volume_base": f"/Volumes/{CATALOG}/bronze/source_files"}
}

result = api_call("POST", "/api/2.0/pipelines", pipeline_config)
PIPELINE_ID = result.get("pipeline_id", "")

if not PIPELINE_ID:
    print("  Pipeline may already exist, searching...")
    pipelines = api_call("GET", "/api/2.0/pipelines")
    for p in pipelines.get("statuses", []):
        if p.get("name") == PIPELINE_NAME:
            PIPELINE_ID = p.get("pipeline_id", "")
            break

if PIPELINE_ID:
    print(f"  Pipeline: {PIPELINE_ID}")
    api_call("POST", f"/api/2.0/pipelines/{PIPELINE_ID}/updates", {"full_refresh": True})
    print("  Pipeline triggered. Waiting for completion (up to 10 min)...")
    for attempt in range(60):
        time.sleep(10)
        info = api_call("GET", f"/api/2.0/pipelines/{PIPELINE_ID}")
        state = info.get("state", "UNKNOWN")
        if state in ("IDLE", "RUNNING"):
            latest = info.get("latest_updates", [{}])
            if latest and latest[0].get("state") in ("COMPLETED", "FAILED"):
                print(f"  Pipeline {latest[0]['state']}")
                break
        if attempt % 6 == 0:
            print(f"  Waiting... ({attempt * 10}s)")
else:
    print("  ERROR: Could not create pipeline")

print("Phase 3 complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 4: Security

# COMMAND ----------

print("=== Phase 4: Security ===")
run_sql(f"CREATE OR REPLACE FUNCTION {CATALOG}.bronze.mask_employee_id(emp_id STRING) RETURNS STRING RETURN CONCAT('EMP-***', RIGHT(emp_id, 3))")
run_sql(f"ALTER TABLE {CATALOG}.bronze.store_adjustments SET TAGS ('sensitivity' = 'high')")
print("Phase 4 complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5: Serving Tables

# COMMAND ----------

print("=== Phase 5: Serving Tables ===")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.serving.at_risk_skus (
        anomaly_id STRING, store_id STRING, store_name STRING, sku_id STRING, sku_name STRING,
        category STRING, department STRING, region STRING, risk_tier STRING, primary_anomaly_type STRING,
        composite_risk_score DOUBLE, velocity_score DOUBLE, adjustment_score DOUBLE,
        stock_consistency_score DOUBLE, shipment_gap_score DOUBLE, shrinkage_score DOUBLE,
        system_quantity INT, calculated_on_hand INT, stock_discrepancy INT,
        financial_impact DOUBLE, recommended_action STRING, explanation_text STRING,
        priority_rank INT, detected_date DATE, status STRING, assigned_to STRING, updated_ts TIMESTAMP
    ) CLUSTER BY (store_id, risk_tier)
""")

spark.sql(f"TRUNCATE TABLE {CATALOG}.serving.at_risk_skus")
spark.sql(f"""
    INSERT INTO {CATALOG}.serving.at_risk_skus
    SELECT anomaly_id, store_id, store_name, sku_id, sku_name, category, department, region,
           risk_tier, primary_anomaly_type, composite_risk_score, velocity_score, adjustment_score,
           stock_consistency_score, shipment_gap_score, shrinkage_score,
           CAST(system_quantity AS INT), CAST(calculated_on_hand AS INT), CAST(stock_discrepancy AS INT),
           financial_impact, recommended_action, explanation_text,
           CAST(priority_rank AS INT), detected_date,
           'open', NULL, CURRENT_TIMESTAMP()
    FROM {CATALOG}.gold.gold_anomaly_summary
""")
count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.serving.at_risk_skus").collect()[0][0]
print(f"  at_risk_skus: {count} rows")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.serving.store_validations (
        validation_id STRING, store_id STRING, sku_id STRING, validation_type STRING,
        physical_count INT, variance_from_system INT, notes STRING,
        validated_by STRING, validated_at TIMESTAMP
    ) CLUSTER BY (store_id)
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.serving.correction_history (
        correction_id STRING, store_id STRING, sku_id STRING, correction_type STRING,
        old_system_quantity INT, new_system_quantity INT, quantity_corrected INT,
        financial_impact DOUBLE, approved_by STRING, approved_at TIMESTAMP, outcome STRING
    ) CLUSTER BY (store_id)
""")
print("Phase 5 complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6: Vector Search

# COMMAND ----------

print("=== Phase 6: Vector Search ===")
VS_ENDPOINT = "perpetual-inventory-vs-endpoint"
VS_INDEX = f"{CATALOG}.gold.anomaly_summary_vs_index"

# Enable CDF
run_sql(f"ALTER TABLE {CATALOG}.gold.gold_anomaly_summary SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Create endpoint
result = api_call("POST", "/api/2.0/vector-search/endpoints", {
    "name": VS_ENDPOINT, "endpoint_type": "STANDARD"
})
print(f"  VS Endpoint: {result.get('name', 'may already exist')}")

# Wait for endpoint
for i in range(30):
    info = api_call("GET", f"/api/2.0/vector-search/endpoints/{VS_ENDPOINT}")
    state = info.get("endpoint_status", {}).get("state", "PENDING")
    if state == "ONLINE":
        print(f"  Endpoint ONLINE")
        break
    if i % 6 == 0:
        print(f"  Waiting for endpoint... ({state})")
    time.sleep(10)

# Create index
result = api_call("POST", "/api/2.0/vector-search/indexes", {
    "name": VS_INDEX, "endpoint_name": VS_ENDPOINT,
    "primary_key": "anomaly_id", "index_type": "DELTA_SYNC",
    "delta_sync_index_spec": {
        "source_table": f"{CATALOG}.gold.gold_anomaly_summary",
        "pipeline_type": "TRIGGERED",
        "embedding_source_columns": [{"name": "search_text", "model_endpoint_name": "databricks-gte-large-en"}],
        "columns_to_sync": ["anomaly_id","store_name","sku_name","category","risk_tier",
                            "primary_anomaly_type","composite_risk_score","financial_impact","explanation_text"]
    }
})
print(f"  VS Index: {result.get('name', 'may already exist')}")
print("Phase 6 complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 7: Genie Space

# COMMAND ----------

print("=== Phase 7: Genie Space ===")
genie_result = api_call("POST", "/api/2.0/genie/spaces", {
    "display_name": "Inventory Accuracy Intelligence",
    "description": "Ask about inventory anomalies, ghost inventory, and store health.",
    "warehouse_id": WAREHOUSE_ID,
    "table_identifiers": [f"{CATALOG}.gold.gold_sku_risk_scores",
                          f"{CATALOG}.gold.gold_store_health",
                          f"{CATALOG}.gold.gold_anomaly_summary"],
    "text_instructions": [
        "Composite risk score is weighted: velocity 25%, stock consistency 25%, adjustments 20%, shrinkage 20%, shipment gap 10%.",
        "Ghost inventory: system_quantity > 0 but zero sales for 30+ days. primary_anomaly_type = 'ghost_inventory'.",
        "Risk tiers: CRITICAL >= 0.75, HIGH >= 0.50, MEDIUM >= 0.30, LOW < 0.30.",
    ],
    "curated_questions": [
        "Which SKUs are likely out-of-stock but show inventory?",
        "Which stores have highest PI distortion?",
        "Show me critical anomalies in Electronics",
    ],
})
GENIE_ID = genie_result.get("space_id", genie_result.get("id", ""))
print(f"  Genie Space: {GENIE_ID or 'could not create'}")
print("Phase 7 complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 8: Databricks App

# COMMAND ----------

print("=== Phase 8: App Deployment ===")

# Create app
result = api_call("POST", "/api/2.0/apps", {
    "name": APP_NAME,
    "description": "FreshMart Inventory Intelligence - PI Accuracy Engine"
})
print(f"  App: {result.get('name', 'may already exist')}")

# Upload app files from Git Folder
git_root_ws = notebook_path.rsplit("/", 1)[0]
app_source = f"{git_root_ws}/perpetual-inventory-app"

def upload_file(src_workspace_path, dst_workspace_path, language=None):
    """Upload a file from Git Folder to app workspace path."""
    try:
        # Export from Git Folder
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        export_resp = requests.get(
            f"{HOST}/api/2.0/workspace/export",
            headers={"Authorization": f"Bearer {token}"},
            params={"path": src_workspace_path, "format": "AUTO", "direct_download": True},
            timeout=30,
        )
        if export_resp.status_code != 200:
            print(f"  Skip: {src_workspace_path} (not found)")
            return False

        content_b64 = base64.b64encode(export_resp.content).decode("utf-8")

        # Import to app path
        import_data = {
            "path": dst_workspace_path,
            "content": content_b64,
            "format": "AUTO",
            "overwrite": True,
        }
        if language:
            import_data["language"] = language

        api_call("POST", "/api/2.0/workspace/import", import_data)
        return True
    except Exception as e:
        print(f"  Upload error: {e}")
        return False

# Generate app.yaml dynamically with workspace-specific values
app_yaml_content = f"""command:
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
    value: {SERVING_ENDPOINT}
  - name: PI_CATALOG
    value: {CATALOG}
  - name: DATABRICKS_WAREHOUSE_ID
    value: {WAREHOUSE_ID}
  - name: GENIE_SPACE_ID
    value: "{GENIE_ID if 'GENIE_ID' in dir() and GENIE_ID else ''}"
"""

# Upload generated app.yaml
print("  Uploading generated app.yaml...")
app_yaml_b64 = base64.b64encode(app_yaml_content.encode()).decode("utf-8")
api_call("POST", "/api/2.0/workspace/import", {
    "path": f"{APP_WORKSPACE_PATH}/app.yaml",
    "content": app_yaml_b64,
    "format": "AUTO",
    "overwrite": True,
})

# Upload backend files (excluding app.yaml since we generated it above)
backend_files = [
    "app.py", "requirements.txt",
    "server/__init__.py", "server/config.py", "server/warehouse.py",
    "server/llm.py", "server/vector_search.py", "server/agent.py",
    "server/routes/__init__.py", "server/routes/dashboard.py",
    "server/routes/anomalies.py", "server/routes/agent_route.py",
    "server/routes/validations.py", "server/routes/stores.py",
    "server/routes/analytics.py",
]

print("  Uploading backend files...")
for f in backend_files:
    src = f"{app_source}/{f}"
    dst = f"{APP_WORKSPACE_PATH}/{f}"
    lang = "PYTHON" if f.endswith(".py") else None
    upload_file(src, dst, lang)

# Upload frontend dist files
print("  Uploading frontend dist...")
# Note: Frontend must be pre-built (npm run build) before cloning to workspace
# The dist/ folder should be committed to the repo
dist_path = f"{app_source}/frontend/dist"
try:
    dist_files = dbutils.fs.ls(dist_path.replace("/Workspace", "dbfs:"))
    # Upload each file
    for fi in dist_files:
        name = fi.name
        src = f"{dist_path}/{name}"
        dst = f"{APP_WORKSPACE_PATH}/frontend/dist/{name}"
        upload_file(src, dst)
except:
    print("  Note: frontend/dist not found in repo. Build frontend locally and re-push.")

# Deploy
print("  Deploying app...")
deploy_result = api_call("POST", f"/api/2.0/apps/{APP_NAME}/deployments", {
    "source_code_path": APP_WORKSPACE_PATH,
})
DEPLOY_ID = deploy_result.get("deployment_id", "")
print(f"  Deployment: {DEPLOY_ID or 'triggered'}")
print("Phase 8 complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 9: Post-Deployment

# COMMAND ----------

print("=== Phase 9: Post-Deployment ===")

# Get app service principal
app_info = api_call("GET", f"/api/2.0/apps/{APP_NAME}")
SP_ID = app_info.get("service_principal_client_id", "")
APP_URL = app_info.get("url", "")

if SP_ID:
    print(f"  Service Principal: {SP_ID}")

    # Add resources
    api_call("PUT", f"/api/2.0/apps/{APP_NAME}", {
        "name": APP_NAME,
        "resources": [
            {"name": "sql-warehouse", "sql_warehouse": {"id": WAREHOUSE_ID, "permission": "CAN_USE"}},
            {"name": "serving-endpoint", "serving_endpoint": {"name": SERVING_ENDPOINT, "permission": "CAN_QUERY"}},
        ]
    })
    print("  Resources attached")

    # Grant permissions
    for sql in [
        f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{SP_ID}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `{SP_ID}`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO `{SP_ID}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.bronze TO `{SP_ID}`",
        f"GRANT SELECT ON SCHEMA {CATALOG}.bronze TO `{SP_ID}`",
        f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.serving TO `{SP_ID}`",
        f"GRANT ALL PRIVILEGES ON SCHEMA {CATALOG}.serving TO `{SP_ID}`",
    ]:
        try:
            spark.sql(sql)
        except:
            pass
    print("  Permissions granted")
else:
    print("  Could not get SP. Grant permissions manually after deployment.")

# MLflow experiment
exp_path = f"/Users/{CURRENT_USER}/perpetual-inventory-engine/pi-agent"
api_call("POST", "/api/2.0/mlflow/experiments/create", {"name": exp_path})
print(f"  MLflow experiment: {exp_path}")

print(f"""
{'='*60}
  DEPLOYMENT COMPLETE
{'='*60}
  Catalog:   {CATALOG}
  Pipeline:  {PIPELINE_ID if 'PIPELINE_ID' in dir() else 'N/A'}
  Genie:     {GENIE_ID if 'GENIE_ID' in dir() else 'N/A'}
  App:       {APP_URL or APP_NAME}
  VS Index:  {VS_INDEX if 'VS_INDEX' in dir() else 'N/A'}
  MLflow:    {exp_path}
{'='*60}
""")
