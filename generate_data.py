"""
Perpetual Inventory Engine - Mock Data Generator
==================================================
Tier 1 + UC Write: Polars + NumPy + Mimesis → Databricks Connect bridge

Tables (written to {CATALOG}.bronze.*):
  - sku_master            (5,000 rows)
  - store_master          (50 rows)
  - inventory_ledger      (250,000 rows)
  - shipment_events       (50,000 rows)
  - sales_transactions    (500,000 rows)
  - store_adjustments     (30,000 rows)

Also writes CSVs to Volume: {CATALOG}.bronze.source_files
Generates _anomaly_labels.csv ground truth for validation.

Run:
  source config.env
  uv run --with polars --with numpy --with mimesis \
         --with "databricks-connect>=16.4,<17.0" generate_data.py
"""

import json
import os
import numpy as np
import polars as pl
from mimesis import Generic
from mimesis.locales import Locale
from datetime import date, timedelta

# ── Config ────────────────────────────────────────────────────────────
CATALOG = os.environ.get("PI_CATALOG", "perpetual_inventory_engine")
SCHEMA = "bronze"
SEED = 42
rng = np.random.default_rng(SEED)
g = Generic(locale=Locale.EN, seed=SEED)

TODAY = date(2026, 4, 9)
NINETY_DAYS_AGO = TODAY - timedelta(days=90)

# ── Constants ─────────────────────────────────────────────────────────
CATEGORIES = ["Grocery", "Electronics", "Apparel", "Home", "Health & Beauty", "Toys", "Sports"]
CATEGORY_WEIGHTS = np.array([0.30, 0.15, 0.15, 0.15, 0.10, 0.08, 0.07])

SUBCATEGORIES = {
    "Grocery": ["Canned Goods", "Snacks", "Dairy", "Frozen", "Beverages", "Produce", "Bakery", "Deli"],
    "Electronics": ["Phones", "Tablets", "Headphones", "Cables", "Batteries", "Smart Home", "Gaming", "Cameras"],
    "Apparel": ["Mens", "Womens", "Kids", "Shoes", "Activewear", "Outerwear", "Accessories", "Underwear"],
    "Home": ["Kitchen", "Bedding", "Bath", "Furniture", "Decor", "Storage", "Lighting", "Garden"],
    "Health & Beauty": ["Skincare", "Haircare", "Oral Care", "Vitamins", "Cosmetics", "Fragrance", "First Aid", "Supplements"],
    "Toys": ["Action Figures", "Board Games", "Puzzles", "Outdoor", "Building Sets", "Dolls", "STEM", "Plush"],
    "Sports": ["Fitness", "Camping", "Cycling", "Running", "Team Sports", "Water Sports", "Winter Sports", "Yoga"],
}

DEPARTMENTS = {
    "Grocery": "Food & Beverage",
    "Electronics": "Hard Goods", "Home": "Hard Goods", "Toys": "Hard Goods", "Sports": "Hard Goods",
    "Apparel": "Soft Goods", "Health & Beauty": "Soft Goods",
}

SUPPLIERS = [
    "Global Supply Co", "PremiumGoods Inc", "DirectSource LLC", "ValueChain Dist",
    "FreshLine Logistics", "TechParts Global", "HomeEssentials Corp", "SportMax Supply",
    "NaturalBest Trading", "QualityFirst Ltd", "MegaDist International", "SwiftShip Co",
    "BrightStar Imports", "PeakPerformance Dist", "TrueValue Wholesale",
]

BRANDS = [
    "FreshMart Essentials", "GreenLeaf", "TechEdge", "ComfortPlus", "ActivePeak",
    "HomeNest", "PureGlow", "SmartChoice", "KidZone", "TrailBlazer",
    "VitalLife", "UrbanStyle", "SunCrest", "ProFit", "CleanWave",
]

PRICE_RANGES = {
    "Grocery": (1.50, 25.00), "Electronics": (9.99, 499.99), "Apparel": (12.99, 149.99),
    "Home": (5.99, 199.99), "Health & Beauty": (3.99, 79.99), "Toys": (7.99, 89.99),
    "Sports": (9.99, 199.99),
}

MARKUP_RANGES = {
    "Grocery": (1.3, 1.8), "Electronics": (1.5, 2.5), "Apparel": (2.0, 3.5),
    "Home": (1.8, 3.0), "Health & Beauty": (2.0, 3.0), "Toys": (1.8, 2.5),
    "Sports": (1.5, 2.5),
}

REGIONS = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]
STORE_TYPES = ["superstore", "neighborhood", "express"]
STORE_TYPE_COUNTS = {"superstore": 15, "neighborhood": 20, "express": 15}

ADJUSTMENT_TYPES = ["cycle_count", "damage", "theft", "correction", "receiving_error"]
ADJUSTMENT_TYPE_WEIGHTS = np.array([0.40, 0.20, 0.15, 0.15, 0.10])

REASON_CODES = {
    "cycle_count": ["CYCLE_COUNT_VARIANCE", "SCHEDULED_AUDIT", "SPOT_CHECK"],
    "damage": ["DAMAGED_IN_TRANSIT", "DAMAGED_ON_SHELF", "WATER_DAMAGE", "CUSTOMER_RETURN_DAMAGED"],
    "theft": ["THEFT_CONFIRMED", "THEFT_SUSPECTED", "ORGANIZED_RETAIL_CRIME"],
    "correction": ["SYSTEM_ERROR_FIX", "DUPLICATE_ENTRY", "WRONG_SKU_POSTED", "MANUAL_RECOUNT"],
    "receiving_error": ["SHORT_SHIPMENT", "WRONG_ITEM_RECEIVED", "QUANTITY_MISMATCH"],
}

# ── 1. SKU Master (5,000 rows) ───────────────────────────────────────
print("Generating sku_master...")
N_SKUS = 5_000

sku_categories = rng.choice(CATEGORIES, size=N_SKUS, p=CATEGORY_WEIGHTS)
sku_subcategories = [rng.choice(SUBCATEGORIES[cat]) for cat in sku_categories]
sku_departments = [DEPARTMENTS[cat] for cat in sku_categories]
sku_brands = rng.choice(BRANDS, size=N_SKUS)
sku_suppliers = rng.choice(SUPPLIERS, size=N_SKUS)

sku_names = [f"{b} {s}" for b, s in zip(sku_brands, sku_subcategories)]

unit_costs = np.array([
    round(rng.uniform(*PRICE_RANGES[cat]), 2) for cat in sku_categories
])
retail_prices = np.array([
    round(uc * rng.uniform(*MARKUP_RANGES[cat]), 2)
    for uc, cat in zip(unit_costs, sku_categories)
])

upcs = [f"{rng.integers(100000000000, 999999999999)}" for _ in range(N_SKUS)]

sku_master = pl.DataFrame({
    "sku_id": [f"SKU-{10001 + i}" for i in range(N_SKUS)],
    "name": sku_names,
    "category": sku_categories.tolist(),
    "subcategory": sku_subcategories,
    "department": sku_departments,
    "unit_cost": unit_costs,
    "retail_price": retail_prices,
    "supplier": sku_suppliers.tolist(),
    "upc": upcs,
})
print(f"  sku_master: {len(sku_master)} rows")

# ── 2. Store Master (50 rows) ────────────────────────────────────────
print("Generating store_master...")
N_STORES = 50

store_names_list = []
store_types_list = []
store_regions = []
store_cities = []
store_states = []
shrinkage_profiles = []
sq_footages = []
managers = []

idx = 0
for stype, count in STORE_TYPE_COUNTS.items():
    for _ in range(count):
        city = g.address.city()
        state = g.address.state(abbr=True)
        region = rng.choice(REGIONS)
        suffix = {"superstore": "Superstore", "neighborhood": "Market", "express": "Express"}[stype]
        store_names_list.append(f"FreshMart {city} {suffix}")
        store_types_list.append(stype)
        store_regions.append(region)
        store_cities.append(city)
        store_states.append(state)

        if stype == "superstore":
            sq_footages.append(int(rng.integers(80000, 120001)))
        elif stype == "neighborhood":
            sq_footages.append(int(rng.integers(30000, 50001)))
        else:
            sq_footages.append(int(rng.integers(5000, 15001)))

        # Shrinkage: 20% high, 50% medium, 30% low
        shrinkage_profiles.append(rng.choice(["high", "medium", "low"], p=[0.20, 0.50, 0.30]))
        managers.append(f"{g.person.first_name()} {g.person.last_name()}")
        idx += 1

store_master = pl.DataFrame({
    "store_id": [f"STR-{i+1:03d}" for i in range(N_STORES)],
    "store_name": store_names_list,
    "region": store_regions,
    "city": store_cities,
    "state": store_states,
    "store_type": store_types_list,
    "shrinkage_profile": shrinkage_profiles,
    "square_footage": sq_footages,
    "manager_name": managers,
})
print(f"  store_master: {len(store_master)} rows")

# ── Build lookups ─────────────────────────────────────────────────────
sku_ids = sku_master["sku_id"].to_list()
store_ids = store_master["store_id"].to_list()
sku_cat_map = dict(zip(sku_master["sku_id"].to_list(), sku_master["category"].to_list()))
sku_cost_map = dict(zip(sku_master["sku_id"].to_list(), sku_master["unit_cost"].to_list()))
sku_price_map = dict(zip(sku_master["sku_id"].to_list(), sku_master["retail_price"].to_list()))
store_shrinkage_map = dict(zip(store_master["store_id"].to_list(), store_master["shrinkage_profile"].to_list()))
store_type_map = dict(zip(store_master["store_id"].to_list(), store_master["store_type"].to_list()))

# Daily velocity by category (units/day for a typical store)
CATEGORY_VELOCITY = {
    "Grocery": 3.0, "Electronics": 0.3, "Apparel": 0.5, "Home": 0.4,
    "Health & Beauty": 0.8, "Toys": 0.4, "Sports": 0.3,
}

# ── Select anomaly pairs ─────────────────────────────────────────────
print("\nSelecting anomaly pairs...")
all_pairs = [(sid, skid) for sid in store_ids for skid in sku_ids]
N_TOTAL_PAIRS = len(all_pairs)
N_ANOMALIES = int(N_TOTAL_PAIRS * 0.10)  # 10%

# Bias toward high-shrinkage stores
pair_weights = np.array([
    3.0 if store_shrinkage_map[sid] == "high" else
    1.5 if store_shrinkage_map[sid] == "medium" else 1.0
    for sid, _ in all_pairs
])
pair_weights /= pair_weights.sum()

anomaly_indices = rng.choice(len(all_pairs), size=N_ANOMALIES, replace=False, p=pair_weights)
anomaly_set = set(anomaly_indices)
anomaly_pairs = [all_pairs[i] for i in anomaly_indices]

# Assign anomaly types
ANOMALY_TYPES = ["ghost_inventory", "systematic_inflation", "pos_scanning_error", "shrinkage_spike", "velocity_mismatch"]
ANOMALY_TYPE_PCTS = [0.35, 0.20, 0.15, 0.15, 0.15]
anomaly_type_assignments = rng.choice(ANOMALY_TYPES, size=N_ANOMALIES, p=ANOMALY_TYPE_PCTS)
anomaly_map = {}  # (store_id, sku_id) -> anomaly_type
for i, (sid, skid) in enumerate(anomaly_pairs):
    anomaly_map[(sid, skid)] = anomaly_type_assignments[i]

# Suspicious employees for systematic_inflation
SUSPICIOUS_EMPLOYEES = [f"EMP-{rng.integers(900, 999)}" for _ in range(10)]

# Suspicious cashiers for POS scanning errors
SUSPICIOUS_CASHIERS = [f"CSH-{rng.integers(800, 899)}" for _ in range(8)]

print(f"  Total pairs: {N_TOTAL_PAIRS:,}, Anomalous: {N_ANOMALIES:,} ({N_ANOMALIES/N_TOTAL_PAIRS*100:.1f}%)")
for at in ANOMALY_TYPES:
    c = sum(1 for v in anomaly_map.values() if v == at)
    print(f"    {at}: {c:,}")

# ── 3. Sales Transactions (500,000 rows) ─────────────────────────────
print("\nGenerating sales_transactions...")
N_SALES = 500_000

sales_rows = []
txn_counter = 0

# Pre-compute which pairs should have zero/low sales (ghost_inventory)
ghost_pairs = {k for k, v in anomaly_map.items() if v == "ghost_inventory"}
# POS scanning error pairs
pos_error_pairs = {k for k, v in anomaly_map.items() if v == "pos_scanning_error"}

# Generate sales weighted by store type and category velocity
for _ in range(N_SALES):
    store_id = rng.choice(store_ids)
    sku_id = rng.choice(sku_ids)
    pair = (store_id, sku_id)

    # Ghost inventory pairs get no sales in last 30 days
    if pair in ghost_pairs:
        # Put sale >35 days ago
        days_ago = int(rng.integers(35, 91))
    else:
        days_ago = int(rng.integers(0, 91))

    sale_date = TODAY - timedelta(days=days_ago)
    channel = rng.choice(["pos", "ecomm"], p=[0.80, 0.20])
    qty = int(rng.integers(1, 6))
    price = sku_price_map[sku_id]
    discount = round(float(rng.uniform(0, 0.15)), 2)
    unit_price = round(price * (1 - discount), 2)

    # POS scanning errors: scanned_sku_id differs
    if pair in pos_error_pairs and days_ago < 30 and rng.random() < 0.3:
        # Scan a cheaper item
        cheap_skus = [s for s in sku_ids if sku_cat_map[s] == "Grocery" and s != sku_id]
        scanned_sku = rng.choice(cheap_skus) if cheap_skus else sku_id
        cashier_id = rng.choice(SUSPICIOUS_CASHIERS)
    else:
        scanned_sku = sku_id
        cashier_id = f"CSH-{rng.integers(100, 799)}"

    txn_counter += 1
    sales_rows.append({
        "txn_id": f"TXN-{txn_counter:08d}",
        "store_id": store_id,
        "sku_id": sku_id,
        "scanned_sku_id": scanned_sku,
        "quantity_sold": qty,
        "sale_date": sale_date.isoformat(),
        "channel": channel,
        "unit_price": unit_price,
        "discount_pct": discount,
        "cashier_id": cashier_id,
    })

sales_transactions = pl.DataFrame(sales_rows)
print(f"  sales_transactions: {len(sales_transactions)} rows")

# ── 4. Shipment Events (50,000 rows) ─────────────────────────────────
print("Generating shipment_events...")
N_SHIPMENTS = 50_000

shipment_rows = []
for i in range(N_SHIPMENTS):
    store_id = rng.choice(store_ids)
    sku_id = rng.choice(sku_ids)
    pair = (store_id, sku_id)

    days_ago = int(rng.integers(0, 91))
    received_date = TODAY - timedelta(days=days_ago)
    shipped_date = received_date - timedelta(days=int(rng.integers(1, 6)))

    qty_shipped = int(rng.integers(10, 101))
    # 95% no discrepancy, 5% short
    if rng.random() < 0.05:
        qty_received = qty_shipped - int(rng.integers(1, 4))
    else:
        qty_received = qty_shipped

    shipment_type = rng.choice(["vendor", "transfer", "return"], p=[0.85, 0.10, 0.05])

    shipment_rows.append({
        "shipment_id": f"SHP-{i+1:06d}",
        "store_id": store_id,
        "sku_id": sku_id,
        "quantity_shipped": qty_shipped,
        "quantity_received": max(0, qty_received),
        "received_date": received_date.isoformat(),
        "shipped_date": shipped_date.isoformat(),
        "shipment_type": shipment_type,
        "dc_id": f"DC-{rng.integers(1, 6):02d}",
        "po_number": f"PO-{rng.integers(100000, 999999)}",
    })

shipment_events = pl.DataFrame(shipment_rows)
print(f"  shipment_events: {len(shipment_events)} rows")

# ── 5. Store Adjustments (30,000 rows) ───────────────────────────────
print("Generating store_adjustments...")
N_ADJUSTMENTS = 30_000

# Reserve some adjustments for systematic_inflation anomalies
inflation_pairs = [(k, v) for k, v in anomaly_map.items() if v == "systematic_inflation"]
shrinkage_spike_pairs = [(k, v) for k, v in anomaly_map.items() if v == "shrinkage_spike"]

adj_rows = []
adj_counter = 0

# First: inject systematic inflation adjustments (~5 per pair)
for (sid, skid), _ in inflation_pairs:
    n_adj = int(rng.integers(3, 9))
    for _ in range(n_adj):
        days_ago = int(rng.integers(0, 91))
        adj_date = TODAY - timedelta(days=days_ago)
        qty_change = int(rng.integers(5, 21))  # Always positive
        adj_counter += 1
        adj_rows.append({
            "adjustment_id": f"ADJ-{adj_counter:06d}",
            "store_id": sid,
            "sku_id": skid,
            "adjustment_type": "correction",
            "quantity_change": qty_change,
            "adjustment_date": adj_date.isoformat(),
            "adjusted_by": rng.choice(SUSPICIOUS_EMPLOYEES),
            "reason_code": "MANUAL_RECOUNT",
            "supervisor_approved": False,
        })

# Second: inject shrinkage spike adjustments
for (sid, skid), _ in shrinkage_spike_pairs:
    # Cluster in a 3-5 day window
    spike_start = int(rng.integers(5, 30))
    for d in range(int(rng.integers(1, 4))):
        adj_date = TODAY - timedelta(days=spike_start + d)
        qty_change = -int(rng.integers(20, 101))  # Large negative
        adj_counter += 1
        adj_rows.append({
            "adjustment_id": f"ADJ-{adj_counter:06d}",
            "store_id": sid,
            "sku_id": skid,
            "adjustment_type": rng.choice(["correction", "cycle_count"]),
            "quantity_change": qty_change,
            "adjustment_date": adj_date.isoformat(),
            "adjusted_by": f"EMP-{rng.integers(100, 500)}",
            "reason_code": rng.choice(["SYSTEM_ERROR_FIX", "SCHEDULED_AUDIT"]),
            "supervisor_approved": bool(rng.random() < 0.5),
        })

# Fill rest with normal adjustments
n_remaining = N_ADJUSTMENTS - len(adj_rows)
for _ in range(n_remaining):
    store_id = rng.choice(store_ids)
    sku_id = rng.choice(sku_ids)
    adj_type = rng.choice(ADJUSTMENT_TYPES, p=ADJUSTMENT_TYPE_WEIGHTS)
    days_ago = int(rng.integers(0, 91))
    adj_date = TODAY - timedelta(days=days_ago)

    if adj_type in ("damage", "theft"):
        qty_change = -int(rng.integers(1, 6))
    elif adj_type == "receiving_error":
        qty_change = int(rng.integers(-3, 4))
    elif adj_type == "correction":
        qty_change = int(rng.integers(-5, 6))
    else:  # cycle_count
        qty_change = int(rng.integers(-10, 11))

    reason = rng.choice(REASON_CODES[adj_type])
    adj_counter += 1
    adj_rows.append({
        "adjustment_id": f"ADJ-{adj_counter:06d}",
        "store_id": store_id,
        "sku_id": sku_id,
        "adjustment_type": adj_type,
        "quantity_change": qty_change,
        "adjustment_date": adj_date.isoformat(),
        "adjusted_by": f"EMP-{rng.integers(100, 500)}",
        "reason_code": reason,
        "supervisor_approved": bool(rng.random() < 0.7),
    })

store_adjustments = pl.DataFrame(adj_rows[:N_ADJUSTMENTS])
print(f"  store_adjustments: {len(store_adjustments)} rows")

# ── 6. Inventory Ledger (250,000 rows) ───────────────────────────────
print("Generating inventory_ledger...")

ledger_rows = []
for store_id in store_ids:
    for sku_id in sku_ids:
        pair = (store_id, sku_id)
        cat = sku_cat_map[sku_id]
        base_velocity = CATEGORY_VELOCITY[cat]

        # Store type multiplier
        stype = store_type_map[store_id]
        type_mult = {"superstore": 3.0, "neighborhood": 1.5, "express": 0.5}[stype]
        expected_daily = base_velocity * type_mult

        # Base system quantity
        base_qty = max(0, int(expected_daily * rng.uniform(15, 45)))

        # Last count was 30-365 days ago
        days_since_count = int(rng.integers(30, 366))
        last_count_date = TODAY - timedelta(days=days_since_count)
        last_counted_qty = max(0, base_qty + int(rng.integers(-5, 6)))

        reorder_point = max(1, int(expected_daily * 7))
        max_shelf = int(expected_daily * 60)

        system_quantity = base_qty
        anomaly_type = anomaly_map.get(pair)

        if anomaly_type == "ghost_inventory":
            # System shows inventory but nothing is actually there
            system_quantity = int(rng.integers(50, 201))
        elif anomaly_type == "systematic_inflation":
            # Inflated by repeated upward adjustments
            system_quantity = base_qty + int(rng.integers(30, 100))
        elif anomaly_type == "velocity_mismatch":
            # System shows less than what was sold (negative phantom)
            system_quantity = max(0, base_qty - int(rng.integers(20, 60)))
        elif anomaly_type == "shrinkage_spike":
            # Sudden unexplained loss
            system_quantity = max(0, base_qty - int(rng.integers(15, 50)))

        pi_variance = system_quantity - last_counted_qty

        ledger_rows.append({
            "store_id": store_id,
            "sku_id": sku_id,
            "system_quantity": system_quantity,
            "last_counted_quantity": last_counted_qty,
            "last_count_date": last_count_date.isoformat(),
            "reorder_point": reorder_point,
            "max_shelf_capacity": max_shelf,
            "on_hand_value": round(system_quantity * sku_cost_map[sku_id], 2),
            "pi_variance": pi_variance,
        })

inventory_ledger = pl.DataFrame(ledger_rows)
print(f"  inventory_ledger: {len(inventory_ledger)} rows")

# ── Generate anomaly labels ground truth ─────────────────────────────
print("\nGenerating anomaly labels...")
label_rows = []
for (sid, skid), atype in anomaly_map.items():
    severity = "critical" if atype in ("ghost_inventory", "systematic_inflation") else "high"
    label_rows.append({
        "store_id": sid,
        "sku_id": skid,
        "anomaly_type": atype,
        "anomaly_severity": severity,
    })
anomaly_labels = pl.DataFrame(label_rows)
print(f"  anomaly_labels: {len(anomaly_labels)} rows")

# ── Write CSVs to local ──────────────────────────────────────────────
print("\n--- Writing local CSVs ---")
output_dir = "/tmp/perpetual_inventory_csvs"
os.makedirs(output_dir, exist_ok=True)

tables = {
    "sku_master": sku_master,
    "store_master": store_master,
    "inventory_ledger": inventory_ledger,
    "shipment_events": shipment_events,
    "sales_transactions": sales_transactions,
    "store_adjustments": store_adjustments,
}

for name, df in tables.items():
    df.write_csv(f"{output_dir}/{name}.csv")
    print(f"  {name}.csv ({len(df):,} rows)")

anomaly_labels.write_csv(f"{output_dir}/_anomaly_labels.csv")
print(f"  _anomaly_labels.csv ({len(anomaly_labels):,} rows)")

# ── Write to Unity Catalog via Databricks Connect ────────────────────
print("\n--- Writing to Unity Catalog ---")
profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
os.environ["DATABRICKS_CONFIG_PROFILE"] = profile
print(f"  Using profile: {profile}")

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().getOrCreate()

for table_name, df in tables.items():
    print(f"  Writing {CATALOG}.{SCHEMA}.{table_name}...")
    pandas_df = df.to_pandas()
    spark_df = spark.createDataFrame(pandas_df)
    (spark_df.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}"))
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table_name}").count()
    print(f"    done - {count:,} rows")

# ── Upload CSVs to Volume ────────────────────────────────────────────
print("\n--- Uploading CSVs to Volume ---")
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/source_files"

for table_name in tables:
    local_path = f"{output_dir}/{table_name}.csv"
    csv_df = spark.read.option("header", "true").option("inferSchema", "true").csv(local_path)
    csv_df.write.mode("overwrite").option("header", "true").csv(f"{volume_path}/{table_name}")
    print(f"  {table_name}.csv -> {volume_path}/{table_name}/")

# Upload anomaly labels to volume only (not as UC table)
labels_local = f"{output_dir}/_anomaly_labels.csv"
labels_df = spark.read.option("header", "true").option("inferSchema", "true").csv(labels_local)
labels_df.write.mode("overwrite").option("header", "true").csv(f"{volume_path}/_anomaly_labels")
print(f"  _anomaly_labels.csv -> {volume_path}/_anomaly_labels/")

print("\n=== Data generation complete! ===")
print(f"  Catalog: {CATALOG}")
print(f"  Tables: {', '.join(tables.keys())}")
print(f"  Anomalies injected: {len(anomaly_map):,} SKU-store pairs")
spark.stop()
