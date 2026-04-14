# Databricks notebook source
# MAGIC %md
# MAGIC # Perpetual Inventory Engine - Lakeflow Declarative Pipeline
# MAGIC Bronze → Silver → Gold medallion architecture for inventory anomaly detection.
# MAGIC
# MAGIC **Bronze**: Raw ingestion from Volume CSVs (6 tables)
# MAGIC **Silver**: Enriched velocity, adjustment patterns, stock movements (3 tables)
# MAGIC **Gold**: Risk scores, store health, anomaly summary (3 tables)

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Volume base path - parameterized via pipeline config or default
VOLUME_BASE = spark.conf.get("pipeline.volume_base", "/Volumes/perpetual_inventory_engine/bronze/source_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Raw Ingestion from Volume

# COMMAND ----------

@dlt.table(
    name="bronze_sku_master",
    comment="Raw SKU/product master data",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_sku_id", "sku_id IS NOT NULL")
def bronze_sku_master():
    return (spark.read.option("header", "true").option("inferSchema", "true")
            .csv(f"{VOLUME_BASE}/sku_master"))

# COMMAND ----------

@dlt.table(
    name="bronze_store_master",
    comment="Raw store master data",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
def bronze_store_master():
    return (spark.read.option("header", "true").option("inferSchema", "true")
            .csv(f"{VOLUME_BASE}/store_master"))

# COMMAND ----------

@dlt.table(
    name="bronze_inventory_ledger",
    comment="Raw perpetual inventory ledger - system quantities per SKU-store",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_sku_id", "sku_id IS NOT NULL")
def bronze_inventory_ledger():
    return (spark.read.option("header", "true").option("inferSchema", "true")
            .csv(f"{VOLUME_BASE}/inventory_ledger"))

# COMMAND ----------

@dlt.table(
    name="bronze_shipment_events",
    comment="Raw inbound shipment records",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_shipment_id", "shipment_id IS NOT NULL")
def bronze_shipment_events():
    return (spark.read.option("header", "true").option("inferSchema", "true")
            .csv(f"{VOLUME_BASE}/shipment_events"))

# COMMAND ----------

@dlt.table(
    name="bronze_sales_transactions",
    comment="Raw POS and e-commerce sales transactions",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_txn_id", "txn_id IS NOT NULL")
def bronze_sales_transactions():
    return (spark.read.option("header", "true").option("inferSchema", "true")
            .csv(f"{VOLUME_BASE}/sales_transactions"))

# COMMAND ----------

@dlt.table(
    name="bronze_store_adjustments",
    comment="Raw store-level inventory adjustments",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("valid_adjustment_id", "adjustment_id IS NOT NULL")
def bronze_store_adjustments():
    return (spark.read.option("header", "true").option("inferSchema", "true")
            .csv(f"{VOLUME_BASE}/store_adjustments"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Enriched & Calculated

# COMMAND ----------

@dlt.table(
    name="silver_inventory_velocity",
    comment="Sales velocity metrics per SKU-store: 7d/30d/90d velocity, trend, days of supply",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_sku_id", "sku_id IS NOT NULL")
def silver_inventory_velocity():
    sales = dlt.read("bronze_sales_transactions")
    ledger = dlt.read("bronze_inventory_ledger")
    skus = dlt.read("bronze_sku_master")

    # Aggregate sales by store-sku over time windows
    sales_agg = (
        sales
        .withColumn("sale_date", F.col("sale_date").cast("date"))
        .groupBy("store_id", "sku_id")
        .agg(
            F.sum(F.when(F.datediff(F.current_date(), F.col("sale_date")) <= 7,
                         F.col("quantity_sold")).otherwise(0)).alias("total_units_sold_7d"),
            F.sum(F.when(F.datediff(F.current_date(), F.col("sale_date")) <= 30,
                         F.col("quantity_sold")).otherwise(0)).alias("total_units_sold_30d"),
            F.sum(F.col("quantity_sold")).alias("total_units_sold_90d"),
            F.max("sale_date").alias("last_sale_date"),
            F.count("txn_id").alias("transaction_count_90d"),
        )
    )

    result = (
        sales_agg
        .join(ledger.select("store_id", "sku_id", "system_quantity", "reorder_point"),
              on=["store_id", "sku_id"], how="right")
        .join(skus.select("sku_id", "category", "retail_price", "department"),
              on="sku_id", how="left")
        .fillna(0, subset=["total_units_sold_7d", "total_units_sold_30d", "total_units_sold_90d", "transaction_count_90d"])
        .withColumn("daily_velocity_7d",  F.round(F.col("total_units_sold_7d") / 7.0, 4))
        .withColumn("daily_velocity_30d", F.round(F.col("total_units_sold_30d") / 30.0, 4))
        .withColumn("daily_velocity_90d", F.round(F.col("total_units_sold_90d") / 90.0, 4))
        .withColumn("velocity_trend",
                    F.round(
                        (F.col("daily_velocity_7d") - F.col("daily_velocity_90d")) /
                        F.greatest(F.col("daily_velocity_90d"), F.lit(0.01)), 4))
        .withColumn("days_of_supply",
                    F.round(F.col("system_quantity") / F.greatest(F.col("daily_velocity_30d"), F.lit(0.01)), 1))
        .withColumn("days_since_last_sale",
                    F.when(F.col("last_sale_date").isNotNull(),
                           F.datediff(F.current_date(), F.col("last_sale_date")))
                    .otherwise(F.lit(999)))
        .withColumn("zero_velocity_flag",
                    (F.col("days_since_last_sale") > 30) & (F.col("system_quantity") > 0))
    )
    return result

# COMMAND ----------

@dlt.table(
    name="silver_adjustment_patterns",
    comment="Adjustment frequency and patterns per SKU-store: type distribution, suspicious flags",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_sku_id", "sku_id IS NOT NULL")
def silver_adjustment_patterns():
    adjustments = dlt.read("bronze_store_adjustments")
    shipments = dlt.read("bronze_shipment_events")

    adj_agg = (
        adjustments
        .withColumn("adjustment_date", F.col("adjustment_date").cast("date"))
        .groupBy("store_id", "sku_id")
        .agg(
            F.count("adjustment_id").alias("total_adjustments_90d"),
            F.sum(F.when(F.col("quantity_change") > 0, 1).otherwise(0)).alias("total_positive_adjustments"),
            F.sum(F.when(F.col("quantity_change") < 0, 1).otherwise(0)).alias("total_negative_adjustments"),
            F.sum("quantity_change").alias("net_adjustment_quantity"),
            F.avg(F.abs(F.col("quantity_change"))).alias("avg_adjustment_magnitude"),
            F.max(F.abs(F.col("quantity_change"))).alias("max_single_adjustment"),
            F.collect_set("adjustment_type").alias("adjustment_types_arr"),
            F.countDistinct("adjusted_by").alias("distinct_adjusters"),
            F.avg(F.when(F.col("supervisor_approved") == True, 1.0).otherwise(0.0)).alias("pct_supervisor_approved"),
        )
        .withColumn("adjustment_frequency_per_week",
                    F.round(F.col("total_adjustments_90d") / 13.0, 2))
        .withColumn("adjustment_types_json", F.to_json(F.col("adjustment_types_arr")))
        .drop("adjustment_types_arr")
    )

    # Check for upward adjustments without matching shipments
    # Get positive corrections per store-sku
    pos_corrections = (
        adjustments
        .filter((F.col("quantity_change") > 0) & (F.col("adjustment_type") == "correction"))
        .withColumn("adjustment_date", F.col("adjustment_date").cast("date"))
        .select("store_id", "sku_id", "adjustment_date")
    )

    ship_dates = (
        shipments
        .withColumn("received_date", F.col("received_date").cast("date"))
        .select("store_id", "sku_id", "received_date")
    )

    # Left join: for each positive correction, check if there's a shipment within +/- 3 days
    matched = (
        pos_corrections.alias("a")
        .join(ship_dates.alias("s"),
              (F.col("a.store_id") == F.col("s.store_id")) &
              (F.col("a.sku_id") == F.col("s.sku_id")) &
              (F.abs(F.datediff(F.col("a.adjustment_date"), F.col("s.received_date"))) <= 3),
              how="left")
        .groupBy(F.col("a.store_id"), F.col("a.sku_id"))
        .agg(
            F.count("a.adjustment_date").alias("positive_corrections_count"),
            F.sum(F.when(F.col("s.received_date").isNull(), 1).otherwise(0)).alias("unmatched_corrections_count"),
        )
        .withColumn("upward_without_shipment_flag",
                    F.col("unmatched_corrections_count") >= 2)
        .select(
            F.col("a.store_id").alias("store_id"),
            F.col("a.sku_id").alias("sku_id"),
            "upward_without_shipment_flag"
        )
    )

    result = (
        adj_agg
        .join(matched, on=["store_id", "sku_id"], how="left")
        .fillna(False, subset=["upward_without_shipment_flag"])
    )
    return result

# COMMAND ----------

@dlt.table(
    name="silver_stock_movements",
    comment="Net stock flow reconciliation: calculated vs reported inventory",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dlt.expect_or_drop("valid_sku_id", "sku_id IS NOT NULL")
def silver_stock_movements():
    ledger = dlt.read("bronze_inventory_ledger")
    shipments = dlt.read("bronze_shipment_events")
    sales = dlt.read("bronze_sales_transactions")
    adjustments = dlt.read("bronze_store_adjustments")

    # Aggregate shipments received
    ship_agg = (
        shipments
        .groupBy("store_id", "sku_id")
        .agg(
            F.sum("quantity_received").alias("total_received"),
            F.sum("quantity_shipped").alias("total_shipped_to_store"),
            F.sum(F.col("quantity_shipped") - F.col("quantity_received")).alias("total_receiving_discrepancy"),
            F.max(F.col("received_date").cast("date")).alias("last_shipment_date"),
        )
    )

    # Aggregate sales
    sales_agg = (
        sales
        .groupBy("store_id", "sku_id")
        .agg(F.sum("quantity_sold").alias("total_sold"))
    )

    # Aggregate adjustments
    adj_agg = (
        adjustments
        .groupBy("store_id", "sku_id")
        .agg(F.sum("quantity_change").alias("total_adjustments_net"))
    )

    result = (
        ledger
        .select("store_id", "sku_id", "system_quantity", "last_counted_quantity",
                F.col("last_count_date").cast("date").alias("last_count_date"),
                "reorder_point")
        .join(ship_agg, on=["store_id", "sku_id"], how="left")
        .join(sales_agg, on=["store_id", "sku_id"], how="left")
        .join(adj_agg, on=["store_id", "sku_id"], how="left")
        .fillna(0, subset=["total_received", "total_shipped_to_store", "total_receiving_discrepancy",
                           "total_sold", "total_adjustments_net"])
        # Calculated on-hand = last count + received - sold + net adjustments
        .withColumn("calculated_on_hand",
                    F.col("last_counted_quantity") + F.col("total_received") -
                    F.col("total_sold") + F.col("total_adjustments_net"))
        .withColumn("stock_discrepancy",
                    F.col("system_quantity") - F.col("calculated_on_hand"))
        .withColumn("discrepancy_pct",
                    F.round(F.col("stock_discrepancy") /
                            F.greatest(F.col("system_quantity"), F.lit(1)) * 100, 2))
        .withColumn("unexplained_loss",
                    F.when(F.col("stock_discrepancy") < -5, F.abs(F.col("stock_discrepancy"))).otherwise(0))
        .withColumn("unexplained_gain",
                    F.when(F.col("stock_discrepancy") > 5, F.col("stock_discrepancy")).otherwise(0))
        .withColumn("days_since_last_shipment",
                    F.when(F.col("last_shipment_date").isNotNull(),
                           F.datediff(F.current_date(), F.col("last_shipment_date")))
                    .otherwise(F.lit(999)))
    )
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Risk Scores & Summaries

# COMMAND ----------

@dlt.table(
    name="gold_sku_risk_scores",
    comment="Composite anomaly risk score per SKU-store with 5 component scores",
    table_properties={"quality": "gold"}
)
def gold_sku_risk_scores():
    velocity = dlt.read("silver_inventory_velocity")
    adjustments = dlt.read("silver_adjustment_patterns")
    movements = dlt.read("silver_stock_movements")

    base = (
        velocity
        .join(adjustments, on=["store_id", "sku_id"], how="left")
        .join(movements.select(
            "store_id", "sku_id", "calculated_on_hand", "stock_discrepancy",
            "discrepancy_pct", "unexplained_loss", "unexplained_gain",
            "total_received", "last_counted_quantity", "days_since_last_shipment",
            "last_shipment_date"
        ), on=["store_id", "sku_id"], how="left")
        .fillna(0, subset=["total_adjustments_90d", "total_positive_adjustments",
                           "total_negative_adjustments", "net_adjustment_quantity",
                           "avg_adjustment_magnitude", "distinct_adjusters",
                           "adjustment_frequency_per_week", "unexplained_loss",
                           "stock_discrepancy", "discrepancy_pct"])
        .fillna(False, subset=["upward_without_shipment_flag"])
        .fillna(0.7, subset=["pct_supervisor_approved"])
    )

    scored = (
        base
        # ── Velocity Score (0.25 weight) ──
        .withColumn("velocity_score",
            F.least(F.lit(1.0),
                F.when((F.col("zero_velocity_flag") == True) & (F.col("system_quantity") > 50), 1.0)
                .when((F.col("zero_velocity_flag") == True) & (F.col("system_quantity") > 20), 0.8)
                .when(F.col("velocity_trend") < -0.8, 0.6)
                .when((F.col("days_since_last_sale") > 21) & (F.col("system_quantity") > 10), 0.5)
                .otherwise(F.least(F.lit(1.0), F.col("days_since_last_sale") / 60.0 * 0.3))
            ))

        # ── Stock Consistency Score (0.25 weight) ──
        .withColumn("stock_consistency_score",
            F.least(F.lit(1.0),
                F.when(F.abs(F.col("discrepancy_pct")) > 50, 1.0)
                .when(F.abs(F.col("discrepancy_pct")) > 25, 0.8)
                .when(F.abs(F.col("discrepancy_pct")) > 10, 0.5)
                .otherwise(F.abs(F.col("discrepancy_pct")) / 20.0)
            ))

        # ── Adjustment Score (0.20 weight) ──
        .withColumn("adjustment_score",
            F.least(F.lit(1.0),
                F.when((F.col("upward_without_shipment_flag") == True) &
                       (F.col("total_positive_adjustments") >= 3), 1.0)
                .when(F.col("adjustment_frequency_per_week") > 2, 0.8)
                .when((F.col("pct_supervisor_approved") < 0.3) &
                      (F.col("total_adjustments_90d") > 5), 0.7)
                .when((F.col("distinct_adjusters") == 1) &
                      (F.col("total_adjustments_90d") > 3), 0.6)
                .otherwise(F.least(F.lit(1.0), F.col("total_adjustments_90d") / 20.0 * 0.3))
            ))

        # ── Shrinkage Score (0.20 weight) ──
        .withColumn("_loss_rate",
            F.col("unexplained_loss") /
            F.greatest(F.col("total_received") + F.col("last_counted_quantity"), F.lit(1)))
        .withColumn("shrinkage_score",
            F.least(F.lit(1.0),
                F.when(F.col("_loss_rate") > 0.3, 1.0)
                .when(F.col("_loss_rate") > 0.15, 0.7)
                .when(F.col("_loss_rate") > 0.05, 0.4)
                .otherwise(F.col("_loss_rate") * 2.0)
            ))

        # ── Shipment Gap Score (0.10 weight) ──
        .withColumn("_expected_reorder_interval",
            F.col("system_quantity") / F.greatest(F.col("daily_velocity_30d"), F.lit(0.01)))
        .withColumn("shipment_gap_score",
            F.least(F.lit(1.0),
                F.when((F.col("days_since_last_shipment") > F.col("_expected_reorder_interval") * 2) &
                       (F.col("system_quantity") > F.col("reorder_point")), 0.9)
                .when(F.col("days_since_last_shipment") > F.col("_expected_reorder_interval") * 1.5, 0.6)
                .otherwise(F.col("days_since_last_shipment") /
                           (F.col("_expected_reorder_interval") * 3 + 1))
            ))

        # ── Composite Risk Score ──
        .withColumn("composite_risk_score",
            F.round(
                F.col("velocity_score") * 0.25 +
                F.col("stock_consistency_score") * 0.25 +
                F.col("adjustment_score") * 0.20 +
                F.col("shrinkage_score") * 0.20 +
                F.col("shipment_gap_score") * 0.10,
            4))

        # ── Risk Tier ──
        .withColumn("risk_tier",
            F.when(F.col("composite_risk_score") >= 0.75, "CRITICAL")
            .when(F.col("composite_risk_score") >= 0.50, "HIGH")
            .when(F.col("composite_risk_score") >= 0.30, "MEDIUM")
            .otherwise("LOW"))

        # ── Explanation Text ──
        .withColumn("explanation_text",
            F.concat_ws("; ",
                F.when(F.col("velocity_score") >= 0.7,
                       F.concat(F.lit("Zero/low velocity: no sales for "),
                                F.col("days_since_last_sale").cast("string"),
                                F.lit(" days with "),
                                F.col("system_quantity").cast("string"),
                                F.lit(" units on hand"))),
                F.when(F.col("adjustment_score") >= 0.7,
                       F.concat(F.lit("Suspicious adjustments: "),
                                F.col("total_positive_adjustments").cast("string"),
                                F.lit(" upward adjustments without matching shipments"))),
                F.when(F.col("stock_consistency_score") >= 0.7,
                       F.concat(F.lit("Inventory mismatch: "),
                                F.col("discrepancy_pct").cast("string"),
                                F.lit("% discrepancy between calculated and reported"))),
                F.when(F.col("shipment_gap_score") >= 0.7,
                       F.lit("Overdue for replenishment but system shows adequate stock")),
                F.when(F.col("shrinkage_score") >= 0.7,
                       F.concat(F.lit("High unexplained loss: "),
                                F.col("unexplained_loss").cast("string"),
                                F.lit(" units unaccounted for"))),
            ))

        .drop("_loss_rate", "_expected_reorder_interval")
        .select(
            "store_id", "sku_id", "category", "department", "retail_price",
            "system_quantity", "calculated_on_hand", "stock_discrepancy",
            "daily_velocity_30d", "days_since_last_sale",
            "total_adjustments_90d", "total_positive_adjustments",
            "unexplained_loss",
            "velocity_score", "stock_consistency_score", "adjustment_score",
            "shrinkage_score", "shipment_gap_score",
            "composite_risk_score", "risk_tier", "explanation_text",
        )
    )
    return scored

# COMMAND ----------

@dlt.table(
    name="gold_store_health",
    comment="Store-level inventory health metrics and PI accuracy estimates",
    table_properties={"quality": "gold"}
)
def gold_store_health():
    risk = dlt.read("gold_sku_risk_scores")
    stores = dlt.read("bronze_store_master")

    store_agg = (
        risk
        .groupBy("store_id")
        .agg(
            F.count("sku_id").alias("total_skus"),
            F.sum(F.when(F.col("risk_tier") == "CRITICAL", 1).otherwise(0)).alias("critical_risk_skus"),
            F.sum(F.when(F.col("risk_tier") == "HIGH", 1).otherwise(0)).alias("high_risk_skus"),
            F.sum(F.when(F.col("risk_tier") == "MEDIUM", 1).otherwise(0)).alias("medium_risk_skus"),
            F.sum(F.when(F.col("risk_tier") == "LOW", 1).otherwise(0)).alias("low_risk_skus"),
            F.avg("composite_risk_score").alias("avg_composite_score"),
            # Ghost inventory value: system_quantity * retail_price where velocity_score >= 0.8
            F.sum(F.when(F.col("velocity_score") >= 0.8,
                         F.col("system_quantity") * F.col("retail_price")).otherwise(0)
                  ).alias("total_ghost_inventory_value"),
            # Shrinkage dollars
            F.sum(F.when(F.col("shrinkage_score") >= 0.5,
                         F.col("unexplained_loss") * F.col("retail_price")).otherwise(0)
                  ).alias("estimated_shrinkage_dollars"),
        )
        .withColumn("pct_at_risk",
                    F.round((F.col("critical_risk_skus") + F.col("high_risk_skus")) /
                            F.col("total_skus") * 100, 2))
        .withColumn("pi_accuracy_pct", F.round(100 - F.col("pct_at_risk"), 2))
    )

    result = (
        store_agg
        .join(stores.select("store_id", "store_name", "region", "city", "state",
                            "store_type", "shrinkage_profile"),
              on="store_id", how="left")
    )
    return result

# COMMAND ----------

@dlt.table(
    name="gold_anomaly_summary",
    comment="High-risk SKUs (CRITICAL + HIGH) with anomaly type, financial impact, and search text for Vector Search",
    table_properties={"quality": "gold"}
)
def gold_anomaly_summary():
    risk = dlt.read("gold_sku_risk_scores")
    skus = dlt.read("bronze_sku_master")
    stores = dlt.read("bronze_store_master")

    high_risk = risk.filter(F.col("risk_tier").isin("CRITICAL", "HIGH"))

    result = (
        high_risk
        .join(skus.select(F.col("sku_id"), F.col("name").alias("sku_name"),
                          F.col("unit_cost"), F.col("upc")),
              on="sku_id", how="left")
        .join(stores.select(F.col("store_id"), F.col("store_name"), F.col("region")),
              on="store_id", how="left")
        # Anomaly ID
        .withColumn("anomaly_id", F.md5(F.concat(F.col("store_id"), F.col("sku_id"),
                                                   F.current_date().cast("string"))))
        # Primary anomaly type based on highest component score
        .withColumn("primary_anomaly_type",
            F.when(F.greatest("velocity_score", "adjustment_score", "stock_consistency_score",
                              "shrinkage_score", "shipment_gap_score") == F.col("velocity_score"),
                   "ghost_inventory")
            .when(F.greatest("velocity_score", "adjustment_score", "stock_consistency_score",
                             "shrinkage_score", "shipment_gap_score") == F.col("adjustment_score"),
                  "systematic_inflation")
            .when(F.greatest("velocity_score", "adjustment_score", "stock_consistency_score",
                             "shrinkage_score", "shipment_gap_score") == F.col("stock_consistency_score"),
                  "stock_mismatch")
            .when(F.greatest("velocity_score", "adjustment_score", "stock_consistency_score",
                             "shrinkage_score", "shipment_gap_score") == F.col("shrinkage_score"),
                  "shrinkage_spike")
            .otherwise("replenishment_anomaly"))
        # Financial impact estimate
        .withColumn("financial_impact",
            F.round(F.col("system_quantity") * F.col("retail_price"), 2))
        # Recommended action
        .withColumn("recommended_action",
            F.when(F.col("risk_tier") == "CRITICAL",
                   "Immediate physical count required - flag for store verification")
            .otherwise("Monitor closely - schedule for next cycle count"))
        # Search text for Vector Search embedding
        .withColumn("search_text",
            F.concat_ws(" ",
                F.col("sku_name"), F.col("category"), F.col("department"),
                F.col("store_name"), F.col("region"),
                F.col("primary_anomaly_type"), F.col("risk_tier"),
                F.col("explanation_text")))
        # Priority rank
        .withColumn("priority_rank",
            F.row_number().over(Window.orderBy(F.desc("composite_risk_score"))))
        .withColumn("detected_date", F.current_date())
        .select(
            "anomaly_id", "store_id", "store_name", "sku_id", "sku_name",
            "category", "department", "region",
            "risk_tier", "primary_anomaly_type",
            "composite_risk_score", "velocity_score", "adjustment_score",
            "stock_consistency_score", "shipment_gap_score", "shrinkage_score",
            "system_quantity", "calculated_on_hand", "stock_discrepancy",
            "financial_impact", "recommended_action", "explanation_text",
            "search_text", "priority_rank", "detected_date",
        )
    )
    return result
