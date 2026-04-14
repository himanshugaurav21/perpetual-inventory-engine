"""SQL Warehouse query layer with 30s TTL cache."""

import json
from typing import Any
from cachetools import TTLCache
from databricks.sdk.service.sql import StatementState

from server.config import get_workspace_client, get_warehouse_id, get_catalog

_cache: TTLCache = TTLCache(maxsize=256, ttl=30)
CATALOG = None


def _cat():
    global CATALOG
    if CATALOG is None:
        CATALOG = get_catalog()
    return CATALOG


def execute_query(sql: str) -> list[dict[str, Any]]:
    cache_key = sql
    if cache_key in _cache:
        return _cache[cache_key]

    w = get_workspace_client()
    response = w.statement_execution.execute_statement(
        warehouse_id=get_warehouse_id(),
        statement=sql,
        wait_timeout="30s",
    )

    if response.status and response.status.state == StatementState.FAILED:
        msg = response.status.error.message if response.status.error else "Unknown"
        raise RuntimeError(f"SQL failed: {msg}")

    if not response.result or not response.manifest:
        _cache[cache_key] = []
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    rows = [dict(zip(columns, row)) for row in response.result.data_array]
    _cache[cache_key] = rows
    return rows


# ── Dashboard Queries ─────────────────────────────────────────────────

def get_dashboard_summary() -> dict[str, Any]:
    cat = _cat()
    rows = execute_query(f"""
        SELECT
            COUNT(*) as total_monitored,
            SUM(CASE WHEN risk_tier = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
            SUM(CASE WHEN risk_tier = 'HIGH' THEN 1 ELSE 0 END) as high_count,
            SUM(CASE WHEN risk_tier = 'MEDIUM' THEN 1 ELSE 0 END) as medium_count,
            SUM(CASE WHEN risk_tier = 'LOW' THEN 1 ELSE 0 END) as low_count,
            ROUND(AVG(composite_risk_score), 4) as avg_risk_score
        FROM {cat}.gold.gold_sku_risk_scores
    """)
    metrics = rows[0] if rows else {}

    # Financial exposure
    fin_rows = execute_query(f"""
        SELECT
            ROUND(SUM(financial_impact), 2) as total_financial_exposure,
            COUNT(*) as at_risk_count
        FROM {cat}.gold.gold_anomaly_summary
    """)
    if fin_rows:
        metrics.update(fin_rows[0])

    # PI accuracy
    health_rows = execute_query(f"""
        SELECT ROUND(AVG(pi_accuracy_pct), 2) as avg_pi_accuracy
        FROM {cat}.gold.gold_store_health
    """)
    if health_rows:
        metrics.update(health_rows[0])

    return metrics


def get_risk_distribution() -> list[dict[str, Any]]:
    cat = _cat()
    return execute_query(f"""
        SELECT primary_anomaly_type, risk_tier,
               COUNT(*) as count,
               ROUND(SUM(financial_impact), 2) as total_impact
        FROM {cat}.gold.gold_anomaly_summary
        GROUP BY primary_anomaly_type, risk_tier
        ORDER BY total_impact DESC
    """)


def get_category_distribution() -> list[dict[str, Any]]:
    cat = _cat()
    return execute_query(f"""
        SELECT category, risk_tier, COUNT(*) as count
        FROM {cat}.gold.gold_anomaly_summary
        GROUP BY category, risk_tier
        ORDER BY count DESC
    """)


# ── Anomaly Queries ──────────────────────────────────────────────────

def get_anomalies(
    risk_tier: str | None = None,
    category: str | None = None,
    store_id: str | None = None,
    anomaly_type: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    cat = _cat()
    conditions = []
    if risk_tier:
        conditions.append(f"risk_tier = '{risk_tier}'")
    if category:
        conditions.append(f"category = '{category}'")
    if store_id:
        conditions.append(f"store_id = '{store_id}'")
    if anomaly_type:
        conditions.append(f"primary_anomaly_type = '{anomaly_type}'")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    return execute_query(f"""
        SELECT anomaly_id, store_id, store_name, sku_id, sku_name,
               category, department, region, risk_tier, primary_anomaly_type,
               composite_risk_score, financial_impact,
               system_quantity, stock_discrepancy,
               explanation_text, recommended_action, priority_rank
        FROM {cat}.gold.gold_anomaly_summary
        {where}
        ORDER BY composite_risk_score DESC
        LIMIT {limit} OFFSET {offset}
    """)


def get_anomaly_detail(sku_id: str, store_id: str) -> dict[str, Any] | None:
    cat = _cat()
    rows = execute_query(f"""
        SELECT *
        FROM {cat}.gold.gold_anomaly_summary
        WHERE sku_id = '{sku_id}' AND store_id = '{store_id}'
    """)
    if not rows:
        return None

    detail = rows[0]

    # Get risk score breakdown
    score_rows = execute_query(f"""
        SELECT velocity_score, stock_consistency_score, adjustment_score,
               shrinkage_score, shipment_gap_score,
               composite_risk_score, daily_velocity_30d, days_since_last_sale,
               total_adjustments_90d, total_positive_adjustments, unexplained_loss
        FROM {cat}.gold.gold_sku_risk_scores
        WHERE sku_id = '{sku_id}' AND store_id = '{store_id}'
    """)
    if score_rows:
        detail["scores"] = score_rows[0]

    return detail


# ── Store Queries ────────────────────────────────────────────────────

def get_store_health() -> list[dict[str, Any]]:
    cat = _cat()
    return execute_query(f"""
        SELECT store_id, store_name, region, city, state, store_type,
               shrinkage_profile, total_skus, critical_risk_skus, high_risk_skus,
               pct_at_risk, pi_accuracy_pct, avg_composite_score,
               ROUND(total_ghost_inventory_value, 2) as total_ghost_inventory_value,
               ROUND(estimated_shrinkage_dollars, 2) as estimated_shrinkage_dollars
        FROM {cat}.gold.gold_store_health
        ORDER BY pi_accuracy_pct ASC
    """)


# ── Validation Queries ───────────────────────────────────────────────

def get_validation_queue(store_id: str) -> list[dict[str, Any]]:
    cat = _cat()
    return execute_query(f"""
        SELECT anomaly_id, store_id, store_name, sku_id, sku_name,
               category, risk_tier, composite_risk_score,
               system_quantity, financial_impact,
               explanation_text, recommended_action, status
        FROM {cat}.serving.at_risk_skus
        WHERE store_id = '{store_id}' AND status IN ('open', 'sent_to_store')
        ORDER BY composite_risk_score DESC
    """)


def submit_validation(sku_id: str, store_id: str, validation_type: str,
                      physical_count: int | None, notes: str, validated_by: str) -> bool:
    cat = _cat()
    try:
        vid = f"VAL-{sku_id}-{store_id}"
        variance = f"{physical_count} - system_quantity" if physical_count is not None else "NULL"

        execute_query(f"""
            INSERT INTO {cat}.serving.store_validations
            VALUES ('{vid}', '{store_id}', '{sku_id}', '{validation_type}',
                    {physical_count if physical_count is not None else 'NULL'},
                    {physical_count if physical_count is not None else 'NULL'},
                    '{notes}', '{validated_by}', CURRENT_TIMESTAMP())
        """)

        status_map = {"confirmed": "confirmed", "dismissed": "dismissed", "investigated": "investigating"}
        new_status = status_map.get(validation_type, "open")

        execute_query(f"""
            UPDATE {cat}.serving.at_risk_skus
            SET status = '{new_status}', updated_ts = CURRENT_TIMESTAMP()
            WHERE sku_id = '{sku_id}' AND store_id = '{store_id}'
        """)
        _cache.clear()
        return True
    except Exception as e:
        print(f"Validation failed: {e}")
        return False


# ── Analytics Queries ────────────────────────────────────────────────

def get_analytics_trends() -> dict[str, Any]:
    cat = _cat()

    anomaly_types = execute_query(f"""
        SELECT primary_anomaly_type, COUNT(*) as count,
               ROUND(SUM(financial_impact), 2) as total_impact
        FROM {cat}.gold.gold_anomaly_summary
        GROUP BY primary_anomaly_type
        ORDER BY count DESC
    """)

    dept_accuracy = execute_query(f"""
        SELECT department,
               COUNT(*) as total_skus,
               SUM(CASE WHEN risk_tier IN ('CRITICAL', 'HIGH') THEN 1 ELSE 0 END) as at_risk,
               ROUND(100 - (SUM(CASE WHEN risk_tier IN ('CRITICAL', 'HIGH') THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as accuracy_pct
        FROM {cat}.gold.gold_sku_risk_scores
        GROUP BY department
        ORDER BY accuracy_pct ASC
    """)

    return {"anomaly_types": anomaly_types, "department_accuracy": dept_accuracy}
