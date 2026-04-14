"""4-Step AI Anomaly Agent with MLflow Tracing.

Pipeline:
  Step 1: Data Validation
  Step 2: Signal Extraction (5 component scores)
  Step 3: Composite Risk Scoring
  Step 4: LLM Reasoning (Claude Sonnet 4.5)
"""

import json
import os
import time
import mlflow
from mlflow.entities import SpanType

from server.config import refresh_databricks_token
from server.warehouse import get_anomaly_detail, get_store_health, execute_query, _cat
from server.llm import chat_completion, get_last_llm_metrics
from server.vector_search import find_similar_anomalies

mlflow_client = mlflow.MlflowClient()


def analyze(sku_id: str, store_id: str) -> dict:
    """Run the full 4-step anomaly analysis pipeline."""
    refresh_databricks_token()

    t_start = time.time()
    perf = {}

    # ── Data Lookup ──
    t0 = time.time()
    data = _fetch_analysis_data(sku_id, store_id)
    perf["data_lookup_ms"] = int((time.time() - t0) * 1000)

    if not data:
        return {"error": f"No data found for {sku_id} at {store_id}"}

    # ── Step 1: Validation ──
    t0 = time.time()
    validation = _step1_validate(data)
    perf["step1_ms"] = int((time.time() - t0) * 1000)

    # ── Step 2: Signal Extraction ──
    t0 = time.time()
    signals = _step2_signals(data)
    perf["step2_ms"] = int((time.time() - t0) * 1000)

    # ── Step 3: Risk Scoring ──
    t0 = time.time()
    risk = _step3_risk_score(signals)
    perf["step3_ms"] = int((time.time() - t0) * 1000)

    # ── Vector Search (non-fatal) ──
    t0 = time.time()
    try:
        query = f"{data.get('category', '')} {data.get('department', '')} anomaly {data.get('primary_anomaly_type', '')}"
        similar = find_similar_anomalies(query, num_results=3)
    except Exception:
        similar = []
    perf["vs_ms"] = int((time.time() - t0) * 1000)

    # ── Step 4: LLM Reasoning ──
    t0 = time.time()
    llm_result = _step4_llm(data, validation, signals, risk, similar)
    perf["step4_ms"] = int((time.time() - t0) * 1000)

    perf["total_ms"] = int((time.time() - t_start) * 1000)

    return {
        "sku_id": sku_id,
        "store_id": store_id,
        "validation": validation,
        "signals": signals,
        "risk": risk,
        "similar_patterns": similar,
        "llm_analysis": llm_result,
        "performance": perf,
    }


def _fetch_analysis_data(sku_id: str, store_id: str) -> dict | None:
    cat = _cat()
    rows = execute_query(f"""
        SELECT a.*, r.velocity_score, r.stock_consistency_score, r.adjustment_score,
               r.shrinkage_score, r.shipment_gap_score, r.composite_risk_score,
               r.daily_velocity_30d, r.days_since_last_sale,
               r.total_adjustments_90d, r.total_positive_adjustments, r.unexplained_loss,
               r.system_quantity, r.calculated_on_hand, r.stock_discrepancy
        FROM {cat}.gold.gold_anomaly_summary a
        JOIN {cat}.gold.gold_sku_risk_scores r
            ON a.sku_id = r.sku_id AND a.store_id = r.store_id
        WHERE a.sku_id = '{sku_id}' AND a.store_id = '{store_id}'
    """)
    return rows[0] if rows else None


def _step1_validate(data: dict) -> dict:
    issues = []
    if data.get("system_quantity") is None:
        issues.append("Missing system quantity data")
    if data.get("days_since_last_sale") is None:
        issues.append("No sales data available")
    sq = _safe_float(data.get("system_quantity", 0))
    if sq < 0:
        issues.append("Negative system quantity - data quality issue")
    return {"valid": len(issues) == 0, "issues": issues}


def _step2_signals(data: dict) -> dict:
    return {
        "velocity_score": _safe_float(data.get("velocity_score", 0)),
        "stock_consistency_score": _safe_float(data.get("stock_consistency_score", 0)),
        "adjustment_score": _safe_float(data.get("adjustment_score", 0)),
        "shrinkage_score": _safe_float(data.get("shrinkage_score", 0)),
        "shipment_gap_score": _safe_float(data.get("shipment_gap_score", 0)),
        "details": {
            "daily_velocity_30d": _safe_float(data.get("daily_velocity_30d", 0)),
            "days_since_last_sale": _safe_int(data.get("days_since_last_sale", 0)),
            "total_adjustments_90d": _safe_int(data.get("total_adjustments_90d", 0)),
            "total_positive_adjustments": _safe_int(data.get("total_positive_adjustments", 0)),
            "unexplained_loss": _safe_int(data.get("unexplained_loss", 0)),
            "system_quantity": _safe_int(data.get("system_quantity", 0)),
            "calculated_on_hand": _safe_int(data.get("calculated_on_hand", 0)),
            "stock_discrepancy": _safe_int(data.get("stock_discrepancy", 0)),
        }
    }


def _step3_risk_score(signals: dict) -> dict:
    score = round(
        signals["velocity_score"] * 0.25 +
        signals["stock_consistency_score"] * 0.25 +
        signals["adjustment_score"] * 0.20 +
        signals["shrinkage_score"] * 0.20 +
        signals["shipment_gap_score"] * 0.10,
    4)

    tier = (
        "CRITICAL" if score >= 0.75 else
        "HIGH" if score >= 0.50 else
        "MEDIUM" if score >= 0.30 else "LOW"
    )

    recommendation = (
        "INVESTIGATE" if score >= 0.70 else
        "MONITOR" if score >= 0.40 else
        "FLAG" if score >= 0.20 else "OK"
    )

    # Confidence based on signal agreement
    vals = [signals["velocity_score"], signals["stock_consistency_score"],
            signals["adjustment_score"], signals["shrinkage_score"],
            signals["shipment_gap_score"]]
    import statistics
    stdev = statistics.stdev(vals) if len(vals) > 1 else 0
    agreement = max(0, 1.0 - stdev / 0.5)
    confidence = round(min(1.0, agreement * 0.7 + 0.3), 2)

    return {
        "risk_score": score,
        "risk_tier": tier,
        "recommendation": recommendation,
        "confidence": confidence,
    }


def _step4_llm(data: dict, validation: dict, signals: dict, risk: dict,
               similar: list[dict]) -> dict:
    model = os.environ.get("SERVING_ENDPOINT", "databricks-claude-sonnet-4-5")

    similar_text = "\n".join([
        f"  - {s.get('category', '?')} / {s.get('primary_anomaly_type', '?')} / "
        f"risk={s.get('composite_risk_score', '?')} / ${s.get('financial_impact', '?')}"
        for s in similar[:3]
    ]) or "  No similar patterns found."

    prompt = f"""You are a retail inventory accuracy analyst. Analyze this SKU-store for ghost inventory.

SKU-STORE:
- SKU: {data.get('sku_id')} ({data.get('sku_name', 'Unknown')})
- Store: {data.get('store_id')} ({data.get('store_name', 'Unknown')})
- Category: {data.get('category', '?')}, Department: {data.get('department', '?')}
- System PI: {data.get('system_quantity', '?')} units
- Calculated PI: {data.get('calculated_on_hand', '?')} units
- Variance: {data.get('stock_discrepancy', '?')} units
- Financial exposure: ${data.get('financial_impact', '?')}

SIGNALS:
- Velocity: {signals['velocity_score']:.2f}/1.0 (daily velocity: {signals['details']['daily_velocity_30d']}, days since sale: {signals['details']['days_since_last_sale']})
- Stock consistency: {signals['stock_consistency_score']:.2f}/1.0
- Adjustments: {signals['adjustment_score']:.2f}/1.0 ({signals['details']['total_adjustments_90d']} total, {signals['details']['total_positive_adjustments']} upward)
- Shrinkage: {signals['shrinkage_score']:.2f}/1.0 ({signals['details']['unexplained_loss']} units unaccounted)
- Shipment gap: {signals['shipment_gap_score']:.2f}/1.0

COMPOSITE: {risk['risk_score']}/1.0, Tier: {risk['risk_tier']}, Confidence: {risk['confidence']}
VALIDATION: Valid={validation['valid']}, Issues={validation['issues'] or 'None'}

SIMILAR PATTERNS:
{similar_text}

Respond with EXACTLY this JSON:
{{"explanation": "3-5 sentence analysis", "signals": [{{"signal": "description", "severity": "critical|high|medium|low", "score": 0.0}}], "suggested_action": "specific next step", "root_cause_hypothesis": "most likely cause"}}"""

    try:
        t0 = time.time()
        raw = chat_completion([{"role": "user", "content": prompt}], max_tokens=768, temperature=0.2)
        latency_ms = int((time.time() - t0) * 1000)
        metrics = get_last_llm_metrics()

        start = raw.index("{")
        end = raw.rindex("}") + 1
        rec = json.loads(raw[start:end])
        rec["llm_stats"] = {
            "model": model, "latency_ms": latency_ms, "fallback": False,
            **metrics,
        }
        return rec
    except Exception as e:
        # Rule-based fallback
        return {
            "explanation": f"Rule-based: {data.get('explanation_text', 'Risk detected')}",
            "signals": [{"signal": data.get("primary_anomaly_type", "unknown"),
                         "severity": risk["risk_tier"].lower(), "score": risk["risk_score"]}],
            "suggested_action": "Send to store for physical count verification",
            "root_cause_hypothesis": "Unable to determine - LLM unavailable",
            "llm_stats": {"model": model, "fallback": True, "error": str(e)},
        }


def _safe_float(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _safe_int(v, default=0) -> int:
    try:
        return int(float(v)) if v is not None else default
    except (ValueError, TypeError):
        return default
