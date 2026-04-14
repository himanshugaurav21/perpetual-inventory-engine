from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from server.agent import analyze
from server.warehouse import execute_query, _cat, get_anomalies
from server.config import get_workspace_host, get_oauth_token, get_warehouse_id
from server.genie import ask_genie, ask_genie_followup, is_genie_configured
import json
import time

router = APIRouter(prefix="/api")


@router.post("/anomalies/{sku_id}/{store_id}/analyze")
def analyze_anomaly(sku_id: str, store_id: str):
    try:
        result = analyze(sku_id, store_id)
        if "error" in result:
            raise HTTPException(status_code=404, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies/top-critical")
def top_critical_anomalies():
    """Return top 20 CRITICAL anomalies for dropdown selection."""
    return get_anomalies(risk_tier="CRITICAL", limit=20)


@router.get("/genie/status")
def genie_status():
    """Check if Genie is configured."""
    return {"genie_enabled": is_genie_configured(), "mode": "genie" if is_genie_configured() else "ai-sql"}


class GenieRequest(BaseModel):
    question: str
    conversation_id: str | None = None


@router.post("/genie/ask")
def genie_ask(body: GenieRequest):
    """Ask a question - uses Genie API if configured, otherwise LLM text-to-SQL."""

    if is_genie_configured():
        # Follow-up if conversation exists
        if body.conversation_id:
            result = ask_genie_followup(body.conversation_id, body.question)
        else:
            result = ask_genie(body.question)
        if "error" not in result:
            return result
        # If Genie fails, fall through to LLM

    # LLM text-to-SQL fallback
    return _llm_text_to_sql(body.question)


def _llm_text_to_sql(question: str) -> dict:
    """Generate SQL from natural language using Foundation Model API."""
    cat = _cat()
    from server.llm import chat_completion

    schema_context = f"""You have access to these Databricks tables in catalog '{cat}':

1. {cat}.gold.gold_sku_risk_scores - 250K rows, one per SKU-store pair
   Columns: store_id, sku_id, category, department, retail_price, system_quantity,
   calculated_on_hand, stock_discrepancy, daily_velocity_30d, days_since_last_sale,
   total_adjustments_90d, total_positive_adjustments, unexplained_loss,
   velocity_score (0-1), stock_consistency_score (0-1), adjustment_score (0-1),
   shrinkage_score (0-1), shipment_gap_score (0-1), composite_risk_score (0-1),
   risk_tier (CRITICAL/HIGH/MEDIUM/LOW), explanation_text

2. {cat}.gold.gold_store_health - 50 rows, one per store
   Columns: store_id, store_name, region, city, state, store_type, shrinkage_profile,
   total_skus, critical_risk_skus, high_risk_skus, medium_risk_skus, low_risk_skus,
   pct_at_risk, pi_accuracy_pct, avg_composite_score, total_ghost_inventory_value,
   estimated_shrinkage_dollars

3. {cat}.gold.gold_anomaly_summary - ~49K rows, CRITICAL+HIGH risk only
   Columns: anomaly_id, store_id, store_name, sku_id, sku_name, category, department,
   region, risk_tier, primary_anomaly_type (ghost_inventory/systematic_inflation/
   stock_mismatch/shrinkage_spike/replenishment_anomaly), composite_risk_score,
   velocity_score, adjustment_score, stock_consistency_score, shipment_gap_score,
   shrinkage_score, system_quantity, calculated_on_hand, stock_discrepancy,
   financial_impact, recommended_action, explanation_text, priority_rank, detected_date

Risk tiers: CRITICAL >= 0.75, HIGH >= 0.50, MEDIUM >= 0.30, LOW < 0.30.
Ghost inventory = system shows stock but zero sales for 30+ days.
PI accuracy = percentage of SKU-stores NOT flagged as critical/high risk."""

    prompt = f"""{schema_context}

Convert this question to a single Databricks SQL query. Return ONLY the SQL, no explanation.
Always LIMIT results to 25 rows max. Use fully qualified table names with catalog.

Question: {question}

SQL:"""

    sql = ""
    try:
        sql = chat_completion([{"role": "user", "content": prompt}], max_tokens=500, temperature=0.1)
        sql = sql.strip()
        if sql.startswith("```"):
            lines = sql.split("\n")
            sql = "\n".join(lines[1:] if len(lines) > 1 else lines)
        if sql.endswith("```"):
            sql = sql[:-3]
        sql = sql.strip().rstrip(";")

        # Safety: block mutations
        sql_upper = sql.upper()
        if any(kw in sql_upper for kw in ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "CREATE", "TRUNCATE"]):
            return {"question": question, "sql": sql, "error": "Only SELECT queries are allowed.", "source": "ai-sql"}

        t0 = time.time()
        rows = execute_query(sql)
        query_ms = int((time.time() - t0) * 1000)

        return {
            "question": question,
            "sql": sql,
            "results": rows[:25],
            "row_count": len(rows),
            "query_ms": query_ms,
            "source": "ai-sql",
        }
    except Exception as e:
        return {
            "question": question,
            "sql": sql,
            "error": str(e),
            "source": "ai-sql",
        }
