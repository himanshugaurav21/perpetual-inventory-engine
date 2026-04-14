from fastapi import APIRouter, HTTPException, Query
from server.warehouse import get_anomalies, get_anomaly_detail

router = APIRouter(prefix="/api")


@router.get("/anomalies")
def list_anomalies(
    risk_tier: str | None = Query(None),
    category: str | None = Query(None),
    store_id: str | None = Query(None),
    anomaly_type: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    anomalies = get_anomalies(risk_tier=risk_tier, category=category,
                              store_id=store_id, anomaly_type=anomaly_type,
                              limit=limit, offset=offset)
    return {"anomalies": anomalies, "count": len(anomalies)}


@router.get("/anomalies/{sku_id}/{store_id}")
def anomaly_detail(sku_id: str, store_id: str):
    detail = get_anomaly_detail(sku_id, store_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Anomaly not found")
    return detail
