from fastapi import APIRouter
from server.warehouse import get_dashboard_summary, get_risk_distribution, get_category_distribution

router = APIRouter(prefix="/api")


@router.get("/dashboard/summary")
def dashboard_summary():
    metrics = get_dashboard_summary()
    distribution = get_risk_distribution()
    categories = get_category_distribution()
    return {"metrics": metrics, "distribution": distribution, "categories": categories}
