from fastapi import APIRouter
from server.warehouse import get_analytics_trends

router = APIRouter(prefix="/api")


@router.get("/analytics/trends")
def analytics_trends():
    return get_analytics_trends()
