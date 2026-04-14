from fastapi import APIRouter
from server.warehouse import get_store_health

router = APIRouter(prefix="/api")


@router.get("/stores/health")
def store_health():
    stores = get_store_health()
    return {"stores": stores, "count": len(stores)}
