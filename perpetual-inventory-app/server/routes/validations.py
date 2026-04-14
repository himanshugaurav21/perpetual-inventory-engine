from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from server.warehouse import get_validation_queue, submit_validation

router = APIRouter(prefix="/api")


class ValidationRequest(BaseModel):
    sku_id: str
    store_id: str
    validation_type: str  # confirmed, dismissed, investigated
    physical_count: int | None = None
    notes: str = ""
    validated_by: str = "store_team"


@router.get("/validations/queue/{store_id}")
def validation_queue(store_id: str):
    items = get_validation_queue(store_id)
    return {"items": items, "count": len(items)}


@router.post("/validations")
def submit_store_validation(body: ValidationRequest):
    ok = submit_validation(
        sku_id=body.sku_id,
        store_id=body.store_id,
        validation_type=body.validation_type,
        physical_count=body.physical_count,
        notes=body.notes,
        validated_by=body.validated_by,
    )
    if not ok:
        raise HTTPException(status_code=500, detail="Validation submission failed")
    return {"status": "ok", "message": f"Validation recorded for {body.sku_id} at {body.store_id}"}
