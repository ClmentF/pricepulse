from fastapi import APIRouter, HTTPException

router = APIRouter(tags=["queries"])


@router.get("/history/{product_id}")
def get_history(product_id: str, days: int = 30, source: str | None = None):
    """Courbe de prix depuis BigQuery. Implémenté à l'étape 8."""
    raise HTTPException(status_code=501, detail="Non implémenté — étape 8 (BigQuery)")
