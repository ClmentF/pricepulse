from fastapi import APIRouter, HTTPException, Query

from bq import query_price_history

router = APIRouter(tags=["queries"])


@router.get("/history/{product_id}")
def get_history(
    product_id: str,
    days:   int          = Query(30, ge=1, le=365, description="Fenêtre en jours"),
    source: str | None   = Query(None, description="Filtrer par source (amazon/fnac/cdiscount)"),
):
    """Courbe de prix agrégée par jour depuis BigQuery."""
    try:
        rows = query_price_history(product_id, days, source)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"BigQuery indisponible : {exc}")

    if not rows:
        raise HTTPException(status_code=404, detail=f"Aucun historique pour '{product_id}'")

    return {
        "product_id": product_id,
        "days":       days,
        "source":     source,
        "history":    rows,
    }
