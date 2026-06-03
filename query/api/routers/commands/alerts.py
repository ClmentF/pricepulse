from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.orm import Session

from db import get_db

router = APIRouter(prefix="/alerts", tags=["commands"])


class CreateAlertRequest(BaseModel):
    product_id: str
    user_email: EmailStr
    threshold: float


@router.post("", status_code=201)
def create_alert(payload: CreateAlertRequest, db: Session = Depends(get_db)):
    """Crée une alerte prix pour un produit."""
    row = db.execute(
        text("""
            INSERT INTO price_alerts (product_id, user_email, threshold)
            VALUES (:product_id, :user_email, :threshold)
            RETURNING id, product_id, user_email, threshold, active, created_at
        """),
        payload.model_dump(),
    ).fetchone()
    db.commit()
    return {
        "id": row.id,
        "product_id": row.product_id,
        "user_email": row.user_email,
        "threshold": float(row.threshold),
        "active": row.active,
        "created_at": row.created_at.isoformat(),
    }


@router.delete("/{alert_id}", status_code=204)
def delete_alert(alert_id: int, db: Session = Depends(get_db)):
    """Supprime une alerte."""
    result = db.execute(
        text("DELETE FROM price_alerts WHERE id = :id"),
        {"id": alert_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail=f"Alerte {alert_id} introuvable")
