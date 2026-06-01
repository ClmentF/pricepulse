from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from cache import cache_get, cache_set
from db import get_db

router = APIRouter(tags=["queries"])


@router.get("/prices")
def get_prices(
    product_id: str = Query(..., description="Identifiant du produit"),
    db: Session = Depends(get_db),
):
    """Prix courants pour un produit sur toutes les sources."""
    rows = db.execute(
        text("""
            SELECT cp.source, cp.price, cp.currency, cp.url, cp.scraped_at,
                   p.product_name
            FROM current_prices cp
            JOIN products p ON p.product_id = cp.product_id
            WHERE cp.product_id = :product_id
            ORDER BY cp.price ASC
        """),
        {"product_id": product_id},
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Produit '{product_id}' introuvable")

    return {
        "product_id": product_id,
        "product_name": rows[0].product_name,
        "prices": [
            {
                "source": r.source,
                "price": float(r.price),
                "currency": r.currency,
                "url": r.url,
                "scraped_at": r.scraped_at.isoformat(),
            }
            for r in rows
        ],
    }


@router.get("/compare")
def compare(
    q: str = Query(..., description="Terme de recherche (nom produit)"),
    db: Session = Depends(get_db),
):
    """Recherche un produit par nom et compare les prix multi-sources. Cache 5 min."""
    cache_key = f"compare:{q.lower().strip()}"
    if cached := cache_get(cache_key):
        return cached

    rows = db.execute(
        text("""
            SELECT p.product_id, p.product_name,
                   cp.source, cp.price, cp.currency, cp.url, cp.scraped_at
            FROM products p
            JOIN current_prices cp ON cp.product_id = p.product_id
            WHERE p.product_name ILIKE :q
            ORDER BY p.product_id, cp.price ASC
        """),
        {"q": f"%{q}%"},
    ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"Aucun produit correspondant à '{q}'")

    # Regroupe par produit
    products: dict[str, dict] = {}
    for r in rows:
        if r.product_id not in products:
            products[r.product_id] = {
                "product_id": r.product_id,
                "product_name": r.product_name,
                "prices": [],
            }
        products[r.product_id]["prices"].append({
            "source": r.source,
            "price": float(r.price),
            "currency": r.currency,
            "url": r.url,
            "scraped_at": r.scraped_at.isoformat(),
        })

    result = list(products.values())
    cache_set(cache_key, result)
    return result


@router.get("/cheapest")
def cheapest(
    category: Optional[str] = Query(None, description="Filtrer par catégorie"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Top N produits les moins chers du moment. Cache 5 min."""
    cache_key = f"cheapest:{category}:{limit}"
    if cached := cache_get(cache_key):
        return cached

    rows = db.execute(
        text("""
            SELECT DISTINCT ON (cp.product_id)
                p.product_id, p.product_name, p.category,
                cp.source, cp.price, cp.currency, cp.url
            FROM current_prices cp
            JOIN products p ON p.product_id = cp.product_id
            WHERE (:category IS NULL OR p.category ILIKE :category)
            ORDER BY cp.product_id, cp.price ASC
            LIMIT :limit
        """),
        {"category": f"%{category}%" if category else None, "limit": limit},
    ).fetchall()

    result = [
        {
            "product_id": r.product_id,
            "product_name": r.product_name,
            "category": r.category,
            "source": r.source,
            "price": float(r.price),
            "currency": r.currency,
            "url": r.url,
        }
        for r in rows
    ]
    cache_set(cache_key, result)
    return result
