from fastapi import FastAPI

from routers.commands import alerts
from routers.queries import health, history, prices

app = FastAPI(
    title="PricePulse API",
    version="1.0.0",
    description="Comparateur de prix e-commerce temps réel",
)

# ── Query side ────────────────────────────────────────────────────────────────
app.include_router(prices.router)
app.include_router(history.router)
app.include_router(health.router)

# ── Command side ──────────────────────────────────────────────────────────────
app.include_router(alerts.router)
