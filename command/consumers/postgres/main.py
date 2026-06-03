import json
import logging
import os

from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("consumer-postgres")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
TOPIC_VALIDATED = os.getenv("KAFKA_TOPIC_VALIDATED", "validated-prices")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://pp_user:pp_pass@localhost:5432/pricepulse")

# ── SQL ───────────────────────────────────────────────────────────────────────

# Garantit que le produit existe avant l'upsert sur current_prices (FK)
_SQL_UPSERT_PRODUCT = text("""
    INSERT INTO products (product_id, product_name)
    VALUES (:product_id, :product_name)
    ON CONFLICT (product_id) DO UPDATE
        SET product_name = EXCLUDED.product_name
""")

_SQL_UPSERT_PRICE = text("""
    INSERT INTO current_prices (product_id, source, price, currency, url, scraped_at)
    VALUES (:product_id, :source, :price, :currency, :url, :scraped_at::timestamp)
    ON CONFLICT (product_id, source) DO UPDATE SET
        price      = EXCLUDED.price,
        currency   = EXCLUDED.currency,
        url        = EXCLUDED.url,
        scraped_at = EXCLUDED.scraped_at,
        updated_at = NOW()
""")

# Alertes actives dont le seuil est franchi par le nouveau prix
_SQL_CHECK_ALERTS = text("""
    SELECT id, user_email, threshold
    FROM price_alerts
    WHERE product_id = :product_id
      AND active     = TRUE
      AND :price    <= threshold
""")

# ── Kafka ─────────────────────────────────────────────────────────────────────


def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_VALIDATED,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="postgres-group",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


# ── Database ──────────────────────────────────────────────────────────────────


def make_engine() -> Engine:
    return create_engine(DATABASE_URL, pool_pre_ping=True)


# ── Traitement ────────────────────────────────────────────────────────────────


def process(msg: dict, engine: Engine) -> None:
    with engine.begin() as conn:
        # 1. Upsert produit (résout la FK avant current_prices)
        conn.execute(_SQL_UPSERT_PRODUCT, {
            "product_id":   msg["product_id"],
            "product_name": msg.get("product_name", ""),
        })

        # 2. Upsert prix courant
        conn.execute(_SQL_UPSERT_PRICE, {
            "product_id": msg["product_id"],
            "source":     msg["source"],
            "price":      msg["price"],
            "currency":   msg.get("currency", "EUR"),
            "url":        msg.get("url", ""),
            "scraped_at": msg["scraped_at"],
        })

        # 3. Vérification des alertes dans la même transaction
        rows = conn.execute(_SQL_CHECK_ALERTS, {
            "product_id": msg["product_id"],
            "price":      msg["price"],
        }).fetchall()

    for row in rows:
        send_alert_email(
            alert_id=row.id,
            user_email=row.user_email,
            product_id=msg["product_id"],
            source=msg["source"],
            price=msg["price"],
            threshold=float(row.threshold),
        )

    logger.info(
        "Upsert %s [%s] %.2f %s — %d alerte(s) déclenchée(s)",
        msg["product_id"], msg["source"], msg["price"],
        msg.get("currency", "EUR"), len(rows),
    )


def send_alert_email(
    alert_id: int,
    user_email: str,
    product_id: str,
    source: str,
    price: float,
    threshold: float,
) -> None:
    # TODO étape 9 : remplacer par SendGrid (sendgrid.SendGridAPIClient)
    logger.info(
        "ALERT #%d → %s : %s [%s] à %.2f€ ≤ seuil %.2f€",
        alert_id, user_email, product_id, source, price, threshold,
    )


# ── Boucle principale ─────────────────────────────────────────────────────────


def main() -> None:
    engine = make_engine()
    consumer = make_consumer()
    logger.info("Consumer PostgreSQL démarré — écoute %s", TOPIC_VALIDATED)

    for msg in consumer:
        try:
            process(msg.value, engine)
            consumer.commit()
        except Exception as exc:
            logger.error(
                "Échec upsert %s: %s",
                msg.value.get("product_id", "?"), exc,
            )
            # Pas de commit : le message sera retraité au prochain démarrage


if __name__ == "__main__":
    main()
