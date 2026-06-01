import json
import logging
import os
import re
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("normalisation")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "raw-prices")
TOPIC_VALIDATED = os.getenv("KAFKA_TOPIC_VALIDATED", "validated-prices")
TOPIC_DEAD = os.getenv("KAFKA_TOPIC_DEAD", "dead-letter")

KNOWN_SOURCES = {"amazon", "fnac", "cdiscount"}

# Symboles de devise détectables dans price_raw
CURRENCY_SYMBOLS: dict[str, str] = {
    "€": "EUR",
    "$": "USD",
    "£": "GBP",
}

# Fallback devise par source si aucun symbole trouvé
SOURCE_CURRENCY: dict[str, str] = {
    "amazon": "EUR",
    "fnac": "EUR",
    "cdiscount": "EUR",
}


# ── Parsing ───────────────────────────────────────────────────────────────────


def parse_price(price_raw: str, source: str) -> tuple[float, str]:
    """
    Convertit price_raw (ex: "969,00 €", "1.234,56€", "$12.99") en (float, devise ISO).
    Lève ValueError si le parsing est impossible ou si price <= 0.
    """
    if not price_raw or not price_raw.strip():
        raise ValueError("price_raw vide")

    raw = price_raw.strip()

    # Détection devise depuis le symbole présent dans la chaîne
    currency = SOURCE_CURRENCY.get(source, "EUR")
    for symbol, code in CURRENCY_SYMBOLS.items():
        if symbol in raw:
            currency = code
            break

    # Garde uniquement chiffres, virgules et points
    cleaned = re.sub(r"[^\d,.]", "", raw)
    if not cleaned:
        raise ValueError(f"impossible d'extraire un nombre de {price_raw!r}")

    # Résolution du séparateur décimal selon la position relative
    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(".") > cleaned.rfind(","):
            # Format anglais : 1,234.56
            cleaned = cleaned.replace(",", "")
        else:
            # Format français : 1.234,56
            cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        # Décimale française seule : 969,00
        cleaned = cleaned.replace(",", ".")

    try:
        price = float(cleaned)
    except ValueError:
        raise ValueError(f"conversion float impossible : {cleaned!r} (source: {price_raw!r})")

    if price <= 0:
        raise ValueError(f"prix doit être > 0, reçu {price}")

    return price, currency


# ── Kafka helpers ─────────────────────────────────────────────────────────────


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,   # ordre garanti
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_RAW,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="normalisation-group",
        enable_auto_commit=False,          # commit manuel après republication
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


# ── Traitement d'un message ───────────────────────────────────────────────────


def normalise(raw: dict, producer: KafkaProducer) -> None:
    source = raw.get("source", "")

    if source not in KNOWN_SOURCES:
        raise ValueError(f"source inconnue : {source!r}")

    price, currency = parse_price(raw.get("price_raw", ""), source)

    validated = {
        "product_id": raw["product_id"],
        "product_name": raw.get("product_name", ""),
        "source": source,
        "price": price,
        "currency": currency,
        "price_incl_tax": True,
        "url": raw.get("url", ""),
        "scraped_at": raw.get("scraped_at", ""),
        "normalized_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    producer.send(TOPIC_VALIDATED, key=source, value=validated)
    producer.flush()
    logger.info(
        "Validé   %s [%s] %.2f %s",
        validated["product_id"], source, price, currency,
    )


def send_dead_letter(raw: dict, reason: str, producer: KafkaProducer) -> None:
    source = raw.get("source", "unknown")
    key = source if source in KNOWN_SOURCES else "unknown"
    payload = {**raw, "dead_letter_reason": reason}
    producer.send(TOPIC_DEAD, key=key, value=payload)
    producer.flush()
    logger.warning(
        "Dead-letter %s [%s]: %s",
        raw.get("product_id", "?"), source, reason,
    )


# ── Boucle principale ─────────────────────────────────────────────────────────


def main() -> None:
    producer = make_producer()
    consumer = make_consumer()
    logger.info("Consumer normalisation démarré — écoute %s", TOPIC_RAW)

    for msg in consumer:
        raw = msg.value
        try:
            normalise(raw, producer)
        except Exception as exc:
            send_dead_letter(raw, str(exc), producer)
        finally:
            # Commit offset dans tous les cas : message traité ou envoyé en dead-letter
            consumer.commit()


if __name__ == "__main__":
    main()
