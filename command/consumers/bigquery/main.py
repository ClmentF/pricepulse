import json
import logging
import os
import time

from google.cloud import bigquery
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("consumer-bigquery")

KAFKA_BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
TOPIC_VALIDATED    = os.getenv("KAFKA_TOPIC_VALIDATED", "validated-prices")
BIGQUERY_PROJECT   = os.getenv("BIGQUERY_PROJECT", "pricepulse-gcp")
BIGQUERY_DATASET   = os.getenv("BIGQUERY_DATASET", "pricepulse")
BIGQUERY_TABLE     = os.getenv("BIGQUERY_TABLE", "price_history")
BATCH_SIZE         = int(os.getenv("BQ_BATCH_SIZE", "100"))
FLUSH_INTERVAL_SEC = float(os.getenv("BQ_FLUSH_INTERVAL", "5"))

TABLE_ID = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

# ── BigQuery ──────────────────────────────────────────────────────────────────


def make_bq_client() -> bigquery.Client:
    # Application Default Credentials :
    #   - GKE : Workload Identity (aucune clé JSON)
    #   - Local : gcloud auth application-default login
    #             ou GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
    return bigquery.Client(project=BIGQUERY_PROJECT)


def to_bq_row(msg_value: dict) -> dict:
    return {
        "product_id":    msg_value["product_id"],
        "product_name":  msg_value.get("product_name", ""),
        "source":        msg_value["source"],
        "price":         float(msg_value["price"]),
        "currency":      msg_value.get("currency", "EUR"),
        "url":           msg_value.get("url", ""),
        "scraped_at":    msg_value["scraped_at"],
        "normalized_at": msg_value.get("normalized_at"),
    }


def flush(buffer: list[ConsumerRecord], bq_client: bigquery.Client) -> None:
    rows = [to_bq_row(msg.value) for msg in buffer]
    errors = bq_client.insert_rows_json(TABLE_ID, rows)
    if errors:
        raise RuntimeError(f"BigQuery streaming insert errors: {errors}")
    logger.info("Flush %d lignes → %s", len(rows), TABLE_ID)


# ── Kafka ─────────────────────────────────────────────────────────────────────


def make_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_VALIDATED,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="bigquery-group",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


# ── Boucle principale ─────────────────────────────────────────────────────────


def main() -> None:
    bq_client = make_bq_client()
    consumer  = make_consumer()
    logger.info(
        "Consumer BigQuery démarré — écoute %s (batch=%d, flush=%ss)",
        TOPIC_VALIDATED, BATCH_SIZE, FLUSH_INTERVAL_SEC,
    )

    buffer:     list[ConsumerRecord] = []
    last_flush: float = time.monotonic()

    while True:
        # poll() avec timeout pour permettre le flush temporel même sans messages
        records = consumer.poll(timeout_ms=1000)

        for msgs in records.values():
            buffer.extend(msgs)

        elapsed = time.monotonic() - last_flush
        should_flush = buffer and (
            len(buffer) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL_SEC
        )

        if should_flush:
            try:
                flush(buffer, bq_client)
                consumer.commit()     # commit après INSERT confirmé
                buffer.clear()
                last_flush = time.monotonic()
            except Exception as exc:
                logger.error("Échec flush BigQuery : %s — offset non commité", exc)
                # Buffer conservé, retry au prochain cycle


if __name__ == "__main__":
    main()
