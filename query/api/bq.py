import os

from google.cloud import bigquery

BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "pricepulse-gcp")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "pricepulse")
BIGQUERY_TABLE   = os.getenv("BIGQUERY_TABLE", "price_history")

TABLE_ID = f"`{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`"

# Singleton thread-safe — réutilisé par tous les workers Uvicorn
_client: bigquery.Client | None = None


def get_client() -> bigquery.Client:
    global _client
    if _client is None:
        # ADC : Workload Identity sur GKE, gcloud auth application-default login en local
        _client = bigquery.Client(project=BIGQUERY_PROJECT)
    return _client


def query_price_history(
    product_id: str,
    days: int,
    source: str | None,
) -> list[dict]:
    """
    Retourne l'historique des prix agrégé par jour depuis BigQuery.
    Filtre optionnel par source.
    """
    source_clause = "AND source = @source" if source else ""

    params: list[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("product_id", "STRING", product_id),
        bigquery.ScalarQueryParameter("days",       "INT64",  days),
    ]
    if source:
        params.append(bigquery.ScalarQueryParameter("source", "STRING", source))

    sql = f"""
        SELECT
            DATE(scraped_at)  AS date,
            source,
            AVG(price)        AS avg_price,
            MIN(price)        AS min_price,
            MAX(price)        AS max_price,
            COUNT(*)          AS data_points
        FROM {TABLE_ID}
        WHERE product_id = @product_id
          AND scraped_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
          {source_clause}
        GROUP BY date, source
        ORDER BY date ASC, source
    """

    job = get_client().query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return [
        {
            "date":        row.date.isoformat(),
            "source":      row.source,
            "avg_price":   round(row.avg_price, 2),
            "min_price":   round(row.min_price, 2),
            "max_price":   round(row.max_price, 2),
            "data_points": row.data_points,
        }
        for row in job.result()
    ]
