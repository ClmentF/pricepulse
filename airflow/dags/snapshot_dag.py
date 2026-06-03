"""
DAG snapshot hebdomadaire — lundi à 6h
Exporte price_history depuis BigQuery vers GCS au format Parquet, puis notifie Slack.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "pricepulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def export_bigquery_to_gcs(**kwargs) -> str:
    from google.cloud import bigquery

    project = os.getenv("BIGQUERY_PROJECT", "pricepulse-gcp")
    dataset = os.getenv("BIGQUERY_DATASET", "pricepulse")
    bucket  = os.getenv("GCS_SNAPSHOT_BUCKET", "pricepulse-snapshots")
    ds      = kwargs["ds"]   # date d'exécution YYYY-MM-DD

    destination = f"gs://{bucket}/snapshots/{ds}/price_history_*.parquet"

    client = bigquery.Client(project=project)
    job = client.extract_table(
        f"{project}.{dataset}.price_history",
        destination,
        job_config=bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.PARQUET,
            compression=bigquery.Compression.SNAPPY,
        ),
    )
    job.result()
    print(f"Export terminé → {destination}")
    return destination


def notify_slack(**kwargs) -> None:
    import requests

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        print("SLACK_WEBHOOK_URL non configurée — notification ignorée")
        return

    destination = kwargs["ti"].xcom_pull(task_ids="export_bigquery_to_gcs")
    requests.post(
        webhook_url,
        json={"text": f":floppy_disk: Snapshot hebdo exporté → `{destination}`"},
        timeout=10,
    ).raise_for_status()


with DAG(
    dag_id="snapshot_hebdomadaire",
    description="Export BigQuery → GCS Parquet tous les lundis à 6h",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["command", "snapshot"],
) as dag:

    export = PythonOperator(
        task_id="export_bigquery_to_gcs",
        python_callable=export_bigquery_to_gcs,
    )

    notify = PythonOperator(
        task_id="notify_slack",
        python_callable=notify_slack,
    )

    export >> notify
