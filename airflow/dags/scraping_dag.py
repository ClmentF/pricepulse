"""
DAG scraping — toutes les 2h
Lance les spiders Scrapy (Amazon, Fnac, Cdiscount) qui POSTent vers NiFi.
"""
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

SCRAPERS_DIR = os.getenv("SCRAPERS_BASE_DIR", "/opt/airflow/command/scrapers")

default_args = {
    "owner": "pricepulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def run_spider(spider_dir: str, spider_name: str, **kwargs) -> None:
    result = subprocess.run(
        ["scrapy", "crawl", spider_name],
        cwd=spider_dir,
        capture_output=True,
        text=True,
        timeout=1200,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Spider '{spider_name}' a échoué :\n{result.stderr}")


with DAG(
    dag_id="scraping",
    description="Lance les spiders Scrapy toutes les 2h → NiFi → Kafka raw-prices",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["command", "scraping"],
) as dag:

    scrape_amazon = PythonOperator(
        task_id="scrape_amazon",
        python_callable=run_spider,
        op_kwargs={"spider_dir": f"{SCRAPERS_DIR}/amazon", "spider_name": "amazon"},
    )

    scrape_fnac = PythonOperator(
        task_id="scrape_fnac",
        python_callable=run_spider,
        op_kwargs={"spider_dir": f"{SCRAPERS_DIR}/fnac", "spider_name": "fnac"},
    )

    scrape_cdiscount = PythonOperator(
        task_id="scrape_cdiscount",
        python_callable=run_spider,
        op_kwargs={"spider_dir": f"{SCRAPERS_DIR}/cdiscount", "spider_name": "cdiscount"},
    )

    scrape_amazon >> scrape_fnac >> scrape_cdiscount
