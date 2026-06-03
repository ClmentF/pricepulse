#!/bin/bash
set -e

airflow db upgrade

# Crée l'admin si inexistant (idempotent)
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@pricepulse.dev \
    2>/dev/null || true

# Scheduler en arrière-plan + webserver au premier plan
airflow scheduler &
exec airflow webserver
