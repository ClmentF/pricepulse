"""
DAG alertes prix — toutes les heures
Requête PostgreSQL pour les alertes actives dont le seuil est franchi → email SendGrid.
Filet de sécurité batch en complément de la vérification temps-réel du consumer PostgreSQL.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "pricepulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_alerts(**kwargs) -> list[dict]:
    from sqlalchemy import create_engine, text

    engine = create_engine(os.getenv("DATABASE_URL"))
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT
                pa.id,
                pa.user_email,
                pa.product_id,
                pa.threshold,
                cp.price,
                cp.source,
                p.product_name
            FROM price_alerts pa
            JOIN current_prices cp ON cp.product_id = pa.product_id
            JOIN products       p  ON p.product_id  = pa.product_id
            WHERE pa.active    = TRUE
              AND cp.price    <= pa.threshold
        """)).fetchall()

    alerts = [
        {
            "id":           r.id,
            "user_email":   r.user_email,
            "product_id":   r.product_id,
            "product_name": r.product_name,
            "price":        float(r.price),
            "threshold":    float(r.threshold),
            "source":       r.source,
        }
        for r in rows
    ]
    print(f"{len(alerts)} alerte(s) à envoyer")
    kwargs["ti"].xcom_push(key="alerts", value=alerts)
    return alerts


def send_emails(**kwargs) -> None:
    import sendgrid
    from sendgrid.helpers.mail import Mail

    alerts = kwargs["ti"].xcom_pull(task_ids="check_alerts", key="alerts") or []
    if not alerts:
        return

    api_key = os.getenv("SENDGRID_API_KEY", "")
    if not api_key:
        print("SENDGRID_API_KEY non configurée — emails ignorés")
        return

    sg = sendgrid.SendGridAPIClient(api_key=api_key)
    sender = os.getenv("SENDGRID_FROM_EMAIL", "noreply@pricepulse.dev")

    for alert in alerts:
        message = Mail(
            from_email=sender,
            to_emails=alert["user_email"],
            subject=f"[PricePulse] Prix en baisse : {alert['product_name']}",
            plain_text_content=(
                f"Bonne nouvelle ! {alert['product_name']} est disponible à "
                f"{alert['price']}€ sur {alert['source']}, "
                f"en dessous de votre seuil de {alert['threshold']}€.\n\n"
                f"Voir les prix : https://api.pricepulse.dev/v1/prices?product_id={alert['product_id']}"
            ),
        )
        sg.send(message)
        print(f"Email envoyé → {alert['user_email']} | {alert['product_id']} à {alert['price']}€")


with DAG(
    dag_id="alertes_prix",
    description="Vérifie les alertes prix actives toutes les heures → email SendGrid",
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["command", "alertes"],
) as dag:

    check = PythonOperator(
        task_id="check_alerts",
        python_callable=check_alerts,
    )

    send = PythonOperator(
        task_id="send_emails",
        python_callable=send_emails,
    )

    check >> send
