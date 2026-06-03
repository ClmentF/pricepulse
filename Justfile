# PricePulse — commandes locales
# Prérequis : just (https://github.com/casey/just)

# ── Stack complète ────────────────────────────────────────────────────────────

# Démarre toute l'infra + app
up:
    docker-compose up -d

# Rebuild + redémarre tout
rebuild:
    docker-compose up -d --build

# Arrête tout
down:
    docker-compose down

# Arrête et supprime les volumes (reset complet)
reset:
    docker-compose down -v

# ── Logs ──────────────────────────────────────────────────────────────────────

logs service="":
    docker-compose logs -f {{service}}

# ── Kafka ─────────────────────────────────────────────────────────────────────

# Liste les topics Kafka
kafka-topics:
    docker-compose exec kafka kafka-topics --bootstrap-server kafka:29092 --list

# Consomme raw-prices en temps réel (ctrl+c pour quitter)
watch-raw:
    docker-compose exec kafka kafka-console-consumer \
        --bootstrap-server kafka:29092 \
        --topic raw-prices \
        --from-beginning

# Consomme validated-prices en temps réel
watch-validated:
    docker-compose exec kafka kafka-console-consumer \
        --bootstrap-server kafka:29092 \
        --topic validated-prices \
        --from-beginning

# Consomme dead-letter en temps réel
watch-dead:
    docker-compose exec kafka kafka-console-consumer \
        --bootstrap-server kafka:29092 \
        --topic dead-letter \
        --from-beginning

# ── Scrapers ──────────────────────────────────────────────────────────────────

# Lance le spider Amazon manuellement
scrape-amazon:
    cd command/scrapers/amazon && scrapy crawl amazon

# ── NiFi ─────────────────────────────────────────────────────────────────────

# Déploie le flow NiFi (à lancer une fois après `just up`)
nifi-setup:
    cd nifi && pip install -r requirements.txt -q && python setup.py

# ── PostgreSQL ────────────────────────────────────────────────────────────────

# Ouvre un shell psql
db-shell:
    docker-compose exec postgres psql -U pp_user -d pricepulse

# ── API ───────────────────────────────────────────────────────────────────────

# Ouvre la doc Swagger
api-docs:
    xdg-open http://localhost:8000/docs 2>/dev/null || open http://localhost:8000/docs

# ── Interfaces web ────────────────────────────────────────────────────────────

# Ouvre l'UI Airflow (admin / admin)
airflow-ui:
    xdg-open http://localhost:8088 2>/dev/null || open http://localhost:8088

# Ouvre l'UI NiFi (admin / adminadminadmin)
nifi-ui:
    xdg-open https://localhost:8443/nifi 2>/dev/null || open https://localhost:8443/nifi
