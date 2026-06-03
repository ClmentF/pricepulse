#!/usr/bin/env python3
"""
Déploie le flow NiFi PricePulse via l'API REST.

Prérequis : NiFi en cours d'exécution sur https://localhost:8443
Usage     : python nifi/setup.py
"""
import sys
import time

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_BASE = "https://localhost:8443/nifi-api"
NIFI_USER = "admin"
NIFI_PASS = "adminadminadmin"
KAFKA_BOOTSTRAP = "kafka:29092"
LISTEN_PORT = "8082"

# ── Auth ──────────────────────────────────────────────────────────────────────


def get_token() -> str:
    r = requests.post(
        f"{NIFI_BASE}/access/token",
        data={"username": NIFI_USER, "password": NIFI_PASS},
        verify=False,
    )
    r.raise_for_status()
    return r.text.strip()


def h(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ── API helpers ───────────────────────────────────────────────────────────────


def get_root_pg_id(token: str) -> str:
    r = requests.get(f"{NIFI_BASE}/flow/process-groups/root", headers=h(token), verify=False)
    r.raise_for_status()
    return r.json()["processGroupFlow"]["id"]


def create_process_group(token: str, root_id: str) -> dict:
    r = requests.post(
        f"{NIFI_BASE}/process-groups/{root_id}/process-groups",
        headers=h(token),
        json={
            "revision": {"version": 0},
            "component": {"name": "PricePulse", "position": {"x": 200, "y": 200}},
        },
        verify=False,
    )
    r.raise_for_status()
    return r.json()


def create_processor(
    token: str, pg_id: str, name: str, type_: str, props: dict, pos: dict
) -> dict:
    r = requests.post(
        f"{NIFI_BASE}/process-groups/{pg_id}/processors",
        headers=h(token),
        json={
            "revision": {"version": 0},
            "component": {
                "name": name,
                "type": type_,
                "position": pos,
                "config": {"properties": props},
            },
        },
        verify=False,
    )
    r.raise_for_status()
    return r.json()


def auto_terminate(token: str, proc: dict, relationships: list[str]) -> None:
    """Auto-termine les relations qui n'ont pas de connexion aval."""
    r = requests.put(
        f"{NIFI_BASE}/processors/{proc['id']}",
        headers=h(token),
        json={
            "revision": proc["revision"],
            "component": {
                "id": proc["id"],
                "config": {
                    "autoTerminatedRelationships": relationships,
                },
            },
        },
        verify=False,
    )
    r.raise_for_status()
    return r.json()


def create_connection(
    token: str, pg_id: str, src_id: str, dst_id: str, relationships: list[str]
) -> dict:
    r = requests.post(
        f"{NIFI_BASE}/process-groups/{pg_id}/connections",
        headers=h(token),
        json={
            "revision": {"version": 0},
            "component": {
                "source": {"id": src_id, "type": "PROCESSOR", "groupId": pg_id},
                "destination": {"id": dst_id, "type": "PROCESSOR", "groupId": pg_id},
                "selectedRelationships": relationships,
                "backPressureObjectThreshold": "10000",
                "backPressureDataSizeThreshold": "1 GB",
            },
        },
        verify=False,
    )
    r.raise_for_status()
    return r.json()


def start_processor(token: str, proc: dict) -> None:
    requests.put(
        f"{NIFI_BASE}/processors/{proc['id']}/run-status",
        headers=h(token),
        json={"revision": proc["revision"], "state": "RUNNING"},
        verify=False,
    ).raise_for_status()


def wait_for_nifi(retries: int = 20, delay: int = 10) -> str:
    print("Attente du démarrage de NiFi", end="", flush=True)
    for _ in range(retries):
        try:
            token = get_token()
            print(" OK")
            return token
        except Exception:
            print(".", end="", flush=True)
            time.sleep(delay)
    print()
    raise RuntimeError("NiFi injoignable après plusieurs tentatives")


# ── Flow deployment ───────────────────────────────────────────────────────────


def main() -> None:
    token = wait_for_nifi()

    print("Root process group...")
    root_id = get_root_pg_id(token)

    print("Création process group PricePulse...")
    pg = create_process_group(token, root_id)
    pg_id = pg["id"]

    # 1. ListenHTTP ─────────────────────────────────────────────────────────
    print("Processor: ListenHTTP")
    listen = create_processor(
        token, pg_id,
        name="ListenHTTP",
        type_="org.apache.nifi.processors.standard.ListenHTTP",
        props={"Base Path": "contentListener", "Listening Port": LISTEN_PORT},
        pos={"x": 0, "y": 0},
    )

    # 2. EvaluateJsonPath ───────────────────────────────────────────────────
    print("Processor: EvaluateJsonPath")
    eval_json = create_processor(
        token, pg_id,
        name="EvaluateJsonPath",
        type_="org.apache.nifi.processors.standard.EvaluateJsonPath",
        props={
            "Destination": "flowfile-attribute",
            "price_raw":  "$.price_raw",
            "source":     "$.source",
            "product_id": "$.product_id",
        },
        pos={"x": 0, "y": 200},
    )

    # 3. RouteOnAttribute ───────────────────────────────────────────────────
    print("Processor: RouteOnAttribute")
    route = create_processor(
        token, pg_id,
        name="RouteOnAttribute",
        type_="org.apache.nifi.processors.standard.RouteOnAttribute",
        props={
            "Routing Strategy": "Route to Property name",
            # Valide si price_raw non vide ET source connue
            "to-kafka": "${price_raw:isEmpty():not():and(${source:in('amazon','fnac','cdiscount')})}",
        },
        pos={"x": 0, "y": 400},
    )

    # 4. PublishKafka — raw-prices ──────────────────────────────────────────
    print("Processor: PublishKafka [raw-prices]")
    pub_raw = create_processor(
        token, pg_id,
        name="PublishKafka [raw-prices]",
        type_="org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        props={
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "topic":             "raw-prices",
            "kafka-key":         "${source}",
            "acks":              "all",
        },
        pos={"x": 300, "y": 600},
    )

    # 5. PublishKafka — dead-letter ─────────────────────────────────────────
    print("Processor: PublishKafka [dead-letter]")
    pub_dead = create_processor(
        token, pg_id,
        name="PublishKafka [dead-letter]",
        type_="org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6",
        props={
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "topic":             "dead-letter",
            "kafka-key":         "${source}",
            "acks":              "all",
        },
        pos={"x": -300, "y": 600},
    )

    # Auto-terminate feuilles mortes
    auto_terminate(token, pub_raw,  ["success", "failure"])
    auto_terminate(token, pub_dead, ["success", "failure"])

    # Connexions ────────────────────────────────────────────────────────────
    print("Connexions...")
    create_connection(token, pg_id, listen["id"],    eval_json["id"], ["success"])
    create_connection(token, pg_id, eval_json["id"], route["id"],     ["matched"])
    create_connection(token, pg_id, eval_json["id"], pub_dead["id"],  ["unmatched", "failure"])
    create_connection(token, pg_id, route["id"],     pub_raw["id"],   ["to-kafka"])
    create_connection(token, pg_id, route["id"],     pub_dead["id"],  ["unmatched"])

    # Démarrage ─────────────────────────────────────────────────────────────
    print("Démarrage des processors...")
    for proc in [listen, eval_json, route, pub_raw, pub_dead]:
        start_processor(token, proc)
        time.sleep(0.3)

    print("\nFlow NiFi PricePulse déployé avec succès")
    print(f"  UI NiFi    : https://localhost:8443/nifi")
    print(f"  ListenHTTP : http://localhost:{LISTEN_PORT}/contentListener")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as exc:
        print(f"Erreur HTTP {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
