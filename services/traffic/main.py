import os
import time
import random
import logging
import httpx
import numpy as np
import uuid
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

# configuracion desde las variables de Entorno

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRAFFIC] %(message)s")
log = logging.getLogger(__name__)

CACHE_URL       = os.getenv("CACHE_URL", "http://cache:8002")
METRICS_URL     = os.getenv("METRICS_URL", "http://metrics:8003")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DISTRIBUTION    = os.getenv("DISTRIBUTION", "zipf").lower()
TOTAL_REQUESTS  = int(os.getenv("TOTAL_REQUESTS", 1000))
REQUEST_RATE    = int(os.getenv("REQUEST_RATE", 10))
SPIKE_ENABLED   = os.getenv("SPIKE_ENABLED", "false").lower() == "true"
SPIKE_AT        = int(os.getenv("SPIKE_AT", 500))
SPIKE_RATE      = int(os.getenv("SPIKE_RATE", 100))
SPIKE_DURATION  = int(os.getenv("SPIKE_DURATION", 10))
MODE            = os.getenv("MODE", "async").lower()

TOPIC_QUERIES = "queries"

ZONES   = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["Q1", "Q2", "Q3", "Q5"]


def generate_zipf_index(n, alpha=1.2):
    p = 1.0 / np.power(np.arange(1, n + 1), alpha)
    p /= p.sum()
    return np.random.choice(np.arange(n), p=p)


def build_query_message(distribution: str) -> dict:
    zone = (
        random.choice(ZONES)
        if distribution == "uniform"
        else ZONES[generate_zipf_index(len(ZONES))]
    )
    q_type = random.choice(QUERIES)

    msg = {
        "query_id":       str(uuid.uuid4()),
        "query_type":     q_type,
        "zone_id":        zone,
        "confidence_min": round(random.uniform(0.1, 0.8), 2),
        "retry_count":    0,
        "created_at":     time.time(),
    }
    if q_type == "Q5":
        msg["bins"] = random.randint(5, 15)
    return msg


def make_producer() -> KafkaProducer:
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5,
            )
            log.info(f"KafkaProducer conectado a {KAFKA_BOOTSTRAP}")
            return p
        except KafkaError as e:
            log.warning(f"Kafka no disponible aún ({e}). Reintentando en 3s...")
            time.sleep(3)


def send_sync(msg: dict, i: int):
    """Envía la consulta directamente al cache (modo síncrono, sin Kafka)."""
    t_start = time.perf_counter()
    try:
        resp = httpx.post(
            f"{CACHE_URL}/query",
            json={
                "query_type":     msg["query_type"],
                "zone_id":        msg.get("zone_id"),
                "confidence_min": msg.get("confidence_min", 0.0),
                "bins":           msg.get("bins", 5),
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        latency_ms = round((time.perf_counter() - t_start) * 1000, 2)
        data   = resp.json()
        source = data.get("source", "unknown")

        try:
            httpx.post(
                f"{METRICS_URL}/record",
                json={
                    "event":                 "hit" if source == "cache" else "miss",
                    "query_id":              msg["query_id"],
                    "query_type":            msg["query_type"],
                    "zone_id":               msg.get("zone_id"),
                    "latency_ms":            latency_ms,
                    "retry_count":           0,
                    "recovered":             False,
                    "end_to_end_latency_ms": latency_ms,
                },
                timeout=2.0,
            )
        except Exception:
            pass

        log.info(
            f"[{i+1}/{TOTAL_REQUESTS}] SYNC {msg['query_type']} "
            f"zona={msg['zone_id']} fuente={source} {latency_ms}ms"
        )

    except Exception as e:
        # Todo lo que sigue está DENTRO del except, con indentación correcta
        latency_ms = round((time.perf_counter() - t_start) * 1000, 2)
        log.error(f"[{i+1}/{TOTAL_REQUESTS}] Error en llamada síncrona: {e}")
        try:
            httpx.post(
                f"{METRICS_URL}/record",
                json={
                    "event":      "lost",
                    "query_id":   msg["query_id"],
                    "query_type": msg["query_type"],
                    "zone_id":    msg.get("zone_id"),
                    "latency_ms": latency_ms,
                },
                timeout=2.0,
            )
        except Exception:
            pass


def run_simulation():
    log.info(
        f"Modo: {MODE.upper()} | {TOTAL_REQUESTS} requests "
        f"({DISTRIBUTION}) a {REQUEST_RATE} req/s"
    )
    if SPIKE_ENABLED:
        log.info(
            f"Spike activado: en request #{SPIKE_AT}, "
            f"{SPIKE_RATE} req/s durante {SPIKE_DURATION}s"
        )

    if MODE == "async":
        time.sleep(50)
        producer = make_producer()
    else:
        log.info("Modo síncrono — esperando 10s a que cache y responder estén listos...")
        time.sleep(10)
        producer = None

    delay    = 1.0 / REQUEST_RATE
    in_spike = False

    for i in range(TOTAL_REQUESTS):

        if MODE == "async" and SPIKE_ENABLED and i == SPIKE_AT and not in_spike:
            log.info(f"=== SPIKE iniciado en request {i} ===")
            in_spike    = True
            spike_start = time.time()
            spike_delay = 1.0 / SPIKE_RATE

            while time.time() - spike_start < SPIKE_DURATION:
                msg = build_query_message(DISTRIBUTION)
                producer.send(TOPIC_QUERIES, msg)
                time.sleep(spike_delay)

            producer.flush()
            in_spike = False
            log.info("=== SPIKE finalizado ===")

        msg = build_query_message(DISTRIBUTION)

        if MODE == "sync":
            send_sync(msg, i)
        else:
            try:
                future = producer.send(TOPIC_QUERIES, msg)
                future.get(timeout=5)
                log.info(
                    f"[{i+1}/{TOTAL_REQUESTS}] Publicado: "
                    f"{msg['query_type']} zona={msg['zone_id']} id={msg['query_id'][:8]}"
                )
            except Exception as e:
                log.error(f"Error publicando mensaje {i+1}: {e}")

        time.sleep(delay)

    if MODE == "async" and producer:
        producer.flush()

    if MODE == "async":
        log.info("Simulación completada — todos los mensajes publicados en Kafka.")
    else:
        log.info("Simulación completada.")


if __name__ == "__main__":
    run_simulation()