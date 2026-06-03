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

CACHE_URL = os.getenv("CACHE_URL", "http://cache:8002")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
DISTRIBUTION    = os.getenv("DISTRIBUTION", "zipf").lower()
TOTAL_REQUESTS  = int(os.getenv("TOTAL_REQUESTS", 1000))
REQUEST_RATE    = int(os.getenv("REQUEST_RATE", 10))   # req/s
SPIKE_ENABLED   = os.getenv("SPIKE_ENABLED", "false").lower() == "true"
SPIKE_AT        = int(os.getenv("SPIKE_AT", 500))      # despues de cuantos requests
SPIKE_RATE      = int(os.getenv("SPIKE_RATE", 100))    # req/s durante el spike
SPIKE_DURATION  = int(os.getenv("SPIKE_DURATION", 10)) # segundos del spike

TOPIC_QUERIES = "queries"

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["Q1", "Q2", "Q3", "Q5"]

def generate_zipf_index(n, alpha=1.2):
    """Distribución de Zipf: los primeros elementos son mucho más probables."""
    p = 1.0 / np.power(np.arange(1, n + 1), alpha)
    p /= p.sum()
    return np.random.choice(np.arange(n), p=p)


#como se construye el mensaje ahora con id unico
def build_query_message(distribution: str) -> dict:
    zone = (
        random.choice(ZONES)
        if distribution == "uniform"
        else ZONES[generate_zipf_index(len(ZONES))]
    )
    q_type = random.choice(QUERIES)

    msg = {
        "query_id":      str(uuid.uuid4()),   # ← NUEVO: ID único por consulta
        "query_type":    q_type,
        "zone_id":       zone,
        "confidence_min": round(random.uniform(0.1, 0.8), 2),
        "retry_count":   0,                   # ← NUEVO: contador de reintentos
        "created_at":    time.time(),          # ← NUEVO: timestamp de creación
    }
    if q_type == "Q5":
        msg["bins"] = random.randint(5, 15)
    return msg

#aca se inicializza el producer con reintentos
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


def run_simulation():
    log.info(
        f"Iniciando simulación: {TOTAL_REQUESTS} requests "
        f"({DISTRIBUTION}) a {REQUEST_RATE} req/s"
    )
    if SPIKE_ENABLED:
        log.info(
            f"Spike activado: en request #{SPIKE_AT}, "
            f"{SPIKE_RATE} req/s durante {SPIKE_DURATION}s"
        )

    # Esperar a que Kafka esté disponible
    time.sleep(15)
    producer = make_producer()

    delay = 1.0 / REQUEST_RATE
    in_spike = False

    for i in range(TOTAL_REQUESTS):

        #Spike de tráfico 
        if SPIKE_ENABLED and i == SPIKE_AT and not in_spike:
            log.info(f"=== SPIKE iniciado en request {i} ===")
            in_spike = True
            spike_start = time.time()
            spike_delay = 1.0 / SPIKE_RATE

            while time.time() - spike_start < SPIKE_DURATION:
                msg = build_query_message(DISTRIBUTION)
                producer.send(TOPIC_QUERIES, msg)
                time.sleep(spike_delay)

            producer.flush()
            in_spike = False
            log.info("=== SPIKE finalizado ===")

        #Consulta normal
        msg = build_query_message(DISTRIBUTION)

        try:
            future = producer.send(TOPIC_QUERIES, msg)
            future.get(timeout=5)   # confirmar entrega
            log.info(
                f"[{i+1}/{TOTAL_REQUESTS}] Publicado: "
                f"{msg['query_type']} zona={msg['zone_id']} id={msg['query_id'][:8]}"
            )
        except Exception as e:
            log.error(f"Error publicando mensaje {i+1}: {e}")

        time.sleep(delay)

    producer.flush()
    log.info("Simulación completada — todos los mensajes publicados en Kafka.")

if __name__ == "__main__":
    run_simulation()