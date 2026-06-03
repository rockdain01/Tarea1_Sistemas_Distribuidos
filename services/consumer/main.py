import os
import json
import time
import uuid
import logging
import httpx

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [CONSUMER] %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSRTAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
CACHE_URL = os.getenv("CACHE_URL", "http://cache:8002")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics:8003")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "consumer-group")

TOPIC_QUERIES = "queries"
TOPIC_RETRY = "retry"
TOPIC_DLQ = "dlq"

def make_producer() -> KafkaProducer: 
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSRTAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5
            )
            log.info("Kafka Producer conectado")
            return p
        except KafkaError as e:
            log.warning(f"Error al conectar Kafka Producer: {e}. Reintentando en 5 segundos...")
            time.sleep(5)

def make_consumer(topics: list[str]) -> KafkaConsumer:
    while True:
        try:
            c = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BOOTSRTAP,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                session_timeout_ms=30_000,
                heartbeat_interval_ms=10_000,
                
            )
            log.info(f"Kafka Consumer conectado a: {topics}")
            return c
        except KafkaError as e:
            log.warning(f"Error al conectar Kafka Consumer: {e}. Reintentando en 5 segundos...")
            time.sleep(5) 

def send_metrics(payload: dict):
    try:
        httpx.post(f"{METRICS_URL}/record", json=payload, timeout=2.0)
    except Exception:
        pass

def process_message(msg: dict, producer: KafkaProducer) ->bool:
    query_id = msg.get("query_id", str(uuid.uuid4()))
    retry_count = msg.get("retry_count", 0)
    create_time = msg.get("created_at", time.time())

    cache_payload = {
        "query_type": msg.get("query_type"),
        "zone_id": msg.get("zone_id"),
        "zone_a": msg.get("zone_a"),
        "zone_b": msg.get("zone_b"),
        "confidence_min": msg.get("confidence_min", 0.0),
        "bins": msg.get("bins", 5),
    }

    t_start = time.perf_counter()

    try:
        resp = httpx.post(f"{CACHE_URL}/query", json=cache_payload, timeout=30.0)
        resp.raise_for_status()
        latency_ms = round((time.perf_counter() - t_start) * 1000, 2)
        data = resp.json()
        source = data.get("source", "unknown")

        log.info(
            f"OK. query_id={query_id} tipo={msg['query_type']}"
            f" fuente={source} latencia={latency_ms}ms retries={retry_count}"
        )

        send_metrics({
            "event": "hit" if source == "cache" else "miss",
            "query_id": query_id,
            "query_type": msg["query_type"],
            "zone_id": msg.get("zone_id") or msg.get("zone_a"),
            "latency_ms": latency_ms,
            "retry_count": retry_count,
            "recovered": retry_count > 0,#esto sera true en caso de que venga del retry
            "end_to_end_latency_ms": round((time.time() - create_time) * 1000, 2),
        })
        return True
    except Exception as e:
        latency_ms = round((time.perf_counter() - t_start) * 1000, 2)
        log.warning(
            f"FAIL query_id = {query_id} retry={retry_count}/{MAX_RETRIES} error={str(e)}"
        )

        if retry_count >= MAX_RETRIES:

            dlq_msg = {**msg, "failure_reason": str(e), "failed_at": time.time()}
            producer.send(TOPIC_DLQ, dlq_msg)
            producer.flush()
            log.error(f"DLQ  query_id={query_id} - agoto {MAX_RETRIES} reintentos. Enviado a DLQ.")

            send_metrics({
                "event": "dlq",
                "query_id": query_id,
                "query_type": msg.get("query_type",""),
                "zone_id": msg.get("zone_id", ""),
                "latency_ms": latency_ms,
                "retry_count": retry_count,
            })
        else:
            time.sleep(2) 
            retry_msg = {**msg, "retry_count": retry_count + 1}
            producer.send(TOPIC_RETRY, retry_msg)
            producer.flush()
            log.info(f"Reintentando query_id={query_id} - Enviado a topic de retry.")
                
            send_metrics({
                "event": "retry",
                "query_id": query_id,
                "query_type": msg.get("query_type",""),
                "zone_id": msg.get("zone_id", ""),
                "latency_ms": latency_ms,
                "retry_count": retry_count + 1,
            })
        return False
    
def main():
    producer = make_producer()
    consumer = make_consumer([TOPIC_QUERIES, TOPIC_RETRY])

    log.info("Consumer iniciado. Esperando mensajes...")
    for message in consumer:
        msg = message.value
        process_message(msg, producer)

if __name__ == "__main__":
    main()