import os
import time
import random
import logging
import httpx
import numpy as np

# ─────────────────────────────────────────
# Configuración desde Variables de Entorno
# ─────────────────────────────────────────
CACHE_URL = os.getenv("CACHE_URL", "http://cache:8002")
DISTRIBUTION = os.getenv("DISTRIBUTION", "zipf").lower()
TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", 1000))
REQUEST_RATE = int(os.getenv("REQUEST_RATE", 10)) # Solicitudes por segundo

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRAFFIC] %(message)s")
log = logging.getLogger(__name__)

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["Q1", "Q2", "Q3", "Q5"]

def generate_zipf_index(n, alpha=1.2):
    """Distribución de Zipf: los primeros elementos son mucho más probables."""
    p = 1.0 / np.power(np.arange(1, n + 1), alpha)
    p /= p.sum()
    return np.random.choice(np.arange(n), p=p)

def run_simulation():
    delay = 1.0 / REQUEST_RATE
    log.info(f"Iniciando simulación: {TOTAL_REQUESTS} requests ({DISTRIBUTION}) a {REQUEST_RATE} req/s")
    
    # Esperar a que el sistema esté listo
    time.sleep(5)

    for i in range(TOTAL_REQUESTS):
        # 1. Selección de Zona según distribución
        if DISTRIBUTION == "uniform":
            zone = random.choice(ZONES)
        else:
            zone = ZONES[generate_zipf_index(len(ZONES))]

        # 2. Selección de Query y Payload
        q_type = random.choice(QUERIES)
        payload = {
            "query_type": q_type,
            "zone_id": zone,
            "confidence_min": round(random.uniform(0.1, 0.8), 2)
        }
        if q_type == "Q5":
            payload["bins"] = random.randint(5, 15)

        # 3. Envío al Cache
        try:
            with httpx.Client() as client:
                resp = client.post(f"{CACHE_URL}/query", json=payload, timeout=5.0)
                data = resp.json()
                source = data.get("source", "???")
                log.info(f"[{i+1}/{TOTAL_REQUESTS}] {q_type} en {zone} -> {source} ({resp.status_code})")
        except Exception as e:
            log.error(f"Error en request {i+1}: {e}")

        time.sleep(delay)

    log.info("Simulación completada con éxito.")

if __name__ == "__main__":
    run_simulation()