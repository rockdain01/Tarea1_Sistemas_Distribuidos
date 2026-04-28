"""
Almacenamiento de Métricas
Registra todos los eventos del sistema: hits, misses,
latencias, throughput y evictions para análisis posterior.
Incluye cálculo de Cache Efficiency según rúbrica y consulta directa a Redis.
"""
import redis
import os
import time
import json
import logging
from collections import deque
from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [METRICS] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

app = FastAPI(title="Metrics Service", version="1.2.0")

# ─────────────────────────────────────────
# Conexión a Redis para obtener evicciones reales
# ─────────────────────────────────────────
r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# ─────────────────────────────────────────
# Almacén en memoria de eventos
# ─────────────────────────────────────────
events: deque = deque(maxlen=100_000)  # máximo 100k eventos en memoria

# Contadores globales
stats = {
    "hits": 0,
    "misses": 0,
    "total_requests": 0,
    "total_latency_ms": 0.0,
    "hit_times_sum": 0.0,    # para t_cache
    "miss_times_sum": 0.0,   # para t_db
    "latencies": [],       # para calcular p50/p95
    "evictions": 0,
    "start_time": time.time()
}

# ─────────────────────────────────────────
# Modelo de evento
# ─────────────────────────────────────────
class MetricEvent(BaseModel):
    event: str             # "hit" | "miss" | "eviction"
    cache_key: str = ""
    query_type: str = ""
    zone_id: str = ""
    latency_ms: float = 0.0
    responder_time_ms: float = 0.0
    ttl: int = 0

# ─────────────────────────────────────────
# Registrar evento
# ─────────────────────────────────────────
@app.post("/record")
def record_event(evt: MetricEvent):
    timestamp = datetime.utcnow().isoformat()
    event_dict = evt.dict()
    event_dict["timestamp"] = timestamp
    events.append(event_dict)

    stats["total_requests"] += 1
    stats["total_latency_ms"] += evt.latency_ms
    stats["latencies"].append(evt.latency_ms)

    if evt.event == "hit":
        stats["hits"] += 1
        stats["hit_times_sum"] += evt.latency_ms
    elif evt.event == "miss":
        stats["misses"] += 1
        stats["miss_times_sum"] += evt.latency_ms
    elif evt.event == "eviction":
        stats["evictions"] += 1

    return {"recorded": True}

# ─────────────────────────────────────────
# Resumen de las metricas 
# ─────────────────────────────────────────
@app.get("/summary")
def get_summary():
    total = stats["hits"] + stats["misses"]
    hit_rate  = round(stats["hits"]   / total, 4) if total > 0 else 0.0
    miss_rate = round(stats["misses"] / total, 4) if total > 0 else 0.0

    elapsed_s = time.time() - stats["start_time"]
    throughput = round(total / elapsed_s, 2) if elapsed_s > 0 else 0.0

    lats = sorted(stats["latencies"])
    p50 = lats[int(len(lats) * 0.50)] if lats else 0.0
    p95 = lats[int(len(lats) * 0.95)] if lats else 0.0

    # --- CONSULTA DIRECTA A REDIS PARA EVICTIONS ---
    try:
        redis_info = r.info("stats")
        total_evictions = redis_info.get("evicted_keys", 0)
    except Exception as e:
        log.error(f"Error consultando Redis: {e}")
        total_evictions = 0
    
    eviction_rate = round(total_evictions / (elapsed_s / 60), 4) if elapsed_s > 0 else 0.0
    # -----------------------------------------------

    # ─────────────────────────────────────────
    # Cálculo de Cache Efficiency (Rúbrica)
    # Fórmula: (hits * t_cache - misses * t_db) / total
    # ─────────────────────────────────────────
    t_cache = stats["hit_times_sum"] / stats["hits"] if stats["hits"] > 0 else 0.0
    t_db = stats["miss_times_sum"] / stats["misses"] if stats["misses"] > 0 else 0.0
    
    cache_efficiency = (
        (stats["hits"] * t_cache) - (stats["misses"] * t_db)
    ) / total if total > 0 else 0.0

    return {
        "hits": stats["hits"],
        "misses": stats["misses"],
        "total_requests": total,
        "hit_rate": hit_rate,
        "miss_rate": miss_rate,
        "throughput_rps": throughput,
        "latency_p50_ms": round(p50, 3),
        "latency_p95_ms": round(p95, 3),
        "eviction_rate_per_min": eviction_rate,
        "total_evictions_real": total_evictions, # Dato extra para verificar
        "cache_efficiency": round(cache_efficiency, 4),
        "elapsed_seconds": round(elapsed_s, 1)
    }

# ─────────────────────────────────────────
# Exportar todos los eventos (para graficar)
# ─────────────────────────────────────────
@app.get("/events")
def get_events(limit: int = 1000):
    evts = list(events)
    return {"total": len(evts), "events": evts[-limit:]}

# ─────────────────────────────────────────
# Reset de métricas (para experimentos)
# ─────────────────────────────────────────
@app.post("/reset")
def reset_metrics():
    events.clear()
    stats["hits"] = 0
    stats["misses"] = 0
    stats["total_requests"] = 0
    stats["total_latency_ms"] = 0.0
    stats["hit_times_sum"] = 0.0
    stats["miss_times_sum"] = 0.0
    stats["latencies"] = []
    stats["evictions"] = 0
    stats["start_time"] = time.time()
    
    # Intentar resetear estadísticas de Redis también
    try:
        r.config_resetstat()
    except:
        pass

    log.info("Métricas reseteadas.")
    return {"status": "reset"}

@app.get("/health")
def health():
    return {"status": "ok", "events_recorded": len(events)}