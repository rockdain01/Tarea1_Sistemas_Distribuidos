"""
aca esta el almacenamienti de la metricas, para registrar los hit miss, latencia etc
tambien estan los calculos de que pedian en la rubrica
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
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [METRICS] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

app = FastAPI(title="Metrics Service", version="1.2.0")


r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)


# almacen en memoria de eventos

events: deque = deque(maxlen=100_000)  # maximo 100k 

# contadores globales
stats = {
    "hits": 0,
    "misses": 0,
    "total_requests": 0,
    "total_latency_ms": 0.0,
    "hit_times_sum": 0.0,    
    "miss_times_sum": 0.0,   
    "latencies": [],       
    "evictions": 0,
    "start_time": time.time(),
    #aca parte lo de la tarea 2 
    "retries": 0,   
    "dlq_count": 0,   
    "recovered": 0,     
    "end_to_end_ms":     [],    
    "backlog_snapshots": [],
}


# Modelo de evento

class MetricEvent(BaseModel):
    event: str 
    query_id: Optional[str] = "" #nuevo            
    cache_key: Optional[str] = ""
    query_type: Optional[str] = ""
    zone_id: Optional[str] = ""
    latency_ms: float = 0.0
    responder_time_ms: float = 0.0
    ttl: int = 0
    #tarea 2
    retry_count: int = 0
    recovered: bool = False
    end_to_end_ms: float = 0.0

#clase nueva
class BacklogReport(BaseModel):
    topic:   str
    backlog: int           
    timestamp: float = 0.0

# registrar evento

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

    elif evt.event == "retry":
        #nuevo
        stats["retries"] += 1

    elif evt.event == "dlq":
        #nuevo
        stats["dlq_count"] += 1    

    elif evt.event == "eviction":
        stats["evictions"] += 1
    #nuevos
    if evt.recovered:
        stats["recovered"] += 1

    if evt.end_to_end_ms > 0:
        stats["end_to_end_ms"].append(evt.end_to_end_ms)

    return {"recorded": True}    

@app.post("/backlog")
def record_backlog(report: BacklogReport):
    """
    Recibe snapshots del backlog de Kafka.
    El consumer puede llamar a este endpoint periódicamente.
    """
    snapshot = {
        "topic":     report.topic,
        "backlog":   report.backlog,
        "timestamp": report.timestamp or time.time(),
    }
    stats["backlog_snapshots"].append(snapshot)
    # aca solo los últimos 1000 snapshots se mantiene
    if len(stats["backlog_snapshots"]) > 1000:
        stats["backlog_snapshots"] = stats["backlog_snapshots"][-1000:]

    return {"recorded": True}


# resumen de las metricas 

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

    # Latencia end-to-end (desde que traffic publica hasta que se resuelve)
    e2e = sorted(stats["end_to_end_ms"])
    e2e_p50 = e2e[int(len(e2e) * 0.50)] if e2e else 0.0
    e2e_p95 = e2e[int(len(e2e) * 0.95)] if e2e else 0.0

    # CONSULTA DIRECTA A REDIS PARA EVICTIONS 
    try:
        redis_info = r.info("stats")
        total_evictions = redis_info.get("evicted_keys", 0)
    except Exception as e:
        log.error(f"Error consultando Redis: {e}")
        total_evictions = 0
    
    eviction_rate = round(total_evictions / (elapsed_s / 60), 4) if elapsed_s > 0 else 0.0
    
    
    t_cache = stats["hit_times_sum"] / stats["hits"] if stats["hits"] > 0 else 0.0
    t_db = stats["miss_times_sum"] / stats["misses"] if stats["misses"] > 0 else 0.0
    
    cache_efficiency = (
        (stats["hits"] * t_cache) - (stats["misses"] * t_db)
    ) / total if total > 0 else 0.0

    # metricas nuevas de l tarea 2
    all_processed = total + stats["retries"] + stats["dlq_count"]
    retry_rate    = round(stats["retries"]   / all_processed, 4) if all_processed > 0 else 0.0
    dlq_rate      = round(stats["dlq_count"] / all_processed, 4) if all_processed > 0 else 0.0
    recovery_rate = round(stats["recovered"] / stats["retries"], 4) if stats["retries"] > 0 else 0.0

    #último snapshot 
    backlog_by_topic: dict = {}
    for snap in reversed(stats["backlog_snapshots"]):
        if snap["topic"] not in backlog_by_topic:
            backlog_by_topic[snap["topic"]] = snap["backlog"]
        if len(backlog_by_topic) == 3:   
            break

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
        "total_evictions_real": total_evictions, 
        "cache_efficiency": round(cache_efficiency, 4),
        "elapsed_seconds": round(elapsed_s, 1),
        #las nuevas metricas de la tarea 2
        "retries":               stats["retries"],
        "dlq_count":             stats["dlq_count"],
        "recovered":             stats["recovered"],
        "retry_rate":            retry_rate,
        "dlq_rate":              dlq_rate,
        "recovery_rate":         recovery_rate,
        "end_to_end_p50_ms":     round(e2e_p50, 3),
        "end_to_end_p95_ms":     round(e2e_p95, 3),
        "backlog_by_topic":      backlog_by_topic,
    }


@app.get("/events")
def get_events(limit: int = 1000):
    evts = list(events)
    return {"total": len(evts), "events": evts[-limit:]}

@app.get("/backlog/history")
def get_backlog_history(topic: str = "queries", limit: int = 100):
    snaps = [s for s in stats["backlog_snapshots"] if s["topic"] == topic]
    return {"topic": topic, "history": snaps[-limit:]}


@app.post("/reset")
def reset_metrics():
    events.clear()
    stats.update({
        "hits": 0, "misses": 0, "total_requests": 0,
        "hit_times_sum": 0.0, "miss_times_sum": 0.0,
        "latencies": [], "evictions": 0, "start_time": time.time(),
        "retries": 0, "dlq_count": 0, "recovered": 0,
        "end_to_end_ms": [], "backlog_snapshots": [],
    })
    try:
        r.config_resetstat()
    except Exception:
        pass
    log.info("Métricas reseteadas.")
    return {"status": "reset"}

@app.get("/health")
def health():
    return {"status": "ok", "events_recorded": len(events)}