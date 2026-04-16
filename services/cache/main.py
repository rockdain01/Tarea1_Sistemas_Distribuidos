"""
Sistema Caché — Servicio intermediario
Intercepta consultas, gestiona hits/misses con Redis,
delega al Responder en caso de miss y registra métricas.
"""

import os
import time
import json
import logging
import hashlib

import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

# ─────────────────────────────────────────
# Configuración
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CACHE] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", 6379))
RESPONDER_URL  = os.getenv("RESPONDER_URL", "http://responder:8001")
METRICS_URL    = os.getenv("METRICS_URL", "http://metrics:8003")
CACHE_TTL      = int(os.getenv("CACHE_TTL", 60))
PORT           = int(os.getenv("PORT", 8002))

# ─────────────────────────────────────────
# App FastAPI
# ─────────────────────────────────────────
app = FastAPI(title="Cache Service", version="1.0.0")
redis_client: aioredis.Redis = None

@app.on_event("startup")
async def startup():
    global redis_client
    redis_client = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )
    log.info(f"Conectado a Redis en {REDIS_HOST}:{REDIS_PORT}")
    log.info(f"TTL configurado: {CACHE_TTL}s | Responder: {RESPONDER_URL}")

@app.on_event("shutdown")
async def shutdown():
    await redis_client.aclose()

# ─────────────────────────────────────────
# Modelos de request (idénticos al Responder)
# ─────────────────────────────────────────
class QueryRequest(BaseModel):
    query_type: str        # Q1, Q2, Q3, Q4, Q5
    zone_id: str = None
    zone_a: str = None
    zone_b: str = None
    confidence_min: float = 0.0
    bins: int = 5

# ─────────────────────────────────────────
# Generación de cache key
# Formato definido en la tarea sección 5
# ─────────────────────────────────────────
def build_cache_key(req: QueryRequest) -> str:
    qt = req.query_type.upper()
    if qt == "Q1":
        return f"count:{req.zone_id}:conf={req.confidence_min}"
    elif qt == "Q2":
        return f"area:{req.zone_id}:conf={req.confidence_min}"
    elif qt == "Q3":
        return f"density:{req.zone_id}:conf={req.confidence_min}"
    elif qt == "Q4":
        return f"compare:density:{req.zone_a}:{req.zone_b}:conf={req.confidence_min}"
    elif qt == "Q5":
        return f"confidence_dist:{req.zone_id}:bins={req.bins}"
    else:
        raise HTTPException(status_code=400, detail=f"Tipo de consulta inválido: {qt}")

# ─────────────────────────────────────────
# Construcción del payload para el Responder
# ─────────────────────────────────────────
def build_responder_payload(req: QueryRequest) -> tuple[str, dict]:
    qt = req.query_type.upper()
    if qt == "Q1":
        return "/q1", {"zone_id": req.zone_id, "confidence_min": req.confidence_min}
    elif qt == "Q2":
        return "/q2", {"zone_id": req.zone_id, "confidence_min": req.confidence_min}
    elif qt == "Q3":
        return "/q3", {"zone_id": req.zone_id, "confidence_min": req.confidence_min}
    elif qt == "Q4":
        return "/q4", {"zone_a": req.zone_a, "zone_b": req.zone_b, "confidence_min": req.confidence_min}
    elif qt == "Q5":
        return "/q5", {"zone_id": req.zone_id, "bins": req.bins}
    else:
        raise HTTPException(status_code=400, detail=f"Tipo de consulta inválido: {qt}")

# ─────────────────────────────────────────
# Envío de métricas (fire-and-forget)
# ─────────────────────────────────────────
async def send_metric(client: httpx.AsyncClient, payload: dict):
    try:
        await client.post(f"{METRICS_URL}/record", json=payload, timeout=2.0)
    except Exception:
        pass  # Las métricas no deben bloquear el flujo principal

# ─────────────────────────────────────────
# Endpoint principal: procesar consulta
# ─────────────────────────────────────────
@app.post("/query")
async def process_query(req: QueryRequest):
    cache_key = build_cache_key(req)
    t_start = time.perf_counter()

    async with httpx.AsyncClient() as client:

        # ── Intentar cache hit ──────────────────
        cached = await redis_client.get(cache_key)

        if cached:
            # CACHE HIT
            t_total = round((time.perf_counter() - t_start) * 1000, 3)
            log.info(f"HIT  {cache_key} ({t_total}ms)")

            await send_metric(client, {
                "event": "hit",
                "cache_key": cache_key,
                "query_type": req.query_type,
                "zone_id": req.zone_id or req.zone_a,
                "latency_ms": t_total,
                "ttl": CACHE_TTL
            })

            return {
                "source": "cache",
                "cache_key": cache_key,
                "latency_ms": t_total,
                "data": json.loads(cached)
            }

        # ── Cache miss: delegar al Responder ────
        log.info(f"MISS {cache_key} — consultando Responder...")
        endpoint, payload = build_responder_payload(req)

        try:
            t_resp_start = time.perf_counter()
            resp = await client.post(
                f"{RESPONDER_URL}{endpoint}",
                json=payload,
                timeout=30.0
            )
            resp.raise_for_status()
            responder_time = round((time.perf_counter() - t_resp_start) * 1000, 3)
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Error en Responder: {str(e)}")

        result = resp.json()

        # ── Almacenar en Redis con TTL ──────────
        await redis_client.setex(cache_key, CACHE_TTL, json.dumps(result))

        t_total = round((time.perf_counter() - t_start) * 1000, 3)
        log.info(f"MISS {cache_key} → almacenado en caché. Total: {t_total}ms")

        await send_metric(client, {
            "event": "miss",
            "cache_key": cache_key,
            "query_type": req.query_type,
            "zone_id": req.zone_id or req.zone_a,
            "latency_ms": t_total,
            "responder_time_ms": responder_time,
            "ttl": CACHE_TTL
        })

        return {
            "source": "responder",
            "cache_key": cache_key,
            "latency_ms": t_total,
            "responder_time_ms": responder_time,
            "data": result
        }

# ─────────────────────────────────────────
# Endpoint para limpiar caché (útil para experimentos)
# ─────────────────────────────────────────
@app.delete("/cache/flush")
async def flush_cache():
    await redis_client.flushdb()
    log.info("Caché limpiada completamente.")
    return {"status": "flushed"}

# ─────────────────────────────────────────
# Estadísticas de Redis
# ─────────────────────────────────────────
@app.get("/cache/stats")
async def cache_stats():
    info = await redis_client.info()
    return {
        "used_memory_human": info.get("used_memory_human"),
        "keyspace_hits": info.get("keyspace_hits"),
        "keyspace_misses": info.get("keyspace_misses"),
        "evicted_keys": info.get("evicted_keys"),
        "connected_clients": info.get("connected_clients"),
        "maxmemory_policy": info.get("maxmemory_policy"),
        "ttl_configured": CACHE_TTL
    }

# ─────────────────────────────────────────
# Health check
# ─────────────────────────────────────────
@app.get("/health")
async def health():
    try:
        await redis_client.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {"status": "ok", "redis": redis_ok}