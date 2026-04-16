"""
Generador de Respuestas — Servicio Q1-Q5
Carga el dataset en memoria al iniciar y expone endpoints REST
para cada tipo de consulta geoespacial definida en la tarea.
"""

import os
import time
import math
import logging
from collections import defaultdict

import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ─────────────────────────────────────────
# Configuración de logging
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RESPONDER] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Zonas definidas en la tarea (sección 4.2)
# ─────────────────────────────────────────
ZONES = {
    "Z1": {"name": "Providencia",     "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"name": "Las_Condes",      "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"name": "Maipu",           "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"name": "Santiago_Centro", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"name": "Pudahuel",        "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
}

# ─────────────────────────────────────────
# Área de cada zona en km² (precalculada)
# Fórmula: Δlat * Δlon * 111.32 * 111.32 * cos(lat_media)
# ─────────────────────────────────────────
def calc_area_km2(zone: dict) -> float:
    lat_mid = math.radians((zone["lat_min"] + zone["lat_max"]) / 2)
    delta_lat = abs(zone["lat_max"] - zone["lat_min"])
    delta_lon = abs(zone["lon_max"] - zone["lon_min"])
    return delta_lat * delta_lon * (111.32 ** 2) * math.cos(lat_mid)

ZONE_AREA_KM2 = {zid: calc_area_km2(z) for zid, z in ZONES.items()}

# ─────────────────────────────────────────
# Carga del dataset en memoria
# ─────────────────────────────────────────
DATA_DIR = "/app/data"

# data_store[zone_id] = lista de dicts {latitude, longitude, area_in_meters, confidence}
data_store: dict[str, list] = {}

def load_dataset():
    """Carga todos los CSVs de zonas en memoria al iniciar el servicio."""
    log.info("Cargando dataset en memoria...")
    total = 0
    for zone_id, zone in ZONES.items():
        csv_name = f"{zone_id}_{zone['name']}.csv"
        csv_path = os.path.join(DATA_DIR, csv_name)

        if not os.path.exists(csv_path):
            log.warning(f"  Archivo no encontrado: {csv_path} — zona {zone_id} quedará vacía.")
            data_store[zone_id] = []
            continue

        df = pd.read_csv(csv_path, usecols=["latitude", "longitude", "area_in_meters", "confidence"])
        df = df.dropna()
        records = df.to_dict(orient="records")
        data_store[zone_id] = records
        total += len(records)
        log.info(f"  [{zone_id}] {zone['name']}: {len(records):,} edificaciones cargadas.")

    log.info(f"Dataset listo. Total: {total:,} edificaciones en memoria.")

# ─────────────────────────────────────────
# App FastAPI
# ─────────────────────────────────────────
app = FastAPI(title="Responder Service", version="1.0.0")

@app.on_event("startup")
def startup_event():
    load_dataset()

# ─────────────────────────────────────────
# Modelos de request
# ─────────────────────────────────────────
class Q1Request(BaseModel):
    zone_id: str
    confidence_min: float = 0.0

class Q2Request(BaseModel):
    zone_id: str
    confidence_min: float = 0.0

class Q3Request(BaseModel):
    zone_id: str
    confidence_min: float = 0.0

class Q4Request(BaseModel):
    zone_a: str
    zone_b: str
    confidence_min: float = 0.0

class Q5Request(BaseModel):
    zone_id: str
    bins: int = 5

# ─────────────────────────────────────────
# Helper: validar zona
# ─────────────────────────────────────────
def get_zone_records(zone_id: str) -> list:
    if zone_id not in data_store:
        raise HTTPException(status_code=404, detail=f"Zona '{zone_id}' no encontrada.")
    return data_store[zone_id]

# ─────────────────────────────────────────
# Q1 — Conteo de edificios en una zona
# ─────────────────────────────────────────
@app.post("/q1")
def q1_count(req: Q1Request):
    """Cuenta el número total de edificaciones con confidence >= confidence_min."""
    t0 = time.perf_counter()
    records = get_zone_records(req.zone_id)
    count = sum(1 for r in records if r["confidence"] >= req.confidence_min)
    elapsed = round((time.perf_counter() - t0) * 1000, 3)

    log.info(f"Q1 [{req.zone_id}] conf>={req.confidence_min} → {count} edif. ({elapsed}ms)")
    return {
        "query": "Q1",
        "zone_id": req.zone_id,
        "confidence_min": req.confidence_min,
        "count": count,
        "processing_time_ms": elapsed
    }

# ─────────────────────────────────────────
# Q2 — Área promedio y área total
# ─────────────────────────────────────────
@app.post("/q2")
def q2_area(req: Q2Request):
    """Calcula área promedio y total de edificaciones filtradas por confidence."""
    t0 = time.perf_counter()
    records = get_zone_records(req.zone_id)
    areas = [r["area_in_meters"] for r in records if r["confidence"] >= req.confidence_min]

    if not areas:
        raise HTTPException(status_code=404, detail="Sin edificaciones con ese filtro de confianza.")

    avg_area  = round(float(np.mean(areas)), 4)
    total_area = round(float(np.sum(areas)), 4)
    elapsed = round((time.perf_counter() - t0) * 1000, 3)

    log.info(f"Q2 [{req.zone_id}] avg={avg_area}m² total={total_area}m² ({elapsed}ms)")
    return {
        "query": "Q2",
        "zone_id": req.zone_id,
        "confidence_min": req.confidence_min,
        "avg_area_m2": avg_area,
        "total_area_m2": total_area,
        "n": len(areas),
        "processing_time_ms": elapsed
    }

# ─────────────────────────────────────────
# Q3 — Densidad de edificaciones por km²
# ─────────────────────────────────────────
@app.post("/q3")
def q3_density(req: Q3Request):
    """Calcula densidad de edificaciones por km² en la zona."""
    t0 = time.perf_counter()
    records = get_zone_records(req.zone_id)
    count = sum(1 for r in records if r["confidence"] >= req.confidence_min)
    area_km2 = ZONE_AREA_KM2[req.zone_id]
    density = round(count / area_km2, 4) if area_km2 > 0 else 0.0
    elapsed = round((time.perf_counter() - t0) * 1000, 3)

    log.info(f"Q3 [{req.zone_id}] density={density}/km² ({elapsed}ms)")
    return {
        "query": "Q3",
        "zone_id": req.zone_id,
        "confidence_min": req.confidence_min,
        "density_per_km2": density,
        "area_km2": round(area_km2, 4),
        "building_count": count,
        "processing_time_ms": elapsed
    }

# ─────────────────────────────────────────
# Q4 — Comparación de densidad entre dos zonas
# ─────────────────────────────────────────
@app.post("/q4")
def q4_compare(req: Q4Request):
    """Compara densidad de edificaciones entre dos zonas."""
    t0 = time.perf_counter()

    for zid in [req.zone_a, req.zone_b]:
        get_zone_records(zid)  # valida existencia

    def density(zone_id):
        records = data_store[zone_id]
        count = sum(1 for r in records if r["confidence"] >= req.confidence_min)
        return count / ZONE_AREA_KM2[zone_id]

    da = round(density(req.zone_a), 4)
    db = round(density(req.zone_b), 4)
    winner = req.zone_a if da >= db else req.zone_b
    elapsed = round((time.perf_counter() - t0) * 1000, 3)

    log.info(f"Q4 [{req.zone_a} vs {req.zone_b}] {da} vs {db} → winner={winner} ({elapsed}ms)")
    return {
        "query": "Q4",
        "zone_a": req.zone_a,
        "zone_b": req.zone_b,
        "confidence_min": req.confidence_min,
        "density_a": da,
        "density_b": db,
        "winner": winner,
        "processing_time_ms": elapsed
    }

# ─────────────────────────────────────────
# Q5 — Distribución de confianza en una zona
# ─────────────────────────────────────────
@app.post("/q5")
def q5_confidence_dist(req: Q5Request):
    """Calcula la distribución del score de confianza agrupada en bins."""
    t0 = time.perf_counter()
    records = get_zone_records(req.zone_id)
    scores = [r["confidence"] for r in records]

    if not scores:
        raise HTTPException(status_code=404, detail="Sin datos para esta zona.")

    counts, edges = np.histogram(scores, bins=req.bins, range=(0, 1))
    distribution = [
        {
            "bucket": i,
            "min": round(float(edges[i]), 4),
            "max": round(float(edges[i + 1]), 4),
            "count": int(counts[i])
        }
        for i in range(req.bins)
    ]
    elapsed = round((time.perf_counter() - t0) * 1000, 3)

    log.info(f"Q5 [{req.zone_id}] bins={req.bins} ({elapsed}ms)")
    return {
        "query": "Q5",
        "zone_id": req.zone_id,
        "bins": req.bins,
        "distribution": distribution,
        "processing_time_ms": elapsed
    }

# ─────────────────────────────────────────
# Health check
# ─────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "zones_loaded": {zid: len(recs) for zid, recs in data_store.items()}
    }