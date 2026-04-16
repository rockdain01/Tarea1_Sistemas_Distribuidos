# Tarea 1: Sistema Geoespacial Distribuido con Caché 🏢📡

Este proyecto implementa una arquitectura distribuida para consultar datos de edificaciones en 5 comunas de Santiago. Utiliza una estrategia de caché con **Redis** para optimizar los tiempos de respuesta y un sistema de **Métricas** para monitorear el rendimiento.

## 🚀 Requisitos Previos
* Docker y Docker Compose instalados.
* En Mac M2: Docker Desktop (asegurar que el motor esté corriendo).

## 🛠️ Arquitectura
* **Responder (Puerto 8001):** Motor de cálculo en Python (FastAPI) que procesa los datos en memoria.
* **Cache (Puerto 8002):** Intermediario que gestiona Redis y redirige consultas al Responder si no hay un HIT.
* **Metrics (Puerto 8003):** Almacena y calcula el rendimiento (Hit Rate, Latencia P95).
* **Redis:** Base de datos NoSQL para el almacenamiento temporal (TTL: 60s).

## ⚡ Instalación y Ejecución

1.  **Levantar los servicios:**
    Desde la raíz de la carpeta `Tarea1`, ejecuta:
    ```bash
    docker compose up --build
    ```

2.  **Verificar que los datos cargaron:**
    Revisa los logs del contenedor `responder`. Deberías ver:
    `Dataset listo. Total: 163,407 edificaciones en memoria.`

## 🔍 Pruebas de Consultas (Ejemplos)

Puedes realizar pruebas utilizando `curl` desde otra terminal:

### Q1: Conteo de edificios (Providencia)
```bash
curl -X POST http://localhost:8002/query \
  -H "Content-Type: application/json" \
  -d '{"query_type": "Q1", "zone_id": "Z1", "confidence_min": 0.5}'