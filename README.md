# Tarea 2: Procesamiento y Fallback con Apache Kafka

Este proyecto extiende la plataforma de la Tarea 1 incorporando **Apache Kafka** para desacoplar servicios y agregar tolerancia a fallos, evaluando métricas como throughput, latencia, retry rate y recovery rate bajo distintos escenarios.

---

## Microservicios

* **Traffic**: Genera consultas Q1–Q5 en modo `async` (Kafka) o `sync` (HTTP directo).
* **Consumer**: Lee desde Kafka, consulta caché y maneja reintentos y DLQ.
* **Cache**: Gestiona hit/miss con Redis.
* **Responder**: Procesa consultas geoespaciales. Soporta fallos simulados con `FAILURE_RATE`.
* **Metrics**: Registra todas las métricas del sistema.
* **Redis**: Motor de caché en memoria.
* **Kafka + Zookeeper**: Cola de mensajes con tópicos `queries`, `retry` y `dlq`.

---

## Instrucciones de Despliegue

1. Abrir Docker
2. Abrir la terminal desde la raíz del proyecto
3. **Antes de cada prueba, limpiar el entorno:**
    ```bash
    docker compose down -v
    ```
4. Levantar el sistema:
    ```bash
    docker compose up --build --scale consumer=1
    ```
5. Para escalar consumers:
    ```bash
    docker compose up --build --scale consumer=3
    ```

> **Importante:** Siempre ejecutar `docker compose down -v` antes de cambiar de escenario para evitar conflictos con volúmenes y offsets de Kafka.

---

## Ejecución de Escenarios

```bash
bash run_escenarios.sh base        # Sistema síncrono sin Kafka
bash run_escenarios.sh base_falla  # Falla del responder en modo síncrono
bash run_escenarios.sh kafka1      # Kafka con 1 consumer
bash run_escenarios.sh kafka3      # Kafka con 3 consumers
bash run_escenarios.sh falla       # Falla temporal del responder con Kafka
bash run_escenarios.sh spike       # Spike de tráfico
```
la ejecucion completa de los escenarios puede tomar su tiempo por como esta programado, paciencia, siempre al terminar un escenario se entregan las metricas finales, de no aguantar, en otra consola se puede hacer curl http://localhost:8003/summary para ver como van las metricas.
---

## Consulta de Métricas

```bash
curl http://localhost:8003/summary   # Resumen general
curl http://localhost:8003/events    # Eventos registrados
curl -X POST http://localhost:8003/reset  # Resetear métricas
```

---

## Variables de Entorno Relevantes

| Variable | Servicio | Default |
|---|---|---|
| `MODE` | traffic | `async` |
| `DISTRIBUTION` | traffic | `zipf` |
| `TOTAL_REQUESTS` | traffic | `1000` |
| `REQUEST_RATE` | traffic | `10` |
| `FAILURE_RATE` | responder | `0.0` |
| `MAX_RETRIES` | consumer | `3` |
| `CACHE_TTL` | cache | `3600` |


