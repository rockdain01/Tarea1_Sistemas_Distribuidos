set -e

METRICS_URL="http://localhost:8003"
COMPOSE="docker compose"

reset_metrics() {
    echo "Reseteando métricas..."
    curl -s -X POST "$METRICS_URL/reset" > /dev/null
}

get_summary() {
    echo "  → Resultados:"
    curl -s "$METRICS_URL/summary" | python3 -m json.tool
}

get_backlog() {
    echo "  → Backlog actual (consumer lag):"
    docker exec kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --describe \
        --group consumer-group 2>/dev/null | \
        grep -E "queries|retry|dlq" | \
        awk '{print "    topic=" $2 " partition=" $3 " lag=" $6}'
}

wait_traffic() {
    echo "  → Esperando a que traffic termine..."
    while ! $COMPOSE logs traffic 2>&1 | grep -q "completada"; do
        sleep 3
    done
    echo "  → Traffic terminó."
}

wait_consumer() {
    echo "  → Esperando a que consumer vacíe el backlog..."
    while true; do
        lag=$(docker exec kafka kafka-consumer-groups \
            --bootstrap-server localhost:9092 \
            --describe \
            --group consumer-group 2>/dev/null | \
            grep -E "queries|retry" | \
            awk '{sum += $6} END {print sum+0}')
        echo "    Lag pendiente: $lag mensajes"
        if [ "$lag" -eq 0 ]; then
            echo "  → Backlog vacío. Esperando flush de métricas..."
            sleep 10
            break
        fi
        sleep 5
    done
}

SCENARIO=${1:-"menu"}

case $SCENARIO in

    base)
    echo "=== ESCENARIO 1: Sistema base (síncrono, sin Kafka) ==="
    $COMPOSE down -v 2>/dev/null || true
    $COMPOSE up --build --scale consumer=0 -d
    sleep 15
    reset_metrics

    MODE=sync $COMPOSE up traffic

    wait_traffic
    get_summary
    ;;

    base_falla)
    echo "=== ESCENARIO: Falla en sistema síncrono ==="
    $COMPOSE down -v 2>/dev/null || true
    $COMPOSE up --build --scale consumer=0 -d
    sleep 15
    reset_metrics

    echo "  → Iniciando tráfico síncrono..."
    MODE=sync $COMPOSE up -d traffic

    echo "  → En 20s se detendrá el responder..."
    sleep 20
    echo "  → Deteniendo responder..."
    docker stop responder

    echo "  → Responder caído por 30s..."
    sleep 30

    echo "  → Restaurando responder..."
    docker start responder

    wait_traffic
    sleep 5
    get_summary
    ;;

    kafka1)
    echo "=== ESCENARIO 2: Kafka + 1 Consumer ==="
    $COMPOSE down -v 2>/dev/null || true
    $COMPOSE up --build --scale consumer=1 -d
    sleep 50
    reset_metrics
    $COMPOSE up traffic
    wait_traffic
    wait_consumer
    get_summary
    ;;

    kafka3)
    echo "=== ESCENARIO 3: Kafka + 3 Consumers ==="
    $COMPOSE down -v 2>/dev/null || true
    $COMPOSE up --build --scale consumer=3 -d
    sleep 20
    reset_metrics
    $COMPOSE up traffic
    wait_traffic
    wait_consumer
    get_summary
    ;;

    falla)
    echo "=== ESCENARIO 4: Falla temporal del Responder ==="
    $COMPOSE down -v 2>/dev/null || true

    FAILURE_RATE=0.5 $COMPOSE up --build --scale consumer=1 -d
    sleep 20
    reset_metrics

    echo "  → Iniciando tráfico con responder fallando 50% del tiempo..."
    $COMPOSE up -d traffic

    echo "  En 30s se detendrá el responder completamente..."
    sleep 30
    echo "   Deteniendo responder (caída total)..."
    docker stop responder

    sleep 30

    echo "  Restaurando responder..."
    docker start responder
    curl -s -X POST "$METRICS_URL/recovery/start" > /dev/null
    echo "  → Timer de recovery iniciado."

    wait_traffic
    wait_consumer
    curl -s -X POST "$METRICS_URL/recovery/end" > /dev/null
    get_summary
    ;;

    spike)
    echo "=== ESCENARIO 6: Spike de tráfico ==="
    $COMPOSE down -v 2>/dev/null || true
    $COMPOSE up --build --scale consumer=1 -d
    sleep 20
    reset_metrics

    SPIKE_ENABLED=true \
    SPIKE_AT=500 \
    SPIKE_RATE=500 \
    SPIKE_DURATION=10 \
    $COMPOSE up traffic

    wait_traffic
    wait_consumer
    get_summary
    ;;

    recovery)
    echo "=== ESCENARIO 7: Comparación síncrono vs Kafka en recuperación ==="
    echo ""
    echo "Paso 1: Sistema síncrono (Tarea 1)"
    echo "  → Detener responder, observar pérdida de consultas"
    echo ""
    echo "Paso 2: Sistema Kafka"
    echo "  → Las consultas se acumulan en el topic, no se pierden"
    echo "  → Al restaurar el responder, se procesan automáticamente"
    echo ""

    $COMPOSE down -v 2>/dev/null || true
    $COMPOSE up --build --scale consumer=2 -d
    sleep 20
    reset_metrics

    echo "  → Iniciando tráfico..."
    $COMPOSE up -d traffic
    sleep 20

    echo "  → Simulando caída del responder (30s)..."
    docker stop responder
    sleep 30

    echo "  → Restaurando responder — los mensajes en cola se procesarán solos"
    docker start responder
    curl -s -X POST "$METRICS_URL/recovery/start" > /dev/null
    echo "  → Timer de recovery iniciado."

    echo "  → Monitoreando recovery (backlog):"
    for i in $(seq 1 10); do
        sleep 5
        echo -n "    t=$((i*5))s → "
        curl -s "$METRICS_URL/summary" | python3 -c \
            "import sys,json; d=json.load(sys.stdin); \
            print(f\"recovered={d['recovered']} retries={d['retries']} dlq={d['dlq_count']}\")"
    done

    wait_traffic
    wait_consumer
    curl -s -X POST "$METRICS_URL/recovery/end" > /dev/null
    get_summary
    ;;

    *)
    echo "Uso: bash run_escenarios.sh [escenario]"
    echo ""
    echo "Escenarios disponibles:"
    echo "  base        Escenario 1 — sistema síncrono (Tarea 1)"
    echo "  base_falla  Escenario 1b — falla en sistema síncrono"
    echo "  kafka1      Escenario 2 — Kafka + 1 consumer"
    echo "  kafka3      Escenario 3 — Kafka + 3 consumers"
    echo "  falla       Escenario 4 — falla temporal del responder con Kafka"
    echo "  spike       Escenario 6 — spike de tráfico"
    echo "  recovery    Escenario 7 — comparación recuperación"
    ;;
esac