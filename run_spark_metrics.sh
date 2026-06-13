#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
METRICS_DIR="$ROOT_DIR/metrics"
SCRIPT="$ROOT_DIR/spark-process.sh"
COMPOSE_FILE="$ROOT_DIR/Node1/docker-compose.yml"

mkdir -p "$METRICS_DIR"
cd "$ROOT_DIR"

if [[ ! -x "$SCRIPT" ]]; then
  echo "Error: $SCRIPT no existe o no es ejecutable"
  exit 1
fi

CONTAINERS=(spark-master spark-worker1 spark-worker2)
ALL_CONTAINERS=()
for name in "${CONTAINERS[@]}"; do
  if docker ps --format '{{.Names}}' | grep -q "^${name}$"; then
    ALL_CONTAINERS+=("$name")
  fi
done

if [[ ${#ALL_CONTAINERS[@]} -eq 0 ]]; then
  echo "No se encontraron contenedores Spark activos. Asegúrate de levantar los contenedores primero."
  exit 1
fi

METRICS_FILE="$METRICS_DIR/spark_docker_metrics_$(date +%Y%m%d_%H%M%S).log"
SPARK_LOG="$METRICS_DIR/spark_process_output_$(date +%Y%m%d_%H%M%S).log"

echo "Contenedores monitoreados: ${ALL_CONTAINERS[*]}"
echo "Logs guardados en: $METRICS_FILE"
echo "Salida Spark guardada en: $SPARK_LOG"

echo "### Spark metrics run - $(date)" > "$METRICS_FILE"

echo "Iniciando monitor de Docker stats..."
(
  while true; do
    echo "--- $(date +'%Y-%m-%d %H:%M:%S') ---" >> "$METRICS_FILE"
    docker stats --no-stream "${ALL_CONTAINERS[@]}" >> "$METRICS_FILE" 2>&1 || true
    sleep 2
  done
) &
STATS_PID=$!

trap 'echo "Deteniendo monitor..."; kill "$STATS_PID" >/dev/null 2>&1 || true' EXIT INT TERM

START_TIME=$(date +%s)
echo "Inicio: $(date)" >> "$METRICS_FILE"

if ! TIMEFORMAT='Tiempo total: %3lR'; time "$SCRIPT" > "$SPARK_LOG" 2>&1; then
  echo "El script Spark falló. Consulta $SPARK_LOG"
  exit_code=1
else
  exit_code=0
fi

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo "Fin: $(date)" >> "$METRICS_FILE"
echo "Duración total (segundos): $DURATION" >> "$METRICS_FILE"

echo "Deteniendo monitor..."
kill "$STATS_PID" >/dev/null 2>&1 || true
wait "$STATS_PID" 2>/dev/null || true

echo "Métricas y salida almacenadas en $METRICS_DIR"
exit $exit_code
