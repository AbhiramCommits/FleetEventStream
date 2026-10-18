#!/usr/bin/env bash
#
# Runs a sustained load against a running FleetEventStream stack and reports measured
# throughput, lag, DLQ, and end-to-end latency. Requires curl + jq.
#
# Usage:
#   ./scripts/benchmark.sh                         # 2000 ev/s for 60s (defaults)
#   RATE=500 DURATION=60 ./scripts/benchmark.sh    # lower rate for slower hosts
#
set -euo pipefail
cd "$(dirname "$0")/.."

BASE_URL=${BASE_URL:-http://localhost:8080}
RATE=${RATE:-2000}
DURATION=${DURATION:-60}
VEHICLES=${VEHICLES:-50}
DRAIN_TIMEOUT=${DRAIN_TIMEOUT:-600}
OUTPUT=${OUTPUT:-docs/run-logs/benchmark.txt}

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq is required" >&2
  exit 1
fi
if (( RATE < 1 || DURATION < 1 )); then
  echo "ERROR: RATE and DURATION must be positive" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT")"
exec > >(tee "$OUTPUT") 2>&1

echo "=================================================================================="
echo "FleetEventStream benchmark"
echo "Started (UTC):      $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Target:             ${BASE_URL}"
echo "Simulator:          ${RATE} events/sec, ${VEHICLES} vehicles, ${DURATION}s"
echo "Drain timeout:      ${DRAIN_TIMEOUT}s"
echo "=================================================================================="

echo
echo "### Preflight"
HEALTH_CODE=$(curl -sS -o /tmp/fleet-bench-health.json -w '%{http_code}' "$BASE_URL/api/health/pipeline")
echo "GET /api/health/pipeline -> HTTP $HEALTH_CODE"
jq '{healthy,status,kafkaReachable,databaseReachable,totalLag,lagThreshold}' /tmp/fleet-bench-health.json
if [[ "$HEALTH_CODE" != "200" ]]; then
  echo "ERROR: pipeline is not healthy; refusing to benchmark" >&2
  exit 1
fi

# Stop any prior simulator run and let old lag drain first so the baseline is clean.
curl -sS -X POST "$BASE_URL/api/simulator/stop" >/dev/null || true
for _ in $(seq 1 60); do
  LAG=$(curl -sS "$BASE_URL/api/stats/lag" | jq -r '.totalLag // 0')
  [[ "$LAG" == "0" ]] && break
  sleep 1
done
if [[ "${LAG:-1}" != "0" ]]; then
  echo "ERROR: consumer lag did not drain before the benchmark (lag=$LAG)" >&2
  exit 1
fi

BASE=$(curl -sS "$BASE_URL/api/stats/pipeline")
BASE_ROWS=$(echo "$BASE" | jq -r '.vehicleRows')
BASE_PERSISTED=$(echo "$BASE" | jq -r '.persisted')
BASE_DUPLICATES=$(echo "$BASE" | jq -r '.duplicatesSkipped')
BASE_FAILED=$(echo "$BASE" | jq -r '.failed')
BASE_DLQ=$(echo "$BASE" | jq -r '.deadLetterRows')
START_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
echo "Baseline:           rows=$BASE_ROWS persisted=$BASE_PERSISTED duplicates=$BASE_DUPLICATES dlq=$BASE_DLQ"
echo "Latency window since: $START_TS"

echo
echo "### Load phase"
curl -sS -X POST "$BASE_URL/api/simulator/start" \
  -H 'Content-Type: application/json' \
  -d "{\"eventsPerSecond\":${RATE},\"vehicleCount\":${VEHICLES},\"durationSeconds\":${DURATION}}" \
  | jq '{running,targetEventsPerSecond,vehicleCount,durationSeconds}'

PEAK_LAG=0
for ((i = 1; i <= DURATION; i++)); do
  sleep 1
  LAG=$(curl -sS "$BASE_URL/api/stats/lag" | jq -r '.totalLag // 0')
  if (( LAG > PEAK_LAG )); then PEAK_LAG=$LAG; fi
  if (( i % 10 == 0 )); then
    P=$(curl -sS "$BASE_URL/api/stats/pipeline")
    printf "t=%3ss  lag=%-8s persisted_delta=%-8s dlq_delta=%s\n" \
      "$i" "$LAG" "$(($(echo "$P" | jq -r '.persisted') - BASE_PERSISTED))" \
      "$(($(echo "$P" | jq -r '.deadLetterRows') - BASE_DLQ))"
  fi
done

sleep 2
SIM_END=$(curl -sS "$BASE_URL/api/simulator/status")
LOAD_END=$(curl -sS "$BASE_URL/api/stats/pipeline")
LOAD_ROWS=$(echo "$LOAD_END" | jq -r '.vehicleRows')
LOAD_DUPLICATES=$(echo "$LOAD_END" | jq -r '.duplicatesSkipped')
LOAD_FAILED=$(echo "$LOAD_END" | jq -r '.failed')
LOAD_DLQ=$(echo "$LOAD_END" | jq -r '.deadLetterRows')
echo
echo "Simulator final status:"
echo "$SIM_END" | jq '{running,eventsAttempted,eventsPublished,eventsFailed,malformedPublished,elapsedSeconds}'
echo "At end of load: persisted_delta=$((LOAD_ROWS - BASE_ROWS)) lag=$LAG"

echo
echo "### Drain phase"
DRAIN_START=$(date +%s)
DRAINED=0
for ((i = 0; i <= DRAIN_TIMEOUT; i += 2)); do
  LAG=$(curl -sS "$BASE_URL/api/stats/lag" | jq -r '.totalLag // 0')
  if (( LAG > PEAK_LAG )); then PEAK_LAG=$LAG; fi
  if (( LAG == 0 )); then DRAINED=1; break; fi
  if (( i % 20 == 0 )); then printf "drain t=%3ss lag=%s\n" "$i" "$LAG"; fi
  sleep 2
done
DRAIN_SECONDS=$(($(date +%s) - DRAIN_START))
FINAL=$(curl -sS "$BASE_URL/api/stats/pipeline")
FINAL_ROWS=$(echo "$FINAL" | jq -r '.vehicleRows')
FINAL_PERSISTED=$(echo "$FINAL" | jq -r '.persisted')
FINAL_DUPLICATES=$(echo "$FINAL" | jq -r '.duplicatesSkipped')
FINAL_FAILED=$(echo "$FINAL" | jq -r '.failed')
FINAL_DLQ=$(echo "$FINAL" | jq -r '.deadLetterRows')
FINAL_LAG=$(curl -sS "$BASE_URL/api/stats/lag" | jq -r '.totalLag // 0')

LATENCY=$(curl -sS "$BASE_URL/api/stats/latency?windowSeconds=86400&since=${START_TS}")
DLQ_STATS=$(curl -sS "$BASE_URL/api/stats/dlq")

PERSISTED_DELTA=$((FINAL_ROWS - BASE_ROWS))
LOAD_PERSISTED_DELTA=$((LOAD_ROWS - BASE_ROWS))
DUPLICATES_DELTA=$((FINAL_DUPLICATES - BASE_DUPLICATES))
FAILED_DELTA=$((FINAL_FAILED - BASE_FAILED))
DLQ_DELTA=$((FINAL_DLQ - BASE_DLQ))
PUBLISHED=$(echo "$SIM_END" | jq -r '.eventsPublished')
ATTEMPTED=$(echo "$SIM_END" | jq -r '.eventsAttempted')
MALFORMED=$(echo "$SIM_END" | jq -r '.malformedPublished')
TOTAL_SECONDS=$((DURATION + DRAIN_SECONDS))
OFFERED_RATE=$(awk -v n="$ATTEMPTED" -v d="$DURATION" 'BEGIN { printf "%.1f", n / d }')
LOAD_RATE=$(awk -v n="$LOAD_PERSISTED_DELTA" -v d="$DURATION" 'BEGIN { printf "%.1f", n / d }')
DRAIN_RATE=$(awk -v n="$PERSISTED_DELTA" -v d="$TOTAL_SECONDS" 'BEGIN { printf "%.1f", n / d }')
P50=$(echo "$LATENCY" | jq -r '.latency.p50Ms // 0')
P95=$(echo "$LATENCY" | jq -r '.latency.p95Ms // 0')
P99=$(echo "$LATENCY" | jq -r '.latency.p99Ms // 0')

echo
echo "### Results"
printf "%-36s %s\n" "Simulator target rate" "${RATE} events/sec"
printf "%-36s %s\n" "Offered rate (attempted/duration)" "${OFFERED_RATE} events/sec"
printf "%-36s %s\n" "Duration" "${DURATION}s"
printf "%-36s %s\n" "Events attempted" "$ATTEMPTED"
printf "%-36s %s\n" "Events confirmed published" "$PUBLISHED"
printf "%-36s %s\n" "Malformed payloads produced" "$MALFORMED"
printf "%-36s %s\n" "Events persisted by end of load" "$LOAD_PERSISTED_DELTA"
printf "%-36s %s\n" "Events persisted after drain" "$PERSISTED_DELTA"
printf "%-36s %s\n" "Duplicates skipped (this run)" "$DUPLICATES_DELTA"
printf "%-36s %s\n" "Failed deliveries (this run)" "$FAILED_DELTA"
printf "%-36s %s\n" "Dead letters added" "$DLQ_DELTA"
printf "%-36s %s\n" "Dead letters total" "$FINAL_DLQ"
printf "%-36s %s\n" "Peak consumer lag" "$PEAK_LAG"
printf "%-36s %s (%s)\n" "Drained consumer lag" "$FINAL_LAG" "$([[ $DRAINED == 1 ]] && echo "drained in ${DRAIN_SECONDS}s" || echo "NOT drained within ${DRAIN_TIMEOUT}s")"
printf "%-36s %s\n" "Persisted throughput during load" "${LOAD_RATE} events/sec"
printf "%-36s %s\n" "End-to-end persisted throughput" "${DRAIN_RATE} events/sec"
printf "%-36s %s\n" "p50 end-to-end latency" "${P50} ms"
printf "%-36s %s\n" "p95 end-to-end latency" "${P95} ms"
printf "%-36s %s\n" "p99 end-to-end latency" "${P99} ms"
echo
echo "Top DLQ failure reasons:"
echo "$DLQ_STATS" | jq -r '.topFailureReasons[]? | "  \(.count)  \(.failureReason | gsub("\n"; " ") | .[0:110])"'
echo
echo "Finished (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ "$DRAINED" != "1" ]]; then
  echo "WARNING: lag did not fully drain within ${DRAIN_TIMEOUT}s" >&2
  exit 2
fi
