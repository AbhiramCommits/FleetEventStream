#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env is missing. Copy .env.example to .env and set a strong MSSQL_SA_PASSWORD." >&2
  exit 1
fi

docker compose up -d --build "$@"
echo
echo "Waiting for the app to become healthy..."
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8080/actuator/health >/dev/null 2>&1; then
    echo "FleetEventStream is up: http://localhost:8080"
    echo "Kafka bootstrap (host): localhost:9092"
    echo "SQL Server (host):      localhost:1433"
    exit 0
  fi
  sleep 2
done
echo "App did not become healthy in time. Inspect with: docker compose logs -f app" >&2
exit 1
