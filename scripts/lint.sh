#!/usr/bin/env bash
# Cheap static checks that catch the mistakes this project cares about: secrets in tracked
# files, string-concatenated SQL values, and broken browser JavaScript.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Java compile"
./mvnw -q -DskipTests compile

echo "==> JavaScript syntax"
if command -v node >/dev/null 2>&1; then
  node --check src/main/resources/static/app.js
else
  echo "node not installed; skipping JS syntax check"
fi

echo "==> Secret scan (committed workspace, excluding .env/target/.git)"
MSSQL_SECRET=""
if [[ -f .env ]]; then
  MSSQL_SECRET=$(grep -m1 '^MSSQL_SA_PASSWORD=' .env | cut -d= -f2- || true)
fi
if [[ -n "$MSSQL_SECRET" ]]; then
  if grep -RIlF "$MSSQL_SECRET" \
      --exclude-dir=.git --exclude-dir=target --exclude=.env --exclude=.env.example .; then
    echo "ERROR: the live MSSQL_SA_PASSWORD value appears in the workspace" >&2
    exit 1
  fi
  echo "the live MSSQL_SA_PASSWORD value is not present in tracked files"
else
  echo "MSSQL_SA_PASSWORD is not set; skipping the live-secret value scan"
fi
if grep -RInE 'MSSQL_SA_PASSWORD=[^$]' \
    --exclude-dir=.git --exclude-dir=target --exclude=.env --exclude=.env.example \
    --exclude=lint.sh --exclude=step7-lint.txt .; then
  echo "ERROR: a literal MSSQL password assignment appears in the workspace" >&2
  exit 1
fi
echo "no literal MSSQL password assignment found"

echo "==> SQL parameterization scan"
# Direct SQL calls must not concatenate a value into the statement. Dynamic clauses are
# built with fixed fragments and passed as ? placeholders (see VehicleEventRepository).
if grep -RInE 'jdbc\.(query|update|execute)\([^;]*\+' src/main/java; then
  echo "ERROR: direct string concatenation found in a jdbc call" >&2
  exit 1
fi
echo "no direct jdbc concatenation found"
echo "NOTE: dynamic WHERE clauses in VehicleEventRepository use fixed fragments plus ? args."

echo "==> Compose config"
if [[ -f .env ]] && command -v docker >/dev/null 2>&1; then
  docker compose config >/dev/null
  echo "docker compose config valid"
else
  echo "docker/.env unavailable; skipping compose validation"
fi

echo "lint passed"
