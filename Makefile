.PHONY: help up down run test verify bench lint

SHELL := /bin/bash
.ONESHELL:

help:
	@echo "FleetEventStream targets:"
	@echo "  make up      - build and start the full Docker Compose stack"
	@echo "  make down    - stop the stack (keeps volumes)"
	@echo "  make run     - run the app with Maven against local infra (requires .env)"
	@echo "  make test    - unit and MVC slice tests (no Docker)"
	@echo "  make verify  - unit + Testcontainers integration tests"
	@echo "  make bench   - 2,000 events/sec benchmark, writes docs/run-logs/benchmark.txt"
	@echo "  make lint    - compile, JS syntax, and secret/SQL-concatenation checks"

up:
	./scripts/dev-up.sh

down:
	./scripts/dev-down.sh

run:
	set -a; source .env; set +a
	DB_PASSWORD="$$MSSQL_SA_PASSWORD" ./mvnw spring-boot:run

test:
	./mvnw test

verify:
	./mvnw -B clean verify

bench:
	./scripts/benchmark.sh

lint:
	./scripts/lint.sh
