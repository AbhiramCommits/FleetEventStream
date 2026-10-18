# Contributing to FleetEventStream

## Branch naming

Use a short, typed branch name:

| Prefix | Use |
|---|---|
| `feat/` | New behavior (for example `feat/vehicle-history-filter`). |
| `fix/` | Bug fixes (`fix/dlq-source-offset`). |
| `test/` | Test-only changes (`test/replay-idempotency`). |
| `docs/` | Documentation/runbooks. |
| `chore/` | Build, CI, dependency, or refactor work. |
| `perf/` | Measured performance work, with before/after evidence. |

Keep branches focused on one change. Do not mix schema migrations, behavior changes, and
formatting-only edits in one PR.

## Conventional commits

Use the imperative mood and a scope when it helps:

```
feat(consumer): record root-cause error class in DLQ rows

The DLT recoverer was storing the ListenerExecutionFailedException wrapper
message. Add the root cause headers so dead_letter_event is actionable.

Refs: #42
```

Common types: `feat`, `fix`, `test`, `docs`, `chore`, `refactor`, `perf`, `build`, `ci`.
Breaking changes use `!` and a `BREAKING CHANGE:` footer. Keep the subject under ~72 characters.

## Before opening a PR

```bash
# Fast feedback; no Docker required
./mvnw test

# Required for changes touching Kafka, SQL, transactions, replay, or configuration
./mvnw -B verify
```

`verify` starts real Kafka and SQL Server containers through Testcontainers. Docker must be
running. If the integration class is skipped, the PR is not verified: fix Docker rather than
merging with skipped tests. On Docker Engine 29, the Failsafe configuration sets
`-Dapi.version=1.43`; keep that property unless the Testcontainers/docker-java stack is upgraded
and verified.

If you changed the consumer, persistence path, topic settings, or replay logic, also run a short
benchmark and attach the output:

```bash
RATE=500 DURATION=60 OUTPUT=/tmp/benchmark-pr.txt ./scripts/benchmark.sh
```

Do not update numbers in the README from a different machine or a hand-written estimate. If a
measured number changes, update the relevant table and link the new log under `docs/run-logs/`.

## Schema changes

- Flyway migrations in `src/main/resources/db/migration` are immutable once applied.
- Add `V2__...`, `V3__...`; never edit `V1__create_schema.sql`.
- Preserve `ux_vehicle_event_event_id`; it is the idempotency contract.
- Test the migration against a real SQL Server container (`./mvnw -B verify`).
- Call out locking/backfill risk in the PR description.

## Code review checklist

### Correctness and delivery semantics

- [ ] The consumer acknowledges only after a successful idempotent write (or a recognized
      duplicate). No acknowledge-before-write paths.
- [ ] Validation failures are non-retryable and go to the DLQ; transient write failures are
      retryable with backoff.
- [ ] DLQ rows capture source topic/partition/offset, root error class, reason, retry count, and
      raw payload.
- [ ] Replay uses an isolated consumer group and does not commit into the live group.
- [ ] Replaying an already-persisted range adds zero `vehicle_event` rows and increases
      `duplicatesSkipped`.
- [ ] Per-vehicle ordering is preserved by keying on `vehicleId`.

### SQL and data

- [ ] All SQL values are parameterized; no string-concatenated user input.
- [ ] New queries have an index that supports their filter/order.
- [ ] `limit`/paging values are capped and validated.
- [ ] Migrations are forward-only and tested on real SQL Server.
- [ ] Timestamps are stored/interpreted as UTC.

### API and security

- [ ] Path/query/body inputs are validated; errors use the consistent `ApiError` body.
- [ ] 404/400/405/503 behavior is covered by tests.
- [ ] No credentials, connection strings, or real secrets are committed; `.env` stays ignored.
- [ ] New actuator/metrics endpoints do not leak payloads or secrets.
- [ ] Payload size caps remain in place for consumer input.

### Tests and operations

- [ ] Unit/slice tests cover happy and error paths.
- [ ] Integration tests assert real behavior (not mocks) for Kafka/SQL changes.
- [ ] Awaitility timeouts are generous enough for CI but still fail on a real stall.
- [ ] Logs include enough context (partition/offset) without logging full payloads on success.
- [ ] Graceful shutdown still lets in-flight records finish and ack.
- [ ] README/RUNBOOK updated when behavior, config, endpoints, or measured results change.

### CI

- [ ] `.github/workflows/ci.yml` stays valid YAML and both jobs are green.
- [ ] Surefire/Failsafe reports upload even on failure.
- [ ] The build job does not depend on a running database or broker.

## PR description template

```
## What changed
...

## Why
...

## Verification
- [ ] ./mvnw test
- [ ] ./mvnw -B verify (real Testcontainers Kafka + SQL Server)
- [ ] benchmark (if relevant), output: ...

## Risk / rollback
...

## Screenshots / run logs
...
```
