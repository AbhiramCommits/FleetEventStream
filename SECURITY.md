# Security notes

FleetEventStream is a demonstration pipeline, not a hardened production deployment. The
following controls are implemented in the repository; the production gaps are called out
explicitly.

## Credentials and environment

- The SQL Server SA password is **not** committed. `.env` is git-ignored and `.env.example`
  contains only a placeholder. `docker-compose.yml` fails interpolation when
  `MSSQL_SA_PASSWORD` is missing, and the app reads `DB_PASSWORD` from the environment with no
  literal fallback.
- `DB_USERNAME`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `KAFKA_BOOTSTRAP_SERVERS`, and the consumer
  group/concurrency are environment-overridable.
- The local Compose stack connects as `sa` for convenience. In production, create a
  least-privilege login that owns only the application database (or use managed identity) and
  restrict network access to the database.
- The committed evidence logs were scrubbed of the local password with `sed`; the step-3 log
  contains `***REDACTED***` where the original command had the password.

Verify the credential scan (it reads the live value from `.env` and never prints it):

```bash
./scripts/lint.sh
```

## SQL injection

All values are passed as JDBC parameters. The repositories use `JdbcTemplate` with `?`
placeholders, including the variable-length `IN (...)` list in `DeadLetterRepository`, which
builds only `?` placeholders from the list size, never values. Dynamic history predicates append
fixed SQL fragments (`AND event_time >= ?`) and bind the timestamps as arguments.

`scripts/lint.sh` fails if a direct `jdbc.query/update/execute` call concatenates a string into
the statement. The only constant concatenation in the repository is the fixed column list
`"SELECT " + COLUMNS`, which contains no user input.

## Input validation and size limits

- `VehicleEvent` uses Jakarta Validation: non-blank IDs, required timestamp, latitude
  [-90, 90], longitude [-180, 180], speed >= 0, heading [0, 360], bounded alert-code pattern.
- Every path/query parameter has a size or range constraint (`@Size`, `@Min`, `@Max`); replay
  bodies use `@Valid` and bounded lists. A `@RestControllerAdvice` returns a consistent 400
  body for validation failures.
- The consumer rejects payloads over `fleet.kafka.consumer.max-payload-chars` (65,536 by
  default) before JSON parsing. Kafka's fetch size is capped at 1 MiB and the payload column is
  `NVARCHAR(MAX)`; the application-level cap is the protective limit.
- Replay accepts only the three configured topic names, `partition >= 0`,
  `fromOffset >= 0`, and `maxRecords` within the configured cap. It never commits into the live
  consumer group.

## Transport and certificates

- The local JDBC URL uses `encrypt=true;trustServerCertificate=true` because the Compose SQL
  Server uses a self-signed development certificate. Production must use a trusted certificate
  and should not set `trustServerCertificate=true`.
- Kafka in Compose is PLAINTEXT for local development. Production should use TLS/SASL and ACLs;
  this repository does not implement them.

## Exposure and authorization

- Actuator exposes `health`, `info`, `metrics`, and `prometheus`. It does not expose `env`,
  `heapdump`, or `threaddump` over HTTP.
- The simulator and replay endpoints are unauthenticated in this demo. In a real deployment,
  put them behind authentication/authorization and network policy; replay is a privileged
  operation. The dashboard is read-mostly but also exposes those controls.
- The composite health endpoint intentionally reports component reachability; do not expose it
  publicly if that metadata is sensitive.

## Dead-letter data handling

`dead_letter_event.raw_payload` and `vehicle_event.payload` preserve the original telemetry and
can contain location data. Treat them as sensitive, restrict database access, and apply a
retention policy. Failure reasons may contain parser excerpts; they are truncated before insert
but still belong to the same data classification. The application does not log raw payloads at
INFO.

## Shutdown and at-least-once behavior

- `server.shutdown=graceful` and `spring.lifecycle.timeout-per-shutdown-phase=30s` allow the
  web server and listener containers to stop cleanly. Each Kafka listener container has a 25s
  shutdown timeout so an in-flight record can finish and acknowledge.
- The simulator's `@PreDestroy` stops its executor. Records that were never accepted by the
  broker are not counted as published; records accepted by the broker are redelivered and
  deduplicated by `event_id` after restart.
- `dead_letter_event` has a unique index on `(source_topic, partition_id, source_offset)`, so a
  DLQ redelivery after a crash cannot create a duplicate audit row. The Flyway V2 migration
  removes pre-existing duplicates before creating the index.

## Reporting

This is a sample project. If you find a security issue, open a private report with the
maintainers rather than a public issue containing exploit details or credentials.
