# FleetEventStream consumer-failure runbook

This runbook is for the on-call engineer when the telemetry pipeline is unhealthy. It assumes
the Docker Compose stack (containers `fleet-kafka`, `fleet-sqlserver`, `fleet-app`) and the
REST API on `http://localhost:8080`. Replace hostnames/ports if you run the app elsewhere.

Keep credentials out of shell history and tickets. Load the local `.env` (git-ignored) once:

```bash
set -a; source .env; set +a
SQLCMD=(docker exec -i fleet-sqlserver /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -d fleeteventstream)
KAFKA=(docker exec -i fleet-kafka /opt/kafka/bin)
```

## First five minutes

1. Confirm the scope: `curl -s -w '\nHTTP %{http_code}\n' http://localhost:8080/api/health/pipeline | jq .`
   - `200` and `healthy=true`: degraded but serving.
   - `503`: Kafka/DB unreachable or lag over `fleet.health.max-lag` (default 5000).
2. Read the dashboard at `http://localhost:8080` (throughput, per-partition lag, DLQ count).
3. Capture evidence before changing anything:
   ```bash
   mkdir -p /tmp/fleet-incident
   curl -s http://localhost:8080/api/stats/pipeline > /tmp/fleet-incident/pipeline.json
   curl -s http://localhost:8080/api/stats/lag > /tmp/fleet-incident/lag.json
   curl -s http://localhost:8080/api/stats/dlq > /tmp/fleet-incident/dlq.json
   docker compose logs --since=30m app > /tmp/fleet-incident/app.log
   docker compose ps > /tmp/fleet-incident/compose-ps.txt
   ```
4. Do **not** reset offsets, delete topics, or truncate tables until you know whether records
   were already persisted. The replay/idempotency path is safe; destructive shortcuts are not.

## Inspect consumer lag directly

The API and the Kafka tooling should agree. If they do not, trust the broker.

```bash
# Group state and all assigned partitions
"${KAFKA[@]}" kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group fleet-event-consumers

# List groups, including the DLQ consumer
"${KAFKA[@]}" kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# End offsets (LOG-END-OFFSET) per partition
"${KAFKA[@]}" kafka-get-offsets.sh --bootstrap-server localhost:9092 \
  --topic vehicle.telemetry

# Topic health / partition leaders
"${KAFKA[@]}" kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic vehicle.telemetry
```

Interpretation:

- `CURRENT-OFFSET` is the last committed offset, not the last read offset. A small lag right
  after a restart is normal.
- `LAG` growing monotonically means the consumer is slower than the producer or is not polling.
- `CONSUMER-ID` empty with state `Empty` means no live member; check the app.
- Repeated `PreparingRebalance`/`CompletingRebalance` and changing member IDs indicate a
  rebalance storm (see below).
- The DLQ consumer group is `fleet-event-consumers-dlq`; check it separately.

## Inspect dead letters in SQL Server

```bash
# Counts
"${SQLCMD[@]}" -Q "SET NOCOUNT ON;
  SELECT COUNT(*) AS total FROM dead_letter_event;
  SELECT COUNT(*) AS last_hour FROM dead_letter_event
   WHERE failed_at >= DATEADD(hour, -1, SYSUTCDATETIME());"

# Newest rows with source coordinates
"${SQLCMD[@]}" -Q "SET NOCOUNT ON;
  SELECT TOP (20) id, source_topic, partition_id, source_offset,
         error_class, retry_count, failed_at,
         LEFT(failure_reason, 300) AS failure_reason
  FROM dead_letter_event
  ORDER BY failed_at DESC, id DESC;"

# Grouped causes
"${SQLCMD[@]}" -Q "SET NOCOUNT ON;
  SELECT TOP (10) LEFT(failure_reason, 120) AS reason, COUNT(*) AS n
  FROM dead_letter_event
  GROUP BY LEFT(failure_reason, 120)
  ORDER BY n DESC;"

# Raw payload behind a specific row (safe to inspect; do not paste into tickets)
"${SQLCMD[@]}" -Q "SET NOCOUNT ON;
  SELECT raw_payload FROM dead_letter_event WHERE id = 1234;"
```

Use the API for the same data without opening a SQL session:

```bash
curl -s 'http://localhost:8080/api/admin/dlq?page=0&size=20' | jq .
curl -s http://localhost:8080/api/stats/dlq | jq .
```

## Symptom → cause → fix

### (a) Lag climbing steadily

| Symptom | Likely cause | Fix |
|---|---|---|
| `LAG` rises on all three partitions, throughput is flat, DLQ is quiet. | Offered rate exceeds consumer capacity (SQL Server latency, CPU pressure, or too few effective consumers). | Check `docker stats fleet-app fleet-sqlserver`. Reduce simulator rate (`POST /api/simulator/start`), or scale consumer concurrency only if partitions allow it (3 partitions = at most 3 useful threads). Let the backlog drain; do not restart the app while lag is high unless it is wedged. |
| Lag rises and `failed`/DLQ is climbing. | A retryable dependency (DB deadlock, pool exhaustion) is failing writes. | Inspect app logs for SQL errors and Hikari timeouts; see (c). |
| Lag rises only on one partition. | Hot key / uneven vehicle distribution, or one consumer thread is stuck. | Check per-partition lag; look for a poison pill on that partition (b), long GC pause, or a single slow DB connection. |
| Lag is high but stable and drains after the producer stops. | Normal backpressure. The 2,000/s benchmark deliberately shows this behavior. | No action if it drains within the SLO; otherwise lower the offered rate or add capacity. |

### (b) Consumer stuck on a poison pill

| Symptom | Likely cause | Fix |
|---|---|---|
| One partition's lag does not move while other partitions progress; app logs show repeated `ListenerExecutionFailedException`/JSON errors at the same offset. | A malformed or schema-invalid record cannot be processed. Validation failures are non-retryable and should go to the DLQ, so a stuck record usually means the error is not classified as validation (for example an unexpected runtime bug) or the DLQ publish itself is failing. | Identify the offset from the logs/`kafka-consumer-groups.sh`; inspect `dead_letter_event` for the source offset. Fix the payload contract or the consumer. Do not skip/commit past it manually. If the DLQ is unavailable, restore it, then let the error handler recover the record. |
| DLQ grows but `dead_letter_event` does not. | The DLQ consumer is down or its DB writes fail. | Check `fleet-event-consumers-dlq` lag and app logs for `Could not persist dead letter`. Fix DB connectivity, then let the DLQ consumer drain. |

The integration test `poisonPillsLandInDlqWithoutStickingThePartition` covers five poison pills
surrounded by valid events on the same partition.

### (c) DB deadlock / connection pool exhaustion

| Symptom | Likely cause | Fix |
|---|---|---|
| App logs contain SQL error 1205 (deadlock) or `SQLServerException` deadlock messages; some events go to DLQ after retries. | Concurrent writers, long transactions, or lock escalation. | Confirm with `SELECT * FROM sys.dm_exec_requests WHERE blocking_session_id <> 0;`. Retryable failures are already retried 3 times; if frequent, reduce concurrency, add/verify indexes, and keep transactions short. |
| `HikariPool ... Connection is not available, request timed out after 30000ms`; latency spikes and lag grows. | Pool exhausted (`DB_POOL_SIZE`, default 12), DB CPU saturated, or leaked connections. | Check `docker stats` and `SHOW ...`/Hikari metrics. Restart only as a last resort; a restart loses in-flight processing but not committed rows. Reduce load, raise pool size only if SQL Server can handle it. |
| DB unreachable (`The TCP/IP connection to the host ... has failed`). | Container down, wrong host/port, password rotation, or SQL Server still starting. | `docker compose ps`, `docker compose logs sqlserver`, verify `.env` password and `DB_PASSWORD`. The health endpoint returns 503 with `databaseReachable=false`. |

### (d) Duplicate rows / unique index violations

| Symptom | Likely cause | Fix |
|---|---|---|
| `duplicatesSkipped` increases, row count does not. | Expected: a retry, replay, or duplicate producer delivery hit `ux_vehicle_event_event_id`. | No action. This is the idempotency guarantee working. |
| `DuplicateKeyException` appears in logs as an error rather than a skipped duplicate. | The repository catch was bypassed or a different unique constraint fired. | Inspect the exception's constraint/index name. Only `event_id` should be treated as a duplicate; other constraints are data bugs. |
| Two rows look the same but have different `event_id`s. | Upstream producer generated a new id for the same logical event. | Fix idempotency at the producer; the DB cannot detect semantic duplicates. |
| Unique index is missing after a manual schema change. | Someone edited the applied Flyway migration or restored a partial schema. | Never edit `V1__create_schema.sql`; restore the index with a new migration (`V2__...`) and verify `sys.indexes`. |

### (e) Rebalance storms

| Symptom | Likely cause | Fix |
|---|---|---|
| Group state alternates `PreparingRebalance`/`CompletingRebalance`; lag stalls; logs show repeated `Revoking previously assigned partitions` / `partitions assigned`. | Consumer instances restarting, long processing exceeding `max.poll.interval.ms`, or session timeout too short. | Check app restarts/OOM kills (`docker compose ps`, `docker stats`). Reduce `spring.kafka.consumer.max-poll-records` (currently 100) or increase `max.poll.interval.ms` if DB writes are slow. Keep exactly one app instance unless you intend to scale consumers. |
| Members count changes from 3 to 6 and back. | Multiple app instances or duplicate containers. | `docker compose ps`; stop the extra instance. Each instance starts `concurrency=3` consumers. |
| Rebalances coincide with GC pauses or CPU starvation. | Host/container resource pressure. | Give Docker more CPU/RAM, reduce load, inspect `docker stats` and JVM GC logs. |

### (f) Replay gone wrong

| Symptom | Likely cause | Fix |
|---|---|---|
| Replay returns `recordsRead` larger than expected. | Wrong `fromOffset`/`maxRecords`, or the partition end offset advanced while replaying. | Stop further replays, record the returned range, and inspect `kafka-get-offsets.sh` plus `duplicatesSkipped`. Replayed rows are idempotent; nothing is duplicated in `vehicle_event`. |
| DLQ count grows after replaying DLQ rows. | The raw payload is still malformed/schema-invalid; replaying it re-enters the DLQ. | Fix the parser/schema or the producer first. Replaying a poison pill is not a repair. |
| Replay seems to change live consumption or group offsets. | It should not: `ReplayService` uses a throwaway group id, `assign` + `seek`, and never commits into `fleet-event-consumers`. | Verify the live group's `CURRENT-OFFSET` still tracks the broker; check the app logs for the replay client id. |
| Replay to a wrong topic/partition is rejected. | The API allow-lists the three configured topics and validates partition/offset/maxRecords. | Use only `vehicle.telemetry` (or the configured retry/DLQ topic) and a valid partition. |

## How to safely replay from an offset

1. **Stop new work.** Stop the simulator (`POST /api/simulator/stop`) or stop the app if you need
   a hard pause. Replay is safe alongside live traffic, but pausing makes the arithmetic clear.
2. **Wait for lag to reach 0** and record baselines:
   ```bash
   curl -s http://localhost:8080/api/stats/lag | jq '{totalLag,partitions}'
   curl -s http://localhost:8080/api/stats/pipeline | jq .
   "${SQLCMD[@]}" -Q "SET NOCOUNT ON; SELECT COUNT(*) AS vehicle_rows FROM vehicle_event;"
   ```
3. **Confirm the range** using `kafka-get-offsets.sh` and the source partition. Start small.
4. **Replay a bounded range**:
   ```bash
   curl -s -X POST http://localhost:8080/api/admin/replay \
     -H 'Content-Type: application/json' \
     -d '{"topic":"vehicle.telemetry","partition":0,"fromOffset":12345,"maxRecords":100}' | jq .
   ```
5. **Watch** `duplicatesSkipped` increase and `vehicleRows` stay unchanged:
   ```bash
   watch -n 2 "curl -s http://localhost:8080/api/stats/pipeline | jq '{duplicatesSkipped,vehicleRows,deadLetterRows}'"
   ```
6. **Drain** lag to zero and compare counts. Only then repeat with a larger range or the next
   partition.
7. **DLQ replay** only after the root cause is fixed. Replay by id and re-check the DLQ:
   ```bash
   curl -s -X POST http://localhost:8080/api/admin/replay/dlq \
     -H 'Content-Type: application/json' -d '{"ids":[1234,1235]}' | jq .
   ```
8. Record the run in the incident ticket: topic/partition/fromOffset/maxRecords, `recordsRead`,
   `recordsRepublished`, duplicates delta, row-count before/after, and DLQ delta.

Never delete a consumer group or reset offsets to "catch up" without first proving the records
were persisted. If you must reset, the group must be inactive:

```bash
# Inspect first; this is destructive to progress, not to data.
"${KAFKA[@]}" kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group fleet-event-consumers --reset-offsets --to-earliest --topic vehicle.telemetry --dry-run
# Then repeat without --dry-run only after a backup and an explicit change approval.
```

## Rollback note

- **Application rollback:** deploy the previous image/build (`git revert <sha>` and rebuild, or
  retag the previous image). The schema is forward-only: never edit an applied Flyway migration.
  If a schema change is required, add `V2__...` and let Flyway apply it.
- **Consumer pause:** `docker compose stop app` stops processing without losing committed
  offsets. Restarting resumes from the last committed offset; in-flight records are redelivered
  and deduplicated by `event_id`.
- **Data rollback:** take a SQL Server backup before risky migrations/replays (`BACKUP DATABASE
  fleeteventstream TO DISK = '/var/opt/mssql/backup/pre-change.bak' WITH INIT;`). Restore in a
  non-production environment first. Do not truncate `dead_letter_event` during an incident; it
  is the audit trail.
- **Replay rollback:** a replay cannot un-persist rows, but it also cannot create duplicate
  `event_id` rows. If a replay used the wrong range, stop it, record the range, and verify
  `vehicleRows` did not change. Fix the range and replay again if needed.
- **Escalation:** if lag keeps growing after two load reductions and a DB health check, capture
  the evidence bundle from "First five minutes" and escalate to the platform/DB owner.

## Useful log filters

```bash
docker compose logs -f app | grep -E 'Stored dead letter|Duplicate event|Persisted|Replay|ERROR|WARN'
docker compose logs --since=15m sqlserver | grep -iE 'deadlock|error|failed'
```

The app never logs credentials or full connection strings.
