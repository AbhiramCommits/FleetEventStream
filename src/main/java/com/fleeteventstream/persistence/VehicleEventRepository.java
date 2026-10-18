package com.fleeteventstream.persistence;

import com.fleeteventstream.model.VehicleEvent;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository
public class VehicleEventRepository {

    private static final ZoneOffset UTC = ZoneOffset.UTC;
    private static final String COLUMNS =
            "id, event_id, vehicle_id, event_time, latitude, longitude, speed_kph, heading_deg, " +
                    "alert_code, payload, ingested_at";

    private final JdbcTemplate jdbc;

    public VehicleEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Single-statement idempotent insert. The unique index on event_id is the source of
     * truth: a duplicate key means another delivery (or a replay) already wrote the row.
     *
     * @return true when a new row was inserted, false when it was a duplicate
     */
    public boolean insertIfAbsent(VehicleEvent event, String rawPayload) {
        try {
            int rows = jdbc.update("""
                            INSERT INTO vehicle_event
                                (event_id, vehicle_id, event_time, latitude, longitude,
                                 speed_kph, heading_deg, alert_code, payload)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                    event.eventId(),
                    event.vehicleId(),
                    toTimestamp(event.eventTime()),
                    event.latitude(),
                    event.longitude(),
                    event.speedKph(),
                    event.headingDeg(),
                    event.alertCode(),
                    rawPayload);
            return rows > 0;
        } catch (DuplicateKeyException duplicate) {
            return false;
        }
    }

    public Optional<StoredVehicleEvent> findByEventId(String eventId) {
        List<StoredVehicleEvent> rows = jdbc.query(
                "SELECT " + COLUMNS + " FROM vehicle_event WHERE event_id = ?",
                VEHICLE_ROW_MAPPER, eventId);
        return rows.stream().findFirst();
    }

    public List<StoredVehicleEvent> findHistory(String vehicleId, Instant from, Instant to, int limit) {
        StringBuilder sql = new StringBuilder("SELECT " + COLUMNS
                + " FROM vehicle_event WHERE vehicle_id = ?");
        List<Object> args = new ArrayList<>();
        args.add(vehicleId);
        if (from != null) {
            sql.append(" AND event_time >= ?");
            args.add(toTimestamp(from));
        }
        if (to != null) {
            sql.append(" AND event_time <= ?");
            args.add(toTimestamp(to));
        }
        sql.append(" ORDER BY event_time DESC, id DESC OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
        args.add(limit);
        return jdbc.query(sql.toString(), VEHICLE_ROW_MAPPER, args.toArray());
    }

    public List<VehicleSummary> findVehicles(int limit) {
        return jdbc.query("""
                        SELECT vehicle_id, COUNT(*) AS event_count,
                               MAX(event_time) AS last_seen
                        FROM vehicle_event
                        GROUP BY vehicle_id
                        ORDER BY last_seen DESC
                        OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
                        """,
                (rs, rowNum) -> new VehicleSummary(
                        rs.getString("vehicle_id"),
                        rs.getLong("event_count"),
                        toInstant(rs.getObject("last_seen", LocalDateTime.class))),
                limit);
    }

    public List<ThroughputPoint> throughput(Instant since) {
        return jdbc.query("""
                        SELECT DATEADD(second,
                                   DATEDIFF_BIG(second, CAST('1970-01-01T00:00:00' AS DATETIME2), ingested_at),
                                   CAST('1970-01-01T00:00:00' AS DATETIME2)) AS bucket,
                               COUNT(*) AS event_count
                        FROM vehicle_event
                        WHERE ingested_at >= ?
                        GROUP BY DATEADD(second,
                                   DATEDIFF_BIG(second, CAST('1970-01-01T00:00:00' AS DATETIME2), ingested_at),
                                   CAST('1970-01-01T00:00:00' AS DATETIME2))
                        ORDER BY bucket
                        """,
                (rs, rowNum) -> new ThroughputPoint(
                        toInstant(rs.getObject("bucket", LocalDateTime.class)),
                        rs.getLong("event_count")),
                toTimestamp(since));
    }

    public LatencyPercentiles latencyPercentiles(Instant since) {
        List<LatencyPercentiles> rows = jdbc.query("""
                        SELECT DISTINCT
                            PERCENTILE_CONT(0.50) WITHIN GROUP
                                (ORDER BY DATEDIFF_BIG(microsecond, event_time, ingested_at) / 1000.0) OVER () AS p50_ms,
                            PERCENTILE_CONT(0.95) WITHIN GROUP
                                (ORDER BY DATEDIFF_BIG(microsecond, event_time, ingested_at) / 1000.0) OVER () AS p95_ms,
                            PERCENTILE_CONT(0.99) WITHIN GROUP
                                (ORDER BY DATEDIFF_BIG(microsecond, event_time, ingested_at) / 1000.0) OVER () AS p99_ms
                        FROM vehicle_event
                        WHERE ingested_at >= ? AND ingested_at >= event_time
                        """,
                (rs, rowNum) -> new LatencyPercentiles(
                        rs.getDouble("p50_ms"),
                        rs.getDouble("p95_ms"),
                        rs.getDouble("p99_ms")),
                toTimestamp(since));
        return rows.isEmpty() ? new LatencyPercentiles(0, 0, 0) : rows.get(0);
    }

    public long countAll() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM vehicle_event", Long.class);
        return count == null ? 0 : count;
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.valueOf(LocalDateTime.ofInstant(instant, UTC));
    }

    private static Instant toInstant(LocalDateTime localDateTime) {
        return localDateTime == null ? null : localDateTime.toInstant(UTC);
    }

    private static final RowMapper<StoredVehicleEvent> VEHICLE_ROW_MAPPER =
            (ResultSet rs, int rowNum) -> new StoredVehicleEvent(
                    rs.getLong("id"),
                    rs.getString("event_id"),
                    rs.getString("vehicle_id"),
                    toInstant(rs.getObject("event_time", LocalDateTime.class)),
                    rs.getDouble("latitude"),
                    rs.getDouble("longitude"),
                    rs.getDouble("speed_kph"),
                    rs.getDouble("heading_deg"),
                    rs.getString("alert_code"),
                    rs.getString("payload"),
                    toInstant(rs.getObject("ingested_at", LocalDateTime.class)));

    public record VehicleSummary(String vehicleId, long eventCount, Instant lastSeen) {
    }

    public record ThroughputPoint(Instant bucket, long eventCount) {
    }

    public record LatencyPercentiles(double p50Ms, double p95Ms, double p99Ms) {
    }
}
