package com.fleeteventstream.persistence;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

@Repository
public class DeadLetterRepository {

    private static final ZoneOffset UTC = ZoneOffset.UTC;
    private static final String SELECT_RECENT = """
            SELECT id, source_topic, partition_id, source_offset, failure_reason, error_class,
                   raw_payload, failed_at, retry_count
            FROM dead_letter_event
            ORDER BY failed_at DESC, id DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """;
    private static final String SELECT_BY_IDS_PREFIX = """
            SELECT id, source_topic, partition_id, source_offset, failure_reason, error_class,
                   raw_payload, failed_at, retry_count
            FROM dead_letter_event
            WHERE id IN (
            """;

    private final JdbcTemplate jdbc;

    public DeadLetterRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long insert(String sourceTopic, int partitionId, long sourceOffset,
                       String failureReason, String errorClass, String rawPayload, int retryCount) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        try {
            jdbc.update(connection -> {
                PreparedStatement ps = connection.prepareStatement("""
                                INSERT INTO dead_letter_event
                                    (source_topic, partition_id, source_offset, failure_reason,
                                     error_class, raw_payload, retry_count)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, Statement.RETURN_GENERATED_KEYS);
                ps.setString(1, sourceTopic);
                ps.setInt(2, partitionId);
                ps.setLong(3, sourceOffset);
                ps.setString(4, truncate(failureReason, 1000));
                ps.setString(5, truncate(errorClass, 250));
                ps.setString(6, rawPayload);
                ps.setInt(7, retryCount);
                return ps;
            }, keyHolder);
        } catch (DuplicateKeyException duplicate) {
            // The source record was redelivered after the DLQ write but before the source
            // offset was committed. The audit row already exists.
            return 0;
        }
        Number key = keyHolder.getKey();
        return key == null ? 0 : key.longValue();
    }

    public List<DeadLetterEvent> findRecent(int page, int size) {
        int offset = page * size;
        return jdbc.query(SELECT_RECENT, MAPPER, offset, size);
    }

    public List<DeadLetterEvent> findByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return List.of();
        }
        // Only ? placeholders are generated from the list size; values stay bound parameters.
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = SELECT_BY_IDS_PREFIX + placeholders + ") ORDER BY id";
        return jdbc.query(sql, MAPPER, ids.toArray());
    }

    public long countAll() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM dead_letter_event", Long.class);
        return count == null ? 0 : count;
    }

    public long countSince(Instant since) {
        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM dead_letter_event WHERE failed_at >= ?",
                Long.class, Timestamp.valueOf(LocalDateTime.ofInstant(since, UTC)));
        return count == null ? 0 : count;
    }

    public List<FailureReasonCount> topFailureReasons(int limit) {
        return jdbc.query("""
                        SELECT TOP (?) failure_reason, COUNT(*) AS failure_count
                        FROM dead_letter_event
                        GROUP BY failure_reason
                        ORDER BY failure_count DESC
                        """,
                (rs, rowNum) -> new FailureReasonCount(
                        rs.getString("failure_reason"),
                        rs.getLong("failure_count")),
                limit);
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    private static final RowMapper<DeadLetterEvent> MAPPER =
            (ResultSet rs, int rowNum) -> new DeadLetterEvent(
                    rs.getLong("id"),
                    rs.getString("source_topic"),
                    rs.getInt("partition_id"),
                    rs.getLong("source_offset"),
                    rs.getString("failure_reason"),
                    rs.getString("error_class"),
                    rs.getString("raw_payload"),
                    toInstant(rs.getObject("failed_at", LocalDateTime.class)),
                    rs.getInt("retry_count"));

    private static Instant toInstant(LocalDateTime value) {
        return value == null ? null : value.toInstant(UTC);
    }

    public record FailureReasonCount(String failureReason, long count) {
    }
}
