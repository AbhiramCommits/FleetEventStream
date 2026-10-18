-- Dead-letter persistence is idempotent on the original Kafka coordinates. If the app
-- crashes after publishing to the DLQ but before committing the source offset, the source
-- record is redelivered and the same dead letter is published again. This index prevents a
-- duplicate audit row.
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY source_topic, partition_id, source_offset
               ORDER BY id
           ) AS rn
    FROM dead_letter_event
)
DELETE FROM dead_letter_event WHERE id IN (SELECT id FROM ranked WHERE rn > 1);

CREATE UNIQUE INDEX ux_dead_letter_event_source
    ON dead_letter_event (source_topic, partition_id, source_offset);
