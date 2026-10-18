package com.fleeteventstream.consumer;

import com.fleeteventstream.observability.IngestionStats;
import com.fleeteventstream.persistence.DeadLetterRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Persists exhausted/failed records from the DLQ topic. Uses its own listener container
 * factory so a database failure here cannot recursively publish back into the same DLQ.
 */
@Component
public class DeadLetterConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterConsumer.class);

    private static final String HDR_SOURCE_TOPIC = "kafka_dlt-original-topic";
    private static final String HDR_SOURCE_PARTITION = "kafka_dlt-original-partition";
    private static final String HDR_SOURCE_OFFSET = "kafka_dlt-original-offset";
    private static final String HDR_EXCEPTION_FQCN = "kafka_dlt-exception-fqcn";
    private static final String HDR_EXCEPTION_MESSAGE = "kafka_dlt-exception-message";
    private static final String HDR_DELIVERY_ATTEMPT = "kafka_deliveryAttempt";
    private static final String HDR_RETRY_COUNT = "x-fleet-retry-count";
    private static final String HDR_FLEET_ERROR_CLASS = "x-fleet-error-class";
    private static final String HDR_FLEET_FAILURE_REASON = "x-fleet-failure-reason";

    private final DeadLetterRepository repository;
    private final IngestionStats stats;

    public DeadLetterConsumer(DeadLetterRepository repository, IngestionStats stats) {
        this.repository = repository;
        this.stats = stats;
    }

    @KafkaListener(
            id = "deadLetterConsumer",
            topics = "${fleet.kafka.topics.dlq}",
            groupId = "${fleet.kafka.consumer.group-id}-dlq",
            containerFactory = "dlqListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String sourceTopic = header(record.headers(), HDR_SOURCE_TOPIC, record.topic());
        int sourcePartition = intHeader(record.headers(), HDR_SOURCE_PARTITION, record.partition());
        long sourceOffset = longHeader(record.headers(), HDR_SOURCE_OFFSET, record.offset());
        String errorClass = header(record.headers(), HDR_FLEET_ERROR_CLASS,
                header(record.headers(), HDR_EXCEPTION_FQCN, "unknown"));
        String failureReason = header(record.headers(), HDR_FLEET_FAILURE_REASON,
                header(record.headers(), HDR_EXCEPTION_MESSAGE, "unknown failure"));
        int deliveryAttempt = intHeader(record.headers(), HDR_DELIVERY_ATTEMPT, 1);
        int retryCount = intHeader(record.headers(), HDR_RETRY_COUNT,
                Math.max(0, deliveryAttempt - 1));

        try {
            long id = repository.insert(sourceTopic, sourcePartition, sourceOffset,
                    failureReason, errorClass, record.value(), retryCount);
            if (id == 0) {
                stats.recordDlqDuplicate();
                log.debug("Duplicate dead letter skipped for {}-{}@{}",
                        sourceTopic, sourcePartition, sourceOffset);
            } else {
                stats.recordDlqStored();
                log.warn("Stored dead letter id={} source={}-{}@{} class={} reason={}",
                        id, sourceTopic, sourcePartition, sourceOffset, errorClass, failureReason);
            }
            acknowledgment.acknowledge();
        } catch (RuntimeException e) {
            log.error("Could not persist dead letter for {}-{}@{}",
                    sourceTopic, sourcePartition, sourceOffset, e);
            throw e;
        }
    }

    private static String header(Headers headers, String name, String fallback) {
        Header header = headers.lastHeader(name);
        if (header == null || header.value() == null) {
            return fallback;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static int intHeader(Headers headers, String name, int fallback) {
        String value = header(headers, name, null);
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long longHeader(Headers headers, String name, long fallback) {
        String value = header(headers, name, null);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
