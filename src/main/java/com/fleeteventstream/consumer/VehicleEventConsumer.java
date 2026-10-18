package com.fleeteventstream.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleeteventstream.config.properties.FleetKafkaProperties;
import com.fleeteventstream.model.VehicleEvent;
import com.fleeteventstream.observability.IngestionStats;
import com.fleeteventstream.persistence.VehicleEventRepository;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Main telemetry consumer. The offset is acknowledged only after the idempotent write has
 * completed, so a crash between the write and the ack causes a redelivery, never data loss.
 */
@Component
public class VehicleEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(VehicleEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final VehicleEventRepository repository;
    private final IngestionStats stats;
    private final int maxPayloadChars;

    public VehicleEventConsumer(ObjectMapper objectMapper,
                                Validator validator,
                                VehicleEventRepository repository,
                                IngestionStats stats,
                                FleetKafkaProperties properties) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.repository = repository;
        this.stats = stats;
        this.maxPayloadChars = properties.consumer().maxPayloadChars();
    }

    @KafkaListener(
            id = "vehicleEventConsumer",
            topics = "${fleet.kafka.topics.telemetry}",
            groupId = "${fleet.kafka.consumer.group-id}",
            concurrency = "${fleet.kafka.consumer.concurrency}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        stats.recordConsumed();
        long started = System.nanoTime();
        try {
            VehicleEvent event = parseAndValidate(record.value());
            boolean inserted = repository.insertIfAbsent(event, record.value());
            if (inserted) {
                stats.recordPersisted();
                long count = stats.snapshot().persisted();
                if (count % 1000 == 0) {
                    log.info("Persisted {} events (latest vehicle={}, partition={}, offset={})",
                            count, event.vehicleId(), record.partition(), record.offset());
                }
            } else {
                stats.recordDuplicate();
                log.debug("Duplicate event skipped: eventId={}, partition={}, offset={}",
                        event.eventId(), record.partition(), record.offset());
            }
            acknowledgment.acknowledge();
        } catch (RuntimeException e) {
            stats.recordFailed();
            // Do NOT ack. The configured DefaultErrorHandler retries with exponential backoff
            // and, when exhausted, publishes the record to vehicle.telemetry.dlq.
            throw e;
        } finally {
            stats.recordProcessing(started);
        }
    }

    private VehicleEvent parseAndValidate(String payload) {
        if (payload == null || payload.isBlank()) {
            throw new EventValidationException("Telemetry payload is null or blank");
        }
        if (payload.length() > maxPayloadChars) {
            throw new EventValidationException(
                    "Telemetry payload exceeds the " + maxPayloadChars + " character limit");
        }
        VehicleEvent event;
        try {
            event = objectMapper.readValue(payload, VehicleEvent.class);
        } catch (JsonProcessingException e) {
            throw new EventValidationException("Malformed telemetry JSON: " + e.getOriginalMessage(), e);
        }
        Set<ConstraintViolation<VehicleEvent>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String details = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .sorted()
                    .collect(Collectors.joining("; "));
            throw new EventValidationException("Telemetry schema validation failed: " + details);
        }
        return event;
    }
}
