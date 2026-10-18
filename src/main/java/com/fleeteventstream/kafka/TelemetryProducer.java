package com.fleeteventstream.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleeteventstream.model.VehicleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Serializes telemetry to JSON and publishes it keyed by vehicle id. Keying preserves
 * per-vehicle ordering because all records for one key go to the same partition.
 */
@Component
public class TelemetryProducer {

    private static final Logger log = LoggerFactory.getLogger(TelemetryProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    private final AtomicLong published = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    public TelemetryProducer(KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper,
                             @Value("${fleet.kafka.topics.telemetry}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public CompletableFuture<SendResult<String, String>> publish(VehicleEvent event) {
        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            failed.incrementAndGet();
            log.error("Could not serialize event {}", event.eventId(), e);
            return CompletableFuture.failedFuture(e);
        }
        return send(event.vehicleId(), json);
    }

    /**
     * Used by the simulator to inject intentionally unparseable payloads. The key is still
     * valid so poison pills stay ordered with the rest of that vehicle's stream.
     */
    public CompletableFuture<SendResult<String, String>> publishRaw(String key, String rawPayload) {
        return send(key, rawPayload);
    }

    private CompletableFuture<SendResult<String, String>> send(String key, String payload) {
        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topic, key, payload);
        return future.whenComplete((result, ex) -> {
            if (ex != null) {
                failed.incrementAndGet();
                log.warn("Kafka send failed for key {}: {}", key, ex.toString());
            } else {
                published.incrementAndGet();
            }
        });
    }

    public long getPublishedCount() {
        return published.get();
    }

    public long getFailedCount() {
        return failed.get();
    }

    public String getTopic() {
        return topic;
    }
}
