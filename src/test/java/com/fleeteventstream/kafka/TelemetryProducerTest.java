package com.fleeteventstream.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleeteventstream.config.properties.FleetKafkaProperties;
import com.fleeteventstream.model.VehicleEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TelemetryProducerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    private static FleetKafkaProperties kafkaProperties() {
        return new FleetKafkaProperties(
                new FleetKafkaProperties.Topics("vehicle.telemetry", "vehicle.telemetry.retry",
                        "vehicle.telemetry.dlq"),
                new FleetKafkaProperties.Consumer("fleet-event-consumers", "fleet-event-replay", 3, 65536),
                new FleetKafkaProperties.Retry(3, 500, 2.0));
    }

    @Test
    void serializesEventToExpectedJsonAndKeysByVehicleId() throws Exception {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, objectMapper, kafkaProperties());
        VehicleEvent event = new VehicleEvent(
                "11111111-1111-1111-1111-111111111111",
                "veh-0007",
                Instant.parse("2026-10-18T00:00:00Z"),
                37.7749,
                -122.4194,
                48.5,
                271.25,
                "HARD_BRAKE");

        producer.publish(event).join();

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("vehicle.telemetry"), eq("veh-0007"), payload.capture());
        JsonNode json = objectMapper.readTree(payload.getValue());

        assertThat(json.get("eventId").asText()).isEqualTo(event.eventId());
        assertThat(json.get("vehicleId").asText()).isEqualTo("veh-0007");
        assertThat(json.get("eventTime").asText()).isEqualTo("2026-10-18T00:00:00Z");
        assertThat(json.get("latitude").asDouble()).isEqualTo(37.7749);
        assertThat(json.get("longitude").asDouble()).isEqualTo(-122.4194);
        assertThat(json.get("speedKph").asDouble()).isEqualTo(48.5);
        assertThat(json.get("headingDeg").asDouble()).isEqualTo(271.25);
        assertThat(json.get("alertCode").asText()).isEqualTo("HARD_BRAKE");
        assertThat(producer.getPublishedCount()).isEqualTo(1);
        assertThat(producer.getFailedCount()).isZero();
    }

    @Test
    void countsFailedSends() {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, String>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        TelemetryProducer producer = new TelemetryProducer(kafkaTemplate, objectMapper, kafkaProperties());
        producer.publishRaw("veh-0001", "{}");
        future.completeExceptionally(new RuntimeException("broker down"));

        assertThat(producer.getFailedCount()).isEqualTo(1);
        assertThat(producer.getPublishedCount()).isZero();
    }
}
