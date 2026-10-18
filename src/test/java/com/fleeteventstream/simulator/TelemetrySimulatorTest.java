package com.fleeteventstream.simulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleeteventstream.kafka.TelemetryProducer;
import com.fleeteventstream.model.VehicleEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TelemetrySimulatorTest {

    private final TelemetryProducer producer = Mockito.mock(TelemetryProducer.class);
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void producesValidEventsAtRoughlyTheConfiguredRate() throws Exception {
        AtomicInteger published = new AtomicInteger();
        when(producer.publish(any(VehicleEvent.class))).thenAnswer(invocation -> {
            published.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        when(producer.getPublishedCount()).thenReturn(0L);
        when(producer.getFailedCount()).thenReturn(0L);

        TelemetrySimulator simulator = new TelemetrySimulator(producer, 100, 5, 0.0, 0);
        simulator.start(100, 5, 1, 0.0); // 100 events/sec for 1 second
        Thread.sleep(1_150);

        TelemetrySimulator.SimulatorStatus status = simulator.status();
        assertThat(status.running()).isFalse();
        assertThat(status.eventsAttempted()).isBetween(70L, 135L);
        assertThat(published.get()).isEqualTo((int) status.eventsAttempted());
        assertThat(status.actualEventsPerSecond()).isBetween(60.0, 135.0);
        // Every event passes Bean Validation's basic numeric bounds.
        ArgumentCaptor<VehicleEvent> captor = ArgumentCaptor.forClass(VehicleEvent.class);
        verify(producer, atLeastOnce()).publish(captor.capture());
        assertThat(captor.getAllValues()).allSatisfy(event -> {
            assertThat(event.eventId()).isNotBlank();
            assertThat(event.vehicleId()).startsWith("veh-");
            assertThat(event.latitude()).isBetween(-90.0, 90.0);
            assertThat(event.longitude()).isBetween(-180.0, 180.0);
            assertThat(event.speedKph()).isGreaterThanOrEqualTo(0.0);
            assertThat(event.headingDeg()).isBetween(0.0, 360.0);
            assertThat(event.eventTime()).isNotNull();
        });
        simulator.shutdown();
    }

    @Test
    void malformedRateProducesUnparseablePayloads() throws Exception {
        AtomicInteger valid = new AtomicInteger();
        AtomicInteger malformed = new AtomicInteger();
        when(producer.publish(any(VehicleEvent.class))).thenAnswer(invocation -> {
            valid.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        when(producer.publishRaw(anyString(), anyString())).thenAnswer(invocation -> {
            malformed.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });
        when(producer.getPublishedCount()).thenReturn(0L);
        when(producer.getFailedCount()).thenReturn(0L);

        TelemetrySimulator simulator = new TelemetrySimulator(producer, 100, 4, 1.0, 0);
        simulator.start(100, 4, 1, 1.0);
        Thread.sleep(1_150);

        assertThat(malformed.get()).isBetween(70, 135);
        assertThat(valid.get()).isZero();
        assertThat(simulator.status().malformedPublished()).isEqualTo(malformed.get());

        ArgumentCaptor<String> payloads = ArgumentCaptor.forClass(String.class);
        verify(producer, atLeastOnce()).publishRaw(anyString(), payloads.capture());
        List<String> captured = payloads.getAllValues();
        assertThat(captured).isNotEmpty();
        assertThat(captured).allSatisfy(payload -> {
            try {
                objectMapper.readTree(payload);
                // A tree parse of truncated JSON should fail; enforce it explicitly.
                throw new AssertionError("Expected malformed payload but parsed: " + payload);
            } catch (com.fasterxml.jackson.core.JsonProcessingException expected) {
                assertThat(expected).isNotNull();
            }
        });
        simulator.shutdown();
    }

    @Test
    void startValidatesInputs() {
        TelemetrySimulator simulator = new TelemetrySimulator(producer, 10, 2, 0.0, 0);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> simulator.start(0, 2, 1, 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> simulator.start(10, 0, 1, 0.0))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> simulator.start(10, 2, 1, 1.5))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
