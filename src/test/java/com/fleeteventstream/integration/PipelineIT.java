package com.fleeteventstream.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleeteventstream.kafka.TelemetryProducer;
import com.fleeteventstream.model.VehicleEvent;
import com.fleeteventstream.observability.ConsumerLagService;
import com.fleeteventstream.observability.IngestionStats;
import com.fleeteventstream.persistence.DeadLetterEvent;
import com.fleeteventstream.persistence.DeadLetterRepository;
import com.fleeteventstream.persistence.VehicleEventRepository;
import com.fleeteventstream.replay.ReplayService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PipelineIT extends IntegrationTestBase {

    @Autowired
    private TelemetryProducer producer;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private VehicleEventRepository vehicleEvents;
    @Autowired
    private DeadLetterRepository deadLetters;
    @Autowired
    private IngestionStats stats;
    @Autowired
    private ReplayService replayService;
    @Autowired
    private ConsumerLagService lagService;
    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;
    @Autowired
    private AdminClient adminClient;
    @Autowired
    private MockMvc mockMvc;

    @Value("${fleet.kafka.topics.telemetry}")
    private String topic;

    @AfterEach
    void leaveConsumerCaughtUp() {
        awaitLagZero(Duration.ofSeconds(120));
    }

    @Test
    @Order(1)
    void endToEndPersistsOneThousandEventsWithNoDeadLetters() throws Exception {
        long rowsBefore = vehicleEvents.countAll();
        long dlqBefore = deadLetters.countAll();

        List<VehicleEvent> batch = new ArrayList<>();
        Instant base = Instant.now().minusSeconds(10);
        for (int i = 0; i < 1000; i++) {
            batch.add(event("veh-e2e-" + (i % 20), base.plusMillis(i), 30.0 + (i % 60)));
        }
        publishAll(batch);

        awaitRows(rowsBefore + 1000, Duration.ofSeconds(120));
        awaitLagZero(Duration.ofSeconds(120));

        assertThat(vehicleEvents.countAll()).isEqualTo(rowsBefore + 1000);
        assertThat(deadLetters.countAll()).isEqualTo(dlqBefore);
    }

    @Test
    @Order(2)
    void duplicateDeliveryIsIdempotent() throws Exception {
        long rowsBefore = vehicleEvents.countAll();
        long duplicatesBefore = stats.snapshot().duplicatesSkipped();
        List<VehicleEvent> batch = new ArrayList<>();
        Instant now = Instant.now().minusSeconds(1);
        for (int i = 0; i < 100; i++) {
            batch.add(event("veh-idem-" + (i % 5), now.plusMillis(i), 40.0));
        }

        publishAll(batch);
        awaitRows(rowsBefore + 100, Duration.ofSeconds(60));

        publishAll(batch);
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(
                        stats.snapshot().duplicatesSkipped() - duplicatesBefore).isEqualTo(100));
        awaitLagZero(Duration.ofSeconds(60));

        assertThat(vehicleEvents.countAll()).isEqualTo(rowsBefore + 100);
        assertThat(stats.snapshot().duplicatesSkipped() - duplicatesBefore).isEqualTo(100);
    }

    @Test
    @Order(3)
    void poisonPillsLandInDlqWithoutStickingThePartition() throws Exception {
        long rowsBefore = vehicleEvents.countAll();
        long dlqBefore = deadLetters.countAll();
        String vehicleId = "veh-poison-" + UUID.randomUUID().toString().substring(0, 8);
        Instant now = Instant.now().minusSeconds(2);

        List<VehicleEvent> valid = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            valid.add(event(vehicleId, now.plusMillis(i), 50.0));
        }

        // 50 valid, then five different poison pills, then 50 valid. Everything shares one
        // key, so it all lands on the same partition and ordering cannot hide a stuck record.
        for (int i = 0; i < 50; i++) {
            producer.publish(valid.get(i)).get(10, TimeUnit.SECONDS);
        }

        List<String> poison = List.of(
                "{not-json",
                "{\"eventId\":\"" + UUID.randomUUID() + "\",\"vehicleId\":\"" + vehicleId
                        + "\",\"eventTime\":\"" + now + "\",\"latitude\":",
                objectMapper.writeValueAsString(new VehicleEvent(
                        UUID.randomUUID().toString(), vehicleId, now, 200.0, -122.0,
                        40.0, 90.0, null)),
                objectMapper.writeValueAsString(new VehicleEvent(
                        UUID.randomUUID().toString(), vehicleId, now, 37.0, -300.0,
                        40.0, 90.0, "HARD_BRAKE")),
                "{\"eventId\":\"" + UUID.randomUUID() + "\",\"vehicleId\":\"\"}"
        );
        for (int i = 0; i < poison.size(); i++) {
            producer.publishRaw(vehicleId, poison.get(i)).get(10, TimeUnit.SECONDS);
        }

        for (int i = 50; i < 100; i++) {
            producer.publish(valid.get(i)).get(10, TimeUnit.SECONDS);
        }

        awaitRows(rowsBefore + 100, Duration.ofSeconds(90));
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(deadLetters.countAll()).isEqualTo(dlqBefore + 5));
        awaitLagZero(Duration.ofSeconds(90));

        List<DeadLetterEvent> recent = deadLetters.findRecent(0, 10);
        assertThat(recent).hasSizeGreaterThanOrEqualTo(5);
        assertThat(recent.stream().limit(5)).allSatisfy(row -> {
            assertThat(row.sourceTopic()).isEqualTo(topic);
            assertThat(row.partitionId()).isBetween(0, 2);
            assertThat(row.sourceOffset()).isGreaterThanOrEqualTo(0);
            assertThat(row.errorClass()).isNotBlank();
            assertThat(row.failureReason()).isNotBlank();
            assertThat(row.rawPayload()).isNotBlank();
            assertThat(row.retryCount()).isZero();
        });

        // The records after the poison pills committed on the same partition.
        assertThat(vehicleEvents.countAll()).isEqualTo(rowsBefore + 100);
    }

    @Test
    @Order(4)
    void replayOfConsumedOffsetsAddsNoRows() throws Exception {
        awaitLagZero(Duration.ofSeconds(60));
        long rowsBefore = vehicleEvents.countAll();
        long duplicatesBefore = stats.snapshot().duplicatesSkipped();
        long endOffset = endOffset(0);
        assertThat(endOffset).isGreaterThan(0);

        int maxRecords = 1000;
        int expectedRead = (int) Math.min(maxRecords, endOffset);
        ReplayService.ReplaySummary summary =
                replayService.replay(topic, 0, 0, maxRecords);

        assertThat(summary.recordsRead()).isEqualTo(expectedRead);
        assertThat(summary.recordsRepublished()).isEqualTo(expectedRead);
        assertThat(summary.skipped()).isZero();

        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(
                        stats.snapshot().duplicatesSkipped()).isGreaterThan(duplicatesBefore));
        awaitLagZero(Duration.ofSeconds(120));

        assertThat(vehicleEvents.countAll()).isEqualTo(rowsBefore);
        assertThat(stats.snapshot().duplicatesSkipped()).isGreaterThan(duplicatesBefore);
    }

    @Test
    @Order(5)
    void lagBuildsWhilePausedAndDrainsAfterResume() throws Exception {
        awaitLagZero(Duration.ofSeconds(60));
        long rowsBefore = vehicleEvents.countAll();

        MessageListenerContainer container = listenerRegistry.getListenerContainer("vehicleEventConsumer");
        assertThat(container).isNotNull();
        container.pause();
        try {
            String vehicleId = "veh-lag-" + UUID.randomUUID().toString().substring(0, 8);
            List<VehicleEvent> burst = new ArrayList<>();
            Instant now = Instant.now().minusSeconds(1);
            for (int i = 0; i < 300; i++) {
                burst.add(event(vehicleId, now.plusMillis(i), 60.0));
            }
            publishAll(burst);

            await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> assertThat(lagService.lag().totalLag())
                            .isGreaterThanOrEqualTo(300));

            mockMvc.perform(get("/api/stats/lag"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.totalLag", greaterThanOrEqualTo(300)))
                    .andExpect(jsonPath("$.groupState").isNotEmpty());
        } finally {
            container.resume();
        }

        awaitLagZero(Duration.ofSeconds(120));
        awaitRows(rowsBefore + 300, Duration.ofSeconds(120));
        assertThat(vehicleEvents.countAll()).isEqualTo(rowsBefore + 300);
    }

    @Test
    @Order(6)
    void apiServesRealHistoryAndPipelineHealth() throws Exception {
        awaitLagZero(Duration.ofSeconds(60));
        String vehicleId = "veh-api-" + UUID.randomUUID().toString().substring(0, 8);
        Instant base = Instant.now().minusSeconds(30);

        publishAll(List.of(
                event(vehicleId, base, 10.0),
                event(vehicleId, base.plusSeconds(1), 20.0),
                event(vehicleId, base.plusSeconds(2), 30.0)));

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(
                        vehicleEvents.findHistory(vehicleId, base.minusSeconds(1),
                                base.plusSeconds(5), 10)).hasSize(3));

        mockMvc.perform(get("/api/vehicles/{vehicleId}/history", vehicleId)
                        .param("from", base.minusSeconds(1).toString())
                        .param("to", base.plusSeconds(5).toString())
                        .param("limit", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(3))
                .andExpect(jsonPath("$.events[0].speedKph").value(30.0))
                .andExpect(jsonPath("$.events[1].speedKph").value(20.0))
                .andExpect(jsonPath("$.events[2].speedKph").value(10.0));

        mockMvc.perform(get("/api/health/pipeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.healthy").value(true))
                .andExpect(jsonPath("$.kafkaReachable").value(true))
                .andExpect(jsonPath("$.databaseReachable").value(true));

        mockMvc.perform(get("/api/events/{eventId}", "definitely-not-an-event"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404));
    }

    private VehicleEvent event(String vehicleId, Instant eventTime, double speedKph) {
        return new VehicleEvent(
                UUID.randomUUID().toString(),
                vehicleId,
                eventTime,
                37.7749,
                -122.4194,
                speedKph,
                180.0,
                null);
    }

    private void publishAll(List<VehicleEvent> events) throws Exception {
        List<CompletableFuture<SendResult<String, String>>> futures = events.stream()
                .map(producer::publish)
                .toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .orTimeout(60, TimeUnit.SECONDS)
                .join();
    }

    private void awaitRows(long expected, Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(vehicleEvents.countAll())
                        .isGreaterThanOrEqualTo(expected));
    }

    private void awaitLagZero(Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(lagService.lag().totalLag()).isZero());
    }

    private long endOffset(int partition) throws Exception {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Map<TopicPartition, OffsetSpec> request = Map.of(topicPartition, OffsetSpec.latest());
        return adminClient.listOffsets(request).all().get(30, TimeUnit.SECONDS)
                .get(topicPartition).offset();
    }
}
