package com.fleeteventstream.api;

import com.fleeteventstream.observability.ConsumerLagService;
import com.fleeteventstream.observability.IngestionStats;
import com.fleeteventstream.observability.PipelineHealthService;
import com.fleeteventstream.persistence.DeadLetterRepository;
import com.fleeteventstream.persistence.VehicleEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = StatsController.class)
@Import(ApiExceptionHandler.class)
class StatsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private IngestionStats ingestionStats;
    @MockBean
    private VehicleEventRepository vehicleEventRepository;
    @MockBean
    private DeadLetterRepository deadLetterRepository;
    @MockBean
    private ConsumerLagService consumerLagService;
    @MockBean
    private PipelineHealthService pipelineHealthService;

    @Test
    void throughputReturnsBuckets() throws Exception {
        Instant bucket = Instant.parse("2026-10-18T00:00:05Z");
        when(vehicleEventRepository.throughput(any())).thenReturn(List.of(
                new VehicleEventRepository.ThroughputPoint(bucket, 40),
                new VehicleEventRepository.ThroughputPoint(bucket.plusSeconds(1), 60)));

        mockMvc.perform(get("/api/stats/throughput").param("windowSeconds", "60"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.windowSeconds").value(60))
                .andExpect(jsonPath("$.totalEvents").value(100))
                .andExpect(jsonPath("$.peakEventsPerSecond").value(60))
                .andExpect(jsonPath("$.points.length()").value(2));
    }

    @Test
    void throughputRejectsInvalidWindow() throws Exception {
        mockMvc.perform(get("/api/stats/throughput").param("windowSeconds", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void lagReturnsPartitionDetails() throws Exception {
        when(consumerLagService.lag()).thenReturn(new ConsumerLagService.LagReport(
                "fleet-event-consumers", "vehicle.telemetry", 17, "Stable", 3,
                List.of(new ConsumerLagService.PartitionLag(0, 100, 117, 17),
                        new ConsumerLagService.PartitionLag(1, 50, 50, 0),
                        new ConsumerLagService.PartitionLag(2, 10, 10, 0))));

        mockMvc.perform(get("/api/stats/lag"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.groupId").value("fleet-event-consumers"))
                .andExpect(jsonPath("$.totalLag").value(17))
                .andExpect(jsonPath("$.groupState").value("Stable"))
                .andExpect(jsonPath("$.partitions[0].lag").value(17));
    }

    @Test
    void dlqStatsReturnsReasons() throws Exception {
        when(deadLetterRepository.countAll()).thenReturn(12L);
        when(deadLetterRepository.countSince(any())).thenReturn(4L);
        when(deadLetterRepository.topFailureReasons(5)).thenReturn(List.of(
                new DeadLetterRepository.FailureReasonCount("Malformed telemetry JSON", 9)));

        mockMvc.perform(get("/api/stats/dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(12))
                .andExpect(jsonPath("$.lastHour").value(4))
                .andExpect(jsonPath("$.topFailureReasons[0].count").value(9));
    }

    @Test
    void healthReturns200WhenAllComponentsAreUp() throws Exception {
        when(pipelineHealthService.check()).thenReturn(new PipelineHealthService.PipelineHealth(
                true, "UP", true, true, 0L, 5000, true, "Stable",
                Instant.now(), Map.of()));

        mockMvc.perform(get("/api/health/pipeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.healthy").value(true))
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void healthReturns503WhenLagExceedsThreshold() throws Exception {
        when(pipelineHealthService.check()).thenReturn(new PipelineHealthService.PipelineHealth(
                false, "DEGRADED", true, true, 9000L, 5000, false, "Stable",
                Instant.now(), Map.of("lag", "total lag 9000 exceeds 5000")));

        mockMvc.perform(get("/api/health/pipeline"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.healthy").value(false))
                .andExpect(jsonPath("$.status").value("DEGRADED"))
                .andExpect(jsonPath("$.totalLag").value(9000));
    }

    @Test
    void latencyReturnsPercentiles() throws Exception {
        when(vehicleEventRepository.latencyPercentiles(any()))
                .thenReturn(new VehicleEventRepository.LatencyPercentiles(12.5, 40.0, 88.0));

        mockMvc.perform(get("/api/stats/latency").param("windowSeconds", "300"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.windowSeconds").value(300))
                .andExpect(jsonPath("$.latency.p50Ms").value(12.5))
                .andExpect(jsonPath("$.latency.p95Ms").value(40.0))
                .andExpect(jsonPath("$.latency.p99Ms").value(88.0));
    }

    @Test
    void pipelineReturnsCounters() throws Exception {        when(ingestionStats.snapshot()).thenReturn(new IngestionStats.Snapshot(100, 90, 10, 2, 2));
        when(vehicleEventRepository.countAll()).thenReturn(90L);
        when(deadLetterRepository.countAll()).thenReturn(2L);

        mockMvc.perform(get("/api/stats/pipeline"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consumed").value(100))
                .andExpect(jsonPath("$.duplicatesSkipped").value(10))
                .andExpect(jsonPath("$.vehicleRows").value(90));
    }
}
