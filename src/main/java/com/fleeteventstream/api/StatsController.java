package com.fleeteventstream.api;

import com.fleeteventstream.observability.IngestionStats;
import com.fleeteventstream.persistence.DeadLetterRepository;
import com.fleeteventstream.persistence.VehicleEventRepository;
import com.fleeteventstream.observability.ConsumerLagService;
import com.fleeteventstream.observability.PipelineHealthService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@Validated
@RestController
@RequestMapping("/api")
public class StatsController {

    private final IngestionStats ingestionStats;
    private final VehicleEventRepository vehicleEventRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final ConsumerLagService consumerLagService;
    private final PipelineHealthService pipelineHealthService;

    public StatsController(IngestionStats ingestionStats,
                           VehicleEventRepository vehicleEventRepository,
                           DeadLetterRepository deadLetterRepository,
                           ConsumerLagService consumerLagService,
                           PipelineHealthService pipelineHealthService) {
        this.ingestionStats = ingestionStats;
        this.vehicleEventRepository = vehicleEventRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.consumerLagService = consumerLagService;
        this.pipelineHealthService = pipelineHealthService;
    }

    @GetMapping("/stats/pipeline")
    public PipelineStats pipeline() {
        IngestionStats.Snapshot snapshot = ingestionStats.snapshot();
        return new PipelineStats(
                snapshot.consumed(),
                snapshot.persisted(),
                snapshot.duplicatesSkipped(),
                snapshot.failed(),
                snapshot.dlqStored(),
                vehicleEventRepository.countAll(),
                deadLetterRepository.countAll());
    }

    @GetMapping("/stats/throughput")
    public ThroughputResponse throughput(
            @RequestParam(defaultValue = "60") @Min(1) @Max(3600) int windowSeconds) {
        Instant to = Instant.now();
        Instant from = to.minusSeconds(windowSeconds);
        List<VehicleEventRepository.ThroughputPoint> points = vehicleEventRepository.throughput(from);
        long total = points.stream().mapToLong(VehicleEventRepository.ThroughputPoint::eventCount).sum();
        long peak = points.stream().mapToLong(VehicleEventRepository.ThroughputPoint::eventCount).max().orElse(0);
        double average = total / (double) windowSeconds;
        return new ThroughputResponse(windowSeconds, from, to, total, average, peak, points);
    }

    @GetMapping("/stats/lag")
    public ConsumerLagService.LagReport lag() {
        return consumerLagService.lag();
    }

    @GetMapping("/stats/latency")
    public LatencyResponse latency(
            @RequestParam(defaultValue = "300") @Min(1) @Max(86400) int windowSeconds,
            @RequestParam(required = false)
            @org.springframework.format.annotation.DateTimeFormat(iso = org.springframework.format.annotation.DateTimeFormat.ISO.DATE_TIME)
            Instant since) {
        Instant effectiveSince = since != null ? since : Instant.now().minusSeconds(windowSeconds);
        return new LatencyResponse(windowSeconds, effectiveSince,
                vehicleEventRepository.latencyPercentiles(effectiveSince));
    }

    @GetMapping("/stats/dlq")
    public DlqStats dlq() {
        Instant oneHourAgo = Instant.now().minusSeconds(3600);
        return new DlqStats(
                deadLetterRepository.countAll(),
                deadLetterRepository.countSince(oneHourAgo),
                oneHourAgo,
                deadLetterRepository.topFailureReasons(5));
    }

    @GetMapping("/health/pipeline")
    public ResponseEntity<PipelineHealthService.PipelineHealth> pipelineHealth() {
        PipelineHealthService.PipelineHealth health = pipelineHealthService.check();
        return ResponseEntity.status(health.healthy() ? 200 : 503).body(health);
    }

    public record PipelineStats(long consumed, long persisted, long duplicatesSkipped,
                                long failed, long dlqStored, long vehicleRows, long deadLetterRows) {
    }

    public record ThroughputResponse(int windowSeconds, Instant from, Instant to, long totalEvents,
                                     double eventsPerSecond, long peakEventsPerSecond,
                                     List<VehicleEventRepository.ThroughputPoint> points) {
    }

    public record LatencyResponse(int windowSeconds, Instant since,
                                  VehicleEventRepository.LatencyPercentiles latency) {
    }

    public record DlqStats(long total, long lastHour, Instant lastHourSince,
                           List<DeadLetterRepository.FailureReasonCount> topFailureReasons) {
    }
}
