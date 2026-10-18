package com.fleeteventstream.api;

import com.fleeteventstream.observability.IngestionStats;
import com.fleeteventstream.persistence.DeadLetterRepository;
import com.fleeteventstream.persistence.VehicleEventRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/stats")
public class StatsController {

    private final IngestionStats ingestionStats;
    private final VehicleEventRepository vehicleEventRepository;
    private final DeadLetterRepository deadLetterRepository;

    public StatsController(IngestionStats ingestionStats,
                           VehicleEventRepository vehicleEventRepository,
                           DeadLetterRepository deadLetterRepository) {
        this.ingestionStats = ingestionStats;
        this.vehicleEventRepository = vehicleEventRepository;
        this.deadLetterRepository = deadLetterRepository;
    }

    @GetMapping("/pipeline")
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

    public record PipelineStats(long consumed, long persisted, long duplicatesSkipped,
                                long failed, long dlqStored, long vehicleRows, long deadLetterRows) {
    }
}
