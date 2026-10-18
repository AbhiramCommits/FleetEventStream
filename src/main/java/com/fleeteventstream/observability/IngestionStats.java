package com.fleeteventstream.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process counters for the ingestion path, mirrored to Micrometer so the same numbers are
 * available on /actuator/metrics and /actuator/prometheus.
 */
@Component
public class IngestionStats {

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong persisted = new AtomicLong();
    private final AtomicLong duplicatesSkipped = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong dlqStored = new AtomicLong();

    private final Counter consumedCounter;
    private final Counter persistedCounter;
    private final Counter duplicatesCounter;
    private final Counter failedCounter;
    private final Counter dlqStoredCounter;
    private final Counter dlqPublishedCounter;
    private final Counter dlqDuplicatesCounter;
    private final Timer processingTimer;

    public IngestionStats(MeterRegistry registry) {
        this.consumedCounter = Counter.builder("fleet.events.consumed")
                .description("Telemetry records delivered to the consumer")
                .register(registry);
        this.persistedCounter = Counter.builder("fleet.events.persisted")
                .description("Telemetry records inserted as new rows")
                .register(registry);
        this.duplicatesCounter = Counter.builder("fleet.events.duplicates")
                .description("Telemetry records skipped because event_id already existed")
                .register(registry);
        this.failedCounter = Counter.builder("fleet.events.failed")
                .description("Telemetry records whose processing attempt failed")
                .register(registry);
        this.dlqStoredCounter = Counter.builder("fleet.dlq.stored")
                .description("Dead-letter rows stored in SQL Server")
                .register(registry);
        this.dlqPublishedCounter = Counter.builder("fleet.dlq.published")
                .description("Records published to the Kafka DLQ topic")
                .register(registry);
        this.dlqDuplicatesCounter = Counter.builder("fleet.dlq.duplicates")
                .description("Dead-letter redeliveries suppressed by the unique source index")
                .register(registry);
        this.processingTimer = Timer.builder("fleet.events.processing")
                .description("Time spent parsing, validating, and persisting one telemetry record")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public void recordConsumed() {
        consumed.incrementAndGet();
        consumedCounter.increment();
    }

    public void recordPersisted() {
        persisted.incrementAndGet();
        persistedCounter.increment();
    }

    public void recordDuplicate() {
        duplicatesSkipped.incrementAndGet();
        duplicatesCounter.increment();
    }

    public void recordFailed() {
        failed.incrementAndGet();
        failedCounter.increment();
    }

    public void recordDlqStored() {
        dlqStored.incrementAndGet();
        dlqStoredCounter.increment();
    }

    public void recordDlqPublished() {
        dlqPublishedCounter.increment();
    }

    public void recordDlqDuplicate() {
        dlqDuplicatesCounter.increment();
    }

    public void recordProcessing(long startedNanos) {
        processingTimer.record(Duration.ofNanos(System.nanoTime() - startedNanos));
    }

    public Snapshot snapshot() {
        return new Snapshot(consumed.get(), persisted.get(), duplicatesSkipped.get(),
                failed.get(), dlqStored.get());
    }

    public record Snapshot(long consumed, long persisted, long duplicatesSkipped,
                           long failed, long dlqStored) {
    }
}
