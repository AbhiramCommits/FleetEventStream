package com.fleeteventstream.observability;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process counters for the ingestion path. Micrometer meters are layered on top in the
 * observability pass; these raw counters are also cheap to expose as JSON.
 */
@Component
public class IngestionStats {

    private final AtomicLong consumed = new AtomicLong();
    private final AtomicLong persisted = new AtomicLong();
    private final AtomicLong duplicatesSkipped = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong dlqStored = new AtomicLong();

    public void recordConsumed() {
        consumed.incrementAndGet();
    }

    public void recordPersisted() {
        persisted.incrementAndGet();
    }

    public void recordDuplicate() {
        duplicatesSkipped.incrementAndGet();
    }

    public void recordFailed() {
        failed.incrementAndGet();
    }

    public void recordDlqStored() {
        dlqStored.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(consumed.get(), persisted.get(), duplicatesSkipped.get(),
                failed.get(), dlqStored.get());
    }

    public record Snapshot(long consumed, long persisted, long duplicatesSkipped,
                           long failed, long dlqStored) {
    }
}
