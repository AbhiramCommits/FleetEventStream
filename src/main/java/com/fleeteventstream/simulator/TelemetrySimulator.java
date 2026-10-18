package com.fleeteventstream.simulator;

import com.fleeteventstream.config.properties.SimulatorProperties;
import com.fleeteventstream.kafka.TelemetryProducer;
import com.fleeteventstream.model.VehicleEvent;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates a steady stream of simulated connected-vehicle telemetry.
 *
 * <p>The scheduler ticks every 10 ms and emits the number of events that <em>should</em>
 * have been emitted for the elapsed wall-clock time. That keeps the configured rate
 * accurate even when the JVM or Kafka is briefly busy, instead of emitting one event per
 * fixed delay (which silently drifts under load).</p>
 */
@Service
public class TelemetrySimulator {

    private static final Logger log = LoggerFactory.getLogger(TelemetrySimulator.class);
    private static final long TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

    private final TelemetryProducer producer;
    private final SimulatorProperties properties;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong attempted = new AtomicLong();
    private final AtomicLong malformed = new AtomicLong();

    private volatile int eventsPerSecond;
    private volatile int vehicleCount;
    private volatile double malformedRate;
    private volatile int durationSeconds;
    private volatile Instant startedAt;
    private volatile Instant stoppedAt;
    private volatile Instant endsAt;
    private volatile long startNanos;
    private volatile long publishedBaseline;
    private volatile long failedBaseline;

    private volatile ScheduledExecutorService executor;
    private volatile List<VehicleState> vehicles = List.of();

    public TelemetrySimulator(TelemetryProducer producer, SimulatorProperties properties) {
        this.producer = producer;
        this.properties = properties;
        this.eventsPerSecond = properties.eventsPerSecond();
        this.vehicleCount = properties.vehicleCount();
        this.malformedRate = properties.malformedRate();
        this.durationSeconds = properties.durationSeconds();
    }

    public synchronized SimulatorStatus start(int eventsPerSecond, int vehicleCount, int durationSeconds) {
        return start(eventsPerSecond, vehicleCount, durationSeconds, properties.malformedRate());
    }

    public synchronized SimulatorStatus start(int eventsPerSecond, int vehicleCount,
                                              int durationSeconds, double malformedRate) {
        if (eventsPerSecond < 1 || eventsPerSecond > 50_000) {
            throw new IllegalArgumentException("eventsPerSecond must be between 1 and 50000");
        }
        if (vehicleCount < 1 || vehicleCount > 10_000) {
            throw new IllegalArgumentException("vehicleCount must be between 1 and 10000");
        }
        if (durationSeconds < 0) {
            throw new IllegalArgumentException("durationSeconds must be >= 0 (0 means run until stopped)");
        }
        if (malformedRate < 0.0 || malformedRate > 1.0) {
            throw new IllegalArgumentException("malformedRate must be between 0.0 and 1.0");
        }
        stopInternal();

        this.eventsPerSecond = eventsPerSecond;
        this.vehicleCount = vehicleCount;
        this.durationSeconds = durationSeconds;
        this.malformedRate = malformedRate;
        this.attempted.set(0);
        this.malformed.set(0);
        this.publishedBaseline = producer.getPublishedCount();
        this.failedBaseline = producer.getFailedCount();
        this.startedAt = Instant.now();
        this.stoppedAt = null;
        this.endsAt = durationSeconds > 0 ? startedAt.plusSeconds(durationSeconds) : null;
        this.startNanos = System.nanoTime();

        List<VehicleState> states = new ArrayList<>(vehicleCount);
        for (int i = 0; i < vehicleCount; i++) {
            states.add(VehicleState.forIndex(i));
        }
        this.vehicles = states;
        this.running.set(true);

        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "telemetry-simulator");
            thread.setDaemon(true);
            return thread;
        };
        this.executor = Executors.newSingleThreadScheduledExecutor(factory);
        this.executor.scheduleAtFixedRate(this::tick, TICK_NANOS, TICK_NANOS, TimeUnit.NANOSECONDS);

        log.info("Simulator started: {} events/sec, {} vehicles, malformedRate={}, duration={}s",
                eventsPerSecond, vehicleCount, malformedRate, durationSeconds);
        return status();
    }

    private void tick() {
        if (!running.get()) {
            return;
        }
        try {
            long elapsedNanos = System.nanoTime() - startNanos;
            long target = (long) ((elapsedNanos / (double) TimeUnit.SECONDS.toNanos(1)) * eventsPerSecond);
            long toEmit = target - attempted.get();
            for (long i = 0; i < toEmit; i++) {
                emitOne();
            }
            if (endsAt != null && !Instant.now().isBefore(endsAt)) {
                log.info("Simulator reached configured duration of {}s", durationSeconds);
                stopInternal();
            }
        } catch (RuntimeException e) {
            log.error("Simulator tick failed", e);
        }
    }

    private void emitOne() {
        VehicleState state = vehicles.get((int) (attempted.get() % vehicles.size()));
        VehicleEvent event = state.nextEvent();
        attempted.incrementAndGet();
        if (Math.random() < malformedRate) {
            malformed.incrementAndGet();
            // Deliberately truncated JSON: the consumer will fail to parse it and route to the DLQ.
            String broken = String.format(Locale.ROOT,
                    "{\"eventId\":\"%s\",\"vehicleId\":\"%s\",\"eventTime\":\"%s\",\"latitude\":",
                    event.eventId(), event.vehicleId(), event.eventTime());
            producer.publishRaw(event.vehicleId(), broken);
        } else {
            producer.publish(event);
        }
    }

    public synchronized SimulatorStatus stop() {
        stopInternal();
        return status();
    }

    private void stopInternal() {
        boolean wasRunning = running.getAndSet(false);
        if (wasRunning) {
            stoppedAt = Instant.now();
        }
        ScheduledExecutorService current = executor;
        if (current != null) {
            current.shutdownNow();
            // When called from the scheduler's own tick (duration reached), waiting would
            // deadlock the worker until the timeout.
            if (!Thread.currentThread().getName().equals("telemetry-simulator")) {
                try {
                    current.awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            executor = null;
        }
    }

    @PreDestroy
    public void shutdown() {
        stopInternal();
        log.info("Simulator executor stopped");
    }

    public boolean isRunning() {
        return running.get();
    }

    public SimulatorStatus status() {
        Instant start = startedAt;
        boolean isRunning = running.get();
        Instant observedEnd = !isRunning && stoppedAt != null ? stoppedAt : Instant.now();
        long elapsed = start == null ? 0 : java.time.Duration.between(start, observedEnd).getSeconds();
        long published = producer.getPublishedCount() - publishedBaseline;
        long failed = producer.getFailedCount() - failedBaseline;
        long activeSeconds = start == null ? 0 : Math.max(1,
                java.time.Duration.between(start, observedEnd).getSeconds());
        double actualRate = start == null ? 0.0 : attempted.get() / (double) activeSeconds;
        return new SimulatorStatus(
                isRunning,
                eventsPerSecond,
                vehicleCount,
                malformedRate,
                durationSeconds,
                attempted.get(),
                Math.max(0, published),
                Math.max(0, failed),
                malformed.get(),
                actualRate,
                start,
                endsAt,
                elapsed);
    }

    public void setMalformedRate(double rate) {
        this.malformedRate = rate;
    }

    private static final class VehicleState {
        private final String vehicleId;
        private final double baseLatitude;
        private final double baseLongitude;
        private double latitude;
        private double longitude;
        private double speedKph;
        private double headingDeg;

        private VehicleState(String vehicleId, double latitude, double longitude,
                             double speedKph, double headingDeg) {
            this.vehicleId = vehicleId;
            this.baseLatitude = latitude;
            this.baseLongitude = longitude;
            this.latitude = latitude;
            this.longitude = longitude;
            this.speedKph = speedKph;
            this.headingDeg = headingDeg;
        }

        static VehicleState forIndex(int index) {
            // A loose route around San Francisco; each vehicle gets its own lane offset.
            double latitude = 37.7749 + (index % 7) * 0.0012;
            double longitude = -122.4194 - (index % 11) * 0.0015;
            double heading = (index * 37.0) % 360.0;
            double speed = 25.0 + (index % 9) * 6.0;
            return new VehicleState("veh-" + String.format(Locale.ROOT, "%04d", index + 1),
                    latitude, longitude, speed, heading);
        }

        VehicleEvent nextEvent() {
            double jitter = (Math.random() - 0.5) * 6.0;
            speedKph = clamp(speedKph + jitter, 0.0, 140.0);
            headingDeg = (headingDeg + (Math.random() - 0.5) * 8.0 + 360.0) % 360.0;
            double distanceKm = speedKph / 3600.0 * 0.01; // 10 ms of travel
            double radians = Math.toRadians(headingDeg);
            latitude = clamp(latitude + distanceKm / 111.0 * Math.cos(radians), -89.9, 89.9);
            longitude = clamp(longitude + distanceKm / (111.0 * Math.cos(Math.toRadians(latitude)))
                    * Math.sin(radians), -179.9, 179.9);

            // Keep the fleet from wandering away over long runs.
            latitude = latitude + (baseLatitude - latitude) * 0.0005;
            longitude = longitude + (baseLongitude - longitude) * 0.0005;

            String alert = null;
            double roll = Math.random();
            if (roll < 0.008) {
                alert = VehicleEvent.HARD_BRAKE;
                speedKph = Math.max(0.0, speedKph - 15.0);
            } else if (roll < 0.014) {
                alert = VehicleEvent.SPEEDING;
            } else if (roll < 0.018) {
                alert = VehicleEvent.SIGNAL_PHASE_WARNING;
            } else if (roll < 0.021) {
                alert = VehicleEvent.LANE_DEPARTURE;
            } else if (roll < 0.023) {
                alert = VehicleEvent.OBSTACLE_DETECTED;
            }

            return new VehicleEvent(
                    java.util.UUID.randomUUID().toString(),
                    vehicleId,
                    Instant.now(),
                    round(latitude, 6),
                    round(longitude, 6),
                    round(speedKph, 2),
                    round(headingDeg, 2),
                    alert);
        }

        private static double clamp(double value, double min, double max) {
            return Math.max(min, Math.min(max, value));
        }

        private static double round(double value, int places) {
            double factor = Math.pow(10, places);
            return Math.round(value * factor) / factor;
        }
    }

    public record SimulatorStatus(
            boolean running,
            int targetEventsPerSecond,
            int vehicleCount,
            double malformedRate,
            int durationSeconds,
            long eventsAttempted,
            long eventsPublished,
            long eventsFailed,
            long malformedPublished,
            double actualEventsPerSecond,
            Instant startedAt,
            Instant endsAt,
            long elapsedSeconds) {
    }
}
