package com.fleeteventstream.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Instant;

/**
 * A single V2X telemetry sample. Field names are the JSON contract published to Kafka.
 */
public record VehicleEvent(
        @NotBlank @Size(max = 64) String eventId,
        @NotBlank @Size(max = 32) String vehicleId,
        @NotNull @JsonFormat(shape = JsonFormat.Shape.STRING) Instant eventTime,
        @NotNull @DecimalMin("-90.0") @DecimalMax("90.0") Double latitude,
        @NotNull @DecimalMin("-180.0") @DecimalMax("180.0") Double longitude,
        @NotNull @DecimalMin("0.0") Double speedKph,
        @NotNull @DecimalMin("0.0") @DecimalMax("360.0") Double headingDeg,
        @Pattern(regexp = "[A-Z][A-Z0-9_]{0,31}", message = "must be an upper-case alert code") String alertCode) {

    public static final String HARD_BRAKE = "HARD_BRAKE";
    public static final String SPEEDING = "SPEEDING";
    public static final String SIGNAL_PHASE_WARNING = "SIGNAL_PHASE_WARNING";
    public static final String LANE_DEPARTURE = "LANE_DEPARTURE";
    public static final String OBSTACLE_DETECTED = "OBSTACLE_DETECTED";

    public VehicleEvent withEventId(String newEventId) {
        return new VehicleEvent(newEventId, vehicleId, eventTime, latitude, longitude, speedKph, headingDeg, alertCode);
    }
}
