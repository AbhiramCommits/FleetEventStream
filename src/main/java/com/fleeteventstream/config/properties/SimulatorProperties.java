package com.fleeteventstream.config.properties;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "fleet.simulator")
public record SimulatorProperties(
        @Min(1) @Max(50_000) int eventsPerSecond,
        @Min(1) @Max(10_000) int vehicleCount,
        @DecimalMin("0.0") @DecimalMax("1.0") double malformedRate,
        @Min(0) @Max(86_400) int durationSeconds) {
}
