package com.fleeteventstream.config.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "fleet.kafka")
public record FleetKafkaProperties(
        @Valid @NotNull Topics topics,
        @Valid @NotNull Consumer consumer,
        @Valid @NotNull Retry retry) {

    public record Topics(
            @NotBlank String telemetry,
            @NotBlank String retry,
            @NotBlank String dlq) {
    }

    public record Consumer(
            @NotBlank String groupId,
            @NotBlank String replayGroupId,
            @Min(1) @Max(32) int concurrency,
            @Min(256) @Max(10_000_000) int maxPayloadChars) {
    }

    public record Retry(
            @Min(1) @Max(20) int maxAttempts,
            @Min(10) @Max(60_000) long initialIntervalMs,
            @DecimalMin("1.0") @DecimalMax("10.0") double multiplier) {
    }
}
