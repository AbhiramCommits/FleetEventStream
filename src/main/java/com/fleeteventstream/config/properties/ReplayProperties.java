package com.fleeteventstream.config.properties;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "fleet.replay")
public record ReplayProperties(@Min(1) @Max(1_000_000) int maxRecords) {
}
