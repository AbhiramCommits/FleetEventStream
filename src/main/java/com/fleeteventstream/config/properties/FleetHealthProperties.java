package com.fleeteventstream.config.properties;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "fleet.health")
public record FleetHealthProperties(@Min(0) @Max(10_000_000) long maxLag) {
}
