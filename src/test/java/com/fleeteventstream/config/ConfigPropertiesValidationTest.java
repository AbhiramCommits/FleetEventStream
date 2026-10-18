package com.fleeteventstream.config;

import com.fleeteventstream.config.properties.FleetHealthProperties;
import com.fleeteventstream.config.properties.FleetKafkaProperties;
import com.fleeteventstream.config.properties.ReplayProperties;
import com.fleeteventstream.config.properties.SimulatorProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class ConfigPropertiesValidationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(PropertiesConfig.class)
            .withPropertyValues(
                    "fleet.kafka.topics.telemetry=vehicle.telemetry",
                    "fleet.kafka.topics.retry=vehicle.telemetry.retry",
                    "fleet.kafka.topics.dlq=vehicle.telemetry.dlq",
                    "fleet.kafka.consumer.group-id=fleet-event-consumers",
                    "fleet.kafka.consumer.replay-group-id=fleet-event-replay",
                    "fleet.kafka.consumer.concurrency=3",
                    "fleet.kafka.consumer.max-payload-chars=65536",
                    "fleet.kafka.retry.max-attempts=3",
                    "fleet.kafka.retry.initial-interval-ms=500",
                    "fleet.kafka.retry.multiplier=2.0",
                    "fleet.simulator.events-per-second=50",
                    "fleet.simulator.vehicle-count=10",
                    "fleet.simulator.malformed-rate=0.02",
                    "fleet.simulator.duration-seconds=0",
                    "fleet.health.max-lag=5000",
                    "fleet.replay.max-records=100000");

    @Test
    void bindsValidConfiguration() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(SimulatorProperties.class).eventsPerSecond()).isEqualTo(50);
            assertThat(context.getBean(FleetKafkaProperties.class).topics().telemetry())
                    .isEqualTo("vehicle.telemetry");
            assertThat(context.getBean(FleetHealthProperties.class).maxLag()).isEqualTo(5000);
            assertThat(context.getBean(ReplayProperties.class).maxRecords()).isEqualTo(100000);
        });
    }

    @Test
    void failsFastOnInvalidSimulatorRate() {
        runner.withPropertyValues("fleet.simulator.events-per-second=0")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(rootCause(context.getStartupFailure()))
                            .hasMessageContaining("eventsPerSecond");
                });
    }

    @Test
    void failsFastOnBlankTopic() {
        runner.withPropertyValues("fleet.kafka.topics.telemetry= ")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(rootCause(context.getStartupFailure()))
                            .hasMessageContaining("telemetry");
                });
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    @Configuration
    @EnableConfigurationProperties({
            FleetKafkaProperties.class, SimulatorProperties.class,
            FleetHealthProperties.class, ReplayProperties.class})
    static class PropertiesConfig {
    }
}
