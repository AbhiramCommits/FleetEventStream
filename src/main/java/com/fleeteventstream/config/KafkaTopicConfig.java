package com.fleeteventstream.config;

import com.fleeteventstream.config.properties.FleetKafkaProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    private final FleetKafkaProperties properties;

    public KafkaTopicConfig(FleetKafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    public NewTopic vehicleTelemetryTopic() {
        return TopicBuilder.name(properties.topics().telemetry())
                .partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic vehicleTelemetryRetryTopic() {
        return TopicBuilder.name(properties.topics().retry())
                .partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic vehicleTelemetryDlqTopic() {
        return TopicBuilder.name(properties.topics().dlq())
                .partitions(1).replicas(1).build();
    }
}
