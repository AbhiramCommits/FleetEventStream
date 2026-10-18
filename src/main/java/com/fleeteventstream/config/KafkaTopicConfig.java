package com.fleeteventstream.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic vehicleTelemetryTopic(
            @Value("${fleet.kafka.topics.telemetry}") String topic) {
        return TopicBuilder.name(topic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic vehicleTelemetryRetryTopic(
            @Value("${fleet.kafka.topics.retry}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic vehicleTelemetryDlqTopic(
            @Value("${fleet.kafka.topics.dlq}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }
}
