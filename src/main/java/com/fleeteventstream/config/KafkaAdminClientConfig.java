package com.fleeteventstream.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class KafkaAdminClientConfig {

    @Bean(destroyMethod = "close")
    public AdminClient fleetAdminClient(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.CLIENT_ID_CONFIG, "fleet-api-admin",
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000));
    }
}
