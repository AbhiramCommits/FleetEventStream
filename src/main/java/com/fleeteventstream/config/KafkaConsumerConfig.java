package com.fleeteventstream.config;

import com.fleeteventstream.config.properties.FleetKafkaProperties;
import com.fleeteventstream.consumer.EventValidationException;
import com.fleeteventstream.observability.IngestionStats;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    static final String HDR_RETRY_COUNT = "x-fleet-retry-count";
    static final String HDR_FLEET_ERROR_CLASS = "x-fleet-error-class";
    static final String HDR_FLEET_FAILURE_REASON = "x-fleet-failure-reason";

    private final FleetKafkaProperties properties;
    private final IngestionStats stats;

    public KafkaConsumerConfig(FleetKafkaProperties properties, IngestionStats stats) {
        this.properties = properties;
        this.stats = stats;
    }

    /**
     * Retries with exponential backoff and then publishes to the DLQ. Validation failures are
     * non-retryable and go straight to the DLQ.
     */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        FleetKafkaProperties.Retry retry = properties.retry();
        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(Math.max(1, retry.maxAttempts()));
        backOff.setInitialInterval(retry.initialIntervalMs());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(10_000);
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(EventValidationException.class);
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                record.headers().add(HDR_RETRY_COUNT,
                        String.valueOf(Math.max(0, deliveryAttempt - 1))
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        return handler;
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, String> kafkaTemplate) {
        // The main topic has 3 partitions, the DLQ has 1; always publish to partition 0.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (ConsumerRecord<?, ?> record, Exception ex) ->
                        new TopicPartition(properties.topics().dlq(), 0));
        // The exception passed to the recoverer is usually a ListenerExecutionFailedException
        // wrapper. Record the root cause so dead_letter_event has actionable reasons.
        recoverer.addHeadersFunction((record, ex) -> {
            stats.recordDlqPublished();
            Throwable root = ex;
            while (root.getCause() != null && root.getCause() != root) {
                root = root.getCause();
            }
            Headers headers = new RecordHeaders();
            headers.add(HDR_FLEET_ERROR_CLASS,
                    root.getClass().getName().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            String message = root.getMessage() == null ? root.getClass().getSimpleName() : root.getMessage();
            headers.add(HDR_FLEET_FAILURE_REASON,
                    message.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return headers;
        });
        return recoverer;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(properties.consumer().concurrency());
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        // Let in-flight records finish and acknowledge during context shutdown.
        factory.getContainerProperties().setShutdownTimeout(25_000);
        return factory;
    }

    /**
     * DLQ persistence has its own bounded retry loop and intentionally has no recoverer:
     * a DB outage must not make the consumer publish the same record back to the DLQ forever.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> dlqListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        DefaultErrorHandler handler = new DefaultErrorHandler(new FixedBackOff(1_000, 2));
        handler.setAckAfterHandle(true);
        factory.setCommonErrorHandler(handler);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setShutdownTimeout(25_000);
        return factory;
    }
}
