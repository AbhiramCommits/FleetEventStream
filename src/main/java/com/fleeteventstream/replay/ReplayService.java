package com.fleeteventstream.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleeteventstream.persistence.DeadLetterEvent;
import com.fleeteventstream.persistence.DeadLetterRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Replays records from an explicit offset using a manually assigned consumer. The consumer
 * uses a throwaway group id and never commits, so replaying never disturbs the live
 * consumer group's progress.
 */
@Service
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final DeadLetterRepository deadLetterRepository;
    private final String bootstrapServers;
    private final String replayGroupId;
    private final String mainTopic;
    private final Set<String> replayableTopics;
    private final int configuredMaxRecords;

    public ReplayService(KafkaTemplate<String, String> kafkaTemplate,
                         ObjectMapper objectMapper,
                         DeadLetterRepository deadLetterRepository,
                         @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                         @Value("${fleet.kafka.consumer.replay-group-id:fleet-event-replay}") String replayGroupId,
                         @Value("${fleet.kafka.topics.telemetry}") String mainTopic,
                         @Value("${fleet.kafka.topics.retry}") String retryTopic,
                         @Value("${fleet.kafka.topics.dlq}") String dlqTopic,
                         @Value("${fleet.replay.max-records:100000}") int configuredMaxRecords) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.deadLetterRepository = deadLetterRepository;
        this.bootstrapServers = bootstrapServers;
        this.replayGroupId = replayGroupId;
        this.mainTopic = mainTopic;
        this.replayableTopics = Set.of(mainTopic, retryTopic, dlqTopic);
        this.configuredMaxRecords = configuredMaxRecords;
    }

    public ReplaySummary replay(String topic, int partition, long fromOffset, int maxRecords) {
        if (!replayableTopics.contains(topic)) {
            throw new IllegalArgumentException("Topic is not replayable: " + topic);
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be >= 0");
        }
        if (fromOffset < 0) {
            throw new IllegalArgumentException("fromOffset must be >= 0");
        }
        if (maxRecords < 1 || maxRecords > configuredMaxRecords) {
            throw new IllegalArgumentException(
                    "maxRecords must be between 1 and " + configuredMaxRecords);
        }

        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                replayGroupId + "-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "fleet-replay-" + UUID.randomUUID());

        TopicPartition topicPartition = new TopicPartition(topic, partition);
        long recordsRead = 0;
        long republished = 0;
        long skipped = 0;
        long endOffset;

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(topicPartition));
            endOffset = consumer.endOffsets(List.of(topicPartition)).get(topicPartition);
            consumer.seek(topicPartition, Math.min(fromOffset, endOffset));

            int emptyPolls = 0;
            while (recordsRead < maxRecords && consumer.position(topicPartition) < endOffset) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    if (++emptyPolls >= 20) {
                        log.warn("Replay stopped after repeated empty polls at position {}",
                                consumer.position(topicPartition));
                        break;
                    }
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, String> record : records) {
                    if (recordsRead >= maxRecords) {
                        break;
                    }
                    recordsRead++;
                    if (record.value() == null) {
                        skipped++;
                        continue;
                    }
                    try {
                        kafkaTemplate.send(mainTopic, record.key(), record.value())
                                .get(10, TimeUnit.SECONDS);
                        republished++;
                    } catch (Exception e) {
                        skipped++;
                        log.warn("Replay skipped {}-{}@{}: {}", topic, partition, record.offset(),
                                e.toString());
                    }
                }
            }

            // Join the consumer group explicitly so the broker releases the throwaway member
            // immediately instead of waiting for the session timeout.
            try {
                consumer.commitSync();
            } catch (RuntimeException ignored) {
                // No offsets were committed; this is only a best-effort group cleanup.
            }
        }

        log.info("Replay topic={} partition={} fromOffset={} read={} republished={} skipped={} endOffset={}",
                topic, partition, fromOffset, recordsRead, republished, skipped, endOffset);
        return new ReplaySummary(topic, partition, fromOffset, recordsRead, republished, skipped, endOffset);
    }

    public DlqReplaySummary replayDeadLetters(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("ids must not be empty");
        }
        List<Long> uniqueIds = new ArrayList<>(new LinkedHashSet<>(ids));
        List<DeadLetterEvent> rows = deadLetterRepository.findByIds(uniqueIds);
        long republished = 0;
        long skipped = 0;
        for (DeadLetterEvent row : rows) {
            String payload = row.rawPayload();
            if (payload == null || payload.isBlank()) {
                skipped++;
                continue;
            }
            String key = extractVehicleKey(payload);
            try {
                kafkaTemplate.send(mainTopic, key, payload).get(10, TimeUnit.SECONDS);
                republished++;
            } catch (Exception e) {
                skipped++;
                log.warn("DLQ replay skipped id={}: {}", row.id(), e.toString());
            }
        }
        long missing = uniqueIds.size() - rows.size();
        log.info("DLQ replay requested={} found={} republished={} skipped={} missing={}",
                uniqueIds.size(), rows.size(), republished, skipped, missing);
        return new DlqReplaySummary(uniqueIds.size(), rows.size(), republished, skipped, missing);
    }

    private String extractVehicleKey(String payload) {
        try {
            JsonNode node = objectMapper.readTree(payload);
            JsonNode vehicleId = node.get("vehicleId");
            return vehicleId == null ? null : vehicleId.asText();
        } catch (Exception e) {
            return null;
        }
    }

    public record ReplaySummary(String topic, int partition, long fromOffset, long recordsRead,
                                long recordsRepublished, long skipped, long endOffset) {
    }

    public record DlqReplaySummary(long requested, long found, long republished, long skipped,
                                   long missing) {
    }
}
