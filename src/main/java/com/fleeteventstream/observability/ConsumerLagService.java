package com.fleeteventstream.observability;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Computes consumer lag from the broker's committed offsets and end offsets. This is the
 * same data kafka-consumer-groups.sh --describe reports, exposed for the dashboard.
 */
@Service
public class ConsumerLagService {

    private final AdminClient adminClient;
    private final String topic;
    private final String groupId;

    public ConsumerLagService(AdminClient adminClient,
                              @Value("${fleet.kafka.topics.telemetry}") String topic,
                              @Value("${fleet.kafka.consumer.group-id}") String groupId) {
        this.adminClient = adminClient;
        this.topic = topic;
        this.groupId = groupId;
    }

    public LagReport lag() {
        Map<String, TopicDescription> topicDescriptions = await(
                adminClient.describeTopics(List.of(topic)).allTopicNames());
        TopicDescription description = topicDescriptions.get(topic);
        if (description == null) {
            throw new IllegalStateException("Topic not found: " + topic);
        }

        List<TopicPartition> partitions = new ArrayList<>();
        for (int i = 0; i < description.partitions().size(); i++) {
            partitions.add(new TopicPartition(topic, i));
        }

        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets();
        Map<TopicPartition, OffsetSpec> offsetSpecs = new LinkedHashMap<>();
        for (TopicPartition partition : partitions) {
            offsetSpecs.put(partition, OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsetInfo =
                await(adminClient.listOffsets(offsetSpecs).all());
        Map<TopicPartition, Long> endOffsets = new LinkedHashMap<>();
        endOffsetInfo.forEach((partition, info) -> endOffsets.put(partition, info.offset()));

        long totalLag = 0;
        List<PartitionLag> partitionLags = new ArrayList<>();
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata metadata = committed.get(partition);
            long committedOffset = metadata == null ? 0 : Math.max(0, metadata.offset());
            long endOffset = endOffsets.getOrDefault(partition, committedOffset);
            long lag = Math.max(0, endOffset - committedOffset);
            totalLag += lag;
            partitionLags.add(new PartitionLag(partition.partition(), committedOffset, endOffset, lag));
        }

        GroupInfo groupInfo = groupInfo();
        return new LagReport(groupId, topic, totalLag, groupInfo.state(), groupInfo.members(),
                partitionLags);
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
        try {
            return await(adminClient.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata());
        } catch (RuntimeException e) {
            if (rootCause(e) instanceof GroupIdNotFoundException) {
                return Map.of();
            }
            throw e;
        }
    }

    private GroupInfo groupInfo() {
        try {
            Map<String, ConsumerGroupDescription> groups = await(
                    adminClient.describeConsumerGroups(List.of(groupId)).all());
            ConsumerGroupDescription description = groups.get(groupId);
            if (description == null) {
                return new GroupInfo("EMPTY", 0);
            }
            return new GroupInfo(description.state().toString(), description.members().size());
        } catch (RuntimeException e) {
            if (rootCause(e) instanceof GroupIdNotFoundException) {
                return new GroupInfo("EMPTY", 0);
            }
            throw e;
        }
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> T await(org.apache.kafka.common.KafkaFuture<T> future) {
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while querying Kafka", e);
        } catch (ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Kafka admin request failed: " + e.getMessage(), e);
        }
    }

    private record GroupInfo(String state, int members) {
    }

    public record PartitionLag(int partition, long committedOffset, long endOffset, long lag) {
    }

    public record LagReport(String groupId, String topic, long totalLag, String groupState,
                            int members, List<PartitionLag> partitions) {
    }
}
