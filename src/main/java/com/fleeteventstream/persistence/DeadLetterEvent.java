package com.fleeteventstream.persistence;

import java.time.Instant;

public record DeadLetterEvent(
        long id,
        String sourceTopic,
        int partitionId,
        long sourceOffset,
        String failureReason,
        String errorClass,
        String rawPayload,
        Instant failedAt,
        int retryCount) {
}
