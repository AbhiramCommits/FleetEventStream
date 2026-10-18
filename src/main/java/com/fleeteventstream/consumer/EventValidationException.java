package com.fleeteventstream.consumer;

/**
 * Raised when a Kafka record is not a valid {@link com.fleeteventstream.model.VehicleEvent}.
 * This is classified as non-retryable: no amount of retrying will fix malformed JSON or a
 * latitude of 200, so the record goes straight to the DLQ.
 */
public class EventValidationException extends RuntimeException {

    public EventValidationException(String message) {
        super(message);
    }

    public EventValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
