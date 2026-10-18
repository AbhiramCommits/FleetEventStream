package com.fleeteventstream.api;

import org.springframework.http.HttpStatus;

public class EventNotFoundException extends RuntimeException {

    public EventNotFoundException(String message) {
        super(message);
    }

    public static EventNotFoundException forEvent(String eventId) {
        return new EventNotFoundException("No event found with eventId=" + eventId);
    }

    public HttpStatus status() {
        return HttpStatus.NOT_FOUND;
    }
}
