package com.fleeteventstream.persistence;

import java.time.Instant;

public record StoredVehicleEvent(
        long id,
        String eventId,
        String vehicleId,
        Instant eventTime,
        double latitude,
        double longitude,
        double speedKph,
        double headingDeg,
        String alertCode,
        String payload,
        Instant ingestedAt) {
}
