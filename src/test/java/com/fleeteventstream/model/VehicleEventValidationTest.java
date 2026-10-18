package com.fleeteventstream.model;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class VehicleEventValidationTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void acceptsAValidEvent() {
        VehicleEvent event = new VehicleEvent(
                "11111111-1111-1111-1111-111111111111", "veh-0001", Instant.now(),
                37.0, -122.0, 55.0, 90.0, "SPEEDING");
        assertThat(validator.validate(event)).isEmpty();
    }

    @Test
    void rejectsOutOfRangeAndBlankFields() {
        VehicleEvent event = new VehicleEvent(
                " ", "", null,
                91.0, -181.0, -1.0, 361.0, "not-an-alert-code");
        Set<ConstraintViolation<VehicleEvent>> violations = validator.validate(event);

        assertThat(violations).extracting(v -> v.getPropertyPath().toString())
                .contains("eventId", "vehicleId", "eventTime", "latitude", "longitude",
                        "speedKph", "headingDeg", "alertCode");
    }

    @Test
    void allowsNullAlertCode() {
        VehicleEvent event = new VehicleEvent(
                "11111111-1111-1111-1111-111111111112", "veh-0002", Instant.now(),
                0.0, 0.0, 0.0, 0.0, null);
        assertThat(validator.validate(event)).isEmpty();
    }
}
