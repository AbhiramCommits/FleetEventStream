package com.fleeteventstream.api;

import com.fleeteventstream.persistence.StoredVehicleEvent;
import com.fleeteventstream.persistence.VehicleEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = EventController.class)
@Import(ApiExceptionHandler.class)
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private VehicleEventRepository repository;

    private static final StoredVehicleEvent EVENT = new StoredVehicleEvent(
            1, "11111111-1111-1111-1111-111111111111", "veh-0001",
            Instant.parse("2026-10-18T00:00:00Z"), 37.7749, -122.4194,
            42.5, 180.0, "SPEEDING", "{}", Instant.parse("2026-10-18T00:00:01Z"));

    @Test
    void returnsSingleEvent() throws Exception {
        when(repository.findByEventId(EVENT.eventId())).thenReturn(Optional.of(EVENT));

        mockMvc.perform(get("/api/events/{eventId}", EVENT.eventId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.eventId").value(EVENT.eventId()))
                .andExpect(jsonPath("$.vehicleId").value("veh-0001"))
                .andExpect(jsonPath("$.speedKph").value(42.5))
                .andExpect(jsonPath("$.alertCode").value("SPEEDING"));
    }

    @Test
    void unknownEventReturns404WithErrorBody() throws Exception {
        when(repository.findByEventId("missing")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/events/{eventId}", "missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.message").value("No event found with eventId=missing"))
                .andExpect(jsonPath("$.path").value("/api/events/missing"));
    }

    @Test
    void returnsVehicleHistory() throws Exception {
        when(repository.findHistory(eq("veh-0001"), isNull(), isNull(), eq(10)))
                .thenReturn(List.of(EVENT));

        mockMvc.perform(get("/api/vehicles/{vehicleId}/history", "veh-0001").param("limit", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.vehicleId").value("veh-0001"))
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.events[0].eventId").value(EVENT.eventId()));
    }

    @Test
    void historyRejectsBadLimit() throws Exception {
        mockMvc.perform(get("/api/vehicles/veh-0001/history").param("limit", "0"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void historyRejectsReversedDateRange() throws Exception {
        mockMvc.perform(get("/api/vehicles/veh-0001/history")
                        .param("from", "2026-10-18T01:00:00Z")
                        .param("to", "2026-10-18T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("from must be earlier than or equal to to"));
    }

    @Test
    void historyRejectsInvalidDate() throws Exception {
        mockMvc.perform(get("/api/vehicles/veh-0001/history").param("from", "not-a-date"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("Invalid value for parameter 'from'"));
    }

    @Test
    void listsVehicles() throws Exception {
        when(repository.findVehicles(5)).thenReturn(List.of(
                new VehicleEventRepository.VehicleSummary("veh-0001", 7,
                        Instant.parse("2026-10-18T00:00:00Z"))));

        mockMvc.perform(get("/api/vehicles").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.count").value(1))
                .andExpect(jsonPath("$.vehicles[0].vehicleId").value("veh-0001"))
                .andExpect(jsonPath("$.vehicles[0].eventCount").value(7));
    }

    @Test
    void vehiclesRejectsLimitAboveCap() throws Exception {
        mockMvc.perform(get("/api/vehicles").param("limit", "1001"))
                .andExpect(status().isBadRequest());
    }
}
