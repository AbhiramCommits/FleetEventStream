package com.fleeteventstream.api;

import com.fleeteventstream.persistence.StoredVehicleEvent;
import com.fleeteventstream.persistence.VehicleEventRepository;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Size;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@Validated
@RestController
@RequestMapping("/api")
public class EventController {

    private final VehicleEventRepository repository;

    public EventController(VehicleEventRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/events/{eventId}")
    public StoredVehicleEvent getEvent(@PathVariable @Size(max = 64) String eventId) {
        return repository.findByEventId(eventId)
                .orElseThrow(() -> EventNotFoundException.forEvent(eventId));
    }

    @GetMapping("/vehicles/{vehicleId}/history")
    public HistoryResponse history(
            @PathVariable @Size(max = 32) String vehicleId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int limit) {
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException("from must be earlier than or equal to to");
        }
        List<StoredVehicleEvent> events = repository.findHistory(vehicleId, from, to, limit);
        return new HistoryResponse(vehicleId, from, to, limit, events.size(), events);
    }

    @GetMapping("/vehicles")
    public VehiclesResponse vehicles(
            @RequestParam(defaultValue = "50") @Min(1) @Max(1000) int limit) {
        List<VehicleEventRepository.VehicleSummary> vehicles = repository.findVehicles(limit);
        return new VehiclesResponse(vehicles.size(), vehicles);
    }

    public record HistoryResponse(String vehicleId, Instant from, Instant to, int limit,
                                  int count, List<StoredVehicleEvent> events) {
    }

    public record VehiclesResponse(int count, List<VehicleEventRepository.VehicleSummary> vehicles) {
    }
}
