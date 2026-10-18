package com.fleeteventstream.simulator;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/simulator")
public class SimulatorController {

    private final TelemetrySimulator simulator;

    public SimulatorController(TelemetrySimulator simulator) {
        this.simulator = simulator;
    }

    @PostMapping("/start")
    public ResponseEntity<TelemetrySimulator.SimulatorStatus> start(
            @Valid @RequestBody StartSimulatorRequest request) {
        TelemetrySimulator.SimulatorStatus status = simulator.start(
                request.eventsPerSecond(), request.vehicleCount(), request.durationSeconds());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(status);
    }

    @PostMapping("/stop")
    public TelemetrySimulator.SimulatorStatus stop() {
        return simulator.stop();
    }

    @GetMapping("/status")
    public TelemetrySimulator.SimulatorStatus status() {
        return simulator.status();
    }

    public record StartSimulatorRequest(
            @Min(1) @Max(50000) int eventsPerSecond,
            @Min(1) @Max(10000) int vehicleCount,
            @Min(0) @Max(86400) int durationSeconds) {
    }
}
