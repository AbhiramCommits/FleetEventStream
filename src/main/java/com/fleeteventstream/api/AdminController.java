package com.fleeteventstream.api;

import com.fleeteventstream.persistence.DeadLetterEvent;
import com.fleeteventstream.persistence.DeadLetterRepository;
import com.fleeteventstream.replay.ReplayService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Validated
@RestController
@RequestMapping("/api/admin")
public class AdminController {

    private final ReplayService replayService;
    private final DeadLetterRepository deadLetterRepository;

    public AdminController(ReplayService replayService, DeadLetterRepository deadLetterRepository) {
        this.replayService = replayService;
        this.deadLetterRepository = deadLetterRepository;
    }

    @GetMapping("/dlq")
    public DlqPage recentDeadLetters(
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        List<DeadLetterEvent> items = deadLetterRepository.findRecent(page, size);
        return new DlqPage(items, page, size, deadLetterRepository.countAll());
    }

    @PostMapping("/replay")
    public ReplayService.ReplaySummary replay(@Valid @RequestBody ReplayRequest request) {
        return replayService.replay(request.topic(), request.partition(),
                request.fromOffset(), request.maxRecords());
    }

    @PostMapping("/replay/dlq")
    public ReplayService.DlqReplaySummary replayDlq(@Valid @RequestBody DlqReplayRequest request) {
        return replayService.replayDeadLetters(request.ids());
    }

    public record ReplayRequest(
            @NotBlank @Size(max = 255) String topic,
            @Min(0) @Max(10_000) int partition,
            @Min(0) long fromOffset,
            @Min(1) @Max(100_000) int maxRecords) {
    }

    public record DlqReplayRequest(
            @NotEmpty @Size(max = 1000) List<@NotNull @Positive Long> ids) {
    }

    public record DlqPage(List<DeadLetterEvent> items, int page, int size, long total) {
    }
}
