package com.fleeteventstream.api;

import com.fleeteventstream.persistence.DeadLetterEvent;
import com.fleeteventstream.persistence.DeadLetterRepository;
import com.fleeteventstream.replay.ReplayService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = AdminController.class)
@Import(ApiExceptionHandler.class)
class AdminControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ReplayService replayService;
    @MockBean
    private DeadLetterRepository deadLetterRepository;

    @Test
    void listsDeadLetters() throws Exception {
        when(deadLetterRepository.findRecent(0, 20)).thenReturn(List.of(new DeadLetterEvent(
                7, "vehicle.telemetry", 1, 42, "bad json", "JsonParseException",
                "{bad", Instant.parse("2026-10-18T00:00:00Z"), 0)));
        when(deadLetterRepository.countAll()).thenReturn(1L);

        mockMvc.perform(get("/api/admin/dlq"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].sourceTopic").value("vehicle.telemetry"))
                .andExpect(jsonPath("$.items[0].partitionId").value(1));
    }

    @Test
    void rejectsNegativePage() throws Exception {
        mockMvc.perform(get("/api/admin/dlq").param("page", "-1"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void replaysFromOffset() throws Exception {
        when(replayService.replay("vehicle.telemetry", 0, 100, 50))
                .thenReturn(new ReplayService.ReplaySummary("vehicle.telemetry", 0, 100, 50, 50, 0, 150));

        mockMvc.perform(post("/api/admin/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"topic":"vehicle.telemetry","partition":0,"fromOffset":100,"maxRecords":50}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.recordsRead").value(50))
                .andExpect(jsonPath("$.recordsRepublished").value(50));
    }

    @Test
    void rejectsReplayWithoutTopic() throws Exception {
        mockMvc.perform(post("/api/admin/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"partition":0,"fromOffset":0,"maxRecords":10}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.details.topic").exists());
    }

    @Test
    void replaysDlqIds() throws Exception {
        when(replayService.replayDeadLetters(anyList()))
                .thenReturn(new ReplayService.DlqReplaySummary(2, 2, 2, 0, 0));

        mockMvc.perform(post("/api/admin/replay/dlq")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"ids":[1,2]}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.requested").value(2))
                .andExpect(jsonPath("$.republished").value(2));
    }

    @Test
    void rejectsEmptyDlqIds() throws Exception {
        mockMvc.perform(post("/api/admin/replay/dlq")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"ids":[]}
                                """))
                .andExpect(status().isBadRequest());
    }
}
