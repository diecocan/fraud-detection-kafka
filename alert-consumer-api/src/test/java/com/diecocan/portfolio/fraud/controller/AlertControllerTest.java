package com.diecocan.portfolio.fraud.controller;

import com.diecocan.portfolio.fraud.avro.AlertReason;
import com.diecocan.portfolio.fraud.entity.AlertEntity;
import com.diecocan.portfolio.fraud.repository.AlertRepository;
import com.diecocan.portfolio.fraud.sse.AlertBroadcastService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AlertController.class)
class AlertControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    AlertRepository alertRepository;

    @MockitoBean
    AlertBroadcastService broadcastService;

    private AlertEntity sampleAlert(String id, String accountId) {
        return new AlertEntity(id, "txn-" + id, accountId, AlertReason.VELOCITY, 0.9, Instant.now());
    }

    @Test
    void getAllAlerts_returnsPagedAlerts() throws Exception {
        AlertEntity alert = sampleAlert("a1", "acct1");
        when(alertRepository.findAll(any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of(alert)));

        mockMvc.perform(get("/api/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].alertId").value("a1"));
    }

    @Test
    void getAlertsByAccount_returnsMatchingAlerts() throws Exception {
        when(alertRepository.findByAccountId("acct1"))
                .thenReturn(List.of(sampleAlert("a1", "acct1")));

        mockMvc.perform(get("/api/alerts/account/acct1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].accountId").value("acct1"));
    }

    @Test
    void getStats_returnsGroupedCounts() throws Exception {
        // Deliberately not a Mockito mock: Jackson serializing a raw mock of this
        // interface pulls in Mockito's own internal object graph and produces
        // garbage JSON. A plain implementation keeps the response clean.
        AlertRepository.ReasonCount reasonCount = new AlertRepository.ReasonCount() {
            public AlertReason getReason() {
                return AlertReason.VELOCITY;
            }

            public Long getCount() {
                return 5L;
            }
        };
        when(alertRepository.countGroupedByReason()).thenReturn(List.of(reasonCount));

        mockMvc.perform(get("/api/alerts/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].reason").value("VELOCITY"))
                .andExpect(jsonPath("$[0].count").value(5));
    }

    @Test
    void streamAlerts_subscribesToBroadcastService() throws Exception {
        when(broadcastService.subscribe()).thenReturn(new SseEmitter(0L));

        // A real SSE stream never "completes" the way a normal async request does,
        // so we only assert it started and delegated to the broadcast service —
        // not a final dispatched response.
        mockMvc.perform(get("/api/alerts/stream"))
                .andExpect(request().asyncStarted());

        verify(broadcastService).subscribe();
    }
}
