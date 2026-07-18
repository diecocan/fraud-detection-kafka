package com.diecocan.portfolio.fraud.controller;

import com.diecocan.portfolio.fraud.entity.AlertEntity;
import com.diecocan.portfolio.fraud.repository.AlertRepository;
import com.diecocan.portfolio.fraud.sse.AlertBroadcastService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;

@RestController
@RequestMapping("/api/alerts")
public class AlertController {
    private final AlertRepository alertRepository;
    private final AlertBroadcastService broadcastService;

    public AlertController(AlertRepository alertRepository, AlertBroadcastService broadcastService) {
        this.alertRepository = alertRepository;
        this.broadcastService = broadcastService;
    }

    @GetMapping
    public Page<AlertEntity> getAllAlerts(Pageable pageable) {
        return alertRepository.findAll(pageable);
    }

    @GetMapping("/account/{accountId}")
    public List<AlertEntity> getAlertsByAccount(@PathVariable String accountId) {
        return alertRepository.findByAccountId(accountId);
    }

    @GetMapping("/stats")
    public List<AlertRepository.ReasonCount> getStats() {
        return alertRepository.countGroupedByReason();
    }

    @GetMapping("/stream")
    public SseEmitter streamAlerts() {
        return broadcastService.subscribe();
    }
}
