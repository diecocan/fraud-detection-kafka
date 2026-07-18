package com.diecocan.portfolio.fraud.consumer;

import com.diecocan.portfolio.fraud.avro.FraudAlert;
import com.diecocan.portfolio.fraud.entity.AlertEntity;
import com.diecocan.portfolio.fraud.repository.AlertRepository;
import com.diecocan.portfolio.fraud.sse.AlertBroadcastService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AlertConsumer {
    private static final Logger log = LoggerFactory.getLogger(AlertConsumer.class);

    private final AlertRepository alertRepository;
    private final AlertBroadcastService broadcastService;

    public AlertConsumer(AlertRepository alertRepository, AlertBroadcastService broadcastService) {
        this.alertRepository = alertRepository;
        this.broadcastService = broadcastService;
    }

    @KafkaListener(topics = "${alert-consumer-api.topic}")
    public void consume(FraudAlert alert) {
        AlertEntity entity = new AlertEntity(
                alert.getAlertId().toString(),
                alert.getTransactionId().toString(),
                alert.getAccountId().toString(),
                alert.getReason(),
                alert.getRiskScore(),
                alert.getDetectedAt()
        );

        alertRepository.save(entity);
        broadcastService.broadcast(entity);

        log.info("Persisted alert {} ({}) for account {}",
                entity.getAlertId(),
                entity.getReason(),
                entity.getAccountId());
    }
}
