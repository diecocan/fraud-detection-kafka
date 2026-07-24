package com.diecocan.portfolio.fraud.consumer;

import com.diecocan.portfolio.fraud.avro.AlertReason;
import com.diecocan.portfolio.fraud.avro.FraudAlert;
import com.diecocan.portfolio.fraud.entity.AlertEntity;
import com.diecocan.portfolio.fraud.repository.AlertRepository;
import com.diecocan.portfolio.fraud.sse.AlertBroadcastService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AlertConsumerTest {

    @Mock
    AlertRepository alertRepository;

    @Mock
    AlertBroadcastService broadcastService;

    @InjectMocks
    AlertConsumer consumer;

    @Test
    void consume_mapsAvroAlertToEntity_savesAndBroadcasts() {
        Instant detectedAt = Instant.parse("2026-07-24T10:00:00Z");
        FraudAlert avroAlert = FraudAlert.newBuilder()
                .setAlertId("alert-1")
                .setTransactionId("txn-1")
                .setAccountId("acct-1")
                .setReason(AlertReason.VELOCITY)
                .setRiskScore(0.87)
                .setDetectedAt(detectedAt)
                .build();

        consumer.consume(avroAlert);

        ArgumentCaptor<AlertEntity> captor = ArgumentCaptor.forClass(AlertEntity.class);
        verify(alertRepository).save(captor.capture());
        verify(broadcastService).broadcast(captor.getValue());

        AlertEntity saved = captor.getValue();
        assertThat(saved.getAlertId()).isEqualTo("alert-1");
        assertThat(saved.getTransactionId()).isEqualTo("txn-1");
        assertThat(saved.getAccountId()).isEqualTo("acct-1");
        assertThat(saved.getReason()).isEqualTo(AlertReason.VELOCITY);
        assertThat(saved.getRiskScore()).isEqualTo(0.87);
        assertThat(saved.getDetectedAt()).isEqualTo(detectedAt);
    }
}
