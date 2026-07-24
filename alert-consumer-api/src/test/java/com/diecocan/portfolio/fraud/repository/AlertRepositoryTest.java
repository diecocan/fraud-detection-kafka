package com.diecocan.portfolio.fraud.repository;

import com.diecocan.portfolio.fraud.avro.AlertReason;
import com.diecocan.portfolio.fraud.entity.AlertEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@DataJpaTest
class AlertRepositoryTest {

    @Autowired
    private AlertRepository alertRepository;

    @Test
    void findByAccountId_returnsOnlyMatchingAlerts() {
        alertRepository.save(new AlertEntity("a1", "t1", "acct1", AlertReason.VELOCITY, 0.9, Instant.now()));
        alertRepository.save(new AlertEntity("a2", "t2", "acct2", AlertReason.AMOUNT_ANOMALY, 0.5, Instant.now()));

        List<AlertEntity> result = alertRepository.findByAccountId("acct1");

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAlertId()).isEqualTo("a1");
    }

    @Test
    void findByReason_returnsOnlyMatchingAlerts() {
        alertRepository.save(new AlertEntity("a1", "t1", "acct1", AlertReason.IMPOSSIBLE_GEO, 0.9, Instant.now()));
        alertRepository.save(new AlertEntity("a2", "t2", "acct2", AlertReason.AMOUNT_ANOMALY, 0.5, Instant.now()));

        List<AlertEntity> result = alertRepository.findByReason(AlertReason.IMPOSSIBLE_GEO);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getAlertId()).isEqualTo("a1");
    }

    @Test
    void countGroupedByReason_groupsAndCountsCorrectly() {
        alertRepository.save(new AlertEntity("a1", "t1", "acct1", AlertReason.VELOCITY, 0.9, Instant.now()));
        alertRepository.save(new AlertEntity("a2", "t2", "acct2", AlertReason.VELOCITY, 0.8, Instant.now()));
        alertRepository.save(new AlertEntity("a3", "t3", "acct3", AlertReason.AMOUNT_ANOMALY, 0.5, Instant.now()));

        List<AlertRepository.ReasonCount> result = alertRepository.countGroupedByReason();

        assertThat(result)
                .extracting(AlertRepository.ReasonCount::getReason, AlertRepository.ReasonCount::getCount)
                .containsExactlyInAnyOrder(
                        tuple(AlertReason.VELOCITY, 2L),
                        tuple(AlertReason.AMOUNT_ANOMALY, 1L)
                );
    }
}
