package com.diecocan.portfolio.fraud.repository;

import com.diecocan.portfolio.fraud.avro.AlertReason;
import com.diecocan.portfolio.fraud.entity.AlertEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface AlertRepository extends JpaRepository<AlertEntity, String> {
    List<AlertEntity> findByAccountId(String accountId);
    List<AlertEntity> findByReason(AlertReason reason);

    @Query("SELECT a.reason AS reason, COUNT(a) AS count FROM AlertEntity a GROUP BY a.reason")
    List<ReasonCount> countGroupedByReason();

    interface ReasonCount {
        AlertReason getReason();
        Long getCount();

    }
}
