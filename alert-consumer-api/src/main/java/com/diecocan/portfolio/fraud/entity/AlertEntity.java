package com.diecocan.portfolio.fraud.entity;

import com.diecocan.portfolio.fraud.avro.AlertReason;
import jakarta.persistence.*;

import java.time.Instant;

@Entity
@Table(name = "alerts")
public class AlertEntity {

    @Id
    private String alertId;

    private String transactionId;
    private String accountId;

    @Enumerated(EnumType.STRING)
    private AlertReason reason;

    private double riskScore;
    private Instant detectedAt;

    protected AlertEntity() {
        // required by JPA
    }

    public AlertEntity(String alertId,
                       String transactionId,
                       String accountId,
                       AlertReason reason,
                       double riskScore,
                       Instant detectedAt) {
        this.alertId = alertId;
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.reason = reason;
        this.riskScore = riskScore;
        this.detectedAt = detectedAt;
    }

    public String getAlertId() {
        return alertId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public AlertReason getReason() {
        return reason;
    }

    public double getRiskScore() {
        return riskScore;
    }

    public Instant getDetectedAt() {
        return detectedAt;
    }
}
