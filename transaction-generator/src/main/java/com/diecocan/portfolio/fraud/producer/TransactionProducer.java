package com.diecocan.portfolio.fraud.producer;

import com.diecocan.portfolio.fraud.avro.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


@Component
public class TransactionProducer {
    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);

    private record Location(String city, String country) {}
    private static final List<String> MERCHANTS = List.of("Amazon", "Starbucks", "Uber", "Walmart", "Target");
    private static final Map<String, Location> accountHomeLocations = new ConcurrentHashMap<>();

    private static final List<Location> HOME_LOCATIONS = List.of(
            new Location("New York", "US"),
            new Location("Los Angeles", "US"),
            new Location("Miami", "US"),
            new Location("Toronto", "CA"),
            new Location("Mexico City", "MX")
    );

    private static final List<Location> DISTANT_LOCATIONS = List.of(
            new Location("Tokyo", "JP"),
            new Location("Paris", "FR"),
            new Location("Sydney", "AU"),
            new Location("Dubai", "AE"),
            new Location("London", "GB")
    );

    private final KafkaTemplate<String, Transaction> kafkaTemplate;
    private final Random random = new Random();

    @Value("${transaction-generator.topic}")
    private String topic;

    @Value("${transaction-generator.num-accounts}")
    private int numAccounts;

    @Value("${transaction-generator.anomaly-injection-rate}")
    private double anomalyInjectionRate;

    @Value("${transaction-generator.geo-anomaly-injection-rate}")
    private double geoAnomalyInjectionRate;

    public TransactionProducer(KafkaTemplate<String, Transaction> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${transaction-generator.rate-ms}")
    public void generateTransaction() {
        String accountId = "account_" + (random.nextInt(numAccounts) + 1);
        Location homeLocation = accountHomeLocations.computeIfAbsent(accountId, id -> randomFrom(HOME_LOCATIONS));
        boolean isAmountAnomalous = random.nextDouble() < anomalyInjectionRate;
        boolean isGeoAnomalous = random.nextDouble() < geoAnomalyInjectionRate;
        Location location = isGeoAnomalous ? randomFrom(DISTANT_LOCATIONS) : homeLocation;
        double amount = isAmountAnomalous
                ? 3000 + random.nextDouble() * 2000
                : 50 + random.nextDouble() * 450;

        Transaction transaction = Transaction.newBuilder()
                .setTransactionId(UUID.randomUUID().toString())
                .setAccountId(accountId)
                .setAmount(amount)
                .setCurrency("USD")
                .setMerchant(randomFrom(MERCHANTS))
                .setCity(location.city())
                .setCountry(location.country())
                .setTimestamp(Instant.now())
                .build();

        kafkaTemplate.send(topic, accountId, transaction);

        if (isAmountAnomalous) {
            log.warn("Injected AMOUNT ANOMALOUS transaction {} for {}: ${}", transaction.getTransactionId(), accountId, transaction.getAmount());
        } else if (isGeoAnomalous) {
            log.warn("Injected GEO ANOMALOUS transaction {} for {}: ${}", transaction.getTransactionId(), accountId, transaction.getAmount());
        }else {
            log.info("Sent transaction {} for {}: ${}", transaction.getTransactionId(), accountId, transaction.getAmount());
        }
    }

    private <T> T randomFrom(List<T> options) {
        return options.get(random.nextInt(options.size()));
    }
}
