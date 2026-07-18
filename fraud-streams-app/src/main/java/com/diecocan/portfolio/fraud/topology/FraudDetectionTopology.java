package com.diecocan.portfolio.fraud.topology;

import com.diecocan.portfolio.fraud.avro.*;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Configuration
public class FraudDetectionTopology {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionTopology.class);

    @Value("${fraud-streams-app.velocity.threshold}")
    private long threshold;

    @Value("${fraud-streams-app.velocity.window-minutes}")
    private long windowMinutes;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${fraud-streams-app.amount-anomaly.z-score-threshold}")
    private double zScoreThreshold;

    @Value("${fraud-streams-app.impossible-geo.max-travel-minutes}")
    private long maxTravelMinutes;

    @Autowired
    public void buildPipeLine(StreamsBuilder streamsBuilder) {
        SpecificAvroSerde<Transaction> transactionSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<VelocityAggregate> velocityAggregateSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<FraudAlert> fraudAlertSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<AccountProfile> accountProfileSerde = new SpecificAvroSerde<>();

        Map<String, ?> serdeConfigMap = Map.of(
                "schema.registry.url", schemaRegistryUrl,
                "specific.avro.reader", true
        );

        transactionSerde.configure(serdeConfigMap, false);
        velocityAggregateSerde.configure(serdeConfigMap, false);
        fraudAlertSerde.configure(serdeConfigMap, false);
        accountProfileSerde.configure(serdeConfigMap, false);

        KStream<String, Transaction> transactions = streamsBuilder.stream(
                "transactions",
                Consumed.with(Serdes.String(), transactionSerde)
        );

        KTable<String, AccountProfile> accountProfilesFromTopic = streamsBuilder.table(
                "account-profiles",
                Consumed.with(Serdes.String(), accountProfileSerde)
        );

        KTable<String, AccountProfile> accountProfiles = transactions
                .groupByKey()
                .aggregate(
                    () -> AccountProfile.newBuilder()
                                .setAccountId("")
                                .setTxnCount(0L)
                                .setTotalAmount(0.0)
                                .setAvgAmount(0.0)
                                .setStddev(0.0)
                                .setSumSquaredDiff(0.0)
                                .setLastUpdated(Instant.now())
                                .build(),
                    (accountId, transaction, profile) -> {
                        long newCount = profile.getTxnCount() + 1;
                        double delta = transaction.getAmount() - profile.getAvgAmount();
                        double newAvg = profile.getAvgAmount() + delta / newCount;
                        double delta2 = transaction.getAmount() - newAvg;
                        double newSumSquaredDiff = profile.getSumSquaredDiff() + delta * delta2;
                        double newStddev = newCount > 1 ? Math.sqrt(newSumSquaredDiff / newCount) : 0.0;

                        return AccountProfile.newBuilder()
                                .setAccountId(accountId)
                                .setTxnCount(newCount)
                                .setTotalAmount(profile.getTotalAmount() + transaction.getAmount())
                                .setAvgAmount(newAvg)
                                .setStddev(newStddev)
                                .setSumSquaredDiff(newSumSquaredDiff)
                                .setLastUpdated(transaction.getTimestamp())
                                .setLastCity(transaction.getCity())
                                .setLastCountry(transaction.getCountry())
                                .setLastTransactionTimestamp(transaction.getTimestamp())
                                .build();
                    },
                    Materialized.with(Serdes.String(), accountProfileSerde)
                );

        accountProfiles.toStream().to("account-profiles", Produced.with(Serdes.String(), accountProfileSerde));

        KTable<Windowed<String>, VelocityAggregate> velocityAggregates = transactions
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(windowMinutes)))
                .aggregate(
                        () -> VelocityAggregate.newBuilder().setCount(0L).setLastTransactionId("").build(),
                        (accountId, transaction, aggregate) ->
                                VelocityAggregate.newBuilder()
                                        .setCount(aggregate.getCount() + 1)
                                        .setLastTransactionId(transaction.getTransactionId())
                                        .build(),
                        Materialized.with(Serdes.String(), velocityAggregateSerde)
                );

        velocityAggregates.toStream()
                .filter((windowedKey, aggregate) -> aggregate.getCount() >= threshold)
                .map((windowedKey, aggregate) -> {
                    String accountId = windowedKey.key();
                    FraudAlert alert = FraudAlert.newBuilder()
                            .setAlertId(UUID.randomUUID().toString())
                            .setTransactionId(aggregate.getLastTransactionId())
                            .setAccountId(accountId)
                            .setReason(AlertReason.VELOCITY)
                            .setRiskScore(Math.min(1.0, (double) aggregate.getCount() / threshold))
                            .setDetectedAt(Instant.now())
                            .build();

                    return KeyValue.pair(accountId, alert);
                })
                .to("alerts", Produced.with(Serdes.String(), fraudAlertSerde));

        KStream<String, FraudAlert> amountAnomalies = transactions.join(
                accountProfilesFromTopic,
                (transaction, profile) -> {
                    final double profileStddev = profile.getStddev();
                    double zScore = profileStddev > 0 ? (transaction.getAmount() - profile.getAvgAmount()) / profileStddev : 0.0;
                    boolean isAnomaly = Math.abs(zScore) > zScoreThreshold;

                    return isAnomaly
                            ? FraudAlert.newBuilder()
                                .setAlertId(UUID.randomUUID().toString())
                                .setTransactionId(transaction.getTransactionId())
                                .setAccountId(transaction.getAccountId())
                                .setReason(AlertReason.AMOUNT_ANOMALY)
                                .setRiskScore(Math.min(1.0, Math.abs(zScore) / 10.0))
                                .setDetectedAt(transaction.getTimestamp())
                                .build()
                            : null;
                }
        )
       .filter((accountId, alert) -> alert != null);

        amountAnomalies.to("alerts", Produced.with(Serdes.String(), fraudAlertSerde));

        KStream<String, FraudAlert> geoAnomalies = transactions.join(
                        accountProfilesFromTopic,
                        (transaction, profile) -> {
                            if (profile.getLastCountry() == null || profile.getLastCity() == null) {
                                return null;
                            }

                            String lastCountry = profile.getLastCountry().toString();
                            String currentCountry = transaction.getCountry().toString();

                            if (lastCountry.equals(currentCountry)) {
                                return null; // not a geo violation
                            }

                            long minutesSinceLastTxn = Duration.between(
                                    profile.getLastTransactionTimestamp(),
                                    transaction.getTimestamp()
                            ).toMinutes();

                            boolean isImpossible = minutesSinceLastTxn < maxTravelMinutes;

                            return isImpossible
                                    ? FraudAlert.newBuilder()
                                    .setAlertId(UUID.randomUUID().toString())
                                    .setTransactionId(transaction.getTransactionId())
                                    .setAccountId(transaction.getAccountId())
                                    .setReason(AlertReason.IMPOSSIBLE_GEO)
                                    .setRiskScore(1.0)
                                    .setDetectedAt(transaction.getTimestamp())
                                    .build()
                                    : null;
                        }
                )
                .filter((accountId, alert) -> alert != null);

        geoAnomalies.to("alerts", Produced.with(Serdes.String(), fraudAlertSerde));
    }
}
