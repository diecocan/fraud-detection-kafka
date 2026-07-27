package com.diecocan.portfolio.fraud.topology;

import com.diecocan.portfolio.fraud.avro.AccountProfile;
import com.diecocan.portfolio.fraud.avro.AlertReason;
import com.diecocan.portfolio.fraud.avro.FraudAlert;
import com.diecocan.portfolio.fraud.avro.Transaction;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class FraudDetectionTopologyTest {

    private static final String SCHEMA_REGISTRY_URL = "mock://fraud-detection-topology-test";

    private TopologyTestDriver testDriver;

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    private TopologyTestDriver buildDriver(long velocityThreshold, long windowMinutes,
                                            double zScoreThreshold, long maxTravelMinutes) {
        FraudDetectionTopology topologyConfig = new FraudDetectionTopology();
        ReflectionTestUtils.setField(topologyConfig, "threshold", velocityThreshold);
        ReflectionTestUtils.setField(topologyConfig, "windowMinutes", windowMinutes);
        ReflectionTestUtils.setField(topologyConfig, "schemaRegistryUrl", SCHEMA_REGISTRY_URL);
        ReflectionTestUtils.setField(topologyConfig, "zScoreThreshold", zScoreThreshold);
        ReflectionTestUtils.setField(topologyConfig, "maxTravelMinutes", maxTravelMinutes);
        ReflectionTestUtils.setField(topologyConfig, "transactionsTopic", "transactions");
        ReflectionTestUtils.setField(topologyConfig, "accountProfilesTopic", "account-profiles");
        ReflectionTestUtils.setField(topologyConfig, "alertsTopic", "alerts");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topologyConfig.buildPipeLine(streamsBuilder);
        Topology topology = streamsBuilder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams-test-state");

        return new TopologyTestDriver(topology, props);
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> serdeFor() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                "schema.registry.url", SCHEMA_REGISTRY_URL,
                "specific.avro.reader", true
        ), false);
        return serde;
    }

    private TestInputTopic<String, Transaction> transactionsTopic(TopologyTestDriver driver) {
        return driver.createInputTopic("transactions", Serdes.String().serializer(), this.<Transaction>serdeFor().serializer());
    }

    private TestInputTopic<String, AccountProfile> accountProfilesInputTopic(TopologyTestDriver driver) {
        return driver.createInputTopic("account-profiles", Serdes.String().serializer(), this.<AccountProfile>serdeFor().serializer());
    }

    private TestOutputTopic<String, AccountProfile> accountProfilesOutputTopic(TopologyTestDriver driver) {
        return driver.createOutputTopic("account-profiles", Serdes.String().deserializer(), this.<AccountProfile>serdeFor().deserializer());
    }

    private TestOutputTopic<String, FraudAlert> alertsTopic(TopologyTestDriver driver) {
        return driver.createOutputTopic("alerts", Serdes.String().deserializer(), this.<FraudAlert>serdeFor().deserializer());
    }

    private Transaction transaction(String accountId, double amount, String country, String city, Instant timestamp) {
        return Transaction.newBuilder()
                .setTransactionId(UUID.randomUUID().toString())
                .setAccountId(accountId)
                .setAmount(amount)
                .setCurrency("USD")
                .setMerchant("Test Merchant")
                .setCity(city)
                .setCountry(country)
                .setTimestamp(timestamp)
                .build();
    }

    private AccountProfile profile(String accountId, double avgAmount, double stddev,
                                    String lastCountry, String lastCity, Instant lastTransactionTimestamp) {
        return AccountProfile.newBuilder()
                .setAccountId(accountId)
                .setTxnCount(10L)
                .setTotalAmount(avgAmount * 10)
                .setAvgAmount(avgAmount)
                .setStddev(stddev)
                .setSumSquaredDiff(stddev * stddev * 10)
                .setLastUpdated(lastTransactionTimestamp)
                .setLastCity(lastCity)
                .setLastCountry(lastCountry)
                .setLastTransactionTimestamp(lastTransactionTimestamp)
                .build();
    }

    @Test
    void accountProfileAggregation_computesRunningAverageAndStddev() {
        testDriver = buildDriver(1000, 1, 1000.0, 0);
        TestInputTopic<String, Transaction> transactions = transactionsTopic(testDriver);
        TestOutputTopic<String, AccountProfile> accountProfilesOutput = accountProfilesOutputTopic(testDriver);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        transactions.pipeInput("acc1", transaction("acc1", 100.0, "US", "NYC", t0));
        transactions.pipeInput("acc1", transaction("acc1", 200.0, "US", "NYC", t0.plusSeconds(1)));

        List<AccountProfile> profiles = accountProfilesOutput.readValuesToList();

        assertThat(profiles).hasSize(2);
        AccountProfile first = profiles.get(0);
        assertThat(first.getTxnCount()).isEqualTo(1L);
        assertThat(first.getAvgAmount()).isEqualTo(100.0);
        assertThat(first.getStddev()).isEqualTo(0.0);

        AccountProfile second = profiles.get(1);
        assertThat(second.getTxnCount()).isEqualTo(2L);
        assertThat(second.getTotalAmount()).isEqualTo(300.0);
        assertThat(second.getAvgAmount()).isEqualTo(150.0);
        assertThat(second.getStddev()).isEqualTo(50.0);
    }

    @Test
    void velocityRule_firesOnceThresholdReachedWithinWindow() {
        testDriver = buildDriver(3, 1, 1000.0, 0);
        TestInputTopic<String, Transaction> transactions = transactionsTopic(testDriver);
        TestOutputTopic<String, FraudAlert> alerts = alertsTopic(testDriver);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        transactions.pipeInput("acc2", transaction("acc2", 50.0, "US", "NYC", t0), t0);
        transactions.pipeInput("acc2", transaction("acc2", 50.0, "US", "NYC", t0.plusSeconds(1)), t0.plusSeconds(1));
        transactions.pipeInput("acc2", transaction("acc2", 50.0, "US", "NYC", t0.plusSeconds(2)), t0.plusSeconds(2));

        List<FraudAlert> fired = alerts.readValuesToList();

        assertThat(fired).hasSize(1);
        assertThat(fired.get(0).getReason()).isEqualTo(AlertReason.VELOCITY);
        assertThat(fired.get(0).getAccountId().toString()).isEqualTo("acc2");
    }

    @Test
    void velocityRule_doesNotFireBelowThreshold() {
        testDriver = buildDriver(3, 1, 1000.0, 0);
        TestInputTopic<String, Transaction> transactions = transactionsTopic(testDriver);
        TestOutputTopic<String, FraudAlert> alerts = alertsTopic(testDriver);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        transactions.pipeInput("acc2", transaction("acc2", 50.0, "US", "NYC", t0), t0);
        transactions.pipeInput("acc2", transaction("acc2", 50.0, "US", "NYC", t0.plusSeconds(1)), t0.plusSeconds(1));

        assertThat(alerts.isEmpty()).isTrue();
    }

    @Test
    void amountAnomaly_firesWhenZScoreExceedsThreshold() {
        testDriver = buildDriver(1000, 1, 3.0, 0);
        TestInputTopic<String, AccountProfile> accountProfilesInput = accountProfilesInputTopic(testDriver);
        TestInputTopic<String, Transaction> transactions = transactionsTopic(testDriver);
        TestOutputTopic<String, FraudAlert> alerts = alertsTopic(testDriver);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        accountProfilesInput.pipeInput("acc3", profile("acc3", 100.0, 10.0, "US", "NYC", t0));
        transactions.pipeInput("acc3", transaction("acc3", 1000.0, "US", "NYC", t0.plusSeconds(1)));

        List<FraudAlert> fired = alerts.readValuesToList();

        assertThat(fired).anySatisfy(alert -> {
            assertThat(alert.getReason()).isEqualTo(AlertReason.AMOUNT_ANOMALY);
            assertThat(alert.getAccountId().toString()).isEqualTo("acc3");
        });
    }

    @Test
    void amountAnomaly_doesNotFireWithinNormalRange() {
        testDriver = buildDriver(1000, 1, 3.0, 0);
        TestInputTopic<String, AccountProfile> accountProfilesInput = accountProfilesInputTopic(testDriver);
        TestInputTopic<String, Transaction> transactions = transactionsTopic(testDriver);
        TestOutputTopic<String, FraudAlert> alerts = alertsTopic(testDriver);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        accountProfilesInput.pipeInput("acc3", profile("acc3", 100.0, 10.0, "US", "NYC", t0));
        transactions.pipeInput("acc3", transaction("acc3", 105.0, "US", "NYC", t0.plusSeconds(1)));

        List<FraudAlert> fired = alerts.readValuesToList();

        assertThat(fired).noneMatch(alert -> alert.getReason() == AlertReason.AMOUNT_ANOMALY);
    }

    @Test
    void impossibleGeo_firesWhenCountryChangesFasterThanTravelIsPossible() {
        testDriver = buildDriver(1000, 1, 1000.0, 60);
        TestInputTopic<String, AccountProfile> accountProfilesInput = accountProfilesInputTopic(testDriver);
        TestInputTopic<String, Transaction> transactions = transactionsTopic(testDriver);
        TestOutputTopic<String, FraudAlert> alerts = alertsTopic(testDriver);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        accountProfilesInput.pipeInput("acc4", profile("acc4", 100.0, 10.0, "US", "NYC", t0));
        transactions.pipeInput("acc4", transaction("acc4", 105.0, "FR", "Paris", t0.plus(Duration.ofMinutes(10))));

        List<FraudAlert> fired = alerts.readValuesToList();

        assertThat(fired).anySatisfy(alert -> {
            assertThat(alert.getReason()).isEqualTo(AlertReason.IMPOSSIBLE_GEO);
            assertThat(alert.getAccountId().toString()).isEqualTo("acc4");
        });
    }

    @Test
    void impossibleGeo_doesNotFireWhenEnoughTimeHasPassed() {
        testDriver = buildDriver(1000, 1, 1000.0, 60);
        TestInputTopic<String, AccountProfile> accountProfilesInput = accountProfilesInputTopic(testDriver);
        TestInputTopic<String, Transaction> transactions = transactionsTopic(testDriver);
        TestOutputTopic<String, FraudAlert> alerts = alertsTopic(testDriver);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        accountProfilesInput.pipeInput("acc4", profile("acc4", 100.0, 10.0, "US", "NYC", t0));
        transactions.pipeInput("acc4", transaction("acc4", 105.0, "FR", "Paris", t0.plus(Duration.ofMinutes(120))));

        List<FraudAlert> fired = alerts.readValuesToList();

        assertThat(fired).noneMatch(alert -> alert.getReason() == AlertReason.IMPOSSIBLE_GEO);
    }

    @Test
    void impossibleGeo_doesNotFireWhenCountryUnchanged() {
        testDriver = buildDriver(1000, 1, 1000.0, 60);
        TestInputTopic<String, AccountProfile> accountProfilesInput = accountProfilesInputTopic(testDriver);
        TestInputTopic<String, Transaction> transactions = transactionsTopic(testDriver);
        TestOutputTopic<String, FraudAlert> alerts = alertsTopic(testDriver);

        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        accountProfilesInput.pipeInput("acc4", profile("acc4", 100.0, 10.0, "US", "NYC", t0));
        transactions.pipeInput("acc4", transaction("acc4", 105.0, "US", "Boston", t0.plus(Duration.ofMinutes(10))));

        List<FraudAlert> fired = alerts.readValuesToList();

        assertThat(fired).noneMatch(alert -> alert.getReason() == AlertReason.IMPOSSIBLE_GEO);
    }
}
