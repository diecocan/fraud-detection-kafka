package com.diecocan.portfolio.fraud.producer;

import com.diecocan.portfolio.fraud.avro.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TransactionProducerTest {

    @Mock
    KafkaTemplate<String, Transaction> kafkaTemplate;

    TransactionProducer producer;

    @BeforeEach
    void setUp() {
        producer = new TransactionProducer(kafkaTemplate);
        ReflectionTestUtils.setField(producer, "topic", "transactions");
        ReflectionTestUtils.setField(producer, "numAccounts", 10);
    }

    private Transaction captureSentTransaction() {
        ArgumentCaptor<Transaction> captor = ArgumentCaptor.forClass(Transaction.class);
        verify(kafkaTemplate).send(anyString(), anyString(), captor.capture());
        return captor.getValue();
    }

    @Test
    void generateTransaction_sendsToConfiguredTopicWithAccountIdAsKey() {
        ReflectionTestUtils.setField(producer, "anomalyInjectionRate", 0.0);
        ReflectionTestUtils.setField(producer, "geoAnomalyInjectionRate", 0.0);

        producer.generateTransaction();

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Transaction> txnCaptor = ArgumentCaptor.forClass(Transaction.class);
        verify(kafkaTemplate).send(eq("transactions"), keyCaptor.capture(), txnCaptor.capture());

        assertThat(keyCaptor.getValue()).matches("account_(?:[1-9]|10)");
        assertThat(txnCaptor.getValue().getAccountId().toString()).isEqualTo(keyCaptor.getValue());
    }

    @Test
    void generateTransaction_normalTransaction_hasAmountInNormalRange() {
        ReflectionTestUtils.setField(producer, "anomalyInjectionRate", 0.0);
        ReflectionTestUtils.setField(producer, "geoAnomalyInjectionRate", 0.0);

        producer.generateTransaction();

        assertThat(captureSentTransaction().getAmount()).isBetween(50.0, 500.0);
    }

    @Test
    void generateTransaction_amountAnomaly_generatesElevatedAmount() {
        ReflectionTestUtils.setField(producer, "anomalyInjectionRate", 1.0);
        ReflectionTestUtils.setField(producer, "geoAnomalyInjectionRate", 0.0);

        producer.generateTransaction();

        assertThat(captureSentTransaction().getAmount()).isBetween(3000.0, 5000.0);
    }

    @Test
    void generateTransaction_geoAnomaly_usesDistantLocation() {
        ReflectionTestUtils.setField(producer, "anomalyInjectionRate", 0.0);
        ReflectionTestUtils.setField(producer, "geoAnomalyInjectionRate", 1.0);

        producer.generateTransaction();

        assertThat(captureSentTransaction().getCountry().toString())
                .isIn("JP", "FR", "AU", "AE", "GB");
    }

    @Test
    void generateTransaction_alwaysUsesUsdCurrency() {
        ReflectionTestUtils.setField(producer, "anomalyInjectionRate", 0.0);
        ReflectionTestUtils.setField(producer, "geoAnomalyInjectionRate", 0.0);

        producer.generateTransaction();

        assertThat(captureSentTransaction().getCurrency().toString()).isEqualTo("USD");
    }
}
