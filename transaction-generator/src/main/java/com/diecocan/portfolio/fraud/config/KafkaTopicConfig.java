package com.diecocan.portfolio.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${transaction-generator.topic}")
    private String topic;

    @Bean
    public NewTopic transactionsTopic() {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
