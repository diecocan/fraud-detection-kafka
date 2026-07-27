package com.diecocan.portfolio.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${fraud-streams-app.topics.alerts}")
    private String alertsTopic;

    @Value("${fraud-streams-app.topics.account-profiles}")
    private String accountProfilesTopic;

    @Bean
    public NewTopic alertsTopic() {
        return TopicBuilder.name(alertsTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic accountProfilesTopic() {
        return TopicBuilder.name(accountProfilesTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
