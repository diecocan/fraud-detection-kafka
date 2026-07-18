package com.diecocan.portfolio.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic alertsTopic() {
        return TopicBuilder.name("alerts")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic accountProfilesTopic() {
        return TopicBuilder.name("account-profiles")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
