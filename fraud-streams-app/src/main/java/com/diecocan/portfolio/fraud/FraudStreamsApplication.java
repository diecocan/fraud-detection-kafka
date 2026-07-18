package com.diecocan.portfolio.fraud;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class FraudStreamsApplication {
    public static void main(String[] args) {
        SpringApplication.run(FraudStreamsApplication.class, args);
    }
}