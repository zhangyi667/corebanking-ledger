package com.corebanking.ledger.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    @Value("${app.kafka.topics.posting-transaction}")
    private String postingTransactionTopic;

    @Bean
    public NewTopic postingTransactionDlqTopic() {
        return TopicBuilder.name(postingTransactionTopic + ".dlq")
                .partitions(6)
                .replicas(1)
                .build();
    }
}
