package com.aegis.surveillance.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@ConditionalOnProperty(name = "aegis.kafka.enabled", havingValue = "true")
public class KafkaIntegrationConfig {
  @Bean
  NewTopic acceptedFileTopic(@Value("${aegis.kafka.accepted-topic}") String topic) {
    return TopicBuilder.name(topic).partitions(3).replicas(1).build();
  }

  @Bean
  NewTopic mlScanTriggeredTopic(@Value("${aegis.kafka.completed-topic}") String topic) {
    return TopicBuilder.name(topic).partitions(3).replicas(1).build();
  }

  @Bean
  NewTopic acceptedFileDeadLetterTopic(@Value("${aegis.kafka.accepted-topic}") String topic) {
    return TopicBuilder.name(topic + ".dlt").partitions(3).replicas(1).build();
  }

  @Bean
  CommonErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template) {
    var recoverer = new DeadLetterPublishingRecoverer(template,
        (record, error) -> new TopicPartition(record.topic() + ".dlt", record.partition()));
    return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
  }
}
