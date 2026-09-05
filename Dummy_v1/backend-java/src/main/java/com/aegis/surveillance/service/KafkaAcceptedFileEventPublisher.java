package com.aegis.surveillance.service;

import com.aegis.surveillance.model.AcceptedFileEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "aegis.kafka.enabled", havingValue = "true")
public class KafkaAcceptedFileEventPublisher implements AcceptedFileEventPublisher {
  private final KafkaTemplate<String, String> kafka;
  private final ObjectMapper json;
  private final KafkaEventStatusService status;
  private final String topic;

  public KafkaAcceptedFileEventPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json,
      KafkaEventStatusService status, @Value("${aegis.kafka.accepted-topic}") String topic) {
    this.kafka = kafka;
    this.json = json;
    this.status = status;
    this.topic = topic;
  }

  @Override
  public boolean publish(AcceptedFileEvent event) {
    try {
      kafka.send(topic, event.fingerprint(), json.writeValueAsString(event)).get(10, TimeUnit.SECONDS);
      status.published();
      return true;
    } catch (JsonProcessingException exception) {
      status.publishFailed(exception);
      throw new IllegalStateException("Could not serialize accepted-file event", exception);
    } catch (Exception exception) {
      status.publishFailed(exception);
      return false;
    }
  }

  @Override public String mode() { return "KAFKA_EVENT_DRIVEN"; }
}
