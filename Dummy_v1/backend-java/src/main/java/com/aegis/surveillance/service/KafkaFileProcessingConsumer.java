package com.aegis.surveillance.service;

import com.aegis.surveillance.model.AcceptedFileEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "aegis.kafka.enabled", havingValue = "true")
public class KafkaFileProcessingConsumer {
  private final ObjectMapper json;
  private final ScoringGateway scoring;
  private final KafkaTemplate<String, String> kafka;
  private final KafkaEventStatusService status;
  private final String triggeredTopic;

  public KafkaFileProcessingConsumer(ObjectMapper json, ScoringGateway scoring,
      KafkaTemplate<String, String> kafka, KafkaEventStatusService status,
      @Value("${aegis.kafka.completed-topic}") String triggeredTopic) {
    this.json = json;
    this.scoring = scoring;
    this.kafka = kafka;
    this.status = status;
    this.triggeredTopic = triggeredTopic;
  }

  @KafkaListener(topics = "${aegis.kafka.accepted-topic}")
  public void consume(String payload) throws Exception {
    try {
      AcceptedFileEvent event = json.readValue(payload, AcceptedFileEvent.class);
      Map<String, Object> scan = event.dataset().equals("trades")
          ? scoring.scanFileJobs() : Map.of("discovered", 0, "reason", "REFERENCE_DATA_ACCEPTED");
      var triggered = Map.of(
          "eventId", event.eventId(), "sourceFingerprint", event.fingerprint(),
          "dataset", event.dataset(), "scan", scan, "triggeredAt", Instant.now().toString());
      kafka.send(triggeredTopic, event.fingerprint(), json.writeValueAsString(triggered)).get(10, TimeUnit.SECONDS);
      status.consumed();
    } catch (Exception exception) {
      status.processingFailed(exception);
      throw exception;
    }
  }
}
