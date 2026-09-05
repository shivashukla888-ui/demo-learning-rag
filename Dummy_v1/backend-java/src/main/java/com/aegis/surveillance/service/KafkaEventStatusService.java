package com.aegis.surveillance.service;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class KafkaEventStatusService {
  private final boolean enabled;
  private final String acceptedTopic;
  private final String completedTopic;
  private final AtomicLong published = new AtomicLong();
  private final AtomicLong publishFailures = new AtomicLong();
  private final AtomicLong consumed = new AtomicLong();
  private final AtomicLong processingFailures = new AtomicLong();
  private volatile String lastPublishedAt;
  private volatile String lastConsumedAt;
  private volatile String lastError;

  public KafkaEventStatusService(
      @Value("${aegis.kafka.enabled:false}") boolean enabled,
      @Value("${aegis.kafka.accepted-topic:surveillance.file.accepted.v1}") String acceptedTopic,
      @Value("${aegis.kafka.completed-topic:surveillance.ml.scan.triggered.v1}") String completedTopic) {
    this.enabled = enabled;
    this.acceptedTopic = acceptedTopic;
    this.completedTopic = completedTopic;
  }

  public void published() { published.incrementAndGet(); lastPublishedAt = Instant.now().toString(); lastError = null; }
  public void publishFailed(Exception error) { publishFailures.incrementAndGet(); lastError = error.getClass().getSimpleName() + ": " + error.getMessage(); }
  public void consumed() { consumed.incrementAndGet(); lastConsumedAt = Instant.now().toString(); lastError = null; }
  public void processingFailed(Exception error) { processingFailures.incrementAndGet(); lastError = error.getClass().getSimpleName() + ": " + error.getMessage(); }

  public Map<String, Object> status() {
    return Map.ofEntries(
        Map.entry("enabled", enabled),
        Map.entry("mode", enabled ? "KAFKA_EVENT_DRIVEN" : "FILE_WATCHER_FALLBACK"),
        Map.entry("acceptedTopic", acceptedTopic),
        Map.entry("completedTopic", completedTopic),
        Map.entry("deadLetterTopic", acceptedTopic + ".dlt"),
        Map.entry("published", published.get()),
        Map.entry("publishFailures", publishFailures.get()),
        Map.entry("consumed", consumed.get()),
        Map.entry("processingFailures", processingFailures.get()),
        Map.entry("lastPublishedAt", lastPublishedAt == null ? "NEVER" : lastPublishedAt),
        Map.entry("lastConsumedAt", lastConsumedAt == null ? "NEVER" : lastConsumedAt),
        Map.entry("lastError", lastError == null ? "NONE" : lastError));
  }
}
