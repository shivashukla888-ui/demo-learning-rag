package com.aegis.surveillance.service;

import com.aegis.surveillance.model.AcceptedFileEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "aegis.kafka.enabled", havingValue = "false", matchIfMissing = true)
public class NoopAcceptedFileEventPublisher implements AcceptedFileEventPublisher {
  @Override public boolean publish(AcceptedFileEvent event) { return false; }
  @Override public String mode() { return "FILE_WATCHER_FALLBACK"; }
}
