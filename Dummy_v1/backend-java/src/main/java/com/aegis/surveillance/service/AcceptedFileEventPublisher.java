package com.aegis.surveillance.service;

import com.aegis.surveillance.model.AcceptedFileEvent;

public interface AcceptedFileEventPublisher {
  boolean publish(AcceptedFileEvent event);
  String mode();
}
