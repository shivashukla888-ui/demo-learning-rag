package com.aegis.surveillance.controller;

import com.aegis.surveillance.service.KafkaEventStatusService;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/integration")
public class IntegrationController {
  private final KafkaEventStatusService kafka;

  public IntegrationController(KafkaEventStatusService kafka) { this.kafka = kafka; }

  @GetMapping("/kafka")
  public Map<String, Object> kafka() { return kafka.status(); }
}
