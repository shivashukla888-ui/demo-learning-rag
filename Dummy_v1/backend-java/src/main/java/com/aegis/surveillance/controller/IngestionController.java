package com.aegis.surveillance.controller;

import com.aegis.surveillance.model.IngestionBatchRequest;
import com.aegis.surveillance.service.IngestionService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/ingestion")
public class IngestionController {
  private final IngestionService ingestion;

  public IngestionController(IngestionService ingestion) {
    this.ingestion = ingestion;
  }

  @GetMapping("/schema")
  public Map<String, Object> schema() {
    return Map.of(
        "schemaVersion", "trade-event-v1",
        "requiredFields", List.of("tradeId", "orderId", "eventTime", "instrument", "side", "quantity", "price", "accountId", "clientId", "venue"),
        "sideValues", List.of("BUY", "SELL"),
        "timestampFormat", "ISO-8601 UTC",
        "decimalPolicy", "quantity and price must be greater than zero");
  }

  @PostMapping("/trades")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public Map<String, Object> ingest(@Valid @RequestBody IngestionBatchRequest request) {
    return ingestion.ingest(request);
  }

  @GetMapping("/batches/{batchId}")
  public Map<String, Object> batch(@PathVariable String batchId) {
    return ingestion.getBatch(batchId);
  }
}
