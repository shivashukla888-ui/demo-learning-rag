package com.aegis.surveillance.controller;

import com.aegis.surveillance.service.DailyBatchService;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/v1/ingestion/daily-batches")
public class DailyBatchController {
  private final DailyBatchService batches;
  public DailyBatchController(DailyBatchService batches) { this.batches = batches; }

  @GetMapping("/configuration")
  public Map<String, Object> configuration() { return batches.configuration(); }

  @GetMapping
  public List<Map<String, Object>> list() { return batches.batches(); }

  @GetMapping("/{region}/{businessDate}/{batchId}/readiness")
  public Map<String, Object> readiness(@PathVariable String region,
      @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate businessDate,
      @PathVariable String batchId) {
    return batches.readiness(region, businessDate, batchId);
  }

  @PostMapping("/{region}/{businessDate}/{batchId}/alerts")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public Map<String, Object> uploadAlerts(@PathVariable String region,
      @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate businessDate,
      @PathVariable String batchId,
      @RequestPart("file") MultipartFile file) {
    return batches.upload(region, businessDate, batchId, "alerts", file);
  }

  @PostMapping("/{region}/{businessDate}/{batchId}/trades")
  @ResponseStatus(HttpStatus.ACCEPTED)
  public Map<String, Object> uploadTrades(@PathVariable String region,
      @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate businessDate,
      @PathVariable String batchId,
      @RequestPart("files") List<MultipartFile> files) {
    return batches.uploadParquetFiles(region, businessDate, batchId, files);
  }
}
