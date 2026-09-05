package com.aegis.surveillance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aegis.surveillance.config.FileInputProperties;
import com.aegis.surveillance.model.AcceptedFileEvent;
import com.aegis.surveillance.service.AcceptedFileEventPublisher;
import com.aegis.surveillance.service.FileInputService;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;

class FileInputServiceTests {
  @TempDir Path inputRoot;

  @Test
  void routesValidFilesToAcceptedAndReportsReadiness() {
    var service = service();
    var file = new MockMultipartFile("file", "trades-demo.csv", "text/csv",
        "trade_id,order_id\nT-1,O-1\n".getBytes(StandardCharsets.UTF_8));

    var result = service.upload("trades", file);

    assertEquals("ACCEPTED", result.get("status"));
    assertTrue((Boolean) service.readiness().get("ready"));
  }

  @Test
  void quarantinesFilesWithMissingColumns() {
    var service = service();
    var file = new MockMultipartFile("file", "trades-bad.csv", "text/csv",
        "trade_id\nT-1\n".getBytes(StandardCharsets.UTF_8));

    var result = service.upload("trades", file);

    assertEquals("REJECTED", result.get("status"));
    assertFalse((Boolean) service.readiness().get("ready"));
    assertFalse(((List<?>) result.get("errors")).isEmpty());
  }

  @Test
  void bootstrapsBundledSampleOnlyOnce() throws Exception {
    Path samples = inputRoot.resolve("samples");
    Files.createDirectories(samples);
    Files.writeString(samples.resolve("trades-demo.csv"), "trade_id,order_id\nT-1,O-1\n");
    var service = service(samples);

    service.bootstrapSamples();
    service.bootstrapSamples();

    var datasets = (java.util.Map<?, ?>) service.readiness().get("datasets");
    var trades = (java.util.Map<?, ?>) datasets.get("trades");
    assertEquals(1L, trades.get("acceptedFiles"));
    assertEquals(0L, trades.get("pendingFiles"));
  }

  @Test
  void publishesFingerprintEventForAcceptedFile() {
    var publisher = new CapturingPublisher();
    var properties = properties(inputRoot.resolve("missing-samples"));
    var service = new FileInputService(properties, publisher);
    service.initialise();

    var result = service.upload("trades", new MockMultipartFile("file", "trades-event.csv", "text/csv",
        "trade_id,order_id\nT-1,O-1\n".getBytes(StandardCharsets.UTF_8)));

    assertTrue((Boolean) result.get("eventPublished"));
    assertEquals("KAFKA_EVENT_DRIVEN", result.get("deliveryMode"));
    assertEquals(result.get("fingerprint"), publisher.event.fingerprint());
  }

  private FileInputService service() {
    return service(inputRoot.resolve("missing-samples"));
  }

  private FileInputService service(Path samples) {
    var properties = properties(samples);
    var service = new FileInputService(properties);
    service.initialise();
    return service;
  }

  private FileInputProperties properties(Path samples) {
    var properties = new FileInputProperties();
    properties.setRootDirectory(inputRoot);
    properties.setSampleDirectory(samples);
    var trades = new FileInputProperties.Dataset();
    trades.setRequired(true);
    trades.setFilenamePattern("trades*.csv");
    trades.setRequiredColumns(List.of("trade_id", "order_id"));
    var datasets = new LinkedHashMap<String, FileInputProperties.Dataset>();
    datasets.put("trades", trades);
    properties.setDatasets(datasets);
    return properties;
  }

  private static class CapturingPublisher implements AcceptedFileEventPublisher {
    AcceptedFileEvent event;
    @Override public boolean publish(AcceptedFileEvent event) { this.event = event; return true; }
    @Override public String mode() { return "KAFKA_EVENT_DRIVEN"; }
  }
}
