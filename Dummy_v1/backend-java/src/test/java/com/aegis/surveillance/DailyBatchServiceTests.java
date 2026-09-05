package com.aegis.surveillance;

import static org.assertj.core.api.Assertions.assertThat;

import com.aegis.surveillance.config.DailyBatchProperties;
import com.aegis.surveillance.service.DailyBatchService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;

class DailyBatchServiceTests {
  @TempDir Path temporary;

  @Test
  void requiresMatchingRegionSpecificAlertAndParquetPair() {
    var properties = new DailyBatchProperties();
    properties.setRootDirectory(temporary);
    properties.setAllowedRegions(List.of("EMEA", "AMER"));
    var service = new DailyBatchService(properties, new ObjectMapper());
    service.initialise();
    String alert = """
        {"batchId":"SURV-20260905-EMEA","businessDate":"2026-09-05","region":"EMEA",
        "alertId":"ALT-1","ruleId":"FI-WASH-001","ruleVersion":"4.7","typology":"WASH_TRADING",
        "assetClass":"FIXED_INCOME","ruleScore":82,"triggeringTradeIds":["T-1","T-2"]}
        """;
    var first = service.upload("EMEA", LocalDate.parse("2026-09-05"), "SURV-20260905-EMEA", "alerts",
        new MockMultipartFile("file", "alerts.jsonl", "application/json", alert.getBytes()));
    assertThat(((java.util.Map<?, ?>) first.get("readiness")).get("ready")).isEqualTo(false);
    byte[] parquetEnvelope = "PAR1synthetic-test-envelopePAR1".getBytes();
    var second = service.uploadParquetFiles("EMEA", LocalDate.parse("2026-09-05"), "SURV-20260905-EMEA", List.of(
        new MockMultipartFile("files", "trades-part-001.parquet", "application/octet-stream", parquetEnvelope),
        new MockMultipartFile("files", "trades-part-002.parquet", "application/octet-stream", "PAR1second-test-envelopePAR1".getBytes())));
    assertThat(((java.util.Map<?, ?>) second.get("readiness")).get("ready")).isEqualTo(true);
    assertThat(((java.util.Map<?, ?>) second.get("readiness")).get("parquetFiles")).isEqualTo(2);
    assertThat(temporary.resolve("region=EMEA/business_date=2026-09-05/batch_id=SURV-20260905-EMEA/manifest.ready.json")).exists();
  }

  @Test
  void acceptsSingleParquetFileAndPublishesOneDescriptor() throws Exception {
    var properties = new DailyBatchProperties();
    properties.setRootDirectory(temporary);
    properties.setAllowedRegions(List.of("APAC"));
    var mapper = new ObjectMapper();
    var service = new DailyBatchService(properties, mapper);
    service.initialise();
    String alert = """
        {"batchId":"SURV-20260905-APAC","businessDate":"2026-09-05","region":"APAC",
        "alertId":"ALT-2","ruleId":"FX-WASH-001","ruleVersion":"1.0","typology":"WASH_TRADING",
        "assetClass":"FOREIGN_EXCHANGE","triggeringTradeIds":["T-9"]}
        """;
    service.upload("APAC", LocalDate.parse("2026-09-05"), "SURV-20260905-APAC", "alerts",
        new MockMultipartFile("file", "alerts.json", "application/json", alert.getBytes()));
    var result = service.uploadParquetFiles("APAC", LocalDate.parse("2026-09-05"), "SURV-20260905-APAC", List.of(
        new MockMultipartFile("files", "trades.parquet", "application/octet-stream", "PAR1single-test-envelopePAR1".getBytes())));
    assertThat(((java.util.Map<?, ?>) result.get("readiness")).get("parquetFiles")).isEqualTo(1);
    var manifest = mapper.readTree(temporary.resolve(
        "region=APAC/business_date=2026-09-05/batch_id=SURV-20260905-APAC/manifest.ready.json").toFile());
    assertThat(manifest.get("trades").isArray()).isTrue();
    assertThat(manifest.get("trades").size()).isEqualTo(1);
  }
}
