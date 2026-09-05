package com.aegis.surveillance;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.aegis.surveillance.model.IngestionBatchRequest;
import com.aegis.surveillance.model.TradeEvent;
import com.aegis.surveillance.service.IngestionService;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class IngestionServiceTests {
  @Test
  void validatesRowsAndDetectsDuplicateTradeIds() {
    var service = new IngestionService();
    var valid = new TradeEvent("T-1", "O-1", Instant.parse("2026-08-25T10:00:00Z"), "NOVA.L", "BUY",
        new BigDecimal("1000"), new BigDecimal("42.10"), "A-1", "C-1", "XLON");
    var invalid = new TradeEvent("T-2", "O-2", null, "NOVA.L", "HOLD",
        BigDecimal.ZERO, new BigDecimal("42.10"), "A-2", "C-2", "XLON");

    var first = service.ingest(new IngestionBatchRequest("B-1", "OMS", "trade-event-v1", List.of(valid, invalid)));
    assertEquals(1, first.get("accepted"));
    assertEquals(1, first.get("rejected"));

    var second = service.ingest(new IngestionBatchRequest("B-2", "OMS", "trade-event-v1", List.of(valid)));
    assertEquals(1, second.get("duplicates"));
    assertEquals(0, second.get("accepted"));
  }

  @Test
  void repeatedBatchIdIsIdempotent() {
    var service = new IngestionService();
    var trade = new TradeEvent("T-9", "O-9", Instant.parse("2026-08-25T10:00:00Z"), "NOVA.L", "SELL",
        new BigDecimal("500"), new BigDecimal("42.11"), "A-9", "C-9", "XLON");
    var request = new IngestionBatchRequest("B-9", "OMS", "trade-event-v1", List.of(trade));
    assertEquals(service.ingest(request), service.ingest(request));
  }
}
