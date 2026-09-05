package com.aegis.surveillance.service;

import com.aegis.surveillance.model.IngestionBatchRequest;
import com.aegis.surveillance.model.TradeEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class IngestionService {
  private static final Set<String> SUPPORTED_SCHEMA_VERSIONS = Set.of("trade-event-v1");
  private final Set<String> acceptedTradeIds = ConcurrentHashMap.newKeySet();
  private final Map<String, Map<String, Object>> batches = new ConcurrentHashMap<>();

  public synchronized Map<String, Object> ingest(IngestionBatchRequest request) {
    if (!SUPPORTED_SCHEMA_VERSIONS.contains(request.schemaVersion())) {
      throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Unsupported schema version");
    }
    if (batches.containsKey(request.batchId())) {
      return batches.get(request.batchId());
    }

    int accepted = 0;
    int duplicates = 0;
    var rejected = new ArrayList<Map<String, Object>>();
    for (int index = 0; index < request.trades().size(); index++) {
      TradeEvent trade = request.trades().get(index);
      List<String> errors = validate(trade);
      if (!errors.isEmpty()) {
        Map<String, Object> rejection = new LinkedHashMap<>();
        rejection.put("row", index + 1);
        rejection.put("tradeId", trade == null || trade.tradeId() == null ? "" : trade.tradeId());
        rejection.put("errors", errors);
        rejected.add(rejection);
      } else if (!acceptedTradeIds.add(trade.tradeId())) {
        duplicates++;
      } else {
        accepted++;
      }
    }

    Map<String, Object> result = new LinkedHashMap<>();
    result.put("batchId", request.batchId());
    result.put("sourceSystem", request.sourceSystem());
    result.put("schemaVersion", request.schemaVersion());
    result.put("receivedAt", Instant.now().toString());
    result.put("received", request.trades().size());
    result.put("accepted", accepted);
    result.put("rejected", rejected.size());
    result.put("duplicates", duplicates);
    result.put("errors", List.copyOf(rejected));
    result.put("status", rejected.isEmpty() ? "ACCEPTED" : accepted > 0 ? "PARTIALLY_ACCEPTED" : "REJECTED");
    result.put("nextStage", accepted > 0 ? "NORMALISATION_AND_FEATURE_ENGINEERING" : "CORRECT_INPUT_DATA");
    result.put("idempotent", true);
    batches.put(request.batchId(), Map.copyOf(result));
    return result;
  }

  public Map<String, Object> getBatch(String batchId) {
    Map<String, Object> result = batches.get(batchId);
    if (result == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Batch not found");
    return result;
  }

  private List<String> validate(TradeEvent trade) {
    if (trade == null) return List.of("trade record is required");
    var errors = new ArrayList<String>();
    required(trade.tradeId(), "tradeId", errors);
    required(trade.orderId(), "orderId", errors);
    if (trade.eventTime() == null) errors.add("eventTime is required");
    required(trade.instrument(), "instrument", errors);
    if (trade.side() == null || !(trade.side().equals("BUY") || trade.side().equals("SELL"))) errors.add("side must be BUY or SELL");
    positive(trade.quantity(), "quantity", errors);
    positive(trade.price(), "price", errors);
    required(trade.accountId(), "accountId", errors);
    required(trade.clientId(), "clientId", errors);
    required(trade.venue(), "venue", errors);
    return errors;
  }

  private void required(String value, String field, List<String> errors) {
    if (value == null || value.isBlank()) errors.add(field + " is required");
  }

  private void positive(BigDecimal value, String field, List<String> errors) {
    if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) errors.add(field + " must be greater than zero");
  }
}
