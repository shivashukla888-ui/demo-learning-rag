package com.aegis.surveillance.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class CaseQueryService {
  private final JdbcTemplate jdbc;
  private final ScoringGateway scoring;
  private final ObjectMapper json;

  public CaseQueryService(JdbcTemplate jdbc, ScoringGateway scoring, ObjectMapper json) {
    this.jdbc = jdbc; this.scoring = scoring; this.json = json;
  }

  public List<Map<String, Object>> cases() {
    refreshFromMl();
    return jdbc.query("""
        SELECT case_id, trade_id, instrument, typology, risk, confidence, band, status, driver, created_at,
          region, asset_class, alert_count
        FROM surveillance_case ORDER BY risk DESC, created_at DESC LIMIT 250
        """, (rs, row) -> Map.ofEntries(
        Map.entry("id", rs.getString("case_id")), Map.entry("instrument", value(rs.getString("instrument"), "Unknown")),
        Map.entry("typology", rs.getString("typology").replace('_', ' ')), Map.entry("risk", rs.getInt("risk")),
        Map.entry("confidence", Math.round(rs.getDouble("confidence") * 100)),
        Map.entry("alerts", rs.getInt("alert_count")),
        Map.entry("accounts", value(rs.getString("trade_id"), "Pending enrichment")),
        Map.entry("driver", rs.getString("driver")), Map.entry("color", colour(rs.getString("band"))),
        Map.entry("owner", "Unassigned"), Map.entry("status", rs.getString("status")),
        Map.entry("age", "Live"), Map.entry("entityLift", 0),
        Map.entry("region", value(rs.getString("region"), "GLOBAL")),
        Map.entry("assetClass", value(rs.getString("asset_class"), "UNCLASSIFIED")),
        Map.entry("alertCount", rs.getInt("alert_count"))));
  }

  public Map<String, Object> evidence(String caseId) {
    return jdbc.query("""
        SELECT case_id, trade_id, instrument, typology, risk, evidence_refs, driver, model_version
        FROM surveillance_case WHERE case_id=?
        """, rs -> {
          if (!rs.next()) return Map.of("caseId", caseId, "claims", List.of(), "unsupportedClaims", 1);
          return Map.ofEntries(
              Map.entry("caseId", caseId),
              Map.entry("claims", List.of(Map.of(
                  "claim", rs.getString("driver"),
                  "metric", "Hybrid risk score = " + rs.getInt("risk"),
                  "evidenceRefs", parseRefs(rs.getString("evidence_refs"))))),
              Map.entry("instrument", value(rs.getString("instrument"), "Unknown")),
              Map.entry("typology", rs.getString("typology")),
              Map.entry("modelVersion", value(rs.getString("model_version"), "transparent-fallback")),
              Map.entry("unsupportedClaims", 0), Map.entry("synthetic", false));
        }, caseId);
  }

  public Map<String, Object> management() {
    refreshFromMl();
    return jdbc.query("""
        SELECT COUNT(*) total,
          COUNT(*) FILTER (WHERE risk >= 80) high,
          COUNT(*) FILTER (WHERE status = 'CLOSED') closed,
          COALESCE(AVG(risk),0) average_risk
        FROM surveillance_case
        """, rs -> {
          rs.next();
          return Map.of("cases", rs.getLong("total"), "highPriority", rs.getLong("high"),
              "closed", rs.getLong("closed"), "averageRisk", Math.round(rs.getDouble("average_risk")),
              "source", "LIVE_POSTGRESQL", "humanDecisionRequired", true);
        });
  }

  @SuppressWarnings("unchecked")
  public void refreshFromMl() {
    try {
      Map<String, Object> response = scoring.fileJobs();
      for (Object rawJob : (List<Object>) response.getOrDefault("jobs", List.of())) {
        Map<String, Object> job = (Map<String, Object>) rawJob;
        for (Object rawCase : (List<Object>) job.getOrDefault("topCases", List.of())) {
          Map<String, Object> item = (Map<String, Object>) rawCase;
          String tradeId = String.valueOf(item.getOrDefault("tradeId", "UNKNOWN"));
          String caseId = String.valueOf(item.getOrDefault("caseId", "CASE-" + tradeId));
          jdbc.update("""
              INSERT INTO surveillance_case(case_id, source_job_id, trade_id, instrument, typology, risk,
                confidence, band, status, driver, evidence_refs, model_version)
              VALUES (?,?,?,?,?,?,?,?, 'OPEN', ?,?,?)
              ON CONFLICT (case_id) DO UPDATE SET risk=EXCLUDED.risk, confidence=EXCLUDED.confidence,
                band=EXCLUDED.band, driver=EXCLUDED.driver, evidence_refs=EXCLUDED.evidence_refs,
                model_version=EXCLUDED.model_version, updated_at=CURRENT_TIMESTAMP
              """, caseId, String.valueOf(job.getOrDefault("jobId", "")), tradeId,
              String.valueOf(item.getOrDefault("instrument", "Unknown")), "WASH_TRADING",
              ((Number) item.getOrDefault("risk", 0)).intValue(),
              ((Number) item.getOrDefault("confidence", 0)).doubleValue(),
              String.valueOf(item.getOrDefault("band", "LOW")),
              String.valueOf(item.getOrDefault("driver", "Hybrid pattern requires human review")),
              json.writeValueAsString(item.getOrDefault("evidenceRefs", List.of("Trade:" + tradeId))),
              String.valueOf(job.getOrDefault("model", "transparent-fallback")));
        }
      }
    } catch (Exception ignored) {
      // A failed legacy file pipeline leaves the last durable case view available.
    }
    try {
      Map<String, Object> daily = scoring.dailyCases();
      for (Object rawCase : (List<Object>) daily.getOrDefault("cases", List.of())) {
        Map<String, Object> item = (Map<String, Object>) rawCase;
        String caseId = String.valueOf(item.get("id"));
        int alertCount = ((Number) item.getOrDefault("alertCount", 1)).intValue();
        jdbc.update("""
            INSERT INTO surveillance_case(case_id, source_job_id, trade_id, instrument, typology, risk,
              confidence, band, status, driver, evidence_refs, model_version, region, asset_class, alert_count)
            VALUES (?,?,?,?,?,?,?,?, 'OPEN', ?,?,?,?,?,?)
            ON CONFLICT (case_id) DO UPDATE SET risk=EXCLUDED.risk, confidence=EXCLUDED.confidence,
              band=EXCLUDED.band, driver=EXCLUDED.driver, evidence_refs=EXCLUDED.evidence_refs,
              model_version=EXCLUDED.model_version, region=EXCLUDED.region, asset_class=EXCLUDED.asset_class,
              alert_count=EXCLUDED.alert_count, updated_at=CURRENT_TIMESTAMP
            """, caseId, String.valueOf(item.getOrDefault("sourceJobId", "")),
            alertCount + " source alerts", String.valueOf(item.getOrDefault("instrument", "Unknown")),
            String.valueOf(item.getOrDefault("typology", "WASH_TRADING")),
            ((Number) item.getOrDefault("risk", 0)).intValue(),
            ((Number) item.getOrDefault("confidence", 0)).doubleValue(),
            String.valueOf(item.getOrDefault("band", "LOW")),
            String.valueOf(item.getOrDefault("driver", "Hybrid pattern requires human review")),
            json.writeValueAsString(item.getOrDefault("evidenceRefs", List.of())),
            String.valueOf(item.getOrDefault("modelVersion", "transparent-fallback")),
            String.valueOf(item.getOrDefault("region", "GLOBAL")),
            String.valueOf(item.getOrDefault("assetClass", "UNCLASSIFIED")), alertCount);
      }
    } catch (Exception ignored) {
      // A failed daily ML dependency leaves the last durable regional case view available.
    }
  }

  private List<String> parseRefs(String value) {
    try { return json.readValue(value, new TypeReference<List<String>>() {}); }
    catch (JsonProcessingException error) { return List.of(); }
  }
  private String value(String value, String fallback) { return value == null || value.isBlank() ? fallback : value; }
  private String colour(String band) {
    return switch (band) { case "HIGH" -> "#d64545"; case "MEDIUM" -> "#d0a52a"; default -> "#2d9b75"; };
  }
}
