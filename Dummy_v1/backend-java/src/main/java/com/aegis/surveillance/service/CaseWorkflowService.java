package com.aegis.surveillance.service;

import com.aegis.surveillance.model.DecisionRequest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class CaseWorkflowService {
  private final JdbcTemplate jdbc;
  private final Map<String, String> testStatuses = new ConcurrentHashMap<>();
  private final Map<String, List<Map<String, Object>>> testAudit = new ConcurrentHashMap<>();

  public CaseWorkflowService() { this.jdbc = null; }

  @Autowired
  public CaseWorkflowService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  @Transactional
  public synchronized Map<String, Object> decide(String caseId, DecisionRequest request) {
    String current = currentStatus(caseId);
    if (List.of("CLOSED", "ESCALATED").contains(current)) {
      throw new ResponseStatusException(HttpStatus.CONFLICT, "Case already has a terminal disposition");
    }
    if (request.disposition().equals("CLOSE") && !request.role().equals("SUPERVISOR")) {
      throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Supervisor role is required to close a case");
    }
    String next = switch (request.disposition()) {
      case "ESCALATE" -> "ESCALATED";
      case "FURTHER_REVIEW" -> "IN_REVIEW";
      case "FALSE_POSITIVE" -> "PENDING_SUPERVISOR";
      case "CLOSE" -> "CLOSED";
      default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported disposition");
    };
    UUID eventId = UUID.randomUUID();
    Instant occurredAt = Instant.now();
    if (jdbc == null) {
      testStatuses.put(caseId, next);
    } else {
      jdbc.update("""
          INSERT INTO surveillance_case(case_id, typology, risk, confidence, band, status, driver, evidence_refs)
          VALUES (?, 'UNCLASSIFIED', 0, 0, 'LOW', 'OPEN', 'Manual review case', '[]')
          ON CONFLICT (case_id) DO NOTHING
          """, caseId);
      jdbc.update("UPDATE surveillance_case SET status=?, updated_at=CURRENT_TIMESTAMP WHERE case_id=?", next, caseId);
      jdbc.update("""
          INSERT INTO audit_event(event_id, case_id, actor_id, actor_role, action, reason, from_status, to_status, occurred_at)
          VALUES (?,?,?,?,?,?,?,?,?)
          """, eventId, caseId, request.investigatorId(), request.role(), request.disposition(),
          request.reason(), current, next, Timestamp.from(occurredAt));
    }
    var event = Map.<String, Object>ofEntries(
        Map.entry("eventId", eventId.toString()), Map.entry("caseId", caseId),
        Map.entry("actorId", request.investigatorId()), Map.entry("actorRole", request.role()),
        Map.entry("action", request.disposition()), Map.entry("reason", request.reason()),
        Map.entry("fromStatus", current), Map.entry("toStatus", next),
        Map.entry("occurredAt", occurredAt.toString()), Map.entry("humanDecision", true));
    if (jdbc == null) testAudit.computeIfAbsent(caseId, key -> new ArrayList<>()).add(event);
    return event;
  }

  public Map<String, Object> caseState(String caseId) {
    return Map.of("caseId", caseId, "status", currentStatus(caseId),
        "humanDecisionRequired", true, "auditEvents", audit(caseId));
  }

  private String currentStatus(String caseId) {
    if (jdbc == null) return testStatuses.getOrDefault(caseId, "OPEN");
    return jdbc.query("SELECT status FROM surveillance_case WHERE case_id=?",
        rs -> rs.next() ? rs.getString(1) : "OPEN", caseId);
  }

  private List<Map<String, Object>> audit(String caseId) {
    if (jdbc == null) return List.copyOf(testAudit.getOrDefault(caseId, List.of()));
    return jdbc.query("""
        SELECT event_id::text, actor_id, actor_role, action, reason, from_status, to_status, occurred_at
        FROM audit_event WHERE case_id=? ORDER BY occurred_at DESC
        """, (rs, row) -> Map.ofEntries(
        Map.entry("eventId", rs.getString(1)), Map.entry("actorId", rs.getString(2)),
        Map.entry("actorRole", rs.getString(3)), Map.entry("action", rs.getString(4)),
        Map.entry("reason", rs.getString(5)), Map.entry("fromStatus", rs.getString(6)),
        Map.entry("toStatus", rs.getString(7)), Map.entry("occurredAt", rs.getTimestamp(8).toInstant().toString()),
        Map.entry("humanDecision", true)), caseId);
  }
}
