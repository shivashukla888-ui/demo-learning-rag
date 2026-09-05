package com.aegis.surveillance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aegis.surveillance.model.DecisionRequest;
import com.aegis.surveillance.service.CaseWorkflowService;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class CaseWorkflowServiceTests {
  private final CaseWorkflowService workflow = new CaseWorkflowService();

  @Test
  void investigatorCanEscalateWithRecordedRationale() {
    var event = workflow.decide("WT-102", new DecisionRequest(
        "investigator-17", "INVESTIGATOR", "ESCALATE",
        "Evidence indicates linked-account coordination requiring escalation.", null));
    assertEquals("ESCALATED", event.get("toStatus"));
    assertEquals(true, event.get("humanDecision"));
  }

  @Test
  void investigatorCannotCloseCaseWithoutSupervisor() {
    assertThrows(ResponseStatusException.class, () -> workflow.decide("WT-071", new DecisionRequest(
        "investigator-17", "INVESTIGATOR", "CLOSE",
        "Market context fully explains the reviewed trading behaviour.", null)));
  }
}
