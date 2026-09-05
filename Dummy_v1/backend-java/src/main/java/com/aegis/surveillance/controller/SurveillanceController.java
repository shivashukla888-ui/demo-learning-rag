package com.aegis.surveillance.controller;

import com.aegis.surveillance.model.Assessment;
import com.aegis.surveillance.model.DecisionRequest;
import com.aegis.surveillance.model.ScoreRequest;
import com.aegis.surveillance.model.InvestigationCopilotRequest;
import com.aegis.surveillance.service.CaseWorkflowService;
import com.aegis.surveillance.service.CaseQueryService;
import com.aegis.surveillance.service.ScoringGateway;
import jakarta.validation.Valid;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1")
public class SurveillanceController {
  private final ScoringGateway scoring;
  private final CaseWorkflowService workflow;
  private final CaseQueryService queries;

  public SurveillanceController(ScoringGateway scoring, CaseWorkflowService workflow, CaseQueryService queries) {
    this.scoring = scoring;
    this.workflow = workflow;
    this.queries = queries;
  }

  @PostMapping("/alerts/score")
  public Assessment score(@Valid @RequestBody ScoreRequest request) {
    return scoring.score(request);
  }

  @GetMapping("/cases/{caseId}/evidence")
  public Map<String, Object> evidence(@PathVariable String caseId) {
    return queries.evidence(caseId);
  }

  @PostMapping("/cases/{caseId}/copilot")
  public Map<String, Object> copilot(@PathVariable String caseId,
      @Valid @RequestBody InvestigationCopilotRequest request) {
    if (!caseId.equals(request.caseId()))
      throw new org.springframework.web.server.ResponseStatusException(
          HttpStatus.UNPROCESSABLE_ENTITY, "Path caseId must match the evidence packet");
    return scoring.investigationCopilot(request);
  }

  @GetMapping("/cases")
  public List<Map<String, Object>> cases() { return queries.cases(); }

  @GetMapping("/management")
  public Map<String, Object> management() { return queries.management(); }

  @GetMapping("/ml/file-jobs")
  public Map<String, Object> fileJobs() { return scoring.fileJobs(); }

  @GetMapping("/ml/model-card")
  public Map<String, Object> modelCard() { return scoring.modelCard(); }

  @GetMapping("/cases/{caseId}")
  public Map<String, Object> caseState(@PathVariable String caseId) {
    return workflow.caseState(caseId);
  }

  @PostMapping("/cases/{caseId}/decisions")
  @ResponseStatus(HttpStatus.CREATED)
  public Map<String, Object> decide(@PathVariable String caseId, @Valid @RequestBody DecisionRequest request,
                                     HttpServletRequest http) {
    var authenticated = new DecisionRequest(
        String.valueOf(http.getAttribute("aegis.actorId")),
        String.valueOf(http.getAttribute("aegis.actorRole")),
        request.disposition(), request.reason(), request.notes());
    return workflow.decide(caseId, authenticated);
  }
}
