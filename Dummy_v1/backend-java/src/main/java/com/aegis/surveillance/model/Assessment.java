package com.aegis.surveillance.model;

import java.util.List;
import java.util.Map;

public record Assessment(
    String assessmentId,
    String caseId,
    String typology,
    int risk,
    double confidence,
    String band,
    String decisionPolicy,
    List<Contribution> contributions,
    List<String> evidenceRefs,
    double evidenceCoverage,
    List<String> dataGaps,
    List<String> warnings,
    Map<String, String> versions) {

  public record Contribution(
      String feature,
      String label,
      double value,
      double points,
      String direction,
      List<String> evidenceRefs) {}
}
