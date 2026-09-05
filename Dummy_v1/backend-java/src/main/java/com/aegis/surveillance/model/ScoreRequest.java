package com.aegis.surveillance.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public record ScoreRequest(
    @NotBlank String alertId,
    @NotNull Instant asOf,
    String typology,
    String cohort,
    Map<String, Double> features,
    List<String> evidenceRefs) {

  public ScoreRequest {
    typology = typology == null ? "WASH_TRADING" : typology;
    cohort = cohort == null ? "default" : cohort;
    features = features == null ? Map.of() : Map.copyOf(features);
    evidenceRefs = evidenceRefs == null ? List.of() : List.copyOf(evidenceRefs);
  }
}
