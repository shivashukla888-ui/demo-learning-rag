package com.aegis.surveillance.model;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;

public record InvestigationCopilotRequest(
    @NotBlank @Size(max = 100) String caseId,
    @NotBlank @Size(max = 100) String alertId,
    @NotBlank @Pattern(regexp = "AMER|EMEA|APAC|GLOBAL") String region,
    @NotBlank @Pattern(regexp = "FIXED_INCOME|FOREIGN_EXCHANGE|INTEREST_RATE_DERIVATIVES|CREDIT_DERIVATIVES") String assetClass,
    @NotBlank @Pattern(regexp = "WASH_TRADING|SPOOFING|FRONT_RUNNING|INSIDER_DEALING|MANIPULATION") String typology,
    @Min(0) @Max(100) int priority,
    @DecimalMin("0") @DecimalMax("1") double evidenceCoverage,
    @DecimalMin("0") @DecimalMax("1") double modelDisagreement,
    @NotEmpty @Size(max = 10) List<@Size(min = 1, max = 400) String> riskDrivers,
    @Size(max = 8) List<@Size(min = 1, max = 400) String> riskReducers,
    @Size(max = 20) List<@Size(min = 1, max = 400) String> timeline,
    @Size(max = 10) List<@Size(min = 1, max = 400) String> entityRelationships,
    @NotEmpty @Size(max = 50) List<@Size(min = 1, max = 400) String> evidenceRefs,
    @Size(max = 10) List<@Size(min = 1, max = 400) String> dataGaps) {
  public InvestigationCopilotRequest {
    riskReducers = riskReducers == null ? List.of() : List.copyOf(riskReducers);
    timeline = timeline == null ? List.of() : List.copyOf(timeline);
    entityRelationships = entityRelationships == null ? List.of() : List.copyOf(entityRelationships);
    dataGaps = dataGaps == null ? List.of() : List.copyOf(dataGaps);
  }
}
