package com.aegis.surveillance.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record DecisionRequest(
    @NotBlank String investigatorId,
    @NotBlank @Pattern(regexp = "INVESTIGATOR|SUPERVISOR") String role,
    @NotBlank @Pattern(regexp = "ESCALATE|FURTHER_REVIEW|FALSE_POSITIVE|CLOSE") String disposition,
    @NotBlank @Size(min = 15, max = 2000) String reason,
    String notes) {}
