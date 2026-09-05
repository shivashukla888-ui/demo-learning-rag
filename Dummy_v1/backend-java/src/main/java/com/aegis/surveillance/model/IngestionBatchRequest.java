package com.aegis.surveillance.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record IngestionBatchRequest(
    @NotBlank String batchId,
    @NotBlank String sourceSystem,
    @NotBlank String schemaVersion,
    @NotEmpty List<TradeEvent> trades) {}
