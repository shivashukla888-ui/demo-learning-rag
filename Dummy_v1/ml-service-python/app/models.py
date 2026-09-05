from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ScoreRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alertId: str = Field(min_length=3, max_length=80)
    asOf: datetime
    typology: Literal["WASH_TRADING", "SPOOFING", "FRONT_RUNNING", "INSIDER_DEALING", "MANIPULATION"] = "WASH_TRADING"
    cohort: str = Field(default="default", min_length=2, max_length=80)
    features: dict[str, float] = Field(default_factory=dict)
    evidenceRefs: list[str] = Field(default_factory=list, max_length=100)

    @field_validator("features")
    @classmethod
    def validate_features(cls, value: dict[str, float]) -> dict[str, float]:
        invalid = [name for name, feature in value.items() if not 0 <= feature <= 1]
        if invalid:
            raise ValueError(f"Feature values must be between 0 and 1: {', '.join(invalid)}")
        return value


class Contribution(BaseModel):
    feature: str
    label: str
    value: float
    points: float
    direction: Literal["increase", "reduce"]
    evidenceRefs: list[str]


class Assessment(BaseModel):
    assessmentId: str
    caseId: str
    typology: str
    risk: int
    confidence: float
    band: Literal["HIGH", "MEDIUM", "LOW", "INSUFFICIENT_DATA"]
    decisionPolicy: Literal["HUMAN_REVIEW_REQUIRED"]
    contributions: list[Contribution]
    evidenceRefs: list[str]
    evidenceCoverage: float
    dataGaps: list[str]
    warnings: list[str]
    versions: dict[str, str]
    mlRisk: int | None = None
    mlProbability: float | None = None
    anomalyScore: float | None = None
    modelDisagreement: float | None = None
    uncertaintyBand: Literal["LOW", "MEDIUM", "HIGH", "UNAVAILABLE"] = "UNAVAILABLE"
    copilotEligible: bool = False
    modelMode: Literal["TRAINED_HYBRID", "TRANSPARENT_FALLBACK"] = "TRANSPARENT_FALLBACK"


class BatchScoreItem(BaseModel):
    alertId: str = Field(min_length=3, max_length=80)
    typology: Literal["WASH_TRADING", "SPOOFING", "FRONT_RUNNING", "INSIDER_DEALING", "MANIPULATION"] = "WASH_TRADING"
    features: dict[str, float]
    evidenceRefs: list[str] = Field(default_factory=list, max_length=100)

    @field_validator("features")
    @classmethod
    def validate_batch_features(cls, value: dict[str, float]) -> dict[str, float]:
        invalid = [name for name, feature in value.items() if not 0 <= feature <= 1]
        if invalid:
            raise ValueError(f"Feature values must be between 0 and 1: {', '.join(invalid)}")
        return value


class BatchScoreRequest(BaseModel):
    batchId: str = Field(min_length=3, max_length=100)
    records: list[BatchScoreItem] = Field(min_length=1, max_length=10_000)


class EvidenceSummaryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    caseId: str = Field(min_length=3, max_length=100)
    typology: str = Field(default="WASH_TRADING", min_length=3, max_length=80)
    priority: int = Field(ge=0, le=100)
    riskDrivers: list[str] = Field(default_factory=list, max_length=30)
    riskReducers: list[str] = Field(default_factory=list, max_length=30)
    timeline: list[str] = Field(default_factory=list, max_length=100)
    entityRelationships: list[str] = Field(default_factory=list, max_length=50)
    evidenceRefs: list[str] = Field(min_length=1, max_length=100)
    dataGaps: list[str] = Field(default_factory=list, max_length=50)


class EvidenceSummaryResponse(BaseModel):
    caseId: str
    summary: str
    evidenceRefs: list[str]
    model: str
    grounded: Literal[True] = True
    decisionPolicy: Literal["HUMAN_REVIEW_REQUIRED"] = "HUMAN_REVIEW_REQUIRED"


class InvestigationCopilotRequest(BaseModel):
    """Allowlisted, already-tokenised evidence packet. Raw trades are forbidden."""
    model_config = ConfigDict(extra="forbid")

    caseId: str = Field(min_length=3, max_length=100)
    alertId: str = Field(min_length=3, max_length=100)
    region: Literal["AMER", "EMEA", "APAC", "GLOBAL"]
    assetClass: Literal["FIXED_INCOME", "FOREIGN_EXCHANGE", "INTEREST_RATE_DERIVATIVES", "CREDIT_DERIVATIVES"]
    typology: Literal["WASH_TRADING", "SPOOFING", "FRONT_RUNNING", "INSIDER_DEALING", "MANIPULATION"]
    priority: int = Field(ge=0, le=100)
    evidenceCoverage: float = Field(ge=0, le=1)
    modelDisagreement: float = Field(default=0, ge=0, le=1)
    riskDrivers: list[str] = Field(min_length=1, max_length=10)
    riskReducers: list[str] = Field(default_factory=list, max_length=8)
    timeline: list[str] = Field(default_factory=list, max_length=20)
    entityRelationships: list[str] = Field(default_factory=list, max_length=10)
    evidenceRefs: list[str] = Field(min_length=1, max_length=50)
    dataGaps: list[str] = Field(default_factory=list, max_length=10)

    @field_validator("riskDrivers", "riskReducers", "timeline", "entityRelationships", "evidenceRefs", "dataGaps")
    @classmethod
    def bound_evidence_text(cls, values: list[str]) -> list[str]:
        if any(not value.strip() or len(value) > 400 for value in values):
            raise ValueError("Evidence entries must contain 1 to 400 characters")
        return list(dict.fromkeys(value.strip() for value in values))


class CopilotAnalysis(BaseModel):
    """Strict structure returned by the LLM before post-generation guardrails."""
    model_config = ConfigDict(extra="forbid")

    summary: str = Field(min_length=20, max_length=700)
    counterHypothesis: str = Field(min_length=10, max_length=400)
    nextBestActions: list[str] = Field(min_length=1, max_length=4)
    missingEvidence: list[str] = Field(max_length=4)
    confidenceNote: str = Field(min_length=10, max_length=240)
    citedEvidenceRefs: list[str] = Field(min_length=1, max_length=12)


class InvestigationCopilotResponse(BaseModel):
    status: Literal["GENERATED", "CACHED", "GATED", "ABSTAINED"]
    caseId: str
    analysis: CopilotAnalysis | None = None
    reason: str | None = None
    model: str | None = None
    playbookRefs: list[str] = Field(default_factory=list)
    tokenControl: dict[str, int | bool | str] = Field(default_factory=dict)
    grounded: bool = False
    decisionPolicy: Literal["HUMAN_REVIEW_REQUIRED"] = "HUMAN_REVIEW_REQUIRED"


class ModelFeedbackRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    alertId: str = Field(min_length=3, max_length=80)
    caseId: str = Field(min_length=3, max_length=100)
    region: Literal["AMER", "EMEA", "APAC", "GLOBAL"]
    assetClass: Literal["FIXED_INCOME", "FOREIGN_EXCHANGE", "INTEREST_RATE_DERIVATIVES", "CREDIT_DERIVATIVES"]
    investigatorOutcome: Literal["RELEVANT", "FALSE_POSITIVE", "ESCALATED", "MORE_EVIDENCE_REQUIRED"]
    reasonCode: str = Field(min_length=3, max_length=100)
    modelVersion: str = Field(min_length=3, max_length=100)
