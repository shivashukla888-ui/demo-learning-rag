"""Transparent hybrid surveillance scoring with explicit evidence and abstention."""

from dataclasses import dataclass
from math import exp


@dataclass(frozen=True)
class FeatureSpec:
    weight: float
    label: str
    reducer: bool = False


SPECS = {
    "temporal_proximity": FeatureSpec(15, "Trades occurred unusually close together"),
    "quantity_similarity": FeatureSpec(13, "Opposing quantities closely matched"),
    "price_similarity": FeatureSpec(8, "Prices closely matched"),
    "recurrence": FeatureSpec(16, "Pattern repeated within the review window"),
    "position_round_trip": FeatureSpec(17, "Position returned close to its starting level"),
    "common_control": FeatureSpec(18, "Accounts share ownership or control"),
    "behaviour_deviation": FeatureSpec(12, "Behaviour differs from its peer cohort"),
    "illiquidity": FeatureSpec(9, "Activity occurred in a less liquid instrument"),
    "order_cancellation": FeatureSpec(18, "Orders were cancelled before execution"),
    "price_impact": FeatureSpec(15, "Activity had unusual price impact"),
    "information_timing": FeatureSpec(14, "Trading preceded a material event"),
    "client_order_proximity": FeatureSpec(15, "Trading preceded a client order"),
    "volatility_context": FeatureSpec(7, "Market volatility explains part of the activity", True),
    "meaningful_exposure": FeatureSpec(12, "Trading created genuine economic exposure", True),
    "baseline_consistency": FeatureSpec(10, "Behaviour is consistent with the participant baseline", True),
}

TYPOLOGY_FEATURES = {
    "WASH_TRADING": {"temporal_proximity", "quantity_similarity", "recurrence", "position_round_trip", "common_control"},
    "SPOOFING": {"order_cancellation", "price_impact", "recurrence", "behaviour_deviation"},
    "FRONT_RUNNING": {"client_order_proximity", "temporal_proximity", "price_impact", "recurrence"},
    "INSIDER_DEALING": {"information_timing", "behaviour_deviation", "meaningful_exposure", "common_control"},
    "MANIPULATION": {"price_impact", "recurrence", "illiquidity", "common_control"},
}


def _logistic(raw: float) -> int:
    return max(0, min(100, round(100 / (1 + exp(-(raw - 50) / 14)))))


def assess(features: dict[str, float], typology: str = "WASH_TRADING", evidence_refs: list[str] | None = None):
    required = TYPOLOGY_FEATURES.get(typology, TYPOLOGY_FEATURES["WASH_TRADING"])
    supplied_evidence = list(dict.fromkeys(evidence_refs or []))
    contributions = []
    raw = 18.0

    for name, spec in SPECS.items():
        if name not in features:
            continue
        value = float(features[name])
        relevance = 1.0 if name in required else 0.55
        points = round(spec.weight * value * relevance * (-1 if spec.reducer else 1), 2)
        raw += points
        references = [ref for ref in supplied_evidence if ref.lower().startswith(name.split("_")[0].lower())]
        if not references:
            references = [f"Feature:{name}"]
        contributions.append({"feature": name, "label": spec.label, "value": value, "points": points,
                              "direction": "reduce" if spec.reducer else "increase", "evidenceRefs": references})

    missing = sorted(required.difference(features))
    coverage = round((len(required) - len(missing)) / len(required), 2)
    evidence_coverage = round(min(1.0, len(supplied_evidence) / max(1, len(contributions))), 2)
    confidence_value = round(0.2 + 0.55 * coverage + 0.25 * evidence_coverage, 2)
    risk = _logistic(raw)
    warnings = []
    if coverage < 0.6:
        warnings.append("Insufficient typology feature coverage; do not use the score for prioritisation.")
    if evidence_coverage < 0.75:
        warnings.append("Some explanatory claims do not yet have source-system evidence.")
    band = "INSUFFICIENT_DATA" if coverage < 0.6 else "HIGH" if risk >= 80 else "MEDIUM" if risk >= 45 else "LOW"
    return risk, confidence_value, band, contributions, missing, evidence_coverage, warnings


def score(features: dict[str, float]):
    risk, _, _, contributions, _, _, _ = assess(features)
    return risk, contributions


def confidence(features: dict[str, float]):
    _, value, _, _, _, _, _ = assess(features)
    return value
