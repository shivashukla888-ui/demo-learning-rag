from fastapi.testclient import TestClient
from time import sleep

from app.main import app
from app.scoring import assess, confidence, score

client = TestClient(app)


def test_high_risk_features_rank_above_contextual_case():
    high, _ = score({"temporal_proximity": 1, "quantity_similarity": .998, "recurrence": 1, "position_round_trip": .997, "common_control": 1, "illiquidity": .9})
    low, _ = score({"temporal_proximity": .8, "volatility_context": 1, "meaningful_exposure": 1, "baseline_consistency": 1})
    assert high > low


def test_missingness_reduces_confidence_and_forces_abstention():
    risk, conf, band, _, missing, _, warnings = assess({"temporal_proximity": 1}, "WASH_TRADING")
    assert risk >= 0
    assert conf < confidence({"temporal_proximity": 1, "quantity_similarity": 1, "recurrence": 1, "position_round_trip": 1, "common_control": 1})
    assert band == "INSUFFICIENT_DATA" and missing and warnings


def test_spoofing_uses_typology_specific_features():
    risk, _, band, contributions, missing, _, _ = assess(
        {"order_cancellation": 1, "price_impact": .9, "recurrence": .8, "behaviour_deviation": .9},
        "SPOOFING", ["Order:O-1", "Market:M-1", "Trade:T-1", "Profile:P-1"])
    assert risk >= 80 and band == "HIGH" and not missing
    assert {item["feature"] for item in contributions} >= {"order_cancellation", "price_impact"}


def test_api_rejects_out_of_range_features():
    response = client.post("/v1/score", json={"alertId": "A-1", "asOf": "2026-08-25T10:00:00Z", "features": {"recurrence": 1.2}})
    assert response.status_code == 422


def test_model_card_prohibits_autonomous_decisions():
    assert "autonomous enforcement" in client.get("/v1/model-card").json()["notFor"]


def test_batch_scoring_job_processes_multiple_records():
    payload = {"batchId": "TEST-BATCH", "records": [
        {"alertId": f"A-{index}", "typology": "WASH_TRADING", "features": {
            "temporal_proximity": .9, "quantity_similarity": .95, "recurrence": .8,
            "position_round_trip": .9, "common_control": .8}}
        for index in range(20)
    ]}
    submitted = client.post("/v1/batch-score", json=payload)
    assert submitted.status_code == 202
    job_id = submitted.json()["jobId"]
    for _ in range(50):
        result = client.get(f"/v1/batch-jobs/{job_id}").json()
        if result["status"] in {"SUCCEEDED", "FAILED"}:
            break
        sleep(.02)
    assert result["status"] == "SUCCEEDED"
    assert result["processed"] == 20
