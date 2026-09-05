from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from threading import Lock
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query, status

from .ml_model import ProductionModel
from .file_pipeline import TradeFilePipeline
from .daily_pipeline import DailyBatchPipeline
from .llm import OpenAIEvidenceSummarizer, TokenEfficientInvestigationCopilot
from .llm_guardrails import GuardrailViolation
from .models import (Assessment, BatchScoreRequest, EvidenceSummaryRequest,
                     EvidenceSummaryResponse, InvestigationCopilotRequest,
                     InvestigationCopilotResponse, ModelFeedbackRequest, ScoreRequest)
from .scoring import TYPOLOGY_FEATURES, assess

MODEL_CARD = {
    "model": "hybrid-transparent-surveillance-v1.0",
    "purpose": "Prioritise potentially suspicious activity for human investigation",
    "notFor": ["autonomous enforcement", "final market-abuse determination", "case closure"],
    "evaluation": {"precisionAtTopDecile": 0.81, "recall": 0.88, "falsePositiveReduction": 0.42, "calibrationError": 0.04},
    "thresholds": {"high": 80, "medium": 45, "minimumFeatureCoverage": 0.60, "minimumEvidenceCoverage": 0.75},
    "monitoring": ["feature drift", "score drift", "precision by typology", "data completeness", "override rate"],
    "accuracyControls": ["sigmoid probability calibration", "rule-supervised-anomaly disagreement",
        "evidence-coverage abstention", "region and asset-class thresholds", "governed investigator feedback"],
    "llmRole": "Eligible-case investigation aid only; never a scoring or disposition model",
    "dataNotice": "Evaluation metrics are synthetic prototype results and must be revalidated on approved historical data.",
}

TRAINED_MODEL = ProductionModel()
BATCH_SIZE = max(100, min(10_000, int(os.getenv("ML_BATCH_SIZE", "1000"))))
FILE_CHUNK_SIZE = max(1_000, min(50_000, int(os.getenv("ML_FILE_CHUNK_SIZE", "10000"))))
EXECUTOR = ThreadPoolExecutor(max_workers=max(1, min(8, int(os.getenv("ML_BATCH_WORKERS", "2")))))
ML_WEIGHT = max(0.0, min(1.0, float(os.getenv("ML_MODEL_WEIGHT", "0.55"))))
RULE_WEIGHT = 1.0 - ML_WEIGHT
JOBS: dict[str, dict] = {}
JOBS_LOCK = Lock()
FILE_PIPELINE = TradeFilePipeline(TRAINED_MODEL, FILE_CHUNK_SIZE, ML_WEIGHT)
DAILY_PIPELINE = DailyBatchPipeline(TRAINED_MODEL)
LLM_SUMMARIZER = OpenAIEvidenceSummarizer()
INVESTIGATION_COPILOT = TokenEfficientInvestigationCopilot()
FEEDBACK_PATH = Path(os.getenv("MODEL_FEEDBACK_PATH", "/data/feedback/investigator-feedback.jsonl"))
FEEDBACK_LOCK = Lock()


@asynccontextmanager
async def lifespan(_: FastAPI):
    FILE_PIPELINE.start()
    DAILY_PIPELINE.start()
    yield
    FILE_PIPELINE.stop()
    DAILY_PIPELINE.stop()


app = FastAPI(title="Trade Surveillance Navigator ML Scoring Service", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL_CARD["model"], "trainedModel": TRAINED_MODEL.metadata(),
        "llm": {"summary": LLM_SUMMARIZER.status(), "investigationCopilot": INVESTIGATION_COPILOT.status()},
        "dailyPairedBatch": DAILY_PIPELINE.configuration(),
        "humanDecisionRequired": True}


@app.get("/v1/model-card")
def model_card():
    return {**MODEL_CARD, "trainedModel": TRAINED_MODEL.metadata(), "batchConfiguration": {
        "maxApiRecords": 10_000, "apiChunkSize": BATCH_SIZE, "fileChunkSize": FILE_CHUNK_SIZE,
        "maxFileRecords": FILE_PIPELINE.max_records, "fileWorkers": FILE_PIPELINE.file_workers},
        "dailyPairedBatch": DAILY_PIPELINE.configuration()}


@app.get("/v1/daily-batches/configuration")
def daily_batch_configuration():
    return DAILY_PIPELINE.configuration()


@app.get("/v1/daily-batches")
def daily_batch_jobs():
    return {"jobs": DAILY_PIPELINE.status()}


@app.get("/v1/daily-batches/cases")
def daily_batch_cases(limit: int = Query(default=250, ge=1, le=1000)):
    return {"cases": DAILY_PIPELINE.cases(limit)}


@app.post("/v1/daily-batches/scan")
def scan_daily_batches():
    return {"discovered": DAILY_PIPELINE.scan(), "jobs": DAILY_PIPELINE.status()}


@app.post("/v1/model-feedback", status_code=status.HTTP_202_ACCEPTED)
def model_feedback(request: ModelFeedbackRequest):
    event = {**request.model_dump(), "recordedAt": datetime.now(timezone.utc).isoformat(),
             "use": "GOVERNED_RETRAINING_CANDIDATE", "automaticallyRetrainsModel": False}
    FEEDBACK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FEEDBACK_LOCK, FEEDBACK_PATH.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(event, separators=(",", ":")) + "\n")
    return {"status": "RECORDED", "alertId": request.alertId,
            "automaticallyRetrainsModel": False, "humanApprovalRequiredForRetraining": True}


@app.get("/v1/typologies")
def typologies():
    return {name: sorted(features) for name, features in TYPOLOGY_FEATURES.items()}


@app.get("/v1/llm/status")
def llm_status():
    return {"summary": LLM_SUMMARIZER.status(), "investigationCopilot": INVESTIGATION_COPILOT.status()}


@app.post("/v1/investigation-copilot", response_model=InvestigationCopilotResponse)
def investigation_copilot(request: InvestigationCopilotRequest):
    try:
        return INVESTIGATION_COPILOT.analyse(request)
    except GuardrailViolation as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"LLM guardrail blocked the request or response: {exc}") from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
                            detail=f"Investigation copilot failed safely: {type(exc).__name__}") from exc


@app.post("/v1/evidence-summary", response_model=EvidenceSummaryResponse)
def evidence_summary(request: EvidenceSummaryRequest):
    if not LLM_SUMMARIZER.configured:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="OpenAI evidence summarisation is disabled or missing OPENAI_API_KEY/OPENAI_MODEL")
    try:
        summary = LLM_SUMMARIZER.summarize(request)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Evidence summary generation failed: {type(exc).__name__}") from exc
    return EvidenceSummaryResponse(caseId=request.caseId, summary=summary,
        evidenceRefs=list(dict.fromkeys(request.evidenceRefs)), model=LLM_SUMMARIZER.model)

@app.post("/v1/score", response_model=Assessment)
def calculate(req: ScoreRequest):
    risk, conf, band, contributions, missing, evidence_coverage, warnings = assess(req.features, req.typology, req.evidenceRefs)
    trained = TRAINED_MODEL.predict([req.features])
    ml = trained[0] if trained else None
    final_risk = round(risk * RULE_WEIGHT + ml["risk"] * ML_WEIGHT) if ml and band != "INSUFFICIENT_DATA" else risk
    final_band = "INSUFFICIENT_DATA" if band == "INSUFFICIENT_DATA" else "HIGH" if final_risk >= 80 else "MEDIUM" if final_risk >= 45 else "LOW"
    disagreement, uncertainty = _model_diagnostics(risk, ml)
    return Assessment(assessmentId=str(uuid4()), caseId=f"CASE-{req.alertId}", typology=req.typology,
        risk=final_risk, confidence=conf, band=final_band, decisionPolicy="HUMAN_REVIEW_REQUIRED",
        contributions=contributions, evidenceRefs=list(dict.fromkeys(req.evidenceRefs)), evidenceCoverage=evidence_coverage,
        dataGaps=[f"Missing typology feature: {name}" for name in missing], warnings=warnings,
        versions={"model": TRAINED_MODEL.metadata().get("modelVersion", MODEL_CARD["model"]), "features": "surveillance-features-v2", "policy": "human-review-v1"},
        mlRisk=ml["risk"] if ml else None, mlProbability=ml["probability"] if ml else None,
        anomalyScore=ml["anomaly"] if ml else None, modelDisagreement=disagreement, uncertaintyBand=uncertainty,
        copilotEligible=evidence_coverage >= INVESTIGATION_COPILOT.min_coverage and
            (final_risk >= INVESTIGATION_COPILOT.min_priority or (disagreement or 0) >= INVESTIGATION_COPILOT.min_disagreement),
        modelMode="TRAINED_HYBRID" if ml else "TRANSPARENT_FALLBACK")


def _model_diagnostics(rule_risk: int, prediction: dict | None) -> tuple[float | None, str]:
    if not prediction:
        return None, "UNAVAILABLE"
    components = [rule_risk / 100.0, float(prediction["probability"]), float(prediction["anomaly"])]
    disagreement = round(max(components) - min(components), 4)
    probability = float(prediction["probability"])
    uncertainty = "HIGH" if disagreement >= 0.35 or 0.4 <= probability <= 0.6 else "MEDIUM" if disagreement >= 0.2 else "LOW"
    return disagreement, uncertainty


def _run_batch(job_id: str, request: BatchScoreRequest) -> None:
    started = perf_counter()
    try:
        output = []
        for offset in range(0, len(request.records), BATCH_SIZE):
            chunk = request.records[offset:offset + BATCH_SIZE]
            predictions = TRAINED_MODEL.predict([item.features for item in chunk])
            for index, item in enumerate(chunk):
                rule_risk, confidence, band, _, missing, coverage, warnings = assess(item.features, item.typology, item.evidenceRefs)
                ml = predictions[index] if predictions else None
                risk = round(rule_risk * RULE_WEIGHT + ml["risk"] * ML_WEIGHT) if ml and band != "INSUFFICIENT_DATA" else rule_risk
                disagreement, uncertainty = _model_diagnostics(rule_risk, ml)
                output.append({"alertId": item.alertId, "risk": risk,
                    "band": "INSUFFICIENT_DATA" if band == "INSUFFICIENT_DATA" else "HIGH" if risk >= 80 else "MEDIUM" if risk >= 45 else "LOW",
                    "confidence": confidence, "evidenceCoverage": coverage, "missingFeatures": missing, "warnings": warnings,
                    "mlProbability": ml["probability"] if ml else None, "anomalyScore": ml["anomaly"] if ml else None,
                    "modelDisagreement": disagreement, "uncertaintyBand": uncertainty,
                    "copilotEligible": coverage >= INVESTIGATION_COPILOT.min_coverage and
                        (risk >= INVESTIGATION_COPILOT.min_priority or (disagreement or 0) >= INVESTIGATION_COPILOT.min_disagreement)})
        elapsed = max(0.000001, perf_counter() - started)
        with JOBS_LOCK:
            JOBS[job_id].update({"status": "SUCCEEDED", "completedAt": datetime.now(timezone.utc).isoformat(),
                "processed": len(output), "durationSeconds": round(elapsed, 4), "recordsPerSecond": round(len(output) / elapsed, 2), "results": output})
    except Exception as exc:
        with JOBS_LOCK:
            JOBS[job_id].update({"status": "FAILED", "completedAt": datetime.now(timezone.utc).isoformat(), "error": f"{type(exc).__name__}: {exc}"})


@app.post("/v1/batch-score", status_code=status.HTTP_202_ACCEPTED)
def batch_score(request: BatchScoreRequest):
    job_id = str(uuid4())
    job = {"jobId": job_id, "batchId": request.batchId, "status": "QUEUED", "received": len(request.records),
        "modelMode": "TRAINED_HYBRID" if TRAINED_MODEL.available else "TRANSPARENT_FALLBACK",
        "createdAt": datetime.now(timezone.utc).isoformat()}
    with JOBS_LOCK:
        JOBS[job_id] = job
    EXECUTOR.submit(_run_batch, job_id, request)
    return job


@app.get("/v1/batch-jobs/{job_id}")
def batch_job(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Batch job not found")
        return job


@app.post("/v1/file-jobs/scan")
def scan_trade_files():
    return {"discovered": FILE_PIPELINE.scan()}


@app.get("/v1/file-jobs")
def file_jobs():
    return {"jobs": FILE_PIPELINE.status()}
