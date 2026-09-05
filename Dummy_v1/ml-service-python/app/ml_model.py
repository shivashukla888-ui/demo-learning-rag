"""Versioned supervised + anomaly model loading and vectorised inference."""

from __future__ import annotations

import json
import os
from pathlib import Path

import joblib
import numpy as np

from .scoring import SPECS

FEATURE_NAMES = tuple(SPECS.keys())


class ProductionModel:
    def __init__(self, artifact_path: str | None = None):
        self.path = Path(artifact_path or os.getenv("MODEL_PATH", "/app/models/surveillance-model.joblib"))
        self.artifact = None
        self.load_error: str | None = None
        self.reload()

    def reload(self) -> None:
        try:
            loaded = joblib.load(self.path)
            expected = list(FEATURE_NAMES)
            if loaded.get("featureNames") != expected:
                raise ValueError("Model feature contract does not match the running service")
            self.artifact = loaded
            self.load_error = None
        except Exception as exc:
            self.artifact = None
            self.load_error = f"{type(exc).__name__}: {exc}"
            if os.getenv("MODEL_REQUIRED", "false").lower() == "true":
                raise RuntimeError(f"Required model artifact could not be loaded: {self.load_error}") from exc

    @property
    def available(self) -> bool:
        return self.artifact is not None

    def metadata(self) -> dict:
        if not self.available:
            return {"available": False, "mode": "TRANSPARENT_FALLBACK", "path": str(self.path), "loadError": self.load_error}
        metadata = dict(self.artifact["metadata"])
        metadata.update({"available": True, "mode": "TRAINED_HYBRID", "path": str(self.path)})
        return metadata

    def predict(self, feature_records: list[dict[str, float]]) -> list[dict[str, float]]:
        if not self.available:
            return []
        matrix = np.asarray([[float(record.get(name, 0.0)) for name in FEATURE_NAMES] for record in feature_records], dtype=np.float64)
        probability = self.artifact["supervisedModel"].predict_proba(matrix)[:, 1]
        decision = self.artifact["anomalyModel"].decision_function(matrix)
        anomaly = 1.0 / (1.0 + np.exp(np.clip(decision * 8.0, -30, 30)))
        combined = np.clip(probability * 0.82 + anomaly * 0.18, 0.0, 1.0)
        return [
            {"probability": round(float(probability[index]), 6), "anomaly": round(float(anomaly[index]), 6), "risk": int(round(float(combined[index]) * 100))}
            for index in range(len(feature_records))
        ]

    def metadata_json(self) -> str:
        return json.dumps(self.metadata(), sort_keys=True)
