#!/usr/bin/env python3
"""Train a versioned surveillance model from labelled features or deterministic demo data."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import sklearn
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier, IsolationForest
from sklearn.metrics import brier_score_loss, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split

from app.ml_model import FEATURE_NAMES


def demo_training_data(rows: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    matrix = rng.beta(1.8, 3.2, size=(rows, len(FEATURE_NAMES)))
    index = {name: position for position, name in enumerate(FEATURE_NAMES)}
    raw = (
        matrix[:, index["temporal_proximity"]] * 1.4
        + matrix[:, index["quantity_similarity"]] * 1.2
        + matrix[:, index["recurrence"]] * 1.4
        + matrix[:, index["position_round_trip"]] * 1.3
        + matrix[:, index["common_control"]] * 1.5
        + matrix[:, index["order_cancellation"]] * 1.1
        + matrix[:, index["price_impact"]]
        - matrix[:, index["meaningful_exposure"]] * 1.1
        - matrix[:, index["baseline_consistency"]] * 1.0
        + rng.normal(0, 0.35, rows)
    )
    labels = (raw > np.quantile(raw, 0.78)).astype(int)
    return matrix, labels


def labelled_csv(path: Path) -> tuple[np.ndarray, np.ndarray]:
    records, labels = [], []
    with path.open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            records.append([float(row.get(name, 0.0)) for name in FEATURE_NAMES])
            labels.append(int(row["label"]))
    if len(records) < 500:
        raise ValueError("At least 500 labelled rows are required")
    return np.asarray(records, dtype=np.float64), np.asarray(labels, dtype=np.int8)


def train(matrix: np.ndarray, labels: np.ndarray, source: str, output: Path, seed: int) -> dict:
    train_x, test_x, train_y, test_y = train_test_split(matrix, labels, test_size=0.25, random_state=seed, stratify=labels)
    base = HistGradientBoostingClassifier(max_iter=180, max_leaf_nodes=31, learning_rate=0.07, l2_regularization=0.25, random_state=seed)
    supervised = CalibratedClassifierCV(base, method="sigmoid", cv=3).fit(train_x, train_y)
    anomaly = IsolationForest(n_estimators=160, contamination="auto", random_state=seed, n_jobs=-1).fit(train_x[train_y == 0])
    probability = supervised.predict_proba(test_x)[:, 1]
    prediction = probability >= 0.5
    top = max(1, len(test_y) // 10)
    top_indices = np.argsort(probability)[-top:]
    metrics = {
        "rocAuc": round(float(roc_auc_score(test_y, probability)), 4),
        "precision": round(float(precision_score(test_y, prediction, zero_division=0)), 4),
        "recall": round(float(recall_score(test_y, prediction, zero_division=0)), 4),
        "brierScore": round(float(brier_score_loss(test_y, probability)), 4),
        "precisionAtTopDecile": round(float(test_y[top_indices].mean()), 4),
    }
    metadata = {
        "modelVersion": datetime.now(timezone.utc).strftime("surveillance-ml-%Y%m%d%H%M%S"),
        "trainedAt": datetime.now(timezone.utc).isoformat(),
        "trainingSource": source,
        "trainingRows": int(len(matrix)),
        "positiveRate": round(float(labels.mean()), 4),
        "algorithms": ["Calibrated HistGradientBoostingClassifier", "IsolationForest"],
        "libraryVersions": {"numpy": np.__version__, "scikitLearn": sklearn.__version__, "joblib": joblib.__version__},
        "metrics": metrics,
        "productionApproved": False,
        "approvalReason": "Requires validation on approved, temporally separated surveillance outcomes",
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"featureNames": list(FEATURE_NAMES), "supervisedModel": supervised, "anomalyModel": anomaly, "metadata": metadata}, output)
    output.with_suffix(".metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--training-data", type=Path)
    parser.add_argument("--output", type=Path, default=Path("models/surveillance-model.joblib"))
    parser.add_argument("--demo-rows", type=int, default=8_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.training_data:
        matrix, labels = labelled_csv(args.training_data)
        source = f"approved-labelled-file:{args.training_data.name}"
    else:
        matrix, labels = demo_training_data(args.demo_rows, args.seed)
        source = "deterministic-synthetic-demo"
    print(json.dumps(train(matrix, labels, source, args.output, args.seed), indent=2))


if __name__ == "__main__":
    main()
