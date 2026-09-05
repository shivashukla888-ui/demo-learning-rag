"""Streaming trade-file feature extraction and hybrid ML scoring."""

from __future__ import annotations

import csv
import hashlib
import heapq
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from time import perf_counter

from .ml_model import ProductionModel
from .scoring import assess


class TradeFilePipeline:
    def __init__(self, model: ProductionModel, chunk_size: int, ml_weight: float):
        self.model = model
        self.chunk_size = chunk_size
        self.ml_weight = ml_weight
        self.input_root = Path(os.getenv("ML_TRADE_INPUT_ROOT", "/data/input/accepted/trades")).resolve()
        self.output_root = Path(os.getenv("ML_OUTPUT_ROOT", "/data/ml-output")).resolve()
        self.scan_seconds = max(2, int(os.getenv("ML_FILE_SCAN_SECONDS", "5")))
        self.max_records = max(10_000, int(os.getenv("ML_MAX_FILE_RECORDS", "2000000")))
        self.max_state_keys = max(10_000, int(os.getenv("ML_MAX_STATE_KEYS", "250000")))
        self.min_free_disk_bytes = max(0, int(os.getenv("ML_MIN_FREE_DISK_BYTES", "1073741824")))
        self.file_workers = max(1, min(8, int(os.getenv("ML_FILE_WORKERS", "2"))))
        self.jobs: dict[str, dict] = {}
        self.lock = Lock()
        self.stop_event = Event()
        self.worker: Thread | None = None
        self.executor = ThreadPoolExecutor(max_workers=self.file_workers, thread_name_prefix="trade-file-worker")

    def start(self) -> None:
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.worker = Thread(target=self._poll, name="trade-file-ml-pipeline", daemon=True)
        self.worker.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.worker:
            self.worker.join(timeout=2)
        self.executor.shutdown(wait=False, cancel_futures=False)

    def scan(self) -> list[dict]:
        if not self.input_root.is_dir():
            return []
        discovered = []
        for source in sorted(self.input_root.glob("*.csv")):
            fingerprint = self._fingerprint(source)
            summary_path = self.output_root / f"{fingerprint}.summary.json"
            with self.lock:
                active = fingerprint in self.jobs and self.jobs[fingerprint]["status"] in {"QUEUED", "RUNNING"}
            if summary_path.exists() or active:
                continue
            job = {"jobId": fingerprint, "filename": source.name, "status": "QUEUED", "createdAt": self._now()}
            with self.lock:
                self.jobs[fingerprint] = job
            self.executor.submit(self._process, fingerprint, source, summary_path)
            discovered.append(dict(job))
        return discovered

    def status(self) -> list[dict]:
        summaries = []
        if self.output_root.is_dir():
            for path in sorted(self.output_root.glob("*.summary.json"), reverse=True):
                try:
                    summaries.append(json.loads(path.read_text(encoding="utf-8")))
                except (OSError, json.JSONDecodeError):
                    continue
            completed_ids = {item.get("jobId") for item in summaries}
            for path in sorted(self.output_root.glob("*.progress.json"), reverse=True):
                try:
                    progress = json.loads(path.read_text(encoding="utf-8"))
                    if progress.get("jobId") not in completed_ids:
                        summaries.append(progress)
                except (OSError, json.JSONDecodeError):
                    continue
        with self.lock:
            completed_ids = {item.get("jobId") for item in summaries}
            summaries.extend(dict(job) for key, job in self.jobs.items() if key not in completed_ids)
        return summaries

    def _poll(self) -> None:
        while not self.stop_event.is_set():
            self.scan()
            self.stop_event.wait(self.scan_seconds)

    def _process(self, job_id: str, source: Path, summary_path: Path) -> None:
        started = perf_counter()
        with self.lock:
            self.jobs[job_id]["status"] = "RUNNING"
        output_path = self.output_root / f"{job_id}.scores.jsonl"
        progress_path = self.output_root / f"{job_id}.progress.json"
        processed = high = medium = low = insufficient = 0
        previous: dict[str, dict] = {}
        account_state: dict[tuple[str, str], dict[str, float]] = {}
        chunk_features: list[dict[str, float]] = []
        chunk_meta: list[dict] = []
        top_cases: list[tuple[int, str, dict]] = []
        try:
            with source.open(newline="", encoding="utf-8") as input_stream, output_path.open("w", encoding="utf-8") as output_stream:
                for row in csv.DictReader(input_stream):
                    features = self._features(row, previous, account_state)
                    chunk_features.append(features)
                    chunk_meta.append(row)
                    if len(chunk_features) >= self.chunk_size:
                        counts = self._write_chunk(output_stream, chunk_meta, chunk_features, top_cases)
                        processed += counts[0]; high += counts[1]; medium += counts[2]; low += counts[3]; insufficient += counts[4]
                        self._check_limits(processed)
                        self._record_progress(progress_path, job_id, source.name, processed, high, medium, low, insufficient, started)
                        chunk_features, chunk_meta = [], []
                if chunk_features:
                    counts = self._write_chunk(output_stream, chunk_meta, chunk_features, top_cases)
                    processed += counts[0]; high += counts[1]; medium += counts[2]; low += counts[3]; insufficient += counts[4]
                    self._check_limits(processed)
                    self._record_progress(progress_path, job_id, source.name, processed, high, medium, low, insufficient, started)
            elapsed = max(0.000001, perf_counter() - started)
            summary = {"jobId": job_id, "filename": source.name, "status": "SUCCEEDED", "processed": processed,
                "riskBands": {"HIGH": high, "MEDIUM": medium, "LOW": low, "INSUFFICIENT_DATA": insufficient},
                "durationSeconds": round(elapsed, 4), "recordsPerSecond": round(processed / elapsed, 2),
                "model": self.model.metadata().get("modelVersion", "transparent-fallback"),
                "topCases": [item[2] for item in sorted(top_cases, reverse=True)],
                "outputFile": output_path.name, "completedAt": self._now()}
            self._write_json_atomic(summary_path, summary)
            with self.lock:
                self.jobs[job_id] = summary
        except Exception as exc:
            failure = {"jobId": job_id, "filename": source.name, "status": "FAILED", "processed": processed,
                "error": f"{type(exc).__name__}: {exc}", "completedAt": self._now()}
            try:
                self._write_json_atomic(summary_path, failure)
            except OSError:
                pass
            with self.lock:
                self.jobs[job_id] = failure

    def _write_chunk(self, stream, rows: list[dict], features: list[dict[str, float]],
                     top_cases: list[tuple[int, str, dict]]) -> tuple[int, int, int, int, int]:
        predictions = self.model.predict(features)
        bands = {"HIGH": 0, "MEDIUM": 0, "LOW": 0, "INSUFFICIENT_DATA": 0}
        for index, (row, record) in enumerate(zip(rows, features)):
            rule_risk, confidence, band, _, missing, _, _ = assess(record, "WASH_TRADING", [f"Trade:{row.get('trade_id', '')}"])
            ml = predictions[index] if predictions else None
            risk = round(rule_risk * (1 - self.ml_weight) + ml["risk"] * self.ml_weight) if ml and band != "INSUFFICIENT_DATA" else rule_risk
            final_band = "INSUFFICIENT_DATA" if band == "INSUFFICIENT_DATA" else "HIGH" if risk >= 80 else "MEDIUM" if risk >= 45 else "LOW"
            bands[final_band] += 1
            trade_id = row.get("trade_id") or f"ROW-{index}"
            record = {"caseId": f"CASE-{trade_id}", "tradeId": trade_id,
                "instrument": row.get("instrument") or "Unknown", "risk": risk, "band": final_band,
                "confidence": confidence, "mlProbability": ml["probability"] if ml else None,
                "anomalyScore": ml["anomaly"] if ml else None, "missingFeatures": missing,
                "driver": self._driver(record), "evidenceRefs": [f"Trade:{trade_id}"]}
            stream.write(json.dumps(record) + "\n")
            rank_key = hashlib.sha1(json.dumps(row, sort_keys=True).encode()).hexdigest()
            if len(top_cases) < 25:
                heapq.heappush(top_cases, (risk, rank_key, record))
            elif risk > top_cases[0][0]:
                heapq.heapreplace(top_cases, (risk, rank_key, record))
        return len(rows), bands["HIGH"], bands["MEDIUM"], bands["LOW"], bands["INSUFFICIENT_DATA"]

    def _driver(self, features: dict[str, float]) -> str:
        labels = {
            "common_control": "Linked accounts show common control",
            "position_round_trip": "Position returned close to its starting level",
            "recurrence": "Pattern repeated within the review window",
            "quantity_similarity": "Opposing quantities closely matched",
            "behaviour_deviation": "Activity differs from the participant baseline",
        }
        key = max(labels, key=lambda name: features.get(name, 0.0))
        return labels[key]

    def _features(self, row: dict, previous: dict[str, dict], states: dict[tuple[str, str], dict[str, float]]) -> dict[str, float]:
        instrument, account = row.get("instrument", ""), row.get("account_id", "")
        quantity, price = self._number(row.get("quantity")), self._number(row.get("price"))
        prior = previous.get(instrument)
        # Aggregate economic exposure at client level so linked-account round trips are visible.
        key = (row.get("client_id") or account, instrument)
        state = states.setdefault(key, {"count": 0.0, "gross": 0.0, "net": 0.0, "averageQuantity": quantity or 1.0})
        side = 1.0 if row.get("side", "").upper() == "BUY" else -1.0
        state["count"] += 1; state["gross"] += quantity; state["net"] += side * quantity
        average = state["averageQuantity"]
        state["averageQuantity"] = average * .9 + quantity * .1
        opposite = prior is not None and prior.get("side", "").upper() != row.get("side", "").upper()
        quantity_similarity = self._similarity(quantity, self._number(prior.get("quantity")) if prior else 0.0) if opposite else 0.0
        price_similarity = self._similarity(price, self._number(prior.get("price")) if prior else 0.0) if prior else 0.0
        common_control = 1.0 if opposite and prior.get("client_id") == row.get("client_id") and prior.get("account_id") != account else 0.0
        features = {
            "temporal_proximity": 0.9 if prior else 0.0,
            "quantity_similarity": quantity_similarity,
            "price_similarity": price_similarity,
            "recurrence": min(1.0, state["count"] / 8.0),
            "position_round_trip": 1.0 - min(1.0, abs(state["net"]) / max(state["gross"], 1.0)),
            "common_control": common_control,
            "behaviour_deviation": min(1.0, max(0.0, quantity / max(average, 1.0) - 1.0) / 3.0),
            "illiquidity": 0.0, "order_cancellation": 0.0, "price_impact": 0.0,
            "information_timing": 0.0, "client_order_proximity": 0.0, "volatility_context": 0.0,
            "meaningful_exposure": min(1.0, abs(state["net"]) / max(state["gross"], 1.0)),
            "baseline_consistency": max(0.0, 1.0 - min(1.0, abs(quantity - average) / max(average, 1.0))),
        }
        previous[instrument] = dict(row)
        if len(previous) > self.max_state_keys:
            previous.pop(next(iter(previous)))
        if len(states) > self.max_state_keys:
            states.pop(next(iter(states)))
        return features

    def _record_progress(self, path: Path, job_id: str, filename: str, processed: int,
                         high: int, medium: int, low: int, insufficient: int, started: float) -> None:
        elapsed = max(0.000001, perf_counter() - started)
        progress = {"jobId": job_id, "filename": filename, "status": "RUNNING", "processed": processed,
            "riskBands": {"HIGH": high, "MEDIUM": medium, "LOW": low, "INSUFFICIENT_DATA": insufficient},
            "durationSeconds": round(elapsed, 4), "recordsPerSecond": round(processed / elapsed, 2),
            "boundedStateKeys": self.max_state_keys, "updatedAt": self._now()}
        self._write_json_atomic(path, progress)
        with self.lock:
            self.jobs[job_id] = progress

    def _check_limits(self, processed: int) -> None:
        if processed > self.max_records:
            raise ValueError(f"File exceeds configured ML_MAX_FILE_RECORDS={self.max_records}")
        if shutil.disk_usage(self.output_root).free < self.min_free_disk_bytes:
            raise OSError("Processing paused because output storage is below the configured free-space reserve")

    def _write_json_atomic(self, path: Path, value: dict) -> None:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
        os.replace(temporary, path)

    def _fingerprint(self, path: Path) -> str:
        stat = path.stat()
        return hashlib.sha256(f"{path.name}:{stat.st_size}:{stat.st_mtime_ns}".encode()).hexdigest()[:20]
    def _number(self, value) -> float:
        try: return max(0.0, float(value or 0.0))
        except (TypeError, ValueError): return 0.0
    def _similarity(self, left: float, right: float) -> float:
        return min(left, right) / max(left, right) if left > 0 and right > 0 else 0.0
    def _now(self) -> str: return datetime.now(timezone.utc).isoformat()
