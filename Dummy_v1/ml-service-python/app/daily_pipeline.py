"""Region-aware daily alert + Parquet intelligence pipeline.

The existing Java rule alert is immutable. This module reads the daily alert
contract, selects only relevant Parquet context, scores it, groups connected
alerts, and writes a separate enrichment result for human review.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from threading import Event, Lock, Thread
from time import perf_counter
from typing import Iterable

import pyarrow.parquet as pq

from .ml_model import ProductionModel
from .scoring import assess


REQUIRED_ALERT_FIELDS = {
    "alertId", "ruleId", "ruleVersion", "typology", "assetClass",
    "businessDate", "region", "triggeringTradeIds",
}
REQUIRED_PARQUET_COLUMNS = {
    "trade_id", "event_time", "instrument", "asset_class", "region",
    "side", "quantity", "price", "account_id", "client_id",
}
SELECTED_COLUMNS = sorted(REQUIRED_PARQUET_COLUMNS | {"order_id", "venue"})
ASSET_CLASSES = {
    "FIXED_INCOME", "FOREIGN_EXCHANGE", "INTEREST_RATE_DERIVATIVES", "CREDIT_DERIVATIVES",
}


class DailyBatchPipeline:
    def __init__(self, model: ProductionModel):
        self.model = model
        self.input_root = Path(os.getenv("DAILY_INPUT_ROOT", "/data/daily-input")).resolve()
        self.output_root = Path(os.getenv("DAILY_OUTPUT_ROOT", "/data/daily-output")).resolve()
        self.state_path = Path(os.getenv("DAILY_FEATURE_STATE_PATH", "/data/state/rolling-features.json")).resolve()
        self.batch_rows = max(1_000, min(1_000_000, int(os.getenv("PARQUET_BATCH_ROWS", "100000"))))
        self.max_context_per_alert = max(100, int(os.getenv("DAILY_MAX_CONTEXT_PER_ALERT", "5000")))
        self.max_alerts = max(1, int(os.getenv("DAILY_MAX_ALERTS", "500000")))
        self.max_state_keys = max(10_000, int(os.getenv("ML_MAX_STATE_KEYS", "250000")))
        self.scan_seconds = max(2, int(os.getenv("DAILY_SCAN_SECONDS", "5")))
        self.workers = max(1, min(8, int(os.getenv("DAILY_BATCH_WORKERS", "2"))))
        self.rule_weight = float(os.getenv("DAILY_RULE_WEIGHT", "0.30"))
        self.supervised_weight = float(os.getenv("DAILY_SUPERVISED_WEIGHT", "0.55"))
        self.anomaly_weight = float(os.getenv("DAILY_ANOMALY_WEIGHT", "0.15"))
        self.copilot_min_priority = max(0, min(100, int(os.getenv("LLM_MIN_PRIORITY", "70"))))
        self.copilot_min_coverage = max(0.0, min(1.0, float(os.getenv("LLM_MIN_EVIDENCE_COVERAGE", "0.75"))))
        self.copilot_min_disagreement = max(0.0, min(1.0, float(os.getenv("LLM_MIN_MODEL_DISAGREEMENT", "0.35"))))
        self.min_free_disk = max(0, int(os.getenv("ML_MIN_FREE_DISK_BYTES", "1073741824")))
        self.allowed_regions = {
            item.strip().upper() for item in os.getenv("DAILY_ALLOWED_REGIONS", "AMER,EMEA,APAC,GLOBAL").split(",") if item.strip()
        }
        if abs(self.rule_weight + self.supervised_weight + self.anomaly_weight - 1.0) > 0.0001:
            raise ValueError("Daily scoring weights must total 1.0")
        self.jobs: dict[str, dict] = {}
        self.lock = Lock()
        self.stop_event = Event()
        self.worker: Thread | None = None
        self.executor = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="daily-batch-worker")

    def start(self) -> None:
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.worker = Thread(target=self._poll, name="daily-paired-batch-pipeline", daemon=True)
        self.worker.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.worker:
            self.worker.join(timeout=2)
        self.executor.shutdown(wait=False, cancel_futures=False)

    def configuration(self) -> dict:
        return {
            "mode": "DAILY_PAIRED_BATCH", "inputRoot": str(self.input_root), "outputRoot": str(self.output_root),
            "allowedRegions": sorted(self.allowed_regions), "requiredAlertFields": sorted(REQUIRED_ALERT_FIELDS),
            "requiredParquetColumns": sorted(REQUIRED_PARQUET_COLUMNS), "parquetBatchRows": self.batch_rows,
            "maxContextRecordsPerAlert": self.max_context_per_alert, "maxAlertsPerBatch": self.max_alerts,
            "weights": {"existingRule": self.rule_weight, "supervisedML": self.supervised_weight, "anomaly": self.anomaly_weight},
            "copilotEligibility": {"minimumPriority": self.copilot_min_priority,
                                   "minimumEvidenceCoverage": self.copilot_min_coverage,
                                   "minimumModelDisagreement": self.copilot_min_disagreement},
            "controls": {"preserveOriginalAlert": True, "automaticClosure": False, "humanDecisionRequired": True,
                         "rawParquetSentToLlm": False, "directIdentifiersSentToLlm": False},
        }

    def scan(self) -> list[dict]:
        discovered = []
        if not self.input_root.is_dir():
            return discovered
        for manifest_path in sorted(self.input_root.glob("region=*/business_date=*/batch_id=*/manifest.ready.json")):
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            job_id = self._job_id(manifest)
            summary_path = self.output_root / f"{job_id}.summary.json"
            with self.lock:
                active = job_id in self.jobs and self.jobs[job_id]["status"] in {"QUEUED", "RUNNING"}
                if summary_path.exists() or active:
                    continue
                job = {"jobId": job_id, "batchId": manifest.get("batchId"), "businessDate": manifest.get("businessDate"),
                       "region": manifest.get("region"), "status": "QUEUED", "createdAt": self._now()}
                self.jobs[job_id] = job
            self.executor.submit(self._process, job_id, manifest_path, summary_path)
            discovered.append(dict(job))
        return discovered

    def status(self) -> list[dict]:
        values = []
        if self.output_root.is_dir():
            for path in sorted(self.output_root.glob("*.summary.json"), reverse=True):
                try: values.append(json.loads(path.read_text(encoding="utf-8")))
                except (OSError, json.JSONDecodeError): continue
        with self.lock:
            completed = {item.get("jobId") for item in values}
            values.extend(dict(job) for key, job in self.jobs.items() if key not in completed)
        return values

    def cases(self, limit: int = 250) -> list[dict]:
        """Return a bounded, case-level view of completed daily enrichments."""
        grouped: dict[str, dict] = {}
        for summary in self.status():
            if summary.get("status") != "SUCCEEDED" or not summary.get("outputFile"):
                continue
            output_path = self._resolved_file(self.output_root, summary["outputFile"])
            if not output_path.is_file():
                continue
            with output_path.open(encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    item = json.loads(line)
                    case_id = item["clusterId"]
                    current = grouped.setdefault(case_id, {
                        "id": case_id, "sourceJobId": summary["jobId"], "businessDate": item["businessDate"],
                        "region": item["region"], "assetClass": item["assetClass"], "typology": item["typology"],
                        "instrument": item.get("instrument", "Unknown"), "risk": 0, "confidence": 0.0,
                        "band": "LOW", "alertCount": 0, "sourceAlertIds": [], "evidenceRefs": [],
                        "driver": "Hybrid pattern requires human review", "modelVersion": item["modelVersion"],
                        "humanDecisionRequired": True,
                    })
                    current["alertCount"] += 1
                    current["sourceAlertIds"].append(item["sourceAlertId"])
                    current["evidenceRefs"] = list(dict.fromkeys(current["evidenceRefs"] + item.get("evidenceReferences", [])))[:100]
                    if item["aiPriorityScore"] >= current["risk"]:
                        current["risk"] = item["aiPriorityScore"]
                        current["confidence"] = item["confidence"]
                        current["band"] = item["priorityBand"]
                        drivers = item.get("riskDrivers", [])
                        current["driver"] = drivers[0]["label"] if drivers else "Existing alert enriched with behavioural and anomaly context"
        return sorted(grouped.values(), key=lambda item: (-item["risk"], item["id"]))[:max(1, min(limit, 1000))]

    def _poll(self) -> None:
        while not self.stop_event.is_set():
            self.scan()
            self.stop_event.wait(self.scan_seconds)

    def _process(self, job_id: str, manifest_path: Path, summary_path: Path) -> None:
        started = perf_counter()
        with self.lock: self.jobs[job_id]["status"] = "RUNNING"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self._validate_manifest(manifest)
            batch_dir = manifest_path.parent
            alert_path = self._resolved_file(batch_dir, manifest["alerts"]["filename"])
            trade_descriptors = self._trade_descriptors(manifest)
            parquet_paths = [self._resolved_file(batch_dir, descriptor["filename"]) for descriptor in trade_descriptors]
            self._verify_file(alert_path, manifest["alerts"])
            for parquet_path, descriptor in zip(parquet_paths, trade_descriptors):
                self._verify_file(parquet_path, descriptor)
            alerts = self._read_alerts(alert_path, manifest)
            contexts, parquet_rows, selected_rows = self._select_context(parquet_paths, alerts, manifest)
            rolling = self._load_state()
            features = [self._features(alert, contexts[alert["alertId"]], rolling) for alert in alerts]
            predictions = self.model.predict(features)
            clusters = self._clusters(alerts, contexts)
            output = []
            bands = defaultdict(int)
            unmatched = 0
            for index, alert in enumerate(alerts):
                alert_id = alert["alertId"]
                rows = contexts[alert_id]
                if not rows: unmatched += 1
                item = self._enrich(alert, features[index], predictions[index] if predictions else None,
                                    clusters[alert_id], rows)
                bands[item["priorityBand"]] += 1
                output.append(item)
            output_path = self.output_root / f"{job_id}.enriched-alerts.jsonl"
            with output_path.open("w", encoding="utf-8") as stream:
                for item in output: stream.write(json.dumps(item, separators=(",", ":")) + "\n")
            self._update_state(rolling, alerts, contexts)
            elapsed = max(0.000001, perf_counter() - started)
            summary = {
                "jobId": job_id, "batchId": manifest["batchId"], "businessDate": manifest["businessDate"],
                "region": manifest["region"], "status": "SUCCEEDED", "alertsReceived": len(alerts),
                "alertsMatched": len(alerts) - unmatched, "alertsUnmatched": unmatched,
                "parquetRecordsRead": parquet_rows, "recordsSelectedForContext": selected_rows,
                "parquetFilesRead": len(parquet_paths),
                "riskBands": dict(bands), "casesCreated": len(set(clusters.values())),
                "durationSeconds": round(elapsed, 4), "alertsPerSecond": round(len(alerts) / elapsed, 2),
                "modelVersion": self.model.metadata().get("modelVersion", "transparent-fallback"),
                "outputFile": output_path.name,
                "privacy": {"rawRecordsSentToLlm": 0, "directIdentifiersSentToLlm": 0, "humanDecisionRequired": True},
                "completedAt": self._now(),
            }
            self._write_json_atomic(summary_path, summary)
            with self.lock: self.jobs[job_id] = summary
        except Exception as exc:
            failure = {"jobId": job_id, "status": "FAILED", "error": f"{type(exc).__name__}: {exc}", "completedAt": self._now()}
            self._write_json_atomic(summary_path, failure)
            with self.lock: self.jobs[job_id] = failure

    def _validate_manifest(self, manifest: dict) -> None:
        for name in ("batchId", "businessDate", "region", "alerts", "trades"):
            if name not in manifest: raise ValueError(f"Manifest missing {name}")
        if manifest["region"].upper() not in self.allowed_regions: raise ValueError("Manifest region is not allowed")
        datetime.fromisoformat(manifest["businessDate"])
        self._trade_descriptors(manifest)

    def _trade_descriptors(self, manifest: dict) -> list[dict]:
        value = manifest.get("trades")
        descriptors = value if isinstance(value, list) else [value] if isinstance(value, dict) else []
        if not descriptors or any(not isinstance(item, dict) or not item.get("filename") for item in descriptors):
            raise ValueError("Manifest requires one or more Parquet file descriptors")
        filenames = [item["filename"] for item in descriptors]
        if len(filenames) != len(set(filenames)):
            raise ValueError("Manifest contains duplicate Parquet filenames")
        return descriptors

    def _read_alerts(self, path: Path, manifest: dict) -> list[dict]:
        text = path.read_text(encoding="utf-8").strip()
        records = json.loads(text) if text.startswith("[") else [json.loads(line) for line in text.splitlines() if line.strip()]
        if not isinstance(records, list) or not records: raise ValueError("Alert file contains no records")
        if len(records) > self.max_alerts: raise ValueError(f"Alert file exceeds DAILY_MAX_ALERTS={self.max_alerts}")
        seen = set()
        for offset, alert in enumerate(records, start=1):
            missing = REQUIRED_ALERT_FIELDS.difference(alert)
            if missing: raise ValueError(f"Alert {offset} missing fields: {', '.join(sorted(missing))}")
            if alert["alertId"] in seen: raise ValueError(f"Duplicate alertId: {alert['alertId']}")
            seen.add(alert["alertId"])
            if alert["businessDate"] != manifest["businessDate"]: raise ValueError("Alert businessDate does not match manifest")
            if alert["region"].upper() != manifest["region"].upper(): raise ValueError("Alert region does not match manifest")
            if alert.get("batchId") and alert["batchId"] != manifest["batchId"]:
                raise ValueError("Alert batchId does not match manifest")
            if not isinstance(alert.get("triggeringTradeIds"), list) or not alert["triggeringTradeIds"]:
                raise ValueError("Alert triggeringTradeIds must be a non-empty list")
            alert["region"] = alert["region"].upper()
            alert["assetClass"] = alert["assetClass"].upper()
            if alert["assetClass"] not in ASSET_CLASSES: raise ValueError(f"Unsupported assetClass: {alert['assetClass']}")
        return records

    def _select_context(self, paths: list[Path], alerts: list[dict], manifest: dict) -> tuple[dict[str, list[dict]], int, int]:
        by_trade: dict[str, set[str]] = defaultdict(set)
        by_account: dict[str, set[str]] = defaultdict(set)
        by_instrument: dict[str, set[str]] = defaultdict(set)
        by_asset: dict[str, set[str]] = defaultdict(set)
        for alert in alerts:
            alert_id = alert["alertId"]
            for value in alert.get("triggeringTradeIds", []): by_trade[str(value)].add(alert_id)
            for value in alert.get("accountTokens", []): by_account[str(value)].add(alert_id)
            for value in alert.get("instrumentIds", []): by_instrument[str(value)].add(alert_id)
            by_asset[alert["assetClass"]].add(alert_id)
        contexts: dict[str, list[dict]] = {alert["alertId"]: [] for alert in alerts}
        read = selected = 0
        for path in paths:
            parquet = pq.ParquetFile(path)
            columns = set(parquet.schema_arrow.names)
            missing = REQUIRED_PARQUET_COLUMNS.difference(columns)
            if missing: raise ValueError(f"Parquet {path.name} missing columns: {', '.join(sorted(missing))}")
            available = [name for name in SELECTED_COLUMNS if name in columns]
            for batch in parquet.iter_batches(batch_size=self.batch_rows, columns=available):
                rows = batch.to_pylist(); read += len(rows)
                for row in rows:
                    if str(row.get("region", "")).upper() != manifest["region"].upper():
                        raise ValueError(f"Parquet {path.name} contains a row outside the configured region")
                    event_time = self._timestamp(row.get("event_time"))
                    if event_time is None or event_time.date().isoformat() != manifest["businessDate"]:
                        raise ValueError(f"Parquet {path.name} contains an invalid or cross-date event_time")
                    if str(row.get("asset_class", "")).upper() not in ASSET_CLASSES:
                        raise ValueError(f"Parquet {path.name} contains an unsupported asset_class")
                    exact = by_trade.get(str(row.get("trade_id", "")), set())
                    account = by_account.get(str(row.get("account_id", "")), set())
                    instrument = by_instrument.get(str(row.get("instrument", "")), set())
                    asset = by_asset.get(str(row.get("asset_class", "")).upper(), set())
                    candidates = set(exact) | ((set(account) | set(instrument)) & set(asset))
                    for alert_id in candidates:
                        if len(contexts[alert_id]) < self.max_context_per_alert:
                            contexts[alert_id].append(row); selected += 1
                self._check_storage()
        return contexts, read, selected

    def _features(self, alert: dict, rows: list[dict], rolling: dict) -> dict[str, float]:
        quantities = [self._number(row.get("quantity")) for row in rows]
        prices = [self._number(row.get("price")) for row in rows]
        buys = [row for row in rows if str(row.get("side", "")).upper() == "BUY"]
        sells = [row for row in rows if str(row.get("side", "")).upper() == "SELL"]
        similarity = max((self._similarity(self._number(b.get("quantity")), self._number(s.get("quantity")))
                          for b in buys for s in sells), default=0.0)
        price_similarity = max((self._similarity(self._number(b.get("price")), self._number(s.get("price")))
                                for b in buys for s in sells), default=0.0)
        signed = sum(self._number(row.get("quantity")) * (1 if str(row.get("side", "")).upper() == "BUY" else -1) for row in rows)
        gross = sum(quantities)
        clients = defaultdict(set)
        for row in rows: clients[str(row.get("client_id", ""))].add(str(row.get("account_id", "")))
        common_control = 1.0 if any(len(accounts) > 1 for client, accounts in clients.items() if client) else 0.0
        times = sorted(filter(None, (self._timestamp(row.get("event_time")) for row in rows)))
        min_gap = min((right - left).total_seconds() for left, right in zip(times, times[1:])) if len(times) > 1 else 86400
        current_median = median(quantities) if quantities else 0.0
        baseline_values = []
        for account in alert.get("accountTokens", []):
            for instrument in alert.get("instrumentIds", []):
                state = rolling.get(self._state_key(alert["region"], str(account), str(instrument)))
                if state and state.get("meanQuantity"): baseline_values.append(float(state["meanQuantity"]))
        baseline = median(baseline_values) if baseline_values else current_median
        deviation = min(1.0, abs(current_median - baseline) / max(baseline, 1.0)) if baseline else 0.0
        return {
            "temporal_proximity": max(0.0, 1.0 - min(1.0, min_gap / 3600.0)),
            "quantity_similarity": similarity, "price_similarity": price_similarity,
            "recurrence": min(1.0, len(rows) / 8.0),
            "position_round_trip": 1.0 - min(1.0, abs(signed) / max(gross, 1.0)),
            "common_control": common_control, "behaviour_deviation": deviation,
            "illiquidity": 0.0, "order_cancellation": 0.0, "price_impact": 0.0,
            "information_timing": 0.0, "client_order_proximity": 0.0, "volatility_context": 0.0,
            "meaningful_exposure": min(1.0, abs(signed) / max(gross, 1.0)),
            "baseline_consistency": max(0.0, 1.0 - deviation),
        }

    def _clusters(self, alerts: list[dict], contexts: dict[str, list[dict]]) -> dict[str, str]:
        parent = {alert["alertId"]: alert["alertId"] for alert in alerts}
        def find(value):
            while parent[value] != value:
                parent[value] = parent[parent[value]]; value = parent[value]
            return value
        def union(left, right):
            left, right = find(left), find(right)
            if left != right: parent[right] = left
        entity_index: dict[str, list[str]] = defaultdict(list)
        for alert in alerts:
            alert_id = alert["alertId"]
            entities = {f"account:{value}" for value in alert.get("accountTokens", [])}
            entities |= {f"client:{row.get('client_id')}" for row in contexts[alert_id] if row.get("client_id")}
            for entity in entities: entity_index[entity].append(alert_id)
        for members in entity_index.values():
            for other in members[1:]: union(members[0], other)
        groups = defaultdict(list)
        for alert_id in parent: groups[find(alert_id)].append(alert_id)
        result = {}
        for members in groups.values():
            cluster_id = "CASE-" + hashlib.sha256("|".join(sorted(members)).encode()).hexdigest()[:12].upper()
            for alert_id in members: result[alert_id] = cluster_id
        return result

    def _enrich(self, alert: dict, features: dict, prediction: dict | None, cluster_id: str, rows: list[dict]) -> dict:
        rule_score = max(0, min(100, int(alert.get("ruleScore", 50))))
        supervised = float(prediction["probability"]) * 100 if prediction else 0.0
        anomaly = float(prediction["anomaly"]) * 100 if prediction else 0.0
        risk = round(rule_score * self.rule_weight + supervised * self.supervised_weight + anomaly * self.anomaly_weight) if prediction else rule_score
        thresholds = self._thresholds(alert["region"], alert["assetClass"])
        _, confidence, _, contributions, missing, evidence_coverage, warnings = assess(
            features, alert.get("typology", "WASH_TRADING"), [f"Trade:{value}" for value in alert.get("triggeringTradeIds", [])])
        band = "INSUFFICIENT_DATA" if not rows else "HIGH" if risk >= thresholds["high"] else "MEDIUM" if risk >= thresholds["medium"] else "LOW"
        components = [rule_score / 100.0, supervised / 100.0, anomaly / 100.0] if prediction else []
        disagreement = round(max(components) - min(components), 4) if components else None
        probability = supervised / 100.0 if prediction else 0.0
        uncertainty = "UNAVAILABLE" if not prediction else "HIGH" if disagreement >= 0.35 or 0.4 <= probability <= 0.6 else "MEDIUM" if disagreement >= 0.2 else "LOW"
        drivers = [item for item in contributions if item["direction"] == "increase" and item["points"] >= 3]
        reducers = [item for item in contributions if item["direction"] == "reduce" and abs(item["points"]) >= 3]
        return {
            "sourceAlertId": alert["alertId"], "batchId": alert.get("batchId"), "businessDate": alert["businessDate"],
            "region": alert["region"], "assetClass": alert["assetClass"], "typology": alert.get("typology", "WASH_TRADING"),
            "instrument": next((str(row.get("instrument")) for row in rows if row.get("instrument")), "Unknown"),
            "ruleId": alert["ruleId"], "ruleVersion": alert["ruleVersion"], "originalRuleScore": rule_score,
            "aiPriorityScore": risk, "priorityBand": band, "clusterId": cluster_id,
            "modelVersion": self.model.metadata().get("modelVersion", "transparent-fallback"),
            "mlProbability": prediction["probability"] if prediction else None,
            "anomalyScore": prediction["anomaly"] if prediction else None,
            "modelDisagreement": disagreement, "uncertaintyBand": uncertainty,
            "copilotEligible": bool(rows) and evidence_coverage >= self.copilot_min_coverage and
                (risk >= self.copilot_min_priority or (disagreement or 0) >= self.copilot_min_disagreement),
            "confidence": confidence, "evidenceCoverage": evidence_coverage,
            "riskDrivers": [{"feature": item["feature"], "label": item["label"], "points": item["points"]} for item in drivers],
            "riskReducers": [{"feature": item["feature"], "label": item["label"], "points": item["points"]} for item in reducers],
            "graph": {"connectedAccounts": len({row.get("account_id") for row in rows if row.get("account_id")}),
                      "commonControl": features["common_control"] == 1.0},
            "evidenceReferences": [f"Trade:{value}" for value in alert.get("triggeringTradeIds", [])],
            "dataGaps": [f"Missing typology feature: {name}" for name in missing] + ([] if rows else ["No matching Parquet context"]),
            "warnings": warnings, "decisionPolicy": "HUMAN_REVIEW_REQUIRED", "originalAlertPreserved": True,
            "automaticClosureAllowed": False,
        }

    def _thresholds(self, region: str, asset_class: str) -> dict[str, int]:
        base = {"high": 80, "medium": 45}
        region_adjustment = {"AMER": 2, "EMEA": 0, "APAC": 1, "GLOBAL": 0}.get(region, 0)
        asset_adjustment = {"FIXED_INCOME": 0, "FOREIGN_EXCHANGE": 3,
                            "INTEREST_RATE_DERIVATIVES": 1, "CREDIT_DERIVATIVES": -2}.get(asset_class, 0)
        return {"high": max(1, min(99, base["high"] + region_adjustment + asset_adjustment)),
                "medium": max(1, min(99, base["medium"] + region_adjustment + asset_adjustment))}

    def _load_state(self) -> dict:
        try: return json.loads(self.state_path.read_text(encoding="utf-8")) if self.state_path.exists() else {}
        except (OSError, json.JSONDecodeError): return {}

    def _update_state(self, state: dict, alerts: list[dict], contexts: dict[str, list[dict]]) -> None:
        for alert in alerts:
            for row in contexts[alert["alertId"]]:
                key = self._state_key(alert["region"], str(row.get("account_id", "")), str(row.get("instrument", "")))
                prior = state.get(key, {"count": 0, "meanQuantity": 0.0})
                count = int(prior.get("count", 0)) + 1; quantity = self._number(row.get("quantity"))
                prior["meanQuantity"] = float(prior.get("meanQuantity", 0.0)) + (quantity - float(prior.get("meanQuantity", 0.0))) / count
                prior.update({"count": count, "lastBusinessDate": alert["businessDate"], "region": alert["region"], "assetClass": alert["assetClass"]})
                state[key] = prior
        if len(state) > self.max_state_keys:
            keep = sorted(state.items(), key=lambda item: item[1].get("lastBusinessDate", ""), reverse=True)[:self.max_state_keys]
            state = dict(keep)
        self._write_json_atomic(self.state_path, state)

    def _verify_file(self, path: Path, descriptor: dict) -> None:
        if not path.is_file(): raise ValueError(f"Missing file: {path.name}")
        if int(descriptor.get("bytes", -1)) != path.stat().st_size: raise ValueError(f"Size mismatch: {path.name}")
        if descriptor.get("sha256") != self._sha256(path): raise ValueError(f"Checksum mismatch: {path.name}")
    def _resolved_file(self, batch_dir: Path, filename: str) -> Path:
        if not isinstance(filename, str) or Path(filename).name != filename:
            raise ValueError("Manifest filenames must be local to the batch directory")
        path = (batch_dir / filename).resolve()
        if path.parent != batch_dir.resolve():
            raise ValueError("Manifest file escapes the batch directory")
        return path
    def _check_storage(self) -> None:
        if shutil.disk_usage(self.output_root).free < self.min_free_disk: raise OSError("Insufficient output storage reserve")
    def _job_id(self, manifest: dict) -> str:
        trade_hashes = "|".join(str(item.get("sha256", "")) for item in self._trade_descriptors(manifest))
        value = f"{manifest.get('region')}|{manifest.get('businessDate')}|{manifest.get('batchId')}|{manifest.get('alerts',{}).get('sha256')}|{trade_hashes}"
        return hashlib.sha256(value.encode()).hexdigest()[:20]
    def _state_key(self, region: str, account: str, instrument: str) -> str:
        return hashlib.sha256(f"{region}|{account}|{instrument}".encode()).hexdigest()
    def _write_json_atomic(self, path: Path, value: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True); temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(value, indent=2), encoding="utf-8"); os.replace(temporary, path)
    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1 << 20), b""): digest.update(block)
        return digest.hexdigest()
    def _number(self, value) -> float:
        try: return max(0.0, float(value or 0.0))
        except (TypeError, ValueError): return 0.0
    def _similarity(self, left: float, right: float) -> float:
        return min(left, right) / max(left, right) if left > 0 and right > 0 else 0.0
    def _timestamp(self, value):
        try: return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError): return None
    def _now(self) -> str: return datetime.now(timezone.utc).isoformat()
