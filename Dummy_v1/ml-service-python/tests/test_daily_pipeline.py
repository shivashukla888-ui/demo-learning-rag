import hashlib
import json
import time

import pyarrow as pa
import pyarrow.parquet as pq

from app.daily_pipeline import DailyBatchPipeline


class FakeModel:
    def predict(self, records):
        return [{"risk": 88, "probability": .91, "anomaly": .72} for _ in records]
    def metadata(self): return {"modelVersion": "test-daily-model"}


def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()


def test_daily_pair_is_region_validated_scored_and_clustered(tmp_path, monkeypatch):
    batch = tmp_path / "input/region=EMEA/business_date=2026-09-05/batch_id=SURV-20260905-EMEA"
    batch.mkdir(parents=True); output = tmp_path / "output"
    alerts = [
        {"batchId":"SURV-20260905-EMEA","businessDate":"2026-09-05","region":"EMEA","alertId":"ALT-1",
         "ruleId":"FI-WASH-001","ruleVersion":"4.7","typology":"WASH_TRADING","assetClass":"FIXED_INCOME",
         "ruleScore":82,"triggeringTradeIds":["T-1","T-2"],"accountTokens":["ACC-1","ACC-2"],"instrumentIds":["BOND-1"]},
        {"batchId":"SURV-20260905-EMEA","businessDate":"2026-09-05","region":"EMEA","alertId":"ALT-2",
         "ruleId":"FI-WASH-001","ruleVersion":"4.7","typology":"WASH_TRADING","assetClass":"FIXED_INCOME",
         "ruleScore":70,"triggeringTradeIds":["T-3"],"accountTokens":["ACC-2"],"instrumentIds":["BOND-1"]},
    ]
    alert_path=batch/"alerts.jsonl"; alert_path.write_text("\n".join(json.dumps(x) for x in alerts)+"\n")
    rows=[
        {"trade_id":"T-1","order_id":"O-1","event_time":"2026-09-05T09:00:00Z","instrument":"BOND-1","asset_class":"FIXED_INCOME","region":"EMEA","side":"BUY","quantity":1000.0,"price":99.1,"account_id":"ACC-1","client_id":"CLIENT-X","venue":"XLON"},
        {"trade_id":"T-2","order_id":"O-2","event_time":"2026-09-05T09:00:08Z","instrument":"BOND-1","asset_class":"FIXED_INCOME","region":"EMEA","side":"SELL","quantity":999.0,"price":99.1,"account_id":"ACC-2","client_id":"CLIENT-X","venue":"XLON"},
        {"trade_id":"T-3","order_id":"O-3","event_time":"2026-09-05T09:00:16Z","instrument":"BOND-1","asset_class":"FIXED_INCOME","region":"EMEA","side":"BUY","quantity":1000.0,"price":99.1,"account_id":"ACC-2","client_id":"CLIENT-X","venue":"XLON"},
    ]
    parquet_paths=[]
    for index, part_rows in enumerate((rows[:2], rows[2:]), start=1):
        parquet_path=batch/f"trades-part-{index:05d}.parquet"
        pq.write_table(pa.Table.from_pylist(part_rows),parquet_path); parquet_paths.append(parquet_path)
    manifest={"schemaVersion":"daily-paired-batch-v1","batchId":"SURV-20260905-EMEA","businessDate":"2026-09-05","region":"EMEA",
              "alerts":{"filename":"alerts.jsonl","bytes":alert_path.stat().st_size,"sha256":digest(alert_path)},
              "trades":[{"filename":path.name,"bytes":path.stat().st_size,"sha256":digest(path)} for path in parquet_paths]}
    (batch/"manifest.ready.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("DAILY_INPUT_ROOT",str(tmp_path/"input")); monkeypatch.setenv("DAILY_OUTPUT_ROOT",str(output))
    monkeypatch.setenv("DAILY_FEATURE_STATE_PATH",str(tmp_path/"state.json")); monkeypatch.setenv("ML_MIN_FREE_DISK_BYTES","0")
    pipeline=DailyBatchPipeline(FakeModel()); pipeline.start(); pipeline.scan()
    deadline=time.time()+10
    while time.time()<deadline:
        jobs=pipeline.status()
        if jobs and jobs[0]["status"] in {"SUCCEEDED","FAILED"}: break
        time.sleep(.02)
    pipeline.stop()
    assert jobs[0]["status"]=="SUCCEEDED", jobs[0]
    assert jobs[0]["region"]=="EMEA"; assert jobs[0]["alertsMatched"]==2
    enriched=[json.loads(line) for line in (output/jobs[0]["outputFile"]).read_text().splitlines()]
    assert enriched[0]["originalRuleScore"]==82 and enriched[0]["originalAlertPreserved"] is True
    assert enriched[0]["clusterId"]==enriched[1]["clusterId"]
    assert enriched[0]["graph"]["commonControl"] is True
    assert jobs[0]["privacy"]["rawRecordsSentToLlm"]==0
    assert jobs[0]["parquetFilesRead"]==2 and jobs[0]["parquetRecordsRead"]==3
    cases=pipeline.cases()
    assert len(cases)==1 and cases[0]["alertCount"]==2
    assert cases[0]["region"]=="EMEA" and cases[0]["instrument"]=="BOND-1"
