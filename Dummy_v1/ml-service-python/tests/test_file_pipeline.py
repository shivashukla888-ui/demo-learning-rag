import csv
import time

from app.file_pipeline import TradeFilePipeline


class FakeModel:
    def predict(self, records):
        return [{"risk": 70, "probability": .7, "anomaly": .2} for _ in records]

    def metadata(self):
        return {"modelVersion": "test-model"}


def test_streaming_file_pipeline_processes_in_bounded_chunks(tmp_path, monkeypatch):
    input_root, output_root = tmp_path / "input", tmp_path / "output"
    input_root.mkdir()
    source = input_root / "trades-large.csv"
    fields = ["trade_id", "instrument", "account_id", "client_id", "side", "quantity", "price"]
    with source.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for index in range(2_500):
            writer.writerow({"trade_id": f"T-{index}", "instrument": "NOVA.L", "account_id": f"A-{index % 4}",
                "client_id": "C-1", "side": "BUY" if index % 2 == 0 else "SELL", "quantity": 1000, "price": 42.1})
    monkeypatch.setenv("ML_TRADE_INPUT_ROOT", str(input_root))
    monkeypatch.setenv("ML_OUTPUT_ROOT", str(output_root))
    monkeypatch.setenv("ML_MIN_FREE_DISK_BYTES", "0")
    pipeline = TradeFilePipeline(FakeModel(), 500, .55)
    pipeline.start()
    deadline = time.time() + 10
    while time.time() < deadline:
        jobs = pipeline.status()
        if jobs and jobs[0]["status"] in {"SUCCEEDED", "FAILED"}:
            break
        time.sleep(.02)
    pipeline.stop()
    assert jobs[0]["status"] == "SUCCEEDED"
    assert jobs[0]["processed"] == 2_500
    assert (output_root / jobs[0]["outputFile"]).exists()
