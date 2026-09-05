"""Generate a synthetic region-specific daily Alert JSONL + Parquet pair."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""): digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--region", default="EMEA", choices=["EMEA", "AMER", "APAC", "GLOBAL"])
    parser.add_argument("--business-date", default="2026-09-05")
    parser.add_argument("--batch-id", default="SURV-20260905-EMEA")
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--parquet-parts", type=int, default=2,
                        help="Number of Parquet part files; use 1 for a single-file batch")
    args = parser.parse_args()
    output = Path(args.output); output.mkdir(parents=True, exist_ok=True)
    alerts_path = output / "alerts.jsonl"
    alert = {
        "batchId": args.batch_id, "businessDate": args.business_date, "region": args.region,
        "alertId": "ALT-FI-0001", "ruleId": "FI-WASH-001", "ruleVersion": "4.7",
        "typology": "WASH_TRADING", "assetClass": "FIXED_INCOME", "ruleScore": 82,
        "triggeringTradeIds": ["T-000000", "T-000001"],
        "accountTokens": ["ACC-TOKEN-001", "ACC-TOKEN-002"], "instrumentIds": ["BOND-721"],
    }
    alerts_path.write_text(json.dumps(alert) + "\n", encoding="utf-8")
    start = datetime.fromisoformat(args.business_date).replace(tzinfo=timezone.utc)
    records = []
    for index in range(max(2, args.rows)):
        suspicious = index < 16
        records.append({
            "trade_id": f"T-{index:06d}", "order_id": f"O-{index:06d}",
            "event_time": (start + timedelta(seconds=index * 8)).isoformat().replace("+00:00", "Z"),
            "instrument": "BOND-721" if suspicious else f"BOND-{800 + index % 30}",
            "asset_class": "FIXED_INCOME", "region": args.region,
            "side": "BUY" if index % 2 == 0 else "SELL", "quantity": 1_000_000.0 if suspicious else float(10_000 + index),
            "price": 99.95 + (index % 2) * 0.01, "account_id": f"ACC-TOKEN-00{1 + index % 2}" if suspicious else f"ACC-TOKEN-{100 + index % 200}",
            "client_id": "CLIENT-TOKEN-900" if suspicious else f"CLIENT-TOKEN-{1000 + index % 300}", "venue": "XLON",
        })
    part_count = max(1, min(args.parquet_parts, len(records)))
    part_paths = []
    for part in range(part_count):
        part_records = records[part::part_count]
        part_path = output / f"trades-part-{part + 1:05d}.parquet"
        pq.write_table(pa.Table.from_pylist(part_records), part_path, compression="zstd", row_group_size=100_000)
        part_paths.append(part_path)
    manifest = {
        "schemaVersion": "daily-paired-batch-v1", "batchId": args.batch_id,
        "businessDate": args.business_date, "region": args.region, "status": "READY",
        "alerts": {"filename": alerts_path.name, "bytes": alerts_path.stat().st_size, "sha256": sha256(alerts_path)},
        "trades": [
            {"filename": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)} for path in part_paths
        ],
        "privacy": {"identifiers": "TOKENISED", "llmRawDataAllowed": False},
    }
    (output / "manifest.ready.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(output), "alerts": 1, "parquetRows": len(records),
                      "parquetFiles": len(part_paths), "region": args.region}, indent=2))


if __name__ == "__main__": main()
