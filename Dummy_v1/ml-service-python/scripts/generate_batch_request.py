#!/usr/bin/env python3
"""Generate a deterministic multi-record feature request for throughput demonstrations."""

import argparse
import json
import random
from pathlib import Path

FEATURES = (
    "temporal_proximity", "quantity_similarity", "price_similarity", "recurrence", "position_round_trip",
    "common_control", "behaviour_deviation", "illiquidity", "order_cancellation", "price_impact",
    "information_timing", "client_order_proximity", "volatility_context", "meaningful_exposure", "baseline_consistency",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=5_000)
    parser.add_argument("--output", type=Path, default=Path("sample-data/feature-batch-5000.json"))
    args = parser.parse_args()
    if not 1 <= args.records <= 10_000:
        raise ValueError("records must be between 1 and 10,000")
    rng = random.Random(42)
    records = []
    for index in range(args.records):
        suspicious = index % 7 == 0
        features = {name: round(rng.uniform(0.55, 1.0) if suspicious and name in {
            "temporal_proximity", "quantity_similarity", "recurrence", "position_round_trip", "common_control"
        } else rng.uniform(0.0, 0.55), 4) for name in FEATURES}
        records.append({"alertId": f"LOAD-{index:06d}", "typology": "WASH_TRADING", "features": features,
            "evidenceRefs": [f"Trade:T-{index:06d}", f"Position:P-{index:06d}"]})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"batchId": f"throughput-demo-{args.records}", "records": records}), encoding="utf-8")
    print(f"wrote {args.records} records to {args.output}")


if __name__ == "__main__":
    main()
