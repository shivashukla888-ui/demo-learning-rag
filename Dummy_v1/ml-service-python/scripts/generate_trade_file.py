#!/usr/bin/env python3
"""Generate a deterministic trade CSV for file-pipeline throughput demonstrations."""

import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=5_000)
    parser.add_argument("--output", type=Path, default=Path("sample-data/trades-5000.csv"))
    args = parser.parse_args()
    rng = random.Random(42)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fields = ["trade_id", "order_id", "event_time", "instrument", "side", "quantity", "price", "account_id", "client_id", "venue"]
    instruments = ["NOVA.L", "ALTO.N", "QUAD.N", "MEGA.O", "LIQD.P"]
    start = datetime(2026, 8, 25, 9, 30, tzinfo=timezone.utc)
    with args.output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for index in range(args.records):
            cycle = index % 10
            linked = cycle < 4
            writer.writerow({"trade_id": f"T-{index:07d}", "order_id": f"O-{index:07d}",
                "event_time": (start + timedelta(milliseconds=index * 850)).isoformat().replace("+00:00", "Z"),
                "instrument": "NOVA.L" if linked else instruments[index % len(instruments)], "side": "BUY" if index % 2 == 0 else "SELL",
                "quantity": 5000 + (cycle if linked else rng.randint(-1200, 1800)),
                "price": round(42.0 + (index % 80) * .002 + rng.uniform(-.01, .01), 4),
                "account_id": "A-104" if linked and index % 2 == 0 else "A-882" if linked else f"A-{200 + index % 60}",
                "client_id": "CLIENT-LINKED" if linked else f"CLIENT-{index % 60}", "venue": "XLON"})
    print(f"wrote {args.records} trades to {args.output}")


if __name__ == "__main__":
    main()
