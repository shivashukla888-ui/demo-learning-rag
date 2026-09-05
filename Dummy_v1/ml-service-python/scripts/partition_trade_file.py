#!/usr/bin/env python3
"""Split a large trade CSV into independently restartable partitions."""

import argparse
import csv
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("--rows-per-part", type=int, default=100_000)
    parser.add_argument("--output-directory", type=Path, default=Path("runtime-data/input/inbound/trades"))
    args = parser.parse_args()
    if args.rows_per_part < 10_000:
        raise ValueError("rows-per-part must be at least 10,000")
    args.output_directory.mkdir(parents=True, exist_ok=True)
    writer = stream = None
    count = part = 0
    try:
        with args.source.open(newline="", encoding="utf-8") as source:
            reader = csv.DictReader(source)
            if not reader.fieldnames:
                raise ValueError("Source CSV requires a header")
            for row in reader:
                if count % args.rows_per_part == 0:
                    if stream: stream.close()
                    part += 1
                    target = args.output_directory / f"trades-part-{part:05d}.csv"
                    stream = target.open("w", newline="", encoding="utf-8")
                    writer = csv.DictWriter(stream, fieldnames=reader.fieldnames)
                    writer.writeheader()
                writer.writerow(row)
                count += 1
    finally:
        if stream: stream.close()
    print(f"partitioned {count} records into {part} files")


if __name__ == "__main__":
    main()
