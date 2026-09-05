# Performance validation

## Million-record file benchmark

Validation date: 2026-08-25. This is a local development benchmark, not a production capacity guarantee.

| Measure | Result |
|---|---:|
| Input records | 1,000,000 |
| Input CSV size | 91 MB |
| File chunk size | 10,000 records |
| Concurrent file workers | 1 |
| Completed status | `SUCCEEDED` |
| Processing duration | 78.3189 seconds |
| Measured throughput | 12,768.31 records/second |
| Scored JSONL size | 141 MB |
| High / medium / low | 372,212 / 27,787 / 600,001 |

The benchmark exercised CSV streaming, bounded feature-state management, trained gradient-boosting inference, Isolation Forest anomaly inference, transparent-rule blending, JSONL output and atomic progress/completion records. The source and labels were synthetic, so the risk distribution is demonstration evidence only.

## Capacity boundaries

- File path: up to 2,000,000 records and 2 GB per configured file.
- Recommended unit of recovery: 100,000 records per partition.
- API path: up to 10,000 records per asynchronous request; larger workloads use file partitions.
- Default file concurrency: two workers.
- Feature state: bounded to 250,000 entity/instrument keys per worker.
- Output reserve: processing fails safely when free disk falls below 1 GB.

Production sizing must repeat the benchmark on target infrastructure with representative cardinality, file layouts, typology mix, concurrent arrivals, retention, encryption and downstream database writes.
