# Robustness and control design

Trade Surveillance Navigator is a decision-support system. It prioritises potentially suspicious activity; an authorised investigator remains responsible for the final determination.

## End-to-end golden path

1. A regional daily batch supplies one existing Java alert JSON/JSONL contract and one or more corresponding Parquet files.
2. Java validates the region/date contract, extensions, size limits, Parquet envelopes and alert schema; it fingerprints every file and atomically publishes one ready manifest.
3. Python verifies every checksum and required Parquet column, rejects cross-region data, and scans all parts in bounded record batches.
4. Existing surveillance rules identify known patterns.
5. Cohort anomaly, typology features and graph relationships add context.
6. Missing data reduces confidence; it never increases suspicion by default.
7. Related alerts are grouped only when the linkage can be explained.
8. The case summary is generated only from verified evidence identifiers.
9. An investigator reviews the evidence, records rationale and recommends an outcome.
10. Supervisor-controlled dispositions are checked by the workflow API.
11. Every ingestion result, model assessment, evidence view, override and human action is retained in the audit trail.

## Data input controls

| Control | Behaviour |
|---|---|
| Contract | Daily paired-batch v1 binds one Java alert file to one or more Parquet parts for one region, date and batch ID. |
| Isolation | Raw input cannot call scoring or narrative generation directly. |
| Validation | Alert provenance, region/date consistency, Parquet envelopes, checksums, required columns and supported asset classes are enforced. |
| Partitioning | Both a single Parquet file and multiple part files are supported; all descriptors are committed in one ready manifest. |
| Immutability | Once ready, a batch rejects replacement uploads; corrections use a new batch ID. |
| Deduplication | Content fingerprints make part names deterministic and the full manifest fingerprint identifies ML jobs. |
| Quarantine | Invalid and duplicate records are retained separately from accepted records with explicit reasons. |
| Reconciliation | Output reports alerts received/matched/unmatched, Parquet files/records read, selected context, cases created, risk bands and throughput. |
| Regional boundary | Allowed regions are configured centrally and every Parquet row must match its manifest region. |

## Model controls

| Control | Behaviour |
|---|---|
| Typology coverage | Wash trading, spoofing, front-running, insider dealing and manipulation have separate required features. |
| Uncertainty | Below 60% required-feature coverage the service returns `INSUFFICIENT_DATA`. |
| Grounding | Below 75% evidence coverage the service warns that a factual narrative must be withheld. |
| Explainability | Every contribution includes direction, points, a business label and evidence references. |
| Human authority | Every assessment returns `HUMAN_REVIEW_REQUIRED`. |
| Monitoring | Precision, recall, calibration, drift, completeness and override rate are tracked by typology. |
| Artifact loading | `MODEL_REQUIRED=true` fails startup when the configured, feature-compatible model artifact is unavailable. |
| Batch isolation | Accepted files are streamed in bounded chunks; score output and completion summaries are written separately from immutable input. |
| Backpressure | File workers, maximum records, entity-state cardinality and minimum free disk are bounded by configuration. |
| Recovery | Atomic progress is emitted per chunk; completed file fingerprints are idempotent and interrupted partitions are safely recomputed. |
| Approval gate | Every trained artifact records its training source, evaluation metrics and `productionApproved` status. Synthetic models remain unapproved. |
| Privacy | Parquet rows never enter an LLM request; direct identifiers, secrets and prompt-injection patterns are blocked before the call; rolling state keys are SHA-256 tokenised. |
| LLM boundary | A strict allowlisted schema, 1,800/450 input/output token budgets, local playbook retrieval, `store=false`, no tools and post-call citation/DLP checks isolate the provider from raw surveillance data. |
| Feedback | Investigator outcomes are appended to a governed feedback dataset and never trigger automatic online retraining. |

The bundled performance values are synthetic validation targets. Production thresholds must be calibrated on temporally separated, approved historical data, assessed for subgroup bias and approved through model governance.

## Workflow controls

- Dispositions require an authenticated actor, role and meaningful rationale.
- Investigators can escalate or request further review.
- Final closure requires the `SUPERVISOR` role.
- Terminal cases reject duplicate decisions.
- The in-memory audit store is suitable for demonstration only; production uses an append-only transactional store with retention and legal-hold controls.

## Failure-mode behaviour

| Failure | Safe behaviour |
|---|---|
| ML service unavailable | Continue rule ingestion, queue scoring retries and display degraded mode. |
| Graph data stale | Exclude graph contribution, reduce confidence and show a data-quality warning. |
| Evidence record missing | Preserve raw evidence, mark the claim unsupported and withhold the generated narrative. |
| Duplicate market event | Idempotency key prevents duplicate alerts, cases and audit events. |
| Feature or score drift | Alert model operations, retain the prior approved version and invoke rollback criteria. |
| LLM disabled, timeout or provider error | Show the locally scored structured evidence without a narrative; investigation continues. |
| Sensitive data or prompt injection detected | Block before the provider call, return a controlled 422 response and consume zero provider tokens. |
| Unsupported citation or autonomous conclusion | Withhold the generated output; human review continues from verified evidence only. |

## Production hardening backlog

- Replace demonstration in-memory workflow state with PostgreSQL and an append-only audit ledger.
- Add enterprise identity, least-privilege roles, maker-checker approval and session auditing.
- Introduce a message broker, dead-letter queues, replay controls and exactly-once business keys.
- Encrypt data in transit and at rest; apply field-level masking and jurisdiction-specific retention.
- Add contract, integration, load, resilience, security and model-monitoring tests to the deployment pipeline.
- Validate latency and throughput against representative peak trading volumes before release.
