# DESIGN.md — The Ledger: Architectural Decision Record

**Project:** TRP1 Week 5 — Agentic Event Store & Enterprise Audit Infrastructure  
**Scenario:** Apex Financial Services — Multi-Agent Commercial Loan Processing  
**Implementation:** PostgreSQL + asyncpg + FastMCP + Python 3.12+

---

## Section 1: Aggregate Boundary Justification

### Why ComplianceRecord is a Separate Aggregate from LoanApplication

The most tempting simplification is merging `ComplianceRecord` into `LoanApplication` since compliance checks exist only in the context of a loan. This was explicitly rejected for a precise reason: **write contention under parallel agent execution**.

In the Apex scenario, the `CreditAnalysisAgent` and `ComplianceAgent` run concurrently — both processing the same application simultaneously. If both agents wrote to a single `loan-{id}` stream, they would both issue `SELECT FOR UPDATE` on the same `event_streams` row. One transaction holds the lock; the other blocks. When the blocked transaction resumes, it finds the version has advanced and raises `OptimisticConcurrencyError`, forcing a reload and retry.

**Specific failure mode under the merged boundary:**

Under 1,000 applications/hour with 4 agents each, every active loan stream would experience constant row-level lock contention. The `ComplianceAgent` appending `ComplianceRulePassed` and the `CreditAnalysisAgent` appending `CreditAnalysisCompleted` would collide on every application. Each collision requires: reload stream → reconstruct aggregate → re-evaluate business rules → retry append. At peak load this produces retry cascades that degrade write throughput proportionally to agent concurrency.

**The chosen boundary eliminates contention entirely:**

Each aggregate owns exactly one stream with exactly one logical writer at a time:

| Aggregate | Stream | Writer |
|-----------|--------|--------|
| `LoanApplication` | `loan-{id}` | `DecisionOrchestrator`, state transitions |
| `AgentSession` | `agent-{agent_id}-{session_id}` | The specific agent instance |
| `ComplianceRecord` | `compliance-{id}` | `ComplianceAgent` exclusively |
| `AuditLedger` | `audit-{type}-{id}` | Integrity check process |

The `LoanApplication` aggregate reads compliance status via the `ComplianceAuditView` projection — it does not require a lock on the compliance stream to enforce Business Rule 5 (approval requires clearance). This is the correct CQRS pattern: reads from projections, writes to own stream.

**Alternative boundary considered and rejected:** A single `ApplicationAggregate` encompassing all four concerns. Rejected because: (a) any agent's write would lock the entire application record, (b) the aggregate would grow unboundedly as new agent types are added, and (c) a compliance failure would require loading the full credit analysis history, creating unnecessary coupling between unrelated concerns.

---

## Section 2: Projection Strategy

### ApplicationSummary

**Type:** Async (via ProjectionDaemon)  
**SLO commitment:** lag < 500ms  
**Justification:** The loan officer UI reads from this projection on every page load. Inline projection would add the projection write latency to every event append, degrading the write path for all 4 concurrent agents. Async projection decouples write latency from read latency. The 500ms SLO is appropriate because loan decisions are not instantaneous — a brief staleness window does not affect correctness. Measured in testing: 42ms under 10 concurrent applications (well within SLO).

### AgentPerformanceLedger

**Type:** Async (via ProjectionDaemon)  
**SLO commitment:** lag < 500ms  
**Justification:** Performance metrics are analytical — read by compliance officers and model governance teams, not by the real-time loan processing path. No business rule depends on this projection being current to the millisecond. Async is appropriate.

### ComplianceAuditView

**Type:** Async (via ProjectionDaemon) with temporal query support  
**SLO commitment:** lag < 2000ms (relaxed — regulatory queries tolerate staleness)  
**Justification:** Compliance queries are predominantly historical (regulators examining past decisions). The 2000ms SLO reflects this — real-time compliance status is enforced at the aggregate layer, not read from this projection during the write path.

**Snapshot strategy:** Event-count trigger — one snapshot per 50 compliance events per application. Justification: compliance checks are sparse (typically 2–5 per application). A time-based trigger (e.g. every 60 seconds) would produce snapshots with zero new events for most applications. An event-count trigger ensures snapshots are proportional to actual data volume.

**Snapshot invalidation:** Snapshots are never invalidated — they are immutable point-in-time captures. A snapshot at position N captures the compliance state after N events. If the projection is rebuilt from scratch, new snapshots are created during replay. Old snapshots remain valid because the underlying event stream is append-only and immutable.

**`rebuild_from_scratch()` without downtime:** The projection table is truncated and replayed while live reads continue. During rebuild, reads see stale data (empty or partially-rebuilt table). The `meta.projection_lag_ms` field in resource responses signals this to UI consumers. No lock is held on the projection table during rebuild — PostgreSQL MVCC ensures readers see a consistent snapshot of the table at their transaction start time.

---

## Section 3: Concurrency Analysis

### Expected OptimisticConcurrencyErrors at Peak Load

**Scenario:** 100 concurrent applications, 4 agents each = 400 concurrent append operations.

**Collision analysis per stream:**

Each `loan-{id}` stream has at most 2 concurrent writers at any moment (the state machine serialises most transitions naturally — only `FraudScreeningCompleted` and `CreditAnalysisCompleted` can race on the same stream). With 100 applications and a 200ms average processing window, the probability of two agents writing to the same stream within the same 50ms window is approximately:

```
P(collision per stream) ≈ 1 - e^(-λ²t/2) where λ = 2 writers, t = window
Estimated collisions/minute ≈ 100 applications × 0.15 collision probability = ~15/minute
```

At 1,000 applications/hour (steady state), estimated OptimisticConcurrencyErrors: **~90/hour** (~1.5/minute), concentrated on `loan-{id}` streams during the analysis phase.

### Retry Strategy

```
Attempt 1: immediate retry after reload
Attempt 2: 50ms backoff
Attempt 3: 100ms backoff
Attempt 4 (max): 200ms backoff → RetryBudgetExceededError
```

**Maximum retry budget:** 3 retries per command (4 total attempts). Total maximum wait: 350ms. This fits within the SLA for credit analysis completion (target: < 5 seconds end-to-end).

**On budget exhaustion:** `RetryBudgetExceededError` is returned to the caller with `suggested_action: "escalate_to_human_or_circuit_break"`. The orchestrator may choose to: (a) place the application in a manual review queue, (b) assign a different agent instance, or (c) trigger an alert for operations.

**Why not longer budgets:** Exponential backoff beyond 3 retries indicates a systematic collision problem (e.g., multiple orchestrators competing on the same application). The correct response is circuit-breaking, not indefinite retry.

---

## Section 4: Upcasting Inference Decisions

### CreditAnalysisCompleted v1 → v2

**Field: `confidence_score`**
- **Strategy:** null
- **Reasoning:** Confidence scoring was not implemented in the v1 credit model. No deployment log, input data, or proxy metric can reconstruct what the model's internal confidence was on a specific input in 2024. Any fabricated value (e.g., 0.5 as a "neutral" default) would:
  - Corrupt `AgentPerformanceLedger.avg_confidence_score` calculations by introducing artificial data points
  - Create regulatory exposure if the fabricated score influences a downstream compliance decision
  - Misrepresent the model's behaviour in audit records
- **When null is chosen over inference:** When the value is genuinely unknowable from available signals and any fabricated value would corrupt downstream computation or misrepresent historical facts.
- **Downstream consequence of incorrect fabrication:** `AgentPerformanceLedger` would show artificially inflated or deflated average confidence for pre-2026 model versions. Model governance teams comparing v1 and v2 performance would reach incorrect conclusions.
- **Estimated error rate of null:** 0% — null is always the correct answer for genuinely unknown values.

**Field: `model_version`**
- **Strategy:** Temporal bracketing against `MODEL_DEPLOYMENT_TIMELINE`
- **Reasoning:** Production deployment windows are documented in internal records. A credit model did not materialise out of thin air — it was deployed on a known date and retired on a known date. Bracketing by `recorded_at` gives an approximation.
- **Acknowledged error rate:** ~5% of events during A/B testing overlap windows (typically 2-week rollout periods where two model versions are simultaneously active). The `_inferred` suffix flags approximations explicitly.
- **Downstream consequence of incorrect inference:** `AgentPerformanceLedger` may miscount analyses per model version by ~5% during overlap periods. Acceptable for trend analytics; flagged as inferred in audit records.
- **When inference over null:** When the value can be approximated from deterministic external records, the approximation is documented, and the downstream consequence of the approximation is bounded and acceptable.

**Field: `regulatory_basis`**
- **Strategy:** Deterministic lookup from `REGULATION_TIMELINE`
- **Reasoning:** Regulation versions have statutory effective dates — these are public record and not subject to uncertainty. Given `recorded_at`, the active regulation set is deterministic.
- **Estimated error rate:** ~0% — regulation version effective dates are authoritative.

### DecisionGenerated v1 → v2

**Field: `model_versions` dict**
- **Strategy:** Set to `{"session_id": "unknown-requires-backfill"}` for v1 events
- **Reasoning:** Reconstructing model versions from contributing sessions requires async store lookups, which violates the pure-function constraint of upcasters. The stub value explicitly signals that backfill is required.
- **Performance implication:** If we were to perform store lookups in the upcaster (which we do not), each `DecisionGenerated` event load would require O(n) additional reads where n = contributing sessions (typically 2–4). At 1,000 decisions replayed during catch-up, this would add ~4,000 extra DB reads. The stub approach avoids this entirely; a background enrichment job can populate real values.

---

## Section 5: EventStoreDB Comparison

### Mapping PostgreSQL Implementation to EventStoreDB Concepts

| Our PostgreSQL Implementation | EventStoreDB Equivalent |
|-------------------------------|------------------------|
| `events` table with `stream_id` column | Stream partitioning by stream name |
| `event_streams` table with `current_version` | Per-stream metadata and version tracking |
| `SELECT FOR UPDATE` on `event_streams` row | Optimistic concurrency via `expectedVersion` in append API |
| `load_all(from_global_position)` + `global_position` column | `$all` stream subscription from position |
| `ProjectionDaemon` with `projection_checkpoints` | Persistent subscriptions with server-side checkpointing |
| `pg_notify` + polling | Native gRPC streaming subscriptions |
| `UpcasterRegistry` with version chain | Native event versioning and transformation pipeline |
| `audit-{type}-{id}` streams | Custom stream naming conventions |
| Advisory lock coordination for distributed daemon | Native consumer group support |

### What EventStoreDB Gives Us That Our Implementation Must Work Harder For

**1. Built-in persistent subscriptions with server-side checkpointing.**
In our implementation, the `ProjectionDaemon` must manage `projection_checkpoints` table, handle advisory lock coordination for distributed execution, and implement at-least-once delivery manually (~400 lines). EventStoreDB provides this natively — a persistent subscription handles checkpoint management, consumer group coordination, and delivery guarantees at the database level.

**2. Native gRPC streaming.**
Our LISTEN/NOTIFY implementation requires a dedicated database connection per daemon instance and polling fallback. EventStoreDB provides bidirectional gRPC streams with automatic reconnection and backpressure handling.

**3. Purpose-built storage engine.**
EventStoreDB's storage is optimised for append-only sequential writes and stream reads. PostgreSQL's MVCC is general-purpose — our implementation works well but requires careful index design (`idx_events_stream_id`, `idx_events_global_pos`) to achieve comparable read performance.

**4. Projection snapshots.**
EventStoreDB has first-class snapshot support for projections. Our `compliance_audit_snapshots` table replicates this pattern but requires manual snapshot lifecycle management.

**The tradeoff justifying PostgreSQL:** EventStoreDB requires a separate deployment, operational expertise, and infrastructure budget. PostgreSQL is present in virtually every enterprise environment we will encounter as FDEs. The marginal infrastructure cost of our approach is zero if the client already runs Postgres (which they almost always do). EventStoreDB is the right choice for greenfield high-throughput systems; PostgreSQL is the right choice for rapid enterprise deployment into existing infrastructure.

---

## Section 6: What I Would Do Differently

The single most significant architectural decision I would reconsider is **the dual-write pattern for `FraudScreeningCompleted`** — writing the same event to both the agent session stream and the loan stream.

This was motivated by a reasonable goal: the loan stream needs fraud score data for the `ApplicationSummary` projection, but the agent stream is the authoritative record. The solution was to write to both streams in `handle_fraud_screening_completed`.

**What this gets wrong:**

Dual-writing violates the single-writer-per-aggregate invariant. The `FraudScreeningCompleted` event is written to the loan stream without going through the `LoanApplicationAggregate` state machine — it bypasses the business rule enforcement layer. This means the loan stream can receive events that the aggregate logic has not validated. In this specific case the event is safe (it only enriches the fraud score field), but the pattern creates a maintenance trap: any future engineer adding a validation rule to `FraudScreeningCompleted` processing might not realise that the loan stream write bypasses that validation.

**What I would do instead:**

Publish `FraudScreeningCompleted` only to the agent session stream. The `ApplicationSummary` projection would subscribe to both the loan stream and agent session streams, reading fraud scores directly from agent events. This keeps the loan stream as the single authoritative state machine record and eliminates the dual-write. The projection layer is the correct place to join data from multiple streams — not the command handler.

**Why this matters in production:** The dual-write pattern was the earliest shortcut taken under time pressure. It is the kind of decision that looks harmless on day one and becomes a source of subtle bugs on day 90, when a second engineer assumes the loan stream is the complete record and builds a feature on that assumption.

---

*End of DESIGN.md*
