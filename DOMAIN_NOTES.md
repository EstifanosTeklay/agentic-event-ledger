# DOMAIN_NOTES.md — The Ledger: Phase 0 Domain Reconnaissance

**Project:** TRP1 Week 5 — Agentic Event Store & Enterprise Audit Infrastructure  
**Scenario:** Apex Financial Services — Multi-Agent Commercial Loan Processing  
**Author:** TRP1 Candidate  
**Date:** 2026-03-18

---

## Question 1: EDA vs. Event Sourcing — The Architecture Distinction

### Is LangChain-style callback tracing EDA or ES?

LangChain's callback system — and any component that fires trace events via hooks — is **Event-Driven Architecture (EDA)**, not Event Sourcing (ES). The distinction is precise and consequential:

| Dimension | EDA (LangChain callbacks) | Event Sourcing (The Ledger) |
|---|---|---|
| **Purpose** | Notification / observability | Source of truth |
| **Durability** | Fire-and-forget; may be dropped | ACID-committed before action proceeds |
| **Completeness** | Best-effort; gaps are acceptable | Gaps are corruption |
| **Replayability** | Not guaranteed | Mandatory — the entire read model is derived from replay |
| **State derivation** | External state is primary; events are annotations | Events ARE the state; no other representation is authoritative |
| **Loss semantics** | A dropped trace degrades observability | A dropped event corrupts all downstream aggregates |

In LangChain's model, the agent's in-memory state is the authoritative state. The callback merely signals that something happened. If the callback handler throws, the agent continues regardless. The event is secondary.

### What Would Change Under The Ledger?

Redesigning the same component to use The Ledger would require three architectural shifts:

1. **Write-before-execute semantics.** Before the agent takes any action (e.g., calls a tool, emits a decision), it appends an intent event to the event store in the same transaction. The action only proceeds after the append is committed. If the process crashes between commit and execution, the event is the proof that execution was intended — and the agent can reconstruct and resume.

2. **State derives from events, not memory.** The agent's current context window, its pending work items, and its intermediate conclusions are all reconstructed by replaying the `AgentSession` stream — not read from an in-memory dict or a CRUD table. On restart, the agent calls `reconstruct_agent_context()` and continues exactly where it left off.

3. **Events become the audit record, not an annotation.** Downstream projections (ApplicationSummary, AgentPerformanceLedger) are built exclusively by consuming the event stream. There is no other database of record. A compliance officer querying the audit trail is reading from projections derived from immutable events — not from application state that may have been mutated by a bug.

**What you gain:** process-crash safety (Gas Town pattern), time-travel queries, causal traceability across agent boundaries, and regulatory compliance without retrofitting. What you pay: higher write latency (the append must commit before action), and the operational complexity of projection lag management.

---

## Question 2: The Aggregate Question — Considered and Rejected Boundary

### The Four Chosen Aggregates
1. `LoanApplication` — `loan-{application_id}`
2. `AgentSession` — `agent-{agent_id}-{session_id}`
3. `ComplianceRecord` — `compliance-{application_id}`
4. `AuditLedger` — `audit-{entity_type}-{entity_id}`

### Alternative Boundary Considered: Merging ComplianceRecord Into LoanApplication

The most tempting simplification is to eliminate `ComplianceRecord` as a separate aggregate and treat compliance events as a sub-stream of `LoanApplication`. The rationale would be: compliance exists only in the context of a loan application — why have a separate stream?

**Why this was rejected:**

Merging them creates a **concurrency coupling problem** under the Apex scenario's operational model. The `ComplianceAgent` and the `CreditAnalysisAgent` run **in parallel** — both are processing the same application simultaneously and both need to write events. Under the merged model, both agents would be appending to the same `loan-{id}` stream with competing `expected_version` values.

Trace the failure mode:
- CreditAnalysisAgent reads `loan-ABC` at version 3, prepares to append `CreditAnalysisCompleted` at version 4.
- ComplianceAgent simultaneously reads `loan-ABC` at version 3, prepares to append `ComplianceRulePassed` at version 4.
- One wins. The other receives `OptimisticConcurrencyError`, reloads, and retries.
- Under high application volume (1,000 applications/hour × 4 agents), this produces **constant concurrency collisions on every stream**, with retry cascades degrading write throughput.

By separating `ComplianceRecord`, the CreditAnalysisAgent writes exclusively to `loan-{id}` and the ComplianceAgent writes exclusively to `compliance-{id}`. **They never collide.** Each aggregate is owned by exactly one writer at a time. The `LoanApplication` aggregate references compliance status by reading from the `ComplianceAuditView` projection — it does not require a lock on the compliance stream to enforce business rule 5 (approval requires all checks passed).

**The coupling this prevents:** Write contention under parallel agent execution. In a multi-agent system, aggregate boundary design is not just a modelling question — it is a **throughput engineering decision**.

---

## Question 3: Optimistic Concurrency — The Two-Agent Collision Trace

**Scenario:** Two AI agents simultaneously call `append_events` with `expected_version=3` on stream `loan-ABC-001`.

### Exact Operation Sequence

```
T=0ms    Agent-Fraud reads loan-ABC-001: current_version=3
T=0ms    Agent-Credit reads loan-ABC-001: current_version=3

T=5ms    Agent-Fraud constructs FraudScreeningCompleted event
T=5ms    Agent-Credit constructs CreditAnalysisCompleted event

T=10ms   Agent-Fraud enters DB transaction:
           SELECT current_version FROM event_streams
           WHERE stream_id = 'loan-ABC-001' FOR UPDATE
           → returns 3. Matches expected_version=3. ✓
           INSERT INTO events (stream_id, stream_position, ...) VALUES ('loan-ABC-001', 4, ...)
           UPDATE event_streams SET current_version = 4 WHERE stream_id = 'loan-ABC-001'
           COMMIT → SUCCESS. Returns new_version=4.

T=10ms   Agent-Credit enters DB transaction (concurrent, slightly behind):
           SELECT current_version FROM event_streams
           WHERE stream_id = 'loan-ABC-001' FOR UPDATE
           → BLOCKS (Agent-Fraud holds row lock)

T=11ms   Agent-Fraud's COMMIT releases lock.

T=11ms   Agent-Credit's SELECT returns: current_version=4.
           4 ≠ 3 (expected_version).
           ROLLBACK transaction.
           RAISE OptimisticConcurrencyError(
               stream_id='loan-ABC-001',
               expected_version=3,
               actual_version=4
           )
```

### What the Losing Agent Receives

```python
OptimisticConcurrencyError(
    stream_id="loan-ABC-001",
    expected_version=3,
    actual_version=4,
    message="Stream loan-ABC-001 has advanced to version 4; expected 3",
    suggested_action="reload_stream_and_retry"
)
```

### What the Losing Agent Must Do Next

1. **Reload the stream** from position 0 (or from last known checkpoint) via `load_stream("loan-ABC-001")`.
2. **Reconstruct aggregate state** by replaying the now-4-event stream, including the winning agent's newly appended event.
3. **Re-evaluate business rules** against the updated state. Critical question: is the losing agent's analysis still relevant given what the winning agent just committed? For example, if the winning agent already appended a `FraudScreeningCompleted` with `fraud_score=0.95`, the credit agent may need to adjust its risk tier.
4. **Retry the append** with `expected_version=4` if the analysis is still valid.
5. **Apply retry budget**: maximum 3 retries with exponential backoff (50ms, 100ms, 200ms). On exhaustion, return a `RetryBudgetExceeded` error to the caller — do not loop indefinitely.

The `FOR UPDATE` on `event_streams` is the critical implementation detail. It serialises concurrent writers at the row level, ensuring the version check and the insert are atomic without requiring a table-level lock or a SERIALIZABLE isolation level. This is the standard PostgreSQL pattern for optimistic concurrency over a single row.

---

## Question 4: Projection Lag and the Stale Read Problem

**Scenario:** ComplianceAuditView lags 200ms. Loan officer queries "available credit limit" immediately after a disbursement event is committed.

### What the System Does

The `ApplicationSummary` projection has a contractual SLO of **<500ms lag**. At 200ms typical lag, the loan officer's read hits the projection table before the `DisbursementRecorded` event has been applied. They see the **pre-disbursement credit limit**.

This is **expected and correct behaviour** for an eventually consistent system. The event store's data is not wrong — the projection is temporarily behind. The key is communicating this honestly to the user interface.

### Response Strategy

**1. Staleness timestamp on every projection read.**  
Every query against the `ApplicationSummary` projection returns a `data_as_of` field: the `recorded_at` timestamp of the last event the projection has processed. The UI layer compares this against the current timestamp. If `now() - data_as_of > 1 second`, the UI renders a visual indicator:

> ⚠ *Credit limit shown as of 14:03:22. Updates in progress — refresh in a moment.*

This is accurate and non-alarming. It does not say "data may be wrong" — it says "data is current as of this timestamp."

**2. Lag metric exposure.**  
`get_projection_lag("ApplicationSummary")` is called by the health endpoint and surfaced in Grafana. If lag exceeds 500ms (the SLO), an alert fires to the operations team. The loan officer's UI experience is a symptom; the alert is the systemic response.

**3. No read-your-writes bypass in the projection layer.**  
A common but dangerous pattern is to bypass the projection for "critical" reads and load the aggregate stream directly. This breaks the CQRS contract, introduces inconsistent latency for some queries, and creates a two-codepath maintenance problem. The correct approach is to make the projection fast and reliable enough that the SLO is met, and to communicate honestly when it temporarily isn't.

**4. What we do NOT do.**  
We do not fabricate a "real-time" credit limit by merging projection data with unprocessed events on the read path. This would couple the query model to the event store schema, destroy projection encapsulation, and introduce bugs when projections have complex aggregation logic (e.g., averaging fraud scores across multiple agents).

### Communication to the UI

The `ledger://applications/{id}` resource response envelope includes:

```json
{
  "data": { "available_credit_usd": 450000, ... },
  "meta": {
    "data_as_of": "2026-03-18T14:03:22.847Z",
    "projection_lag_ms": 183,
    "slo_ms": 500,
    "slo_status": "OK"
  }
}
```

The UI decides how to render this — the API's job is to provide accurate staleness metadata, not to hide it.

---

## Question 5: Upcasting the CreditDecisionMade Event

### Schema Evolution

**v1 (2024):**
```json
{ "application_id": "...", "decision": "APPROVE", "reason": "..." }
```

**v2 (2026):**
```json
{
  "application_id": "...",
  "decision": "APPROVE",
  "reason": "...",
  "model_version": "...",
  "confidence_score": 0.87,
  "regulatory_basis": "..."
}
```

### The Upcaster Implementation

```python
@registry.register("CreditDecisionMade", from_version=1)
def upcast_credit_decision_v1_to_v2(payload: dict) -> dict:
    recorded_at = payload.get("_recorded_at")  # injected by load path from StoredEvent

    # Inference strategy for model_version:
    # Pre-2026 events were produced by a known set of model versions.
    # We can bracket by recorded_at date against our internal model deployment log.
    # This is an approximation — not a guarantee.
    if recorded_at and recorded_at < "2025-06-01T00:00:00Z":
        inferred_model_version = "credit-model-v1.x-pre-2025H2"
    elif recorded_at and recorded_at < "2026-01-01T00:00:00Z":
        inferred_model_version = "credit-model-v2.x-2025H2"
    else:
        inferred_model_version = "legacy-unknown"

    return {
        **payload,
        # Inferred from deployment timeline — acknowledged approximation
        "model_version": inferred_model_version,

        # Genuinely unknown — do not fabricate
        # Historical events were produced before confidence scoring was implemented.
        # Fabricating a value (e.g., 0.5) would corrupt downstream analytics
        # (e.g., AgentPerformanceLedger avg_confidence calculations).
        # null is semantically correct: "this information did not exist at event time."
        "confidence_score": None,

        # Inferred from the regulation set active at recorded_at.
        # We maintain a regulation_versions table with effective_from/effective_to dates.
        # This lookup is deterministic and auditable.
        "regulatory_basis": _infer_regulatory_basis(recorded_at),
    }
```

### Inference Strategy for model_version

**Approach:** temporal bracketing against internal model deployment records.

We maintain a table `model_deployment_log(model_version TEXT, deployed_at TIMESTAMPTZ, retired_at TIMESTAMPTZ)`. At upcaster registration time, we load this table into memory. For a historical event at `recorded_at`, we find the model version that was active at that timestamp.

**Acknowledged error rate:** Medium. If two model versions overlapped in deployment (A/B testing), we cannot determine which version produced a given event without additional signal. The upcaster logs a `UpcasterInferenceWarning` for any event where the lookup is ambiguous. Downstream consumers of `model_version` must treat the field as `"inferred"` not `"verified"` unless the original event was v2.

**Why not null for model_version?**  
Unlike `confidence_score`, a model version *can* be reasonably inferred from deployment timelines. Returning `null` would force every downstream consumer to handle null and degrade analytics unnecessarily. The inference is imperfect but useful. `confidence_score` cannot be inferred — a score of 0.5 for an unknown event is fabrication that would silently corrupt metrics.

### The null vs. Fabrication Decision Rule

> **Use null** when the value is genuinely unknowable from available signals and any fabricated value would corrupt downstream computation or misrepresent historical facts.  
> **Use inference** when the value can be approximated from deterministic external records (deployment logs, regulation version tables), and the inference is documented, auditable, and flagged as such.

`confidence_score` = **null**. No deployment log can tell us what the model's confidence was on a specific input in 2024. Any number we invent is fiction.  
`model_version` = **inferred with warning**. Deployment logs exist. The approximation is documented.  
`regulatory_basis` = **inferred deterministically**. Regulation versions have known effective dates. This is a lookup, not a guess.

---

## Question 6: Distributed Projection Execution — The Marten Async Daemon Parallel

### What Marten 7.0's Distributed Daemon Does

Marten's Async Daemon in distributed mode uses leader election (via PostgreSQL advisory locks) to assign projection shards to specific nodes. Each node owns a subset of projection event streams. If a node fails, the lock expires and another node claims its shard. This prevents two nodes from processing the same events simultaneously (which would cause duplicate projection updates).

### Python Implementation Strategy

**Coordination primitive: PostgreSQL advisory locks.**

PostgreSQL `pg_try_advisory_lock(key BIGINT)` provides session-scoped, non-blocking lock acquisition. This is the same primitive Marten uses internally. It requires no external dependency (no Redis, no ZooKeeper) and is available in any Postgres installation.

```python
class DistributedProjectionDaemon:
    """
    Each daemon instance attempts to acquire an advisory lock per projection.
    Only the lock-holder processes events for that projection.
    On failure/disconnect, the lock is released automatically by Postgres
    (session-scoped), allowing another instance to take over within one
    poll_interval cycle.
    """

    LOCK_NAMESPACE = 987654  # arbitrary namespace to avoid collision

    async def _try_acquire_projection_lock(
        self, conn: asyncpg.Connection, projection_name: str
    ) -> bool:
        lock_key = self.LOCK_NAMESPACE + hash(projection_name) % 100000
        result = await conn.fetchval(
            "SELECT pg_try_advisory_lock($1)", lock_key
        )
        return result  # True if acquired, False if another node holds it

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        async with self._pool.acquire() as lock_conn:
            for projection in self._projections.values():
                acquired = await self._try_acquire_projection_lock(
                    lock_conn, projection.name
                )
                if acquired:
                    asyncio.create_task(
                        self._run_projection(projection, lock_conn, poll_interval_ms)
                    )
                    # lock_conn stays open for the session duration
                    # Postgres releases lock automatically on disconnect
```

**Failure mode guarded against:** **split-brain dual processing.** Without the advisory lock, two daemon instances (e.g., after a rolling deploy where old and new instances briefly coexist) would both process the same events, applying duplicate updates to projection tables. With the lock, exactly one instance holds the processing rights per projection at any moment. The other instance polls but does not process until the lock is released.

**Secondary failure mode:** **checkpoint drift.** If a daemon crashes after processing events but before committing the checkpoint, it will re-process those events on restart. All projection handlers must therefore be **idempotent** — applying the same event twice must produce the same result as applying it once. For insert-or-update projections (like `ApplicationSummary`), `INSERT ... ON CONFLICT DO UPDATE` achieves this naturally.

**What EventStoreDB gives that we must work harder for:**  
EventStoreDB's persistent subscriptions handle checkpoint management, consumer group coordination, and at-least-once delivery natively at the database level. In our PostgreSQL implementation, we must implement all of this in application code: the `projection_checkpoints` table, the advisory lock coordination, the idempotency guarantees, and the lag metric calculation. This is approximately 400–600 lines of infrastructure code that EventStoreDB makes unnecessary. The tradeoff: PostgreSQL is universally available in enterprise environments; EventStoreDB requires a separate deployment and operational expertise.

---

