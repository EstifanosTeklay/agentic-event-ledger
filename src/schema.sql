-- =============================================================================
-- THE LEDGER — PostgreSQL Event Store Schema
-- TRP1 Week 5: Agentic Event Store & Enterprise Audit Infrastructure
-- =============================================================================
-- Design philosophy:
--   Every column exists for a specific reason documented below.
--   No column was added speculatively. See DESIGN.md for full justification.
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "pg_trgm";    -- future: full-text search on event_type

-- =============================================================================
-- CORE EVENT STORE
-- =============================================================================

CREATE TABLE IF NOT EXISTS events (
    -- event_id: Globally unique identifier for this specific event occurrence.
    --   UUID rather than BIGSERIAL because events are replicated across
    --   environments (dev → staging → prod) and must remain unique without
    --   coordination. gen_random_uuid() from pgcrypto is cryptographically random.
    event_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- stream_id: Identifies the aggregate stream this event belongs to.
    --   Format: "{aggregate_type}-{aggregate_id}", e.g. "loan-ABC-001".
    --   TEXT (not FK) because streams are created implicitly on first append.
    stream_id        TEXT NOT NULL,

    -- stream_position: Position of this event within its stream.
    --   1-based. The pair (stream_id, stream_position) uniquely identifies
    --   an event's place in aggregate history. Used for optimistic concurrency:
    --   expected_version must equal current max(stream_position) before append.
    stream_position  BIGINT NOT NULL,

    -- global_position: Monotonically increasing position across ALL streams.
    --   GENERATED ALWAYS AS IDENTITY guarantees no gaps under normal operation
    --   and no reuse after deletes (which we never do, but the guarantee matters).
    --   Used by the ProjectionDaemon to track its read cursor across all events.
    global_position  BIGINT GENERATED ALWAYS AS IDENTITY,

    -- event_type: Discriminator string identifying the event class.
    --   e.g. "CreditAnalysisCompleted". Not an enum — new event types are added
    --   without schema migrations. The upcaster registry uses this for routing.
    event_type       TEXT NOT NULL,

    -- event_version: Schema version of this event's payload.
    --   Starts at 1. Incremented when the event's payload structure changes.
    --   The UpcasterRegistry uses (event_type, event_version) as its lookup key
    --   to apply the correct migration chain at read time. Never mutated in place.
    event_version    SMALLINT NOT NULL DEFAULT 1,

    -- payload: The event's domain data as JSONB.
    --   JSONB (not JSON) for: indexing support, binary storage efficiency,
    --   and operator support (e.g. payload->>'application_id' in queries).
    --   Schema is enforced at the application layer via Pydantic, not DB constraints,
    --   because event schemas evolve and constraints would require migrations.
    payload          JSONB NOT NULL,

    -- metadata: Cross-cutting infrastructure fields.
    --   Stores: correlation_id, causation_id, recorded_by, schema_version,
    --   environment. Separated from payload to keep domain data clean and to
    --   allow metadata queries without touching business payload structure.
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- recorded_at: Wall-clock timestamp of when this event was persisted.
    --   clock_timestamp() (not now()) because clock_timestamp() returns the
    --   actual current time even inside a transaction, giving accurate ordering
    --   of events appended in the same transaction. TIMESTAMPTZ stores UTC.
    recorded_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    -- Uniqueness constraint: no two events may occupy the same position in
    -- the same stream. This is the database-level enforcement of the optimistic
    -- concurrency control logic implemented in EventStore.append().
    CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- Primary access pattern: load all events for a stream in order.
--   Covers: load_stream(stream_id), aggregate state reconstruction.
CREATE INDEX IF NOT EXISTS idx_events_stream_id
    ON events (stream_id, stream_position);

-- ProjectionDaemon cursor: scan all events from a global position.
--   Covers: load_all(from_global_position), daemon catch-up after restart.
CREATE INDEX IF NOT EXISTS idx_events_global_pos
    ON events (global_position);

-- Event type filtering: load_all(event_types=[...]) for type-specific projections.
--   Covers: ComplianceAuditView subscribing only to compliance event types.
CREATE INDEX IF NOT EXISTS idx_events_type
    ON events (event_type);

-- Temporal queries: compliance state as-of a timestamp, audit time ranges.
--   Covers: get_compliance_at(application_id, timestamp), audit trail range queries.
CREATE INDEX IF NOT EXISTS idx_events_recorded
    ON events (recorded_at);

-- Composite index for temporal queries scoped to a stream.
--   Covers: load_stream with from/to positions AND recorded_at filtering.
CREATE INDEX IF NOT EXISTS idx_events_stream_recorded
    ON events (stream_id, recorded_at);

-- =============================================================================
-- STREAM REGISTRY
-- =============================================================================

CREATE TABLE IF NOT EXISTS event_streams (
    -- stream_id: Primary key, matches events.stream_id.
    --   Explicit registry rather than deriving from events table because:
    --   (1) current_version must be atomically updated with event inserts,
    --   (2) aggregate_type metadata is needed for monitoring and routing,
    --   (3) archival state must be tracked without scanning events.
    stream_id        TEXT PRIMARY KEY,

    -- aggregate_type: The class of aggregate this stream belongs to.
    --   e.g. "LoanApplication", "AgentSession". Used for:
    --   (1) monitoring dashboards (stream count by aggregate type),
    --   (2) selective archival (archive all AgentSession streams older than 90 days),
    --   (3) EventStoreDB migration mapping (stream_id → stream category).
    aggregate_type   TEXT NOT NULL,

    -- current_version: The stream_position of the most recently appended event.
    --   This is the version number checked during optimistic concurrency control.
    --   Updated atomically in the same transaction as the event insert via
    --   SELECT ... FOR UPDATE on this row, then UPDATE.
    current_version  BIGINT NOT NULL DEFAULT 0,

    -- created_at: When this stream was first created (first event appended).
    --   Used for: stream age reporting, hot/cold storage tiering decisions,
    --   and identifying stale streams for archival.
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- archived_at: NULL if active; set when archive_stream() is called.
    --   Archived streams are read-only. The ProjectionDaemon skips events
    --   from archived streams in its catch-up logic.
    --   NULL check is faster than a separate boolean column.
    archived_at      TIMESTAMPTZ,

    -- metadata: Stream-level metadata bag.
    --   Stores: application_id cross-reference, agent_id for AgentSession streams,
    --   custom tags set by the domain layer. Kept separate from events.metadata
    --   so stream-level facts don't pollute individual event metadata.
    metadata         JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Index for aggregate type queries (monitoring, archival selection).
CREATE INDEX IF NOT EXISTS idx_streams_aggregate_type
    ON event_streams (aggregate_type);

-- Index for archival queries (find non-archived streams older than N days).
CREATE INDEX IF NOT EXISTS idx_streams_archived
    ON event_streams (archived_at) WHERE archived_at IS NULL;

-- =============================================================================
-- PROJECTION CHECKPOINTS
-- =============================================================================

CREATE TABLE IF NOT EXISTS projection_checkpoints (
    -- projection_name: Unique identifier for the projection.
    --   e.g. "ApplicationSummary", "AgentPerformanceLedger".
    --   TEXT primary key — projections are few (< 20) and named by convention.
    projection_name  TEXT PRIMARY KEY,

    -- last_position: The global_position of the last event this projection
    --   successfully processed. The daemon resumes from last_position + 1
    --   after restart, ensuring exactly-once processing (combined with
    --   idempotent projection handlers).
    last_position    BIGINT NOT NULL DEFAULT 0,

    -- updated_at: Timestamp of the last checkpoint update.
    --   Used to calculate projection lag: lag_ms = now() - updated_at
    --   when no new events are arriving. Also used for daemon health monitoring.
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- OUTBOX (Guaranteed Event Delivery)
-- =============================================================================

CREATE TABLE IF NOT EXISTS outbox (
    -- id: Unique identifier for this outbox entry.
    --   Separate from event_id because one event may produce multiple
    --   outbox entries (fan-out to multiple destinations).
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- event_id: FK to the event that generated this outbox entry.
    --   Maintains referential integrity — outbox entries cannot exist for
    --   events that were rolled back. ON DELETE RESTRICT prevents orphan entries.
    event_id         UUID NOT NULL REFERENCES events(event_id) ON DELETE RESTRICT,

    -- destination: Target for this publication.
    --   e.g. "redis://streams/loan-events", "kafka://loan-decisions".
    --   TEXT allows routing to heterogeneous message buses without schema changes.
    destination      TEXT NOT NULL,

    -- payload: The message payload to publish.
    --   May differ from events.payload (e.g., enriched with projection data,
    --   or transformed for a specific consumer's schema). JSONB for consistency.
    payload          JSONB NOT NULL,

    -- created_at: When this outbox entry was created (same transaction as event).
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- published_at: NULL until successfully published. Set by the outbox processor.
    --   NULL = pending publication. NOT NULL = delivered. Used for:
    --   (1) polling query (WHERE published_at IS NULL),
    --   (2) delivery latency measurement (published_at - created_at).
    published_at     TIMESTAMPTZ,

    -- attempts: Number of publication attempts. Incremented on each try.
    --   The outbox processor stops retrying after max_attempts (configurable,
    --   default 5). Failed entries are flagged for manual investigation.
    attempts         SMALLINT NOT NULL DEFAULT 0
);

-- Polling index: outbox processor scans for unpublished entries.
--   Partial index on unpublished only — the common case. Published entries
--   (historical) are excluded, keeping the index small and fast.
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (created_at) WHERE published_at IS NULL;

-- FK lookup index: find all outbox entries for a given event.
CREATE INDEX IF NOT EXISTS idx_outbox_event_id
    ON outbox (event_id);

-- =============================================================================
-- PROJECTION READ MODELS
-- =============================================================================

-- ApplicationSummary: Current state of each loan application.
--   One row per application. Updated by ProjectionDaemon as events arrive.
--   SLO: lag < 500ms. This is the primary query target for the loan officer UI.
CREATE TABLE IF NOT EXISTS application_summary (
    application_id          TEXT PRIMARY KEY,
    state                   TEXT NOT NULL DEFAULT 'SUBMITTED',
    applicant_id            TEXT,
    requested_amount_usd    NUMERIC(15, 2),
    approved_amount_usd     NUMERIC(15, 2),
    risk_tier               TEXT,
    fraud_score             NUMERIC(5, 4),
    compliance_status       TEXT,
    decision                TEXT,
    agent_sessions_completed JSONB NOT NULL DEFAULT '[]'::jsonb,
    last_event_type         TEXT,
    last_event_at           TIMESTAMPTZ,
    human_reviewer_id       TEXT,
    final_decision_at       TIMESTAMPTZ,
    -- Staleness metadata returned to UI consumers
    data_as_of              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- AgentPerformanceLedger: Metrics per agent model version.
--   Updated as CreditAnalysisCompleted, DecisionGenerated, HumanReviewCompleted events arrive.
CREATE TABLE IF NOT EXISTS agent_performance_ledger (
    agent_id                TEXT NOT NULL,
    model_version           TEXT NOT NULL,
    analyses_completed      INTEGER NOT NULL DEFAULT 0,
    decisions_generated     INTEGER NOT NULL DEFAULT 0,
    avg_confidence_score    NUMERIC(5, 4),
    avg_duration_ms         NUMERIC(10, 2),
    approve_rate            NUMERIC(5, 4),
    decline_rate            NUMERIC(5, 4),
    refer_rate              NUMERIC(5, 4),
    human_override_rate     NUMERIC(5, 4),
    first_seen_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agent_id, model_version)
);

-- ComplianceAuditView: Full compliance record per application.
--   Supports temporal queries (state at any past timestamp).
--   Each row is one compliance check result — NOT one row per application.
CREATE TABLE IF NOT EXISTS compliance_audit_view (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id          TEXT NOT NULL,
    rule_id                 TEXT NOT NULL,
    rule_version            TEXT NOT NULL,
    regulation_set_version  TEXT,
    status                  TEXT NOT NULL,      -- 'PASSED' | 'FAILED' | 'PENDING'
    failure_reason          TEXT,
    remediation_required    BOOLEAN,
    evidence_hash           TEXT,
    evaluation_timestamp    TIMESTAMPTZ,
    event_global_position   BIGINT NOT NULL,    -- for temporal queries
    recorded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_compliance_application
    ON compliance_audit_view (application_id, recorded_at);

CREATE INDEX IF NOT EXISTS idx_compliance_temporal
    ON compliance_audit_view (application_id, event_global_position);

-- ComplianceAuditSnapshots: Point-in-time snapshots for temporal query efficiency.
--   Avoids full replay on every as-of query. Snapshot strategy: every 50 events
--   per application OR on explicit rebuild request. See DESIGN.md §2.
CREATE TABLE IF NOT EXISTS compliance_audit_snapshots (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id          TEXT NOT NULL,
    snapshot_at_position    BIGINT NOT NULL,    -- global_position at snapshot time
    snapshot_at_timestamp   TIMESTAMPTZ NOT NULL,
    snapshot_data           JSONB NOT NULL,     -- serialised ComplianceAuditView state
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_compliance_snapshots
    ON compliance_audit_snapshots (application_id, snapshot_at_position DESC);

-- =============================================================================
-- REAL-TIME NOTIFICATION
-- =============================================================================

-- Trigger function: notify projection daemon of new events via LISTEN/NOTIFY.
--   This supplements polling with push notification, reducing daemon latency
--   from poll_interval_ms to near-zero for real-time scenarios.
CREATE OR REPLACE FUNCTION notify_event_appended()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'new_event',
        json_build_object(
            'event_id',       NEW.event_id,
            'stream_id',      NEW.stream_id,
            'event_type',     NEW.event_type,
            'global_position', NEW.global_position
        )::text
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_notify_event_appended
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_event_appended();

-- =============================================================================
-- SCHEMA VERSION TRACKING
-- =============================================================================

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    description TEXT NOT NULL
);

INSERT INTO schema_migrations (version, description)
VALUES ('001', 'Initial schema: events, event_streams, projection_checkpoints, outbox, projections, notify trigger')
ON CONFLICT (version) DO NOTHING;
