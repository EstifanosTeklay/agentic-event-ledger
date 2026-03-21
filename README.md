# The Ledger
## Agentic Event Store & Enterprise Audit Infrastructure
### TRP1 Week 5 · Apex Financial Services · Multi-Agent Commercial Loan Processing

---

## Overview

The Ledger is a production-quality event sourcing infrastructure for multi-agent AI systems. It provides an immutable, append-only audit trail for every AI decision in a commercial loan processing pipeline — satisfying regulatory requirements for complete, cryptographically-verifiable decision history.

**Built with:** Python 3.12+, PostgreSQL 14+, asyncpg, FastMCP, Pydantic v2

**Architecture:** CQRS + Event Sourcing + Outbox Pattern + PostgreSQL LISTEN/NOTIFY

---

## Prerequisites

- Python 3.12 or higher
- PostgreSQL 14+ (local installation)
- [uv](https://github.com/astral-sh/uv) package manager

---

## Quick Start

### 1. Install dependencies

```powershell
pip install uv
cd agentic-event-ledger
uv sync
```

### 2. Configure environment

Edit `.env` with your PostgreSQL credentials:

```env
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/ledger
```

### 3. Create the database

Open pgAdmin 4 → right-click Databases → Create → Database → name it `ledger`.

### 4. Run migrations

```powershell
uv run python scripts/run_migrations.py
```

Expected output:
```
✓ Migrations applied successfully.
  ✓  events                ✓  event_streams
  ✓  projection_checkpoints ✓  outbox
  ✓  application_summary   ✓  agent_performance_ledger
  ✓  compliance_audit_view  ✓  compliance_audit_snapshots
```

---

## Running the Test Suite

### Run all 37 tests

```powershell
uv run pytest tests/ -v --tb=short
```

### Run by phase

```powershell
uv run pytest tests/test_concurrency.py -v -s      # Phase 1
uv run pytest tests/test_aggregates.py -v -s       # Phase 2
uv run pytest tests/test_projections.py -v -s      # Phase 3
uv run pytest tests/test_upcasting.py -v -s        # Phase 4
uv run pytest tests/test_gas_town.py -v -s         # Phase 4
uv run pytest tests/test_mcp_lifecycle.py -v -s    # Phase 5
uv run pytest tests/test_phase6.py -v -s           # Phase 6
```

---

## Video Demo Scripts

### Step 1 — The Week Standard
```powershell
uv run python scripts/demo_week_standard.py
```
Complete decision history of application ID X. Full event stream, narrative,
compliance checks, cryptographic integrity. **Measured: 0.28s (target: <60s)**

### Step 2 — Concurrency Under Pressure
```powershell
uv run python scripts/demo_concurrency.py
```
Two agents race on the same stream. One wins, one gets OptimisticConcurrencyError and retries.

### Step 3 — Temporal Compliance Query
```powershell
uv run python scripts/demo_temporal_query.py
```
Compliance state at T1 (all PENDING) vs T2 (AML passed) vs NOW (all passed).

### Step 4 — Upcasting & Immutability
```powershell
uv run python scripts/demo_upcasting.py
```
v1 stored → v2 loaded → raw DB payload unchanged. confidence_score=None (no fabrication).

### Step 5 — Gas Town Crash Recovery
```powershell
uv run python scripts/demo_gas_town.py
```
4 events appended → crash simulated → agent reconstructed from event store alone.

### Step 6 — What-If Counterfactual (Bonus)
```powershell
uv run python scripts/demo_what_if.py
```
HIGH risk substituted for MEDIUM. Confidence floor forces REFER. Regulatory package generated.

---

## MCP Server

```powershell
uv run python -m src.mcp.server
```

### Tools (Command Side — 11 tools)

| Tool | Description |
|------|-------------|
| `submit_application` | Submit a new loan application |
| `start_agent_session` | **Required before any decision tool** (Gas Town) |
| `request_credit_analysis` | Transition to AWAITING_ANALYSIS state |
| `record_credit_analysis` | Record completed credit analysis |
| `record_fraud_screening` | Record completed fraud screening |
| `request_compliance_check` | Initiate compliance checking |
| `record_compliance_check` | Record a compliance rule result |
| `issue_compliance_clearance` | Issue clearance when all checks pass |
| `generate_decision` | Generate AI orchestrator recommendation |
| `record_human_review` | Record loan officer final decision |
| `run_integrity_check` | Run cryptographic hash chain verification |

### Resources (Query Side — 6 resources)

| Resource | SLO |
|----------|-----|
| `ledger://applications/{id}` | p99 < 50ms |
| `ledger://applications/{id}/compliance` | p99 < 200ms |
| `ledger://applications/{id}/compliance/at/{timestamp}` | p99 < 200ms |
| `ledger://applications/{id}/audit-trail` | p99 < 500ms |
| `ledger://agents/{id}/performance` | p99 < 50ms |
| `ledger://agents/{id}/sessions/{session_id}` | p99 < 300ms |
| `ledger://ledger/health` | p99 < 10ms |

---

## Project Structure

```
agentic-event-ledger/
├── src/
│   ├── schema.sql                    # PostgreSQL schema
│   ├── event_store.py                # EventStore core
│   ├── startup.py                    # Application wiring
│   ├── db.py                         # Connection pool
│   ├── models/events.py              # Pydantic event models + exceptions
│   ├── aggregates/
│   │   ├── loan_application.py       # 8-state machine, 6 business rules
│   │   ├── agent_session.py          # Gas Town, model version locking
│   │   ├── compliance_record.py      # Mandatory check tracking
│   │   └── audit_ledger.py           # Append-only, hash chain
│   ├── commands/handlers.py          # All command handlers
│   ├── projections/
│   │   ├── daemon.py                 # ProjectionDaemon + lag metrics
│   │   ├── application_summary.py    # SLO < 500ms
│   │   ├── agent_performance.py      # SLO < 500ms
│   │   └── compliance_audit.py       # SLO < 2000ms + temporal query
│   ├── upcasting/
│   │   ├── registry.py               # UpcasterRegistry
│   │   └── upcasters.py              # v1→v2 migrations with inference docs
│   ├── integrity/
│   │   ├── audit_chain.py            # SHA-256 hash chain
│   │   └── gas_town.py               # reconstruct_agent_context()
│   ├── mcp/
│   │   ├── server.py                 # FastMCP entry point
│   │   ├── tools.py                  # 11 tools
│   │   └── resources.py              # 6 resources
│   ├── what_if/projector.py          # Counterfactual replay
│   └── regulatory/package.py         # Examination package generator
├── tests/                            # 37 tests, all passing
├── scripts/                          # 6 video demo scripts + migration runner
├── DOMAIN_NOTES.md                   # Phase 0 graded deliverable
├── DESIGN.md                         # Architectural decision record
└── pyproject.toml                    # Dependencies (uv)
```

---

## Phase Status

| Phase | Description | Tests | Status |
|-------|-------------|-------|--------|
| 0 | Domain Reconnaissance | — | ✅ |
| 1 | Event Store Core + Concurrency | 5 | ✅ |
| 2 | Aggregates + 6 Business Rules | 8 | ✅ |
| 3 | Projections + Daemon + SLO | 7 | ✅ |
| 4 | Upcasting + Integrity + Gas Town | 6 | ✅ |
| 5 | MCP Server | 1 | ✅ |
| 6 | What-If + Regulatory Package | 10 | ✅ |
| | **Total** | **37/37** | **✅** |
