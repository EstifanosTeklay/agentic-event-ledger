"""
tests/test_upcasting.py
========================
Phase 4 — Upcasting immutability test.

THE CRITICAL TEST:
  1. Store a v1 event in the database
  2. Load it via EventStore.load_stream() → must arrive as v2 (upcasted)
  3. Query the raw DB row → payload must be UNCHANGED (still v1)

Any system where upcasting touches stored events has broken the
core guarantee of event sourcing. This test is mandatory.

Also tests:
  - UpcasterRegistry chain application
  - null confidence_score (no fabrication)
  - model_version inference with _inferred suffix
  - DecisionGenerated v1→v2 migration
"""

from __future__ import annotations

import json
import os
import sys
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Wire upcasters before any EventStore use
from src.startup import initialise
initialise()

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    RiskTier,
)
from src.upcasting.registry import UpcasterRegistry
from src.upcasting.upcasters import build_upcaster_registry

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger"
)


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
    yield p
    await p.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_db(pool):
    async with pool.acquire() as conn:
        await conn.execute(
            "TRUNCATE TABLE events, event_streams, outbox, "
            "projection_checkpoints RESTART IDENTITY CASCADE"
        )
    yield


@pytest_asyncio.fixture
async def store(pool):
    return EventStore(pool=pool)


# =============================================================================
# THE MANDATORY IMMUTABILITY TEST
# =============================================================================

@pytest.mark.asyncio
async def test_upcasting_does_not_mutate_stored_payload(store, pool):
    """
    THE CRITICAL IMMUTABILITY TEST:

    1. Append a CreditAnalysisCompleted event with event_version=1
       (simulating a historical v1 event)
    2. Load it via EventStore.load_stream() → must arrive as v2
    3. Query the raw DB row → stored payload must be UNCHANGED (v1)

    This proves upcasting is a read-time transformation only.
    The event store is append-only and immutable.
    """
    stream_id = "loan-UPCAST-TEST-001"

    # ── Step 1: Store a v1 event directly ────────────────────────────────────
    # We bypass the normal command handler to store a genuine v1 event
    # (simulating a historical event from before v2 was introduced)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
            VALUES ($1, 'LoanApplication', 0)
            ON CONFLICT DO NOTHING
            """,
            stream_id,
        )
        await conn.execute(
            """
            INSERT INTO events
                (stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, 1, 'CreditAnalysisCompleted', 1, $2::jsonb, '{}'::jsonb)
            """,
            stream_id,
            json.dumps({
                "application_id": "UPCAST-TEST-001",
                "agent_id": "agent-credit-001",
                "session_id": "sess-001",
                "risk_tier": "MEDIUM",
                "recommended_limit_usd": "480000.00",
                "analysis_duration_ms": 1200,
                "input_data_hash": "sha256-test-input",
                # NOTE: No model_version, confidence_score, regulatory_basis
                # This is a genuine v1 payload
            }),
        )
        await conn.execute(
            "UPDATE event_streams SET current_version = 1 WHERE stream_id = $1",
            stream_id,
        )

    # ── Step 2: Query raw DB row BEFORE loading via EventStore ────────────────
    async with pool.acquire() as conn:
        raw_before = await conn.fetchrow(
            "SELECT event_version, payload FROM events WHERE stream_id = $1",
            stream_id,
        )

    assert raw_before["event_version"] == 1, "Stored event must be v1"
    _rb = raw_before["payload"]
    if isinstance(_rb, str):
        _rb = __import__('json').loads(_rb)
    raw_payload_before = dict(_rb)
    assert "model_version" not in raw_payload_before, \
        "Raw v1 payload must NOT contain model_version"
    assert "confidence_score" not in raw_payload_before, \
        "Raw v1 payload must NOT contain confidence_score"

    print(f"\n  Raw DB payload (v1): {list(raw_payload_before.keys())}")

    # ── Step 3: Load via EventStore (upcasting applied) ───────────────────────
    loaded_events = await store.load_stream(stream_id)
    assert len(loaded_events) == 1
    loaded_event = loaded_events[0]

    print(f"  Loaded event version: {loaded_event.event_version}")
    print(f"  Loaded payload keys: {list(loaded_event.payload.keys())}")

    # Upcasted event must be v2
    assert loaded_event.event_version == 2, (
        f"Loaded event must be v2 after upcasting, got v{loaded_event.event_version}"
    )

    # v2 fields must be present
    assert "model_version" in loaded_event.payload, \
        "Upcasted v2 event must contain model_version"
    assert "confidence_score" in loaded_event.payload, \
        "Upcasted v2 event must contain confidence_score"
    assert "regulatory_basis" in loaded_event.payload, \
        "Upcasted v2 event must contain regulatory_basis"

    # confidence_score must be None (no fabrication)
    assert loaded_event.payload["confidence_score"] is None, (
        "confidence_score must be None for historical v1 events — "
        "fabricating a score that was never computed would corrupt analytics"
    )

    # model_version must be inferred (has _inferred suffix)
    model_ver = loaded_event.payload["model_version"]
    assert "_inferred" in model_ver or "legacy" in model_ver, (
        f"model_version must be marked as inferred, got: {model_ver}"
    )

    print(f"  model_version (inferred): {loaded_event.payload['model_version']}")
    print(f"  confidence_score (null — correct): {loaded_event.payload['confidence_score']}")
    print(f"  regulatory_basis: {loaded_event.payload['regulatory_basis']}")

    # ── Step 4: THE CRITICAL CHECK — raw DB payload must be UNCHANGED ─────────
    async with pool.acquire() as conn:
        raw_after = await conn.fetchrow(
            "SELECT event_version, payload FROM events WHERE stream_id = $1",
            stream_id,
        )

    assert raw_after["event_version"] == 1, (
        "IMMUTABILITY VIOLATED: stored event_version changed from 1 to "
        f"{raw_after['event_version']} after upcasting"
    )

    _ra = raw_after["payload"]
    if isinstance(_ra, str):
        _ra = __import__('json').loads(_ra)
    raw_payload_after = dict(_ra)
    assert "model_version" not in raw_payload_after, (
        "IMMUTABILITY VIOLATED: model_version was written to the stored payload"
    )
    assert "confidence_score" not in raw_payload_after, (
        "IMMUTABILITY VIOLATED: confidence_score was written to the stored payload"
    )
    assert raw_payload_before == raw_payload_after, (
        "IMMUTABILITY VIOLATED: stored payload changed after upcasting"
    )

    print(f"\n{'='*60}")
    print(f"  UPCASTING IMMUTABILITY TEST — PASSED")
    print(f"{'='*60}")
    print(f"  Stored payload:  v1 (unchanged) ✓")
    print(f"  Loaded payload:  v2 (upcasted)  ✓")
    print(f"  confidence_score: None (no fabrication) ✓")
    print(f"  model_version: {model_ver} ✓")
    print(f"{'='*60}")


# =============================================================================
# REGISTRY UNIT TESTS
# =============================================================================

@pytest.mark.asyncio
async def test_upcaster_registry_chain():
    """UpcasterRegistry applies chained upcasters in version order."""
    registry = UpcasterRegistry()

    @registry.register("TestEvent", from_version=1)
    def v1_to_v2(payload: dict) -> dict:
        return {**payload, "field_v2": "added_by_v2"}

    @registry.register("TestEvent", from_version=2)
    def v2_to_v3(payload: dict) -> dict:
        return {**payload, "field_v3": "added_by_v3"}

    from src.models.events import StoredEvent
    from uuid import uuid4
    from datetime import datetime, timezone

    v1_event = StoredEvent(
        event_id=uuid4(),
        stream_id="test-stream",
        stream_position=1,
        global_position=1,
        event_type="TestEvent",
        event_version=1,
        payload={"original": "data"},
        metadata={},
        recorded_at=datetime.now(timezone.utc),
    )

    result = registry.upcast(v1_event)
    assert result.event_version == 3
    assert result.payload["field_v2"] == "added_by_v2"
    assert result.payload["field_v3"] == "added_by_v3"
    assert result.payload["original"] == "data"
    # Original must be unchanged
    assert v1_event.event_version == 1
    assert "field_v2" not in v1_event.payload
    print("\n✓ Registry chain test: v1 → v2 → v3 applied correctly")


@pytest.mark.asyncio
async def test_upcaster_confidence_score_is_null():
    """CreditAnalysisCompleted v1→v2 must set confidence_score=None, not fabricate."""
    registry = build_upcaster_registry()

    from src.models.events import StoredEvent
    from uuid import uuid4
    from datetime import datetime, timezone

    v1_event = StoredEvent(
        event_id=uuid4(),
        stream_id="loan-test",
        stream_position=1,
        global_position=1,
        event_type="CreditAnalysisCompleted",
        event_version=1,
        payload={
            "application_id": "TEST-001",
            "risk_tier": "LOW",
            "_recorded_at": "2024-06-15T10:00:00Z",
        },
        metadata={},
        recorded_at=datetime(2024, 6, 15, 10, 0, 0, tzinfo=timezone.utc),
    )

    result = registry.upcast(v1_event)
    assert result.event_version == 2
    assert result.payload["confidence_score"] is None, \
        "Must be None — fabrication would corrupt analytics"
    assert result.payload["model_version"] is not None
    assert "_inferred" in result.payload["model_version"]
    print(f"\n✓ confidence_score=None confirmed (no fabrication)")
    print(f"  model_version={result.payload['model_version']}")


@pytest.mark.asyncio
async def test_decision_generated_v1_to_v2():
    """DecisionGenerated v1→v2 adds model_versions dict."""
    registry = build_upcaster_registry()

    from src.models.events import StoredEvent
    from uuid import uuid4
    from datetime import datetime, timezone

    v1_event = StoredEvent(
        event_id=uuid4(),
        stream_id="loan-test",
        stream_position=5,
        global_position=5,
        event_type="DecisionGenerated",
        event_version=1,
        payload={
            "application_id": "TEST-001",
            "orchestrator_agent_id": "agent-orch-001",
            "recommendation": "APPROVE",
            "confidence_score": 0.88,
            "contributing_agent_sessions": ["agent-credit-001-sess-001"],
            "decision_basis_summary": "Strong application",
        },
        metadata={},
        recorded_at=datetime.now(timezone.utc),
    )

    result = registry.upcast(v1_event)
    assert result.event_version == 2
    assert "model_versions" in result.payload
    assert isinstance(result.payload["model_versions"], dict)
    assert len(result.payload["model_versions"]) == 1
    print(f"\n✓ DecisionGenerated v1→v2: model_versions={result.payload['model_versions']}")
