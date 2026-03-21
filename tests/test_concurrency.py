"""
tests/test_concurrency.py
=========================
Phase 1 — The Double-Decision Concurrency Test.

Scenario: Two AI agents simultaneously attempt to append an event to the same
loan application stream at expected_version=3. Exactly one must succeed.
The other must receive OptimisticConcurrencyError. Total stream length = 4.

This test is mandatory and is run during assessment.
It proves the correctness of the optimistic concurrency implementation.
"""

from __future__ import annotations

import asyncio
import os
import sys
from decimal import Decimal

import asyncpg
import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmitted,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    FraudScreeningCompleted,
    OptimisticConcurrencyError,
    RiskTier,
)

# =============================================================================
# TEST DATABASE SETUP
# =============================================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger"
)


@pytest_asyncio.fixture
async def pool():
    """
    Per-test connection pool.
    Scope is function (default) so the pool is created inside the same
    asyncio event loop that the test runs in. A session-scoped pool would
    be created in the session loop and then reused in per-test loops,
    causing 'Future attached to a different loop' errors on Python 3.12+.
    """
    p = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
    yield p
    await p.close()


@pytest_asyncio.fixture(autouse=True)
async def clean_db(pool):
    """
    Truncate test data before each test.
    Uses CASCADE to handle FK constraints (outbox → events).
    """
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
# HELPER: Build a stream at version 3
# =============================================================================

async def build_stream_at_version_3(store: EventStore, application_id: str) -> str:
    """
    Create a loan application stream with 3 events, returning stream_id.
    This brings the stream to version 3, which is the starting point for
    the double-decision concurrency test.
    """
    stream_id = f"loan-{application_id}"

    # Event 1: ApplicationSubmitted (version -1 = new stream)
    await store.append(
        stream_id=stream_id,
        events=[
            ApplicationSubmitted(
                application_id=application_id,
                applicant_id="applicant-001",
                requested_amount_usd=Decimal("500000.00"),
                loan_purpose="Commercial real estate acquisition",
                submission_channel="online",
            )
        ],
        expected_version=-1,
    )

    # Event 2: CreditAnalysisRequested
    await store.append(
        stream_id=stream_id,
        events=[
            CreditAnalysisRequested(
                application_id=application_id,
                assigned_agent_id="agent-credit-001",
                priority="HIGH",
            )
        ],
        expected_version=1,
    )

    # Event 3: FraudScreeningCompleted (brings stream to version 3)
    await store.append(
        stream_id=stream_id,
        events=[
            FraudScreeningCompleted(
                application_id=application_id,
                agent_id="agent-fraud-001",
                fraud_score=0.12,
                anomaly_flags=[],
                screening_model_version="fraud-v3.1",
                input_data_hash="sha256-abc123",
            )
        ],
        expected_version=2,
    )

    # Verify we are at version 3
    version = await store.stream_version(stream_id)
    assert version == 3, f"Expected version 3, got {version}"
    return stream_id


# =============================================================================
# THE DOUBLE-DECISION TEST
# =============================================================================

@pytest.mark.asyncio
async def test_double_decision_exactly_one_wins(store: EventStore, pool):
    """
    Two agents simultaneously append CreditAnalysisCompleted at expected_version=3.

    Assertions:
    (a) Total events in stream = 4 (not 5)
    (b) The winning event has stream_position = 4
    (c) The losing task raises OptimisticConcurrencyError (not silently swallowed)
    """
    application_id = "TEST-CONCURRENCY-001"
    stream_id = await build_stream_at_version_3(store, application_id)

    # Track results from both tasks
    results: dict[str, Exception | int] = {}

    async def agent_task(agent_name: str) -> None:
        """Each agent reads stream at version 3 and tries to append."""
        event = CreditAnalysisCompleted(
            application_id=application_id,
            agent_id=f"agent-{agent_name}",
            session_id=f"session-{agent_name}-001",
            model_version="credit-v2.1",
            confidence_score=0.87 if agent_name == "alpha" else 0.82,
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("450000.00"),
            analysis_duration_ms=1250,
            input_data_hash=f"sha256-{agent_name}-input",
        )
        try:
            new_version = await store.append(
                stream_id=stream_id,
                events=[event],
                expected_version=3,  # Both agents read version 3
                correlation_id="corr-concurrency-test-001",
                causation_id="cmd-credit-analysis-001",
            )
            results[agent_name] = new_version
        except OptimisticConcurrencyError as e:
            results[agent_name] = e

    # Launch both agents simultaneously
    await asyncio.gather(
        agent_task("alpha"),
        agent_task("beta"),
        return_exceptions=True,  # Don't let one exception kill the gather
    )

    # ── Assertion (c): One must have raised OptimisticConcurrencyError ────────
    errors = {k: v for k, v in results.items() if isinstance(v, OptimisticConcurrencyError)}
    successes = {k: v for k, v in results.items() if isinstance(v, int)}

    assert len(errors) == 1, (
        f"Expected exactly 1 OptimisticConcurrencyError, got {len(errors)}. "
        f"Results: {results}"
    )
    assert len(successes) == 1, (
        f"Expected exactly 1 success, got {len(successes)}. "
        f"Results: {results}"
    )

    # ── Assertion (b): Winning event has stream_position = 4 ─────────────────
    winning_version = list(successes.values())[0]
    assert winning_version == 4, (
        f"Expected winning version = 4, got {winning_version}"
    )

    # ── Assertion (a): Total stream length = 4 (not 5) ───────────────────────
    events = await store.load_stream(stream_id)
    assert len(events) == 4, (
        f"Expected 4 total events in stream, got {len(events)}. "
        f"Two agents must not both succeed."
    )

    # ── Verify the error is properly structured (LLM-consumable) ─────────────
    error = list(errors.values())[0]
    error_dict = error.to_dict()
    assert error_dict["error_type"] == "OptimisticConcurrencyError"
    assert error_dict["stream_id"] == stream_id
    assert error_dict["expected_version"] == 3
    assert error_dict["actual_version"] == 4
    assert error_dict["suggested_action"] == "reload_stream_and_retry"

    print(f"\n{'='*60}")
    print(f"  DOUBLE-DECISION CONCURRENCY TEST — ALL ASSERTIONS PASSED")
    print(f"{'='*60}")
    print(f"  Winner : {list(successes.keys())[0]} → appended at stream_position=4 ✓")
    print(f"  Loser  : {list(errors.keys())[0]} → OptimisticConcurrencyError raised ✓")
    print(f"  ASSERTION (a): stream length = {len(events)} (expected 4, NOT 5) ✓")
    print(f"  ASSERTION (b): winning stream_position = {winning_version} (expected 4) ✓")
    print(f"  ASSERTION (c): OptimisticConcurrencyError explicitly raised, not swallowed ✓")
    print(f"  Error fields : stream={error_dict['stream_id']}")
    print(f"                 expected_version={error_dict['expected_version']}")
    print(f"                 actual_version={error_dict['actual_version']}")
    print(f"{'='*60}")


@pytest.mark.asyncio
async def test_losing_agent_can_retry_successfully(store: EventStore, pool):
    """
    After receiving OptimisticConcurrencyError, the losing agent reloads
    the stream and retries. The retry must succeed.
    """
    application_id = "TEST-CONCURRENCY-RETRY-001"
    stream_id = await build_stream_at_version_3(store, application_id)

    # Simulate agent-alpha winning at version 3
    await store.append(
        stream_id=stream_id,
        events=[
            CreditAnalysisCompleted(
                application_id=application_id,
                agent_id="agent-alpha",
                session_id="session-alpha-001",
                model_version="credit-v2.1",
                confidence_score=0.87,
                risk_tier=RiskTier.MEDIUM,
                recommended_limit_usd=Decimal("450000.00"),
                analysis_duration_ms=1250,
                input_data_hash="sha256-alpha-input",
            )
        ],
        expected_version=3,
    )

    # Agent-beta now tries at stale version 3 → gets OptimisticConcurrencyError
    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await store.append(
            stream_id=stream_id,
            events=[
                CreditAnalysisCompleted(
                    application_id=application_id,
                    agent_id="agent-beta",
                    session_id="session-beta-001",
                    model_version="credit-v2.1",
                    confidence_score=0.82,
                    risk_tier=RiskTier.MEDIUM,
                    recommended_limit_usd=Decimal("430000.00"),
                    analysis_duration_ms=1400,
                    input_data_hash="sha256-beta-input",
                )
            ],
            expected_version=3,  # stale
        )

    assert exc_info.value.actual_version == 4

    # Agent-beta reloads stream and retries at correct version
    current_version = await store.stream_version(stream_id)
    assert current_version == 4

    # In a real system the agent would re-evaluate business rules here.
    # For this test, we simulate a second agent appending a different event type.
    from src.models.events import FraudScreeningCompleted as FSC
    # Append a different event at the correct version
    new_version = await store.append(
        stream_id=stream_id,
        events=[
            FSC(
                application_id=application_id,
                agent_id="agent-beta",
                fraud_score=0.08,
                anomaly_flags=[],
                screening_model_version="fraud-v3.1",
                input_data_hash="sha256-beta-fraud-input",
            )
        ],
        expected_version=current_version,
    )

    assert new_version == 5
    events = await store.load_stream(stream_id)
    assert len(events) == 5

    print("\n✓ Retry-after-concurrency-error test passed:")
    print(f"  Stream now at version {new_version} with {len(events)} events")


@pytest.mark.asyncio
async def test_new_stream_creation(store: EventStore):
    """Creating a new stream with expected_version=-1 works correctly."""
    await store.append(
        stream_id="loan-NEW-STREAM-001",
        events=[
            ApplicationSubmitted(
                application_id="NEW-STREAM-001",
                applicant_id="applicant-new",
                requested_amount_usd=Decimal("100000.00"),
                loan_purpose="Equipment financing",
                submission_channel="broker",
            )
        ],
        expected_version=-1,
    )
    version = await store.stream_version("loan-NEW-STREAM-001")
    assert version == 1

    events = await store.load_stream("loan-NEW-STREAM-001")
    assert len(events) == 1
    assert events[0].event_type == "ApplicationSubmitted"
    assert events[0].stream_position == 1
    print("\n✓ New stream creation test passed")


@pytest.mark.asyncio
async def test_wrong_expected_version_raises(store: EventStore):
    """Appending with wrong expected_version raises OptimisticConcurrencyError."""
    await store.append(
        stream_id="loan-VERSION-TEST-001",
        events=[
            ApplicationSubmitted(
                application_id="VERSION-TEST-001",
                applicant_id="applicant-vt",
                requested_amount_usd=Decimal("200000.00"),
                loan_purpose="Working capital",
                submission_channel="direct",
            )
        ],
        expected_version=-1,
    )

    # Stream is now at version 1. Try to append at version 5 (wrong).
    with pytest.raises(OptimisticConcurrencyError) as exc_info:
        await store.append(
            stream_id="loan-VERSION-TEST-001",
            events=[
                CreditAnalysisRequested(
                    application_id="VERSION-TEST-001",
                    assigned_agent_id="agent-credit-001",
                )
            ],
            expected_version=5,  # Wrong — stream is at 1
        )

    err = exc_info.value
    assert err.expected_version == 5
    assert err.actual_version == 1
    assert err.stream_id == "loan-VERSION-TEST-001"
    print("\n✓ Wrong expected_version raises OptimisticConcurrencyError correctly")


@pytest.mark.asyncio
async def test_outbox_written_in_same_transaction(store: EventStore, pool):
    """Outbox entries are written atomically with events."""
    await store.append(
        stream_id="loan-OUTBOX-TEST-001",
        events=[
            ApplicationSubmitted(
                application_id="OUTBOX-TEST-001",
                applicant_id="applicant-ob",
                requested_amount_usd=Decimal("75000.00"),
                loan_purpose="Invoice financing",
                submission_channel="api",
            )
        ],
        expected_version=-1,
    )

    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"
        )

    assert count >= 1, "Outbox entry must be written in the same transaction as the event"
    print(f"\n✓ Outbox test passed: {count} pending outbox entry(ies) created")
