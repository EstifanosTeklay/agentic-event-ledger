"""
scripts/demo_concurrency.py
============================
Video demonstration of optimistic concurrency control.

Shows in real time:
  - Two agents racing to append to the same stream
  - One winning, one receiving OptimisticConcurrencyError
  - The loser reloading the stream and retrying successfully

Run with:
    uv run python scripts/demo_concurrency.py
"""

import asyncio
import os
import sys
from decimal import Decimal

import asyncpg
from dotenv import load_dotenv

load_dotenv()
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

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger")

# ── Terminal colours ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def green(t):  return f"{GREEN}{t}{RESET}"
def red(t):    return f"{RED}{t}{RESET}"
def yellow(t): return f"{YELLOW}{t}{RESET}"
def cyan(t):   return f"{CYAN}{t}{RESET}"
def bold(t):   return f"{BOLD}{t}{RESET}"

# ── Divider ───────────────────────────────────────────────────────────────────
DIV = "─" * 60


async def setup_stream(store: EventStore, application_id: str) -> str:
    """Create a loan stream at version 3 — the starting point for the race."""
    stream_id = f"loan-{application_id}"

    print(f"\n{bold('SETUP')}  Building stream '{stream_id}' to version 3...")

    await store.append(
        stream_id=stream_id,
        events=[ApplicationSubmitted(
            application_id=application_id,
            applicant_id="applicant-apex-001",
            requested_amount_usd=Decimal("750000.00"),
            loan_purpose="Commercial property acquisition",
            submission_channel="broker",
        )],
        expected_version=-1,
    )
    print(f"  {green('✓')} Event 1 appended → ApplicationSubmitted")

    await store.append(
        stream_id=stream_id,
        events=[CreditAnalysisRequested(
            application_id=application_id,
            assigned_agent_id="agent-credit-alpha",
            priority="HIGH",
        )],
        expected_version=1,
    )
    print(f"  {green('✓')} Event 2 appended → CreditAnalysisRequested")

    await store.append(
        stream_id=stream_id,
        events=[FraudScreeningCompleted(
            application_id=application_id,
            agent_id="agent-fraud-001",
            fraud_score=0.07,
            anomaly_flags=[],
            screening_model_version="fraud-v3.1",
            input_data_hash="sha256-demo-input",
        )],
        expected_version=2,
    )
    print(f"  {green('✓')} Event 3 appended → FraudScreeningCompleted")
    print(f"  {bold('Stream is now at version 3.')}")
    return stream_id


async def agent_task(
    name: str,
    store: EventStore,
    stream_id: str,
    application_id: str,
    barrier: asyncio.Barrier,
    results: dict,
) -> None:
    """
    Simulates one AI agent:
      1. Reads stream at version 3
      2. Waits at barrier until both agents are ready (simulates true concurrency)
      3. Attempts to append CreditAnalysisCompleted at expected_version=3
      4. If it loses → reloads stream → retries
    """
    colour = green if name == "ALPHA" else yellow

    print(f"\n  {colour(f'[{name}]')} Reading stream... sees version 3")
    await asyncio.sleep(0.05)  # simulate analysis work

    # ── Both agents arrive here before either appends ─────────────────────────
    print(f"  {colour(f'[{name}]')} Analysis complete. Waiting to append...")
    await barrier.wait()   # release both at the same instant

    event = CreditAnalysisCompleted(
        application_id=application_id,
        agent_id=f"agent-credit-{name.lower()}",
        session_id=f"session-{name.lower()}-001",
        model_version="credit-v2.1",
        confidence_score=0.87 if name == "ALPHA" else 0.82,
        risk_tier=RiskTier.MEDIUM,
        recommended_limit_usd=Decimal("700000.00"),
        analysis_duration_ms=1250,
        input_data_hash=f"sha256-{name.lower()}-input",
    )

    try:
        new_version = await store.append(
            stream_id=stream_id,
            events=[event],
            expected_version=3,   # both read version 3 — only one can win
        )
        results[name] = {"status": "WIN", "version": new_version}
        print(f"  {colour(f'[{name}]')} {green('✓ APPENDED')} → stream now at version {new_version}")

    except OptimisticConcurrencyError as e:
        results[name] = {"status": "LOST", "error": e}
        print(f"  {colour(f'[{name}]')} {red('✗ OptimisticConcurrencyError')} — "
              f"expected v{e.expected_version}, actual v{e.actual_version}")
        print(f"  {colour(f'[{name}]')} {yellow('→ Reloading stream and retrying...')}")

        # Reload stream to get current version
        current_version = await store.stream_version(stream_id)
        print(f"  {colour(f'[{name}]')} Reloaded: stream is now at version {current_version}")

        # Retry at the correct version
        retry_version = await store.append(
            stream_id=stream_id,
            events=[event],
            expected_version=current_version,
        )
        results[name]["retry_version"] = retry_version
        results[name]["status"] = "RETRY_SUCCESS"
        print(f"  {colour(f'[{name}]')} {green('✓ RETRY SUCCEEDED')} → stream now at version {retry_version}")


async def main() -> None:
    print(f"\n{DIV}")
    print(f"{bold('  THE LEDGER — Optimistic Concurrency Control Demo')}")
    print(f"  Apex Financial Services · Multi-Agent Loan Processing")
    print(DIV)

    conn = await asyncpg.connect(dsn=DATABASE_URL)
    await conn.execute(
        "TRUNCATE TABLE events, event_streams, outbox RESTART IDENTITY CASCADE"
    )
    await conn.close()

    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
    store = EventStore(pool=pool)

    application_id = "APEX-DEMO-001"
    stream_id = await setup_stream(store, application_id)

    print(f"\n{DIV}")
    print(f"{bold('THE RACE')}  Two agents simultaneously process application {application_id}")
    print(f"  Both read version=3. Both will try to append at expected_version=3.")
    print(f"  Only ONE can succeed. The other MUST reload and retry.")
    print(DIV)

    results = {}
    barrier = asyncio.Barrier(2)   # holds both tasks until both are ready

    # Launch both agents at the same time
    await asyncio.gather(
        agent_task("ALPHA", store, stream_id, application_id, barrier, results),
        agent_task("BETA",  store, stream_id, application_id, barrier, results),
    )

    # ── Final state verification ──────────────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('RESULT')}")
    print(DIV)

    final_events = await store.load_stream(stream_id)
    final_version = await store.stream_version(stream_id)

    winner = next(k for k, v in results.items() if v["status"] == "WIN")
    loser  = next(k for k, v in results.items() if v["status"] != "WIN")

    print(f"  {green('Winner')}:  Agent {winner} → appended at version {results[winner]['version']}")
    print(f"  {yellow('Loser')}:   Agent {loser}  → got OptimisticConcurrencyError → retried at version {results[loser].get('retry_version', '?')}")
    print(f"\n  {bold('Final stream version:')} {final_version}")
    print(f"  {bold('Total events in stream:')} {len(final_events)}")
    print()

    for e in final_events:
        marker = green("✓") if e.event_type == "CreditAnalysisCompleted" else "·"
        print(f"    {marker}  pos={e.stream_position}  {cyan(e.event_type)}")

    print()

    # Integrity assertion
    assert len(final_events) == 5, f"Expected 5 events, got {len(final_events)}"
    assert final_version == 5
    credit_events = [e for e in final_events if e.event_type == "CreditAnalysisCompleted"]
    assert len(credit_events) == 2   # winner + retry both landed

    print(f"  {green(bold('✓ ASSERTION PASSED:'))} Stream has exactly 5 events.")
    print(f"  {green(bold('✓ ASSERTION PASSED:'))} Concurrency control prevented split-brain state.")
    print(f"  {green(bold('✓ ASSERTION PASSED:'))} Loser successfully recovered via reload-and-retry.")
    print(f"\n{DIV}\n")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
