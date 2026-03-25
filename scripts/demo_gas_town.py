"""
scripts/demo_gas_town.py
=========================
Video Demo — Step 5: Gas Town Agent Crash Recovery

Shows in real time:
  - Agent session started, 4 events appended to event store
  - Process crash simulated (in-memory object deleted)
  - reconstruct_agent_context() called with NO in-memory state
  - Agent resumes with correct context from event store alone

Run with:
    uv run python scripts/demo_gas_town.py
"""

import asyncio
import os
import sys
from decimal import Decimal

import asyncpg
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.startup import initialise
initialise()

from src.commands.handlers import (
    RecordCreditAnalysisCommand,
    RecordFraudScreeningCommand,
    RequestCreditAnalysisCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_request_credit_analysis,
    handle_start_agent_session,
    handle_submit_application,
)
from src.event_store import EventStore
from src.integrity.gas_town import reconstruct_agent_context
from src.models.events import RiskTier, SessionHealthStatus

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger")

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

DIV  = "─" * 64
DIV2 = "═" * 64


async def main() -> None:
    print(f"\n{DIV2}")
    print(f"{bold('  THE LEDGER — Gas Town Agent Crash Recovery Demo')}")
    print(f"  Step 5 of Video Demo · Apex Financial Services")
    print(f"{DIV2}")

    conn = await asyncpg.connect(dsn=DATABASE_URL)
    await conn.execute(
        "TRUNCATE TABLE events, event_streams, outbox RESTART IDENTITY CASCADE"
    )
    await conn.close()

    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=5)
    store = EventStore(pool=pool)

    agent_id   = "agent-credit-apex-001"
    session_id = "sess-apex-prod-001"
    app_id_1   = "APEX-LOAN-001"
    app_id_2   = "APEX-LOAN-002"

    # ── Phase A: Normal agent operation ──────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('PHASE A')}  Normal agent operation — events written to store")
    print(DIV)

    print(f"\n  {cyan('[EVENT 1]')} AgentContextLoaded — agent declares context before any decision")
    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id=agent_id,
            session_id=session_id,
            model_version="credit-v2.1",
            context_source="fresh",
            context_token_count=4096,
        ), store)
    print(f"            model=credit-v2.1 | source=fresh | tokens=4096")
    print(f"            {green('→ Gas Town pattern: context declared BEFORE any decision')}")

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id_1,
            applicant_id="apex-borrower-001",
            requested_amount_usd=Decimal("500000"),
            loan_purpose="Commercial real estate",
        ), store)

    await handle_request_credit_analysis(
        RequestCreditAnalysisCommand(
            application_id=app_id_1,
            assigned_agent_id=agent_id,
        ), store)

    print(f"\n  {cyan('[EVENT 2]')} CreditAnalysisCompleted for {app_id_1}")
    await handle_credit_analysis_completed(
        RecordCreditAnalysisCommand(
            application_id=app_id_1,
            agent_id=agent_id,
            session_id=session_id,
            model_version="credit-v2.1",
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("475000"),
            analysis_duration_ms=1120,
            input_data={"ltv": 0.71, "dscr": 1.35},
            confidence_score=0.88,
        ), store)
    print(f"            risk=MEDIUM | confidence=0.88 | limit=475,000")

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id_2,
            applicant_id="apex-borrower-002",
            requested_amount_usd=Decimal("750000"),
            loan_purpose="Equipment financing",
        ), store)

    await handle_request_credit_analysis(
        RequestCreditAnalysisCommand(
            application_id=app_id_2,
            assigned_agent_id=agent_id,
        ), store)

    print(f"\n  {cyan('[EVENT 3]')} CreditAnalysisCompleted for {app_id_2}")
    await handle_credit_analysis_completed(
        RecordCreditAnalysisCommand(
            application_id=app_id_2,
            agent_id=agent_id,
            session_id=session_id,
            model_version="credit-v2.1",
            risk_tier=RiskTier.LOW,
            recommended_limit_usd=Decimal("720000"),
            analysis_duration_ms=980,
            input_data={"ltv": 0.62, "dscr": 1.82},
            confidence_score=0.94,
        ), store)
    print(f"            risk=LOW | confidence=0.94 | limit=720,000")

    print(f"\n  {cyan('[EVENT 4]')} FraudScreeningCompleted for {app_id_1}")
    await handle_fraud_screening_completed(
        RecordFraudScreeningCommand(
            application_id=app_id_1,
            agent_id=agent_id,
            session_id=session_id,
            fraud_score=0.06,
            anomaly_flags=[],
            screening_model_version="fraud-v3.1",
            input_data={"transactions": 52},
        ), store)
    print(f"            fraud_score=0.06 | flags=none | {green('clean')}")

    # Confirm events are in the store
    agent_stream = f"agent-{agent_id}-{session_id}"
    events_before = await store.load_stream(agent_stream)
    print(f"\n  {green('✓')} Agent stream '{agent_stream}'")
    print(f"    Contains {len(events_before)} events — all persisted to PostgreSQL")

    # ── Phase B: CRASH ────────────────────────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('PHASE B')}  {red('PROCESS CRASH SIMULATED')}")
    print(DIV)

    # Simulate crash by deleting the in-memory reference
    agent_in_memory = {"model": "credit-v2.1", "apps": [app_id_1, app_id_2]}
    print(f"\n  In-memory agent state  : {agent_in_memory}")
    del agent_in_memory
    print(f"  {red('del agent_in_memory')}  ← process crash / kill -9")
    print(f"\n  In-memory agent state  : {red('GONE — all context lost')}")
    print(f"  PostgreSQL event store : {green('INTACT — all 4 events persisted')}")
    print(f"\n  {yellow('Without Gas Town pattern:')} agent would restart blind,")
    print(f"  re-analyse already-processed applications, duplicate decisions.")
    print(f"  {green('With Gas Town pattern:')} agent replays event stream, resumes correctly.")

    await asyncio.sleep(0.5)  # dramatic pause

    # ── Phase C: Reconstruction ───────────────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('PHASE C')}  Reconstructing agent context from event store")
    print(f"         No in-memory state used — pure event replay")
    print(DIV)

    print(f"\n  Calling reconstruct_agent_context(")
    print(f"      agent_id='{agent_id}',")
    print(f"      session_id='{session_id}',")
    print(f"      token_budget=8000")
    print(f"  )")

    context = await reconstruct_agent_context(
        store=store,
        agent_id=agent_id,
        session_id=session_id,
        token_budget=8000,
    )

    print(f"\n  {DIV}")
    print(f"  {bold('RECONSTRUCTED CONTEXT')}")
    print(f"  {DIV}")
    print(f"  model_version          : {green(context.model_version)}")
    print(f"  context_source         : {context.context_source}")
    print(f"  total_events_replayed  : {green(str(context.total_events_replayed))}")
    print(f"  health_status          : {green(context.session_health_status.value)}")
    print(f"  applications_processed : {green(str(context.applications_processed))}")
    print(f"  completed_work         : {len(context.completed_work)} items")
    print(f"  pending_work           : {len(context.pending_work)} items")
    print(f"  token_estimate         : ~{context.context_token_estimate} (budget: 8000)")
    print(f"  needs_reconciliation   : {context.needs_reconciliation}")

    print(f"\n  {bold('Context text (what the agent sees on restart):')}")
    print(f"  {'─'*50}")
    for line in context.context_text.split("\n"):
        print(f"  {line}")
    print(f"  {'─'*50}")

    # ── Assertions ────────────────────────────────────────────────────────────
    assert context.model_version == "credit-v2.1"
    assert context.total_events_replayed >= 3
    assert app_id_1 in context.applications_processed
    assert app_id_2 in context.applications_processed
    assert context.session_health_status == SessionHealthStatus.HEALTHY
    assert not context.needs_reconciliation

    print(f"\n{DIV2}")
    print(f"{bold('  GAS TOWN CRASH RECOVERY DEMO COMPLETE — Step 5 verified')}")
    print(f"  Agent reconstructed from {context.total_events_replayed} events")
    print(f"  Knows about: {context.applications_processed}")
    print(f"  Ready to continue without re-processing completed work")
    print(f"{DIV2}\n")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
