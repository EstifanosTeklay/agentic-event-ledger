"""
scripts/demo_week_standard.py
==============================
Video Demo — Step 1: The Week Standard

"Show me the complete decision history of application ID X"
  - Full event stream
  - All agent actions
  - Compliance checks
  - Human review
  - Causal links
  - Cryptographic integrity verification
  - Must complete in under 60 seconds

Run with:
    uv run python scripts/demo_week_standard.py
"""

import asyncio
import json
import os
import sys
import time
from decimal import Decimal
from datetime import datetime, timezone

import asyncpg
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.startup import initialise
initialise()

from src.mcp.tools import (
    generate_decision,
    issue_compliance_clearance,
    record_compliance_check,
    record_credit_analysis,
    record_fraud_screening,
    record_human_review,
    request_compliance_check,
    request_credit_analysis,
    run_integrity_check_tool,
    start_agent_session,
    submit_application,
)
from src.mcp.resources import (
    get_audit_trail,
    get_application,
    get_application_compliance,
    get_ledger_health,
)
from src.mcp import dependencies as _deps
from src.event_store import EventStore
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.agent_performance import AgentPerformanceLedgerProjection
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.daemon import ProjectionDaemon

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:123@localhost:5432/ledger")

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BLUE   = "\033[94m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

def green(t):  return f"{GREEN}{t}{RESET}"
def red(t):    return f"{RED}{t}{RESET}"
def yellow(t): return f"{YELLOW}{t}{RESET}"
def cyan(t):   return f"{CYAN}{t}{RESET}"
def blue(t):   return f"{BLUE}{t}{RESET}"
def bold(t):   return f"{BOLD}{t}{RESET}"
def dim(t):    return f"{DIM}{t}{RESET}"

DIV  = "─" * 68
DIV2 = "═" * 68

EVENT_COLOURS = {
    "ApplicationSubmitted":      CYAN,
    "CreditAnalysisRequested":   BLUE,
    "CreditAnalysisCompleted":   GREEN,
    "FraudScreeningCompleted":   GREEN,
    "ComplianceCheckRequested":  YELLOW,
    "ComplianceRulePassed":      GREEN,
    "ComplianceRuleFailed":      RED,
    "ComplianceClearanceIssued": GREEN,
    "DecisionGenerated":         CYAN,
    "HumanReviewCompleted":      YELLOW,
    "ApplicationApproved":       GREEN,
    "ApplicationDeclined":       RED,
    "AgentContextLoaded":        BLUE,
}


def colour_event(event_type: str) -> str:
    c = EVENT_COLOURS.get(event_type, "")
    return f"{c}{event_type}{RESET}"


async def build_application(pool, app_id: str) -> None:
    """Drive a complete application lifecycle via MCP tools."""
    store = EventStore(pool=pool)
    _deps._store = store

    agent_id   = "agent-credit-apex-001"
    session_id = "sess-apex-001"
    orch_id    = "agent-orch-apex-001"
    orch_sess  = "sess-orch-apex-001"

    await start_agent_session(
        agent_id=agent_id, session_id=session_id,
        model_version="credit-v2.1", context_source="fresh",
        context_token_count=4096,
    )
    await submit_application(
        application_id=app_id,
        applicant_id="apex-corp-borrower",
        requested_amount_usd=1_200_000.0,
        loan_purpose="Commercial real estate — distribution centre",
        submission_channel="broker",
    )
    await request_credit_analysis(
        application_id=app_id,
        assigned_agent_id=agent_id,
        priority="HIGH",
    )
    await record_credit_analysis(
        application_id=app_id,
        agent_id=agent_id, session_id=session_id,
        model_version="credit-v2.1",
        risk_tier="MEDIUM",
        recommended_limit_usd=1_140_000.0,
        analysis_duration_ms=1340,
        confidence_score=0.88,
        regulatory_basis="REG-2026-Q1",
    )
    await record_fraud_screening(
        application_id=app_id,
        agent_id=agent_id, session_id=session_id,
        fraud_score=0.07,
        screening_model_version="fraud-v3.1",
        anomaly_flags=[],
    )
    await request_compliance_check(
        application_id=app_id,
        regulation_set_version="REG-2026-Q1",
        checks_required=["AML-001", "KYC-001", "SANCTIONS-001"],
    )
    for rule, version in [("AML-001", "v2.1"), ("KYC-001", "v1.8"), ("SANCTIONS-001", "v3.0")]:
        await record_compliance_check(
            application_id=app_id,
            rule_id=rule, rule_version=version, passed=True,
        )
    await issue_compliance_clearance(
        application_id=app_id,
        issuing_agent_id="agent-compliance-001",
        regulation_set_version="REG-2026-Q1",
    )
    await start_agent_session(
        agent_id=orch_id, session_id=orch_sess,
        model_version="orch-v1.0", context_source="fresh",
        context_token_count=2048,
    )
    credit_stream = f"agent-{agent_id}-{session_id}"
    await generate_decision(
        application_id=app_id,
        orchestrator_agent_id=orch_id,
        orchestrator_session_id=orch_sess,
        recommendation="APPROVE",
        confidence_score=0.91,
        contributing_agent_sessions=[credit_stream],
        decision_basis_summary="Strong financials, low fraud, full compliance",
        model_versions={"orchestrator": "orch-v1.0", "credit": "credit-v2.1"},
    )
    await record_human_review(
        application_id=app_id,
        reviewer_id="officer-apex-001",
        final_decision="APPROVE",
        override=False,
    )

    # Run projection daemon
    daemon = ProjectionDaemon(
        store=store,
        projections=[
            ApplicationSummaryProjection(),
            AgentPerformanceLedgerProjection(),
            ComplianceAuditViewProjection(pool=pool),
        ],
        pool=pool,
    )
    await daemon._initialise_checkpoints(pool)
    await daemon._process_batch(pool)
    await daemon._process_batch(pool)


async def main() -> None:
    print(f"\n{DIV2}")
    print(f"{bold('  THE LEDGER — The Week Standard Demo')}")
    print(f"  Step 1 of Video Demo · Apex Financial Services")
    print(f"  \"Show me the complete decision history of application ID X\"")
    print(f"{DIV2}")

    conn = await asyncpg.connect(dsn=DATABASE_URL)
    await conn.execute(
        "TRUNCATE TABLE events, event_streams, outbox, "
        "projection_checkpoints, application_summary, "
        "agent_performance_ledger, compliance_audit_view, "
        "compliance_audit_snapshots RESTART IDENTITY CASCADE"
    )
    await conn.close()

    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=4, max_size=20)
    app_id = "APEX-COMMERCIAL-001"

    # Build the application
    print(f"\n{dim('Building application lifecycle via MCP tools...')}")
    await build_application(pool, app_id)
    print(f"{green('✓')} Application {app_id} driven to FINAL_APPROVED via MCP tools")

    # ── START TIMER ───────────────────────────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('STARTING 60-SECOND CLOCK')} ← timer starts now")
    print(DIV)
    timer_start = time.monotonic()

    # ── QUERY 1: Complete audit trail ────────────────────────────────────────
    print(f"\n{bold('QUERY 1')}  {cyan('ledger://applications/' + app_id + '/audit-trail')}")

    audit_json = await get_audit_trail(application_id=app_id)
    audit = json.loads(audit_json)

    events = audit["data"]["events"]
    narrative = audit["data"]["narrative"]

    print(f"\n  {bold('EVENT STREAM')} ({len(events)} events, chronological order)")
    print(f"  {'POS':<5} {'STREAM':<28} {'EVENT TYPE':<30} {'RECORDED AT'}")
    print(f"  {'─'*5} {'─'*28} {'─'*30} {'─'*24}")

    for e in events:
        stream_short = e["stream_id"][-25:] if len(e["stream_id"]) > 25 else e["stream_id"]
        rec_at = e["recorded_at"][:23] if e.get("recorded_at") else ""
        print(
            f"  {e['stream_position']:<5} "
            f"{stream_short:<28} "
            f"{colour_event(e['event_type']):<38} "
            f"{dim(rec_at)}"
        )

    print(f"\n  {bold('NARRATIVE')} (human-readable lifecycle summary)")
    for i, line in enumerate(narrative, 1):
        print(f"  {cyan(str(i)+'.')} {line}")

    # ── QUERY 2: Compliance audit ─────────────────────────────────────────────
    print(f"\n{bold('QUERY 2')}  {cyan('ledger://applications/' + app_id + '/compliance')}")

    comp_json = await get_application_compliance(application_id=app_id)
    comp = json.loads(comp_json)
    checks = [c for c in comp["data"]["compliance_checks"] if c.get("rule_id") != "__CLEARANCE__"]

    print(f"\n  {'RULE':<16} {'STATUS':<10} {'VERSION':<10} {'EVALUATED AT'}")
    print(f"  {'─'*16} {'─'*10} {'─'*10} {'─'*24}")
    seen = {}
    for c in checks:
        rule = c.get("rule_id", "?")
        status = c.get("status", "?")
        if rule in seen and seen[rule] == "PASSED":
            continue
        seen[rule] = status
        status_fmt = green(f"{'PASSED':<10}") if status == "PASSED" else \
                     red(f"{'FAILED':<10}") if status == "FAILED" else \
                     yellow(f"{'PENDING':<10}")
        eval_at = str(c.get("evaluation_timestamp") or c.get("recorded_at") or "")[:23]
        print(f"  {rule:<16} {status_fmt} {c.get('rule_version','?'):<10} {dim(eval_at)}")

    print(f"\n  {green('✓')} {comp['data']['passed']} rules passed  "
          f"{red('✗') if comp['data']['failed'] else dim('○')} {comp['data']['failed']} failed  "
          f"Regulation: REG-2026-Q1")

    # ── QUERY 3: Current application state ───────────────────────────────────
    print(f"\n{bold('QUERY 3')}  {cyan('ledger://applications/' + app_id)}")

    app_json = await get_application(application_id=app_id)
    app_data = json.loads(app_json)

    if "data" in app_data:
        d = app_data["data"]
        print(f"\n  application_id  : {d.get('application_id')}")
        print(f"  state           : {green(d.get('state', '?'))}")
        print(f"  applicant_id    : {d.get('applicant_id')}")
        print(f"  requested_usd   : ${d.get('requested_amount_usd')}")
        print(f"  approved_usd    : ${green(str(d.get('approved_amount_usd')))}")
        print(f"  risk_tier       : {d.get('risk_tier')}")
        print(f"  fraud_score     : {d.get('fraud_score')}")
        print(f"  compliance      : {green(d.get('compliance_status', '?'))}")
        print(f"  reviewer        : {d.get('human_reviewer_id')}")
        print(f"  data_as_of      : {dim(str(app_data.get('meta', {}).get('data_as_of', '?')))}")

    # ── QUERY 4: Cryptographic integrity ─────────────────────────────────────
    print(f"\n{bold('QUERY 4')}  {cyan('run_integrity_check LoanApplication/' + app_id)}")

    integrity = await run_integrity_check_tool(
        entity_type="LoanApplication",
        entity_id=app_id,
    )

    print(f"\n  events_verified : {integrity['events_verified']}")
    print(f"  chain_valid     : {green(str(integrity['chain_valid']))}")
    print(f"  tamper_detected : {red('TRUE') if integrity['tamper_detected'] else green('False')}")
    print(f"  integrity_hash  : {dim(integrity['integrity_hash'])}")
    print(f"  checked_at      : {dim(integrity['check_timestamp'])}")

    # ── QUERY 5: Ledger health ────────────────────────────────────────────────
    print(f"\n{bold('QUERY 5')}  {cyan('ledger://ledger/health')}")
    health_json = await get_ledger_health()
    health = json.loads(health_json)
    status_colour = green if health["status"] == "OK" else red
    print(f"\n  overall_status  : {status_colour(health['status'])}")
    print(f"  total_events    : {health['total_events_in_store']}")
    for p in health["projections"]:
        lag = p["lag_ms"]
        slo = p["slo_ms"]
        lag_str = green(f"{lag}ms") if lag < slo else red(f"{lag}ms")
        print(f"  {p['projection_name']:<28} lag={lag_str} (SLO: {slo}ms)")

    # ── STOP TIMER ────────────────────────────────────────────────────────────
    elapsed = time.monotonic() - timer_start
    elapsed_ms = elapsed * 1000

    print(f"\n{DIV}")
    if elapsed < 60:
        timer_result = green(f"{elapsed:.2f}s ✓ UNDER 60 SECONDS")
    else:
        timer_result = red(f"{elapsed:.2f}s ✗ OVER 60 SECONDS")

    print(f"{bold('ELAPSED TIME:')} {timer_result}")
    print(DIV)

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{DIV2}")
    print(f"{bold('  THE WEEK STANDARD — COMPLETE')}")
    print(f"{DIV2}")
    print(f"  Application ID  : {app_id}")
    print(f"  Final state     : {green('FINAL_APPROVED')}")
    print(f"  Total events    : {len(events)}")
    print(f"  Agent actions   : credit analysis + fraud screening + orchestrator decision")
    print(f"  Compliance      : {comp['data']['passed']} rules verified (REG-2026-Q1)")
    print(f"  Human review    : officer-apex-001 (no override)")
    print(f"  Hash chain      : {green('VALID — no tampering detected')}")
    print(f"  Query time      : {green(f'{elapsed:.2f}s')} (target: <60s)")
    print(f"{DIV2}\n")

    assert elapsed < 60, f"Week standard failed: took {elapsed:.2f}s (must be < 60s)"
    assert integrity["chain_valid"], "Integrity chain must be valid"
    assert app_data.get("data", {}).get("state") == "FINAL_APPROVED"

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
