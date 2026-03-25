"""
scripts/demo_temporal_query.py
================================
Video Demo — Step 3: Temporal Compliance Query

Shows in real time:
  - A loan application going through compliance checks
  - A timestamp captured BETWEEN two rule evaluations
  - Querying compliance state AT that past timestamp
  - Proving historical state is DISTINCT from current state

Run with:
    uv run python scripts/demo_temporal_query.py
"""

import asyncio
import os
import sys
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import asyncpg
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.commands.handlers import (
    IssueClearanceCommand,
    RecordComplianceRuleCommand,
    RecordCreditAnalysisCommand,
    RequestComplianceCheckCommand,
    RequestCreditAnalysisCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
    handle_credit_analysis_completed,
    handle_issue_compliance_clearance,
    handle_record_compliance_rule,
    handle_request_compliance_check,
    handle_request_credit_analysis,
    handle_start_agent_session,
    handle_submit_application,
)
from src.event_store import EventStore
from src.projections.application_summary import ApplicationSummaryProjection
from src.projections.compliance_audit import ComplianceAuditViewProjection
from src.projections.agent_performance import AgentPerformanceLedgerProjection
from src.projections.daemon import ProjectionDaemon
from src.models.events import RiskTier

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

DIV  = "─" * 64
DIV2 = "═" * 64


def fmt_time(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC")


def print_compliance_table(title: str, rows: list[dict], highlight_rule: str = "") -> None:
    print(f"\n  {bold(title)}")
    print(f"  {'RULE':<12} {'STATUS':<10} {'VERSION':<10} {'RECORDED AT'}")
    print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*30}")
    for r in rows:
        rule_id = r.get("rule_id", "?")
        status  = r.get("status", "?")
        version = r.get("rule_version", "?")
        rec_at  = r.get("recorded_at", "")
        if rec_at:
            rec_at = str(rec_at)[:23]

        if rule_id == "__CLEARANCE__":
            continue

        if status == "PASSED":
            status_fmt = green(f"{'PASSED':<10}")
        elif status == "FAILED":
            status_fmt = red(f"{'FAILED':<10}")
        else:
            status_fmt = yellow(f"{'PENDING':<10}")

        marker = cyan("◄ changed") if rule_id == highlight_rule else ""
        print(f"  {rule_id:<12} {status_fmt} {version:<10} {rec_at}  {marker}")


async def main() -> None:
    print(f"\n{DIV2}")
    print(f"{bold('  THE LEDGER — Temporal Compliance Query Demo')}")
    print(f"  Step 3 of Video Demo · Apex Financial Services")
    print(f"{DIV2}")

    # ── Clean slate ───────────────────────────────────────────────────────────
    conn = await asyncpg.connect(dsn=DATABASE_URL)
    await conn.execute(
        "TRUNCATE TABLE events, event_streams, outbox, "
        "projection_checkpoints, application_summary, "
        "agent_performance_ledger, compliance_audit_view, "
        "compliance_audit_snapshots RESTART IDENTITY CASCADE"
    )
    await conn.close()

    pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
    store = EventStore(pool=pool)

    projections = [
        ApplicationSummaryProjection(),
        AgentPerformanceLedgerProjection(),
        ComplianceAuditViewProjection(pool=pool),
    ]
    daemon = ProjectionDaemon(store=store, projections=projections, pool=pool)
    await daemon._initialise_checkpoints(pool)

    app_id = "APEX-TEMPORAL-001"

    # ── STEP 1: Build application up to compliance stage ─────────────────────
    print(f"\n{bold('SETUP')}  Building application {app_id} to compliance stage...")

    await handle_submit_application(
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="apex-borrower-001",
            requested_amount_usd=Decimal("850000.00"),
            loan_purpose="Commercial real estate — warehouse acquisition",
            submission_channel="broker",
        ), store)
    print(f"  {green('✓')} ApplicationSubmitted")

    await handle_request_credit_analysis(
        RequestCreditAnalysisCommand(
            application_id=app_id,
            assigned_agent_id="agent-credit-001",
            priority="HIGH",
        ), store)

    await handle_start_agent_session(
        StartAgentSessionCommand(
            agent_id="agent-credit-001",
            session_id="sess-credit-001",
            model_version="credit-v2.1",
            context_token_count=4096,
        ), store)

    await handle_credit_analysis_completed(
        RecordCreditAnalysisCommand(
            application_id=app_id,
            agent_id="agent-credit-001",
            session_id="sess-credit-001",
            model_version="credit-v2.1",
            risk_tier=RiskTier.MEDIUM,
            recommended_limit_usd=Decimal("800000.00"),
            analysis_duration_ms=1340,
            input_data={"financials": "strong", "ltv": 0.72},
            confidence_score=0.89,
        ), store)
    print(f"  {green('✓')} CreditAnalysisCompleted (risk=MEDIUM, confidence=0.89)")

    await handle_request_compliance_check(
        RequestComplianceCheckCommand(
            application_id=app_id,
            regulation_set_version="REG-2026-Q1",
            checks_required=["AML-001", "KYC-001", "SANCTIONS-001"],
        ), store)
    print(f"  {green('✓')} ComplianceCheckRequested (3 rules: AML-001, KYC-001, SANCTIONS-001)")

    # Process events so far
    await daemon._process_batch(pool)
    await daemon._process_batch(pool)

    # ── STEP 2: Capture timestamp T1 — ALL rules still PENDING ───────────────
    print(f"\n{DIV}")
    print(f"{bold('TIMELINE')}")
    print(DIV)

    await asyncio.sleep(0.1)
    t1 = datetime.now(timezone.utc)
    print(f"\n  {cyan('T1')} = {fmt_time(t1)}")
    print(f"       All three compliance rules are PENDING at this moment.")
    await asyncio.sleep(0.15)

    # ── STEP 3: Pass AML-001 ──────────────────────────────────────────────────
    await handle_record_compliance_rule(
        RecordComplianceRuleCommand(
            application_id=app_id,
            rule_id="AML-001",
            rule_version="v2.1",
            passed=True,
            evidence_data={"scan_result": "clean", "source": "WorldCheck"},
        ), store)
    await daemon._process_batch(pool)
    await daemon._process_batch(pool)

    await asyncio.sleep(0.1)
    t2 = datetime.now(timezone.utc)
    print(f"  {cyan('T2')} = {fmt_time(t2)}")
    print(f"       AML-001 just passed. KYC-001 and SANCTIONS-001 still PENDING.")
    await asyncio.sleep(0.15)

    # ── STEP 4: Pass KYC-001 ─────────────────────────────────────────────────
    await handle_record_compliance_rule(
        RecordComplianceRuleCommand(
            application_id=app_id,
            rule_id="KYC-001",
            rule_version="v1.8",
            passed=True,
            evidence_data={"id_verified": True, "pep_check": "clear"},
        ), store)
    await daemon._process_batch(pool)
    await daemon._process_batch(pool)

    await asyncio.sleep(0.1)
    t3 = datetime.now(timezone.utc)
    print(f"  {cyan('T3')} = {fmt_time(t3)}")
    print(f"       KYC-001 just passed. SANCTIONS-001 still PENDING.")
    await asyncio.sleep(0.15)

    # ── STEP 5: Pass SANCTIONS-001 and issue clearance ───────────────────────
    await handle_record_compliance_rule(
        RecordComplianceRuleCommand(
            application_id=app_id,
            rule_id="SANCTIONS-001",
            rule_version="v3.0",
            passed=True,
            evidence_data={"ofac_result": "no_match", "un_result": "no_match"},
        ), store)

    await handle_issue_compliance_clearance(
        IssueClearanceCommand(
            application_id=app_id,
            issuing_agent_id="agent-compliance-001",
            regulation_set_version="REG-2026-Q1",
        ), store)
    await daemon._process_batch(pool)
    await daemon._process_batch(pool)

    t_now = datetime.now(timezone.utc)
    print(f"  {cyan('NOW')} = {fmt_time(t_now)}")
    print(f"       All rules PASSED. Compliance clearance issued.")

    # ── STEP 6: Run temporal queries ─────────────────────────────────────────
    compliance_proj = ComplianceAuditViewProjection()

    print(f"\n{DIV}")
    print(f"{bold('TEMPORAL QUERIES')}")
    print(DIV)

    async with pool.acquire() as conn:

        # Query at T1 — all pending
        state_at_t1 = await compliance_proj.get_compliance_at(app_id, t1, conn)
        print_compliance_table(
            f"ledger://applications/{app_id}/compliance?as_of=T1  (all PENDING)",
            state_at_t1,
        )

        # Query at T2 — AML-001 passed
        state_at_t2 = await compliance_proj.get_compliance_at(app_id, t2, conn)
        print_compliance_table(
            f"ledger://applications/{app_id}/compliance?as_of=T2  (AML-001 PASSED)",
            state_at_t2,
            highlight_rule="AML-001",
        )

        # Query at T3 — AML + KYC passed
        state_at_t3 = await compliance_proj.get_compliance_at(app_id, t3, conn)
        print_compliance_table(
            f"ledger://applications/{app_id}/compliance?as_of=T3  (AML+KYC PASSED)",
            state_at_t3,
            highlight_rule="KYC-001",
        )

        # Current state — all passed
        current = await compliance_proj.get_current_compliance(app_id, conn)
        print_compliance_table(
            f"ledger://applications/{app_id}/compliance  (CURRENT — all PASSED)",
            current,
        )

    # ── STEP 7: Assertions ───────────────────────────────────────────────────
    print(f"\n{DIV}")
    print(f"{bold('ASSERTIONS')}")
    print(DIV)

    async with pool.acquire() as conn:
        state_at_t1 = await compliance_proj.get_compliance_at(app_id, t1, conn)
        state_at_t2 = await compliance_proj.get_compliance_at(app_id, t2, conn)
        current     = await compliance_proj.get_current_compliance(app_id, conn)

    t1_statuses  = {r["rule_id"]: r["status"] for r in state_at_t1 if r["rule_id"] != "__CLEARANCE__"}
    t2_statuses  = {r["rule_id"]: r["status"] for r in state_at_t2 if r["rule_id"] != "__CLEARANCE__"}
    cur_statuses = {r["rule_id"]: r["status"] for r in current if r["rule_id"] != "__CLEARANCE__"}

    # T1: all pending
    assert all(s == "PENDING" for s in t1_statuses.values()), f"T1 should all be PENDING: {t1_statuses}"
    print(f"\n  {green(bold('✓'))} At T1: all rules PENDING — {t1_statuses}")

    # T2: only AML passed
    assert t2_statuses.get("AML-001") == "PASSED", f"T2 AML-001 should be PASSED: {t2_statuses}"
    assert t2_statuses.get("KYC-001") == "PENDING", f"T2 KYC-001 should still be PENDING"
    print(f"  {green(bold('✓'))} At T2: AML-001=PASSED, others=PENDING — {t2_statuses}")

    # Current: all passed
    assert all(s == "PASSED" for s in cur_statuses.values()), f"Current should all be PASSED: {cur_statuses}"
    print(f"  {green(bold('✓'))} Current: all rules PASSED — {cur_statuses}")

    # Historical ≠ current
    assert t1_statuses != cur_statuses, "Historical state must differ from current"
    print(f"  {green(bold('✓'))} Historical state at T1 is DISTINCT from current state")

    print(f"\n{DIV2}")
    print(f"{bold('  TEMPORAL QUERY DEMO COMPLETE — Step 3 verified')}")
    print(f"{DIV2}\n")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
