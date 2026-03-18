"""
src/commands/handlers.py
=========================
All command handlers for the Apex Financial Services domain.

Every handler follows the exact same 4-step pattern:
  1. Reconstruct — load aggregate state from event store
  2. Validate    — enforce all business rules BEFORE any state change
  3. Determine   — construct new domain events (pure logic, no I/O)
  4. Append      — write events atomically with optimistic concurrency

Business rules are enforced in the aggregate layer.
Handlers orchestrate aggregates — they do not contain business logic.

Command objects use Pydantic for schema validation.
All commands carry a correlation_id for causal tracing.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.loan_application import LoanApplicationAggregate
from src.event_store import EventStore
from src.models.events import (
    AgentContextLoaded,
    AgentSessionClosed,
    ApplicationApproved,
    ApplicationDeclined,
    ApplicationState,
    ApplicationSubmitted,
    ComplianceClearanceIssued,
    ComplianceCheckRequested,
    ComplianceRuleFailed,
    ComplianceRulePassed,
    CreditAnalysisCompleted,
    CreditAnalysisRequested,
    CreditAnalysisSuperseded,
    DecisionGenerated,
    DomainError,
    FraudScreeningCompleted,
    HumanReviewCompleted,
    PreconditionFailedError,
    Recommendation,
    RiskTier,
    StreamNotFoundError,
)

logger = logging.getLogger(__name__)


def _new_correlation_id() -> str:
    return f"corr-{uuid.uuid4()}"


def hash_inputs(data: Any) -> str:
    """SHA-256 hash of input data for provenance tracking."""
    serialised = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialised.encode()).hexdigest()


# =============================================================================
# COMMAND DATACLASSES
# =============================================================================

@dataclass
class SubmitApplicationCommand:
    application_id:      str
    applicant_id:        str
    requested_amount_usd: Decimal
    loan_purpose:        str
    submission_channel:  str = "online"
    correlation_id:      str = field(default_factory=_new_correlation_id)


@dataclass
class RequestCreditAnalysisCommand:
    application_id:    str
    assigned_agent_id: str
    priority:          str = "NORMAL"
    correlation_id:    str = field(default_factory=_new_correlation_id)


@dataclass
class StartAgentSessionCommand:
    agent_id:             str
    session_id:           str
    model_version:        str
    context_source:       str = "fresh"
    event_replay_from_position: int = 0
    context_token_count:  int = 0
    correlation_id:       str = field(default_factory=_new_correlation_id)


@dataclass
class RecordCreditAnalysisCommand:
    application_id:        str
    agent_id:              str
    session_id:            str
    model_version:         str
    risk_tier:             RiskTier
    recommended_limit_usd: Decimal
    analysis_duration_ms:  int
    input_data:            dict[str, Any]
    confidence_score:      float | None = None
    regulatory_basis:      str | None = None
    correlation_id:        str = field(default_factory=_new_correlation_id)


@dataclass
class RecordFraudScreeningCommand:
    application_id:          str
    agent_id:                str
    session_id:              str
    fraud_score:             float
    anomaly_flags:           list[str]
    screening_model_version: str
    input_data:              dict[str, Any]
    correlation_id:          str = field(default_factory=_new_correlation_id)


@dataclass
class RequestComplianceCheckCommand:
    application_id:        str
    regulation_set_version: str
    checks_required:       list[str]
    correlation_id:        str = field(default_factory=_new_correlation_id)


@dataclass
class RecordComplianceRuleCommand:
    application_id:       str
    rule_id:              str
    rule_version:         str
    passed:               bool
    failure_reason:       str | None = None
    remediation_required: bool = True
    evidence_data:        dict[str, Any] | None = None
    correlation_id:       str = field(default_factory=_new_correlation_id)


@dataclass
class IssueClearanceCommand:
    application_id:        str
    issuing_agent_id:      str
    regulation_set_version: str
    correlation_id:        str = field(default_factory=_new_correlation_id)


@dataclass
class GenerateDecisionCommand:
    application_id:             str
    orchestrator_agent_id:      str
    orchestrator_session_id:    str
    recommendation:             Recommendation
    confidence_score:           float
    contributing_agent_sessions: list[str]
    decision_basis_summary:     str
    model_versions:             dict[str, str]
    correlation_id:             str = field(default_factory=_new_correlation_id)


@dataclass
class RecordHumanReviewCommand:
    application_id:  str
    reviewer_id:     str
    final_decision:  Recommendation
    override:        bool = False
    override_reason: str | None = None
    correlation_id:  str = field(default_factory=_new_correlation_id)


@dataclass
class FinaliseApplicationCommand:
    application_id:     str
    approved:           bool
    approved_amount_usd: Decimal | None = None
    interest_rate:      float | None = None
    conditions:         list[str] = field(default_factory=list)
    decided_by:         str = "auto"
    decline_reasons:    list[str] = field(default_factory=list)
    correlation_id:     str = field(default_factory=_new_correlation_id)


# =============================================================================
# COMMAND HANDLERS
# =============================================================================

async def handle_submit_application(
    cmd: SubmitApplicationCommand,
    store: EventStore,
) -> str:
    """
    Submit a new loan application.
    Creates the loan-{application_id} stream with expected_version=-1.

    Returns:
        stream_id of the created stream.
    """
    stream_id = f"loan-{cmd.application_id}"

    # Guard: application must not already exist
    try:
        existing_version = await store.stream_version(stream_id)
        raise DomainError(
            f"Application '{cmd.application_id}' already exists "
            f"(stream version {existing_version}). "
            "Use a unique application_id.",
            rule_name="DuplicateApplication",
        )
    except StreamNotFoundError:
        pass  # expected — stream does not yet exist

    await store.append(
        stream_id=stream_id,
        events=[
            ApplicationSubmitted(
                application_id=cmd.application_id,
                applicant_id=cmd.applicant_id,
                requested_amount_usd=cmd.requested_amount_usd,
                loan_purpose=cmd.loan_purpose,
                submission_channel=cmd.submission_channel,
            )
        ],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
    )

    logger.info("Application '%s' submitted by '%s'", cmd.application_id, cmd.applicant_id)
    return stream_id


async def handle_start_agent_session(
    cmd: StartAgentSessionCommand,
    store: EventStore,
) -> str:
    """
    Start a new agent session. Writes AgentContextLoaded as the first event.
    REQUIRED before any analysis or decision commands for this session.

    Gas Town pattern: context_source tracks whether this is a fresh start
    or a replay-based recovery after a crash.

    Returns:
        stream_id of the created agent session stream.
    """
    stream_id = AgentSessionAggregate.stream_id_for(cmd.agent_id, cmd.session_id)

    await store.append(
        stream_id=stream_id,
        events=[
            AgentContextLoaded(
                agent_id=cmd.agent_id,
                session_id=cmd.session_id,
                context_source=cmd.context_source,
                event_replay_from_position=cmd.event_replay_from_position,
                context_token_count=cmd.context_token_count,
                model_version=cmd.model_version,
            )
        ],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
    )

    logger.info(
        "Agent session started: agent=%s session=%s model=%s source=%s",
        cmd.agent_id, cmd.session_id, cmd.model_version, cmd.context_source,
    )
    return stream_id


async def handle_request_credit_analysis(
    cmd: RequestCreditAnalysisCommand,
    store: EventStore,
) -> None:
    """Request a credit analysis for an application."""
    # 1. Reconstruct
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate
    app.assert_in_state(ApplicationState.SUBMITTED)
    app.assert_not_terminal()

    # 3. Determine
    events = [
        CreditAnalysisRequested(
            application_id=cmd.application_id,
            assigned_agent_id=cmd.assigned_agent_id,
            priority=cmd.priority,
        )
    ]

    # 4. Append
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
    )


async def handle_credit_analysis_completed(
    cmd: RecordCreditAnalysisCommand,
    store: EventStore,
) -> None:
    """
    Record a completed credit analysis.

    Validates:
      - Application is awaiting analysis
      - Agent session has loaded context (Gas Town Rule)
      - Model version matches session declaration
      - No prior credit analysis for this app (unless superseded)
    """
    # 1. Reconstruct both aggregates
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # 2. Validate
    app.assert_awaiting_credit_analysis()
    app.assert_no_prior_credit_analysis()
    agent.assert_context_loaded()           # Gas Town Rule
    agent.assert_not_closed()
    agent.assert_model_version_current(cmd.model_version)

    # 3. Determine
    events = [
        CreditAnalysisCompleted(
            application_id=cmd.application_id,
            agent_id=cmd.agent_id,
            session_id=cmd.session_id,
            model_version=cmd.model_version,
            confidence_score=cmd.confidence_score,
            risk_tier=cmd.risk_tier,
            recommended_limit_usd=cmd.recommended_limit_usd,
            analysis_duration_ms=cmd.analysis_duration_ms,
            input_data_hash=hash_inputs(cmd.input_data),
            regulatory_basis=cmd.regulatory_basis,
        )
    ]

    # 4. Append (optimistic concurrency on loan stream)
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
        causation_id=cmd.correlation_id,
    )

    logger.info(
        "Credit analysis completed: app=%s agent=%s risk=%s",
        cmd.application_id, cmd.agent_id, cmd.risk_tier.value,
    )


async def handle_fraud_screening_completed(
    cmd: RecordFraudScreeningCommand,
    store: EventStore,
) -> None:
    """
    Record a completed fraud screening.
    Writes to BOTH loan stream (for application state) AND agent stream.

    Note: FraudScreeningCompleted can happen concurrently with credit analysis.
    It does NOT trigger a state transition — it enriches the application record.
    We write to the agent session stream only (avoids concurrency on loan stream
    at this stage).
    """
    # 1. Reconstruct
    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # 2. Validate
    agent.assert_context_loaded()
    agent.assert_not_closed()

    # 3. Determine
    event = FraudScreeningCompleted(
        application_id=cmd.application_id,
        agent_id=cmd.agent_id,
        fraud_score=cmd.fraud_score,
        anomaly_flags=cmd.anomaly_flags,
        screening_model_version=cmd.screening_model_version,
        input_data_hash=hash_inputs(cmd.input_data),
    )

    # 4. Append to agent session stream
    agent_stream = AgentSessionAggregate.stream_id_for(cmd.agent_id, cmd.session_id)
    await store.append(
        stream_id=agent_stream,
        events=[event],
        expected_version=agent.version,
        correlation_id=cmd.correlation_id,
    )

    # Also write a cross-reference event to loan stream for projection consumption
    # This is a denormalised write — the loan stream gets the fraud result too.
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    if app.state not in {ApplicationState.FINAL_APPROVED, ApplicationState.FINAL_DECLINED}:
        await store.append(
            stream_id=f"loan-{cmd.application_id}",
            events=[event],
            expected_version=app.version,
            correlation_id=cmd.correlation_id,
        )

    logger.info(
        "Fraud screening: app=%s agent=%s score=%.3f flags=%s",
        cmd.application_id, cmd.agent_id, cmd.fraud_score, cmd.anomaly_flags,
    )


async def handle_request_compliance_check(
    cmd: RequestComplianceCheckCommand,
    store: EventStore,
) -> None:
    """
    Initiate compliance checking for an application.
    Creates the compliance-{application_id} stream.
    Also advances the loan stream to COMPLIANCE_REVIEW state.
    """
    # 1. Reconstruct loan application
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate
    app.assert_in_state(ApplicationState.ANALYSIS_COMPLETE)

    # 3. Determine
    compliance_event = ComplianceCheckRequested(
        application_id=cmd.application_id,
        regulation_set_version=cmd.regulation_set_version,
        checks_required=cmd.checks_required,
    )

    # 4. Append to compliance stream (new stream)
    await store.append(
        stream_id=f"compliance-{cmd.application_id}",
        events=[compliance_event],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
    )

    # Also write to loan stream to advance state
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[compliance_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
    )


async def handle_record_compliance_rule(
    cmd: RecordComplianceRuleCommand,
    store: EventStore,
) -> None:
    """
    Record a compliance rule evaluation result (pass or fail).
    Writes to the compliance-{application_id} stream.
    """
    # 1. Reconstruct
    compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)

    # 2. Validate
    compliance.assert_initiated()
    compliance.assert_clearance_not_already_issued()
    if cmd.rule_id:
        compliance.assert_rule_in_required_set(cmd.rule_id)

    # 3. Determine
    if cmd.passed:
        event = ComplianceRulePassed(
            application_id=cmd.application_id,
            rule_id=cmd.rule_id,
            rule_version=cmd.rule_version,
            evidence_hash=hash_inputs(cmd.evidence_data or {}),
        )
    else:
        event = ComplianceRuleFailed(
            application_id=cmd.application_id,
            rule_id=cmd.rule_id,
            rule_version=cmd.rule_version,
            failure_reason=cmd.failure_reason or "No reason provided",
            remediation_required=cmd.remediation_required,
        )

    # 4. Append to compliance stream
    await store.append(
        stream_id=f"compliance-{cmd.application_id}",
        events=[event],
        expected_version=compliance.version,
        correlation_id=cmd.correlation_id,
    )


async def handle_issue_compliance_clearance(
    cmd: IssueClearanceCommand,
    store: EventStore,
) -> None:
    """
    Issue compliance clearance when all required checks have passed.
    Business Rule 5: All checks must pass before clearance can be issued.
    """
    # 1. Reconstruct
    compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)

    # 2. Validate (Business Rule 5 enforced here)
    compliance.assert_initiated()
    compliance.assert_clearance_not_already_issued()
    compliance.assert_all_checks_passed()   # ← Business Rule 5

    # 3. Determine
    clearance_event = ComplianceClearanceIssued(
        application_id=cmd.application_id,
        regulation_set_version=cmd.regulation_set_version,
        checks_passed=list(compliance.passed_rules.keys()),
        issuing_agent_id=cmd.issuing_agent_id,
    )

    # 4. Append — compliance stream advances
    await store.append(
        stream_id=f"compliance-{cmd.application_id}",
        events=[clearance_event],
        expected_version=compliance.version,
        correlation_id=cmd.correlation_id,
    )

    # Also advance loan stream to PENDING_DECISION
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[clearance_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
    )


async def handle_generate_decision(
    cmd: GenerateDecisionCommand,
    store: EventStore,
) -> None:
    """
    Orchestrator agent synthesises all analyses into a recommendation.

    Validates:
      - Application is in PENDING_DECISION state
      - Compliance is cleared (Business Rule 5)
      - Confidence floor enforced (Business Rule 4)
      - All contributing sessions processed this application (Business Rule 6)
      - Orchestrator session has loaded context (Gas Town)
    """
    # 1. Reconstruct
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    orchestrator = await AgentSessionAggregate.load(
        store, cmd.orchestrator_agent_id, cmd.orchestrator_session_id
    )

    # 2. Validate
    app.assert_in_state(ApplicationState.PENDING_DECISION)
    app.assert_compliance_cleared()         # Business Rule 5
    orchestrator.assert_context_loaded()    # Gas Town
    orchestrator.assert_not_closed()

    # Business Rule 6: Causal chain — validate contributing sessions
    known_sessions: set[str] = set()
    for session_stream_id in cmd.contributing_agent_sessions:
        # Load each contributing session to verify it processed this application
        parts = session_stream_id.replace("agent-", "", 1).rsplit("-", 1)
        if len(parts) == 2:
            try:
                contributing_agent = await AgentSessionAggregate.load(
                    store, parts[0], parts[1]
                )
                if cmd.application_id in contributing_agent.applications_processed:
                    known_sessions.add(session_stream_id)
            except Exception:
                pass  # session not found — will be caught by assert below

    app.assert_valid_contributing_sessions(cmd.contributing_agent_sessions, known_sessions)

    # Business Rule 4: Confidence floor
    final_recommendation = LoanApplicationAggregate.enforce_confidence_floor(
        cmd.recommendation, cmd.confidence_score
    )

    # 3. Determine
    events = [
        DecisionGenerated(
            application_id=cmd.application_id,
            orchestrator_agent_id=cmd.orchestrator_agent_id,
            recommendation=final_recommendation,
            confidence_score=cmd.confidence_score,
            contributing_agent_sessions=cmd.contributing_agent_sessions,
            decision_basis_summary=cmd.decision_basis_summary,
            model_versions=cmd.model_versions,
        )
    ]

    # 4. Append
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=events,
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
    )

    logger.info(
        "Decision generated: app=%s recommendation=%s confidence=%.3f",
        cmd.application_id, final_recommendation.value, cmd.confidence_score,
    )


async def handle_human_review_completed(
    cmd: RecordHumanReviewCommand,
    store: EventStore,
) -> None:
    """
    Record a human loan officer's review and final decision.
    May override the AI recommendation. Override requires a reason.
    """
    # 1. Reconstruct
    app = await LoanApplicationAggregate.load(store, cmd.application_id)

    # 2. Validate
    app.assert_in_state(
        ApplicationState.APPROVED_PENDING_HUMAN,
        ApplicationState.DECLINED_PENDING_HUMAN,
    )
    app.assert_not_terminal()

    # 3. Determine
    review_event = HumanReviewCompleted(
        application_id=cmd.application_id,
        reviewer_id=cmd.reviewer_id,
        override=cmd.override,
        final_decision=cmd.final_decision,
        override_reason=cmd.override_reason,
    )

    # Also determine the terminal event based on the final decision
    if cmd.final_decision == Recommendation.APPROVE:
        # We need an approved amount — use recommended limit from credit analysis
        approved_amount = app.recommended_limit_usd or app.requested_amount_usd or Decimal("0")
        terminal_event = ApplicationApproved(
            application_id=cmd.application_id,
            approved_amount_usd=approved_amount,
            interest_rate=0.0,  # Would come from pricing engine in full system
            conditions=[],
            approved_by=cmd.reviewer_id,
        )
    else:
        terminal_event = ApplicationDeclined(
            application_id=cmd.application_id,
            decline_reasons=["Human review decision: decline"],
            declined_by=cmd.reviewer_id,
            adverse_action_notice_required=True,
        )

    # 4. Append both events atomically
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=[review_event, terminal_event],
        expected_version=app.version,
        correlation_id=cmd.correlation_id,
    )

    logger.info(
        "Human review: app=%s reviewer=%s decision=%s override=%s",
        cmd.application_id, cmd.reviewer_id,
        cmd.final_decision.value, cmd.override,
    )


async def handle_close_agent_session(
    agent_id: str,
    session_id: str,
    reason: str,
    store: EventStore,
    correlation_id: str | None = None,
) -> None:
    """Close an agent session explicitly."""
    agent = await AgentSessionAggregate.load(store, agent_id, session_id)
    agent.assert_not_closed()

    stream_id = AgentSessionAggregate.stream_id_for(agent_id, session_id)
    await store.append(
        stream_id=stream_id,
        events=[AgentSessionClosed(agent_id=agent_id, session_id=session_id, reason=reason)],
        expected_version=agent.version,
        correlation_id=correlation_id or _new_correlation_id(),
    )
