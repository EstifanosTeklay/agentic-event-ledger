"""
src/mcp/tools.py
=================
MCP Tools — the command (write) side of The Ledger.

8 tools, each mapping to a command handler.
All tools return structured responses. All errors return typed dicts
with suggested_action for autonomous LLM recovery.

Design principle for LLM consumption:
  - Preconditions documented in tool description
  - Errors are typed objects, not plain strings
  - suggested_action enables autonomous recovery without human help
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from src.mcp.server import mcp
from src.mcp.dependencies import get_store, get_db_pool
from src.commands.handlers import (
    FinaliseApplicationCommand,
    GenerateDecisionCommand,
    IssueClearanceCommand,
    RecordComplianceRuleCommand,
    RecordCreditAnalysisCommand,
    RecordFraudScreeningCommand,
    RecordHumanReviewCommand,
    RequestComplianceCheckCommand,
    RequestCreditAnalysisCommand,
    StartAgentSessionCommand,
    SubmitApplicationCommand,
    handle_credit_analysis_completed,
    handle_fraud_screening_completed,
    handle_generate_decision,
    handle_human_review_completed,
    handle_issue_compliance_clearance,
    handle_record_compliance_rule,
    handle_request_compliance_check,
    handle_request_credit_analysis,
    handle_start_agent_session,
    handle_submit_application,
)
from src.integrity.audit_chain import run_integrity_check
from src.models.events import (
    DomainError,
    OptimisticConcurrencyError,
    PreconditionFailedError,
    Recommendation,
    RiskTier,
    StreamNotFoundError,
)

logger = logging.getLogger(__name__)


def _ok(data: dict[str, Any]) -> dict[str, Any]:
    return {"status": "ok", **data}


def _error(e: Exception) -> dict[str, Any]:
    if hasattr(e, "to_dict"):
        return {"status": "error", **e.to_dict()}
    return {
        "status": "error",
        "error_type": type(e).__name__,
        "message": str(e),
        "suggested_action": "check_input_and_retry",
    }


# =============================================================================
# TOOL 1: submit_application
# =============================================================================

@mcp.tool(
    description="""
Submit a new commercial loan application to The Ledger.
Creates the loan-{application_id} event stream.

PRECONDITIONS:
  - application_id must be unique. Duplicate IDs return DuplicateApplication error.

RETURNS:
  stream_id, initial_version (always 1)

ERRORS:
  DomainError(rule_name=DuplicateApplication): application_id already exists
"""
)
async def submit_application(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    submission_channel: str = "online",
) -> dict[str, Any]:
    try:
        store = await get_store()
        stream_id = await handle_submit_application(
            SubmitApplicationCommand(
                application_id=application_id,
                applicant_id=applicant_id,
                requested_amount_usd=Decimal(str(requested_amount_usd)),
                loan_purpose=loan_purpose,
                submission_channel=submission_channel,
            ),
            store,
        )
        return _ok({
            "stream_id": stream_id,
            "initial_version": 1,
            "application_id": application_id,
        })
    except Exception as e:
        logger.error("submit_application failed: %s", e)
        return _error(e)


# =============================================================================
# TOOL 2: start_agent_session
# =============================================================================

@mcp.tool(
    description="""
Start a new agent session. MUST be called before any analysis or decision tool.

This implements the Gas Town pattern: every agent must declare its context
source and model version before making any auditable decision. Calling
record_credit_analysis or generate_decision without first calling
start_agent_session will return a PreconditionFailed error.

PRECONDITIONS:
  - session_id must be unique per agent_id.

RETURNS:
  session_id, stream_id, context_position (always 1)

ERRORS:
  DomainError: session_id already exists for this agent
"""
)
async def start_agent_session(
    agent_id: str,
    session_id: str,
    model_version: str,
    context_source: str = "fresh",
    context_token_count: int = 0,
) -> dict[str, Any]:
    try:
        store = await get_store()
        stream_id = await handle_start_agent_session(
            StartAgentSessionCommand(
                agent_id=agent_id,
                session_id=session_id,
                model_version=model_version,
                context_source=context_source,
                context_token_count=context_token_count,
            ),
            store,
        )
        return _ok({
            "session_id": session_id,
            "stream_id": stream_id,
            "context_position": 1,
            "model_version": model_version,
        })
    except Exception as e:
        logger.error("start_agent_session failed: %s", e)
        return _error(e)


# =============================================================================
# TOOL 3: record_credit_analysis
# =============================================================================

@mcp.tool(
    description="""
Record a completed credit analysis for a loan application.

PRECONDITIONS:
  - An active agent session must exist (call start_agent_session first).
    Without an active session, this returns PreconditionFailed.
  - The application must be in AWAITING_ANALYSIS state.
  - model_version must match the version declared in start_agent_session.
  - No prior credit analysis may exist for this application (unless superseded).

RETURNS:
  event_id, new_stream_version, risk_tier

ERRORS:
  OptimisticConcurrencyError: Another agent appended to the stream simultaneously.
    suggested_action: reload_stream_and_retry
  DomainError(rule_name=GasTownContextRequirement): No active agent session.
  DomainError(rule_name=ModelVersionLocking): model_version mismatch.
"""
)
async def record_credit_analysis(
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    risk_tier: str,
    recommended_limit_usd: float,
    analysis_duration_ms: int,
    confidence_score: float | None = None,
    regulatory_basis: str | None = None,
) -> dict[str, Any]:
    try:
        store = await get_store()
        await handle_credit_analysis_completed(
            RecordCreditAnalysisCommand(
                application_id=application_id,
                agent_id=agent_id,
                session_id=session_id,
                model_version=model_version,
                risk_tier=RiskTier(risk_tier),
                recommended_limit_usd=Decimal(str(recommended_limit_usd)),
                analysis_duration_ms=analysis_duration_ms,
                input_data={
                    "application_id": application_id,
                    "risk_tier": risk_tier,
                    "confidence_score": confidence_score,
                },
                confidence_score=confidence_score,
                regulatory_basis=regulatory_basis,
            ),
            store,
        )
        version = await store.stream_version(f"loan-{application_id}")
        return _ok({
            "application_id": application_id,
            "new_stream_version": version,
            "risk_tier": risk_tier,
            "recommended_limit_usd": recommended_limit_usd,
        })
    except Exception as e:
        logger.error("record_credit_analysis failed: %s", e)
        return _error(e)


# =============================================================================
# TOOL 4: record_fraud_screening
# =============================================================================

@mcp.tool(
    description="""
Record a completed fraud screening for a loan application.

PRECONDITIONS:
  - An active agent session must exist (call start_agent_session first).
  - fraud_score must be between 0.0 and 1.0.

RETURNS:
  event_id, new_stream_version, fraud_score

ERRORS:
  DomainError(rule_name=GasTownContextRequirement): No active agent session.
  ValueError: fraud_score outside 0.0-1.0 range.
"""
)
async def record_fraud_screening(
    application_id: str,
    agent_id: str,
    session_id: str,
    fraud_score: float,
    screening_model_version: str,
    anomaly_flags: list[str] | None = None,
) -> dict[str, Any]:
    try:
        if not 0.0 <= fraud_score <= 1.0:
            raise ValueError(f"fraud_score must be 0.0-1.0, got {fraud_score}")
        store = await get_store()
        await handle_fraud_screening_completed(
            RecordFraudScreeningCommand(
                application_id=application_id,
                agent_id=agent_id,
                session_id=session_id,
                fraud_score=fraud_score,
                anomaly_flags=anomaly_flags or [],
                screening_model_version=screening_model_version,
                input_data={"application_id": application_id},
            ),
            store,
        )
        return _ok({
            "application_id": application_id,
            "fraud_score": fraud_score,
            "anomaly_flags": anomaly_flags or [],
            "risk_level": "HIGH" if fraud_score > 0.7 else "MEDIUM" if fraud_score > 0.3 else "LOW",
        })
    except Exception as e:
        logger.error("record_fraud_screening failed: %s", e)
        return _error(e)


# =============================================================================
# TOOL 5: record_compliance_check
# =============================================================================

@mcp.tool(
    description="""
Record the result of a compliance rule evaluation (pass or fail).

To initiate compliance checking, first call request_compliance_check.
Then call this tool once per rule.

PRECONDITIONS:
  - ComplianceCheckRequested must have been issued for this application.
  - rule_id must be in the required checks list.
  - Clearance must not already be issued.

RETURNS:
  check_id, compliance_status, rule_id

ERRORS:
  DomainError(rule_name=ComplianceNotInitiated): No compliance check requested.
  DomainError(rule_name=UnknownComplianceRule): rule_id not in required set.
"""
)
async def record_compliance_check(
    application_id: str,
    rule_id: str,
    rule_version: str,
    passed: bool,
    failure_reason: str | None = None,
    regulation_set_version: str = "REG-2026-Q1",
    checks_required: list[str] | None = None,
) -> dict[str, Any]:
    try:
        store = await get_store()

        # If checks_required provided, initiate compliance first
        if checks_required:
            await handle_request_compliance_check(
                RequestComplianceCheckCommand(
                    application_id=application_id,
                    regulation_set_version=regulation_set_version,
                    checks_required=checks_required,
                ),
                store,
            )

        await handle_record_compliance_rule(
            RecordComplianceRuleCommand(
                application_id=application_id,
                rule_id=rule_id,
                rule_version=rule_version,
                passed=passed,
                failure_reason=failure_reason,
                evidence_data={"rule_id": rule_id, "passed": passed},
            ),
            store,
        )
        return _ok({
            "application_id": application_id,
            "rule_id": rule_id,
            "compliance_status": "PASSED" if passed else "FAILED",
            "rule_version": rule_version,
        })
    except Exception as e:
        logger.error("record_compliance_check failed: %s", e)
        return _error(e)


# =============================================================================
# TOOL 6: generate_decision
# =============================================================================

@mcp.tool(
    description="""
Generate an AI orchestrator decision for a loan application.

PRECONDITIONS:
  - An active orchestrator agent session must exist (call start_agent_session first).
  - Application must be in PENDING_DECISION state.
  - Compliance clearance must be issued (all required checks passed).
  - All contributing_agent_sessions must have processed this application.
  - confidence_score < 0.6 forces recommendation=REFER regardless of input
    (regulatory requirement enforced in domain layer — cannot be overridden).

RETURNS:
  decision_id, recommendation, confidence_score, final_recommendation

ERRORS:
  DomainError(rule_name=ComplianceDependency): Compliance not cleared.
  DomainError(rule_name=CausalChainEnforcement): Invalid contributing sessions.
  OptimisticConcurrencyError: Concurrent write detected.
    suggested_action: reload_stream_and_retry
"""
)
async def generate_decision(
    application_id: str,
    orchestrator_agent_id: str,
    orchestrator_session_id: str,
    recommendation: str,
    confidence_score: float,
    contributing_agent_sessions: list[str],
    decision_basis_summary: str,
    model_versions: dict[str, str] | None = None,
) -> dict[str, Any]:
    try:
        store = await get_store()
        rec = Recommendation(recommendation)
        await handle_generate_decision(
            GenerateDecisionCommand(
                application_id=application_id,
                orchestrator_agent_id=orchestrator_agent_id,
                orchestrator_session_id=orchestrator_session_id,
                recommendation=rec,
                confidence_score=confidence_score,
                contributing_agent_sessions=contributing_agent_sessions,
                decision_basis_summary=decision_basis_summary,
                model_versions=model_versions or {},
            ),
            store,
        )
        # Confidence floor may have changed recommendation
        from src.aggregates.loan_application import LoanApplicationAggregate
        final_rec = LoanApplicationAggregate.enforce_confidence_floor(
            rec, confidence_score
        )
        return _ok({
            "application_id": application_id,
            "recommendation": final_rec.value,
            "original_recommendation": recommendation,
            "confidence_score": confidence_score,
            "confidence_floor_applied": final_rec.value != recommendation,
        })
    except Exception as e:
        logger.error("generate_decision failed: %s", e)
        return _error(e)


# =============================================================================
# TOOL 7: record_human_review
# =============================================================================

@mcp.tool(
    description="""
Record a human loan officer's final review decision.

The loan officer may accept or override the AI recommendation.
If override=True, override_reason is required.

PRECONDITIONS:
  - Application must be in APPROVED_PENDING_HUMAN or DECLINED_PENDING_HUMAN state.
  - If override=True, override_reason must be provided.

RETURNS:
  final_decision, application_state, override_applied

ERRORS:
  DomainError: Application not in reviewable state.
  ValueError: override=True but override_reason not provided.
"""
)
async def record_human_review(
    application_id: str,
    reviewer_id: str,
    final_decision: str,
    override: bool = False,
    override_reason: str | None = None,
) -> dict[str, Any]:
    try:
        store = await get_store()
        await handle_human_review_completed(
            RecordHumanReviewCommand(
                application_id=application_id,
                reviewer_id=reviewer_id,
                final_decision=Recommendation(final_decision),
                override=override,
                override_reason=override_reason,
            ),
            store,
        )
        from src.aggregates.loan_application import LoanApplicationAggregate
        app = await LoanApplicationAggregate.load(store, application_id)
        return _ok({
            "application_id": application_id,
            "final_decision": final_decision,
            "application_state": app.state.value,
            "override_applied": override,
            "reviewer_id": reviewer_id,
        })
    except Exception as e:
        logger.error("record_human_review failed: %s", e)
        return _error(e)


# =============================================================================
# TOOL 8: run_integrity_check
# =============================================================================

@mcp.tool(
    description="""
Run a cryptographic integrity check on an entity's audit chain.

Verifies the SHA-256 hash chain over all events for the entity.
Any tampering with stored events will be detected and reported.

PRECONDITIONS:
  - Caller must have compliance role (enforced at authentication layer).
  - Rate limited to 1 check per minute per entity (enforced in production).

RETURNS:
  check_result with chain_valid, tamper_detected, events_verified, integrity_hash

ERRORS:
  StreamNotFoundError: Entity stream not found.
"""
)
async def run_integrity_check_tool(
    entity_type: str,
    entity_id: str,
) -> dict[str, Any]:
    try:
        store = await get_store()
        result = await run_integrity_check(store, entity_type, entity_id)
        return _ok({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "events_verified": result.events_verified,
            "chain_valid": result.chain_valid,
            "tamper_detected": result.tamper_detected,
            "integrity_hash": result.integrity_hash[:16] + "...",
            "check_timestamp": result.check_timestamp.isoformat(),
            "failure_detail": result.failure_detail,
        })
    except Exception as e:
        logger.error("run_integrity_check failed: %s", e)
        return _error(e)


# =============================================================================
# HELPER TOOL: request_compliance_check (initiation)
# =============================================================================

@mcp.tool(
    description="""
Initiate compliance checking for a loan application.
Must be called before record_compliance_check.

PRECONDITIONS:
  - Application must be in ANALYSIS_COMPLETE state.

RETURNS:
  compliance_stream_id, checks_required
"""
)
async def request_compliance_check(
    application_id: str,
    regulation_set_version: str,
    checks_required: list[str],
) -> dict[str, Any]:
    try:
        store = await get_store()
        await handle_request_compliance_check(
            RequestComplianceCheckCommand(
                application_id=application_id,
                regulation_set_version=regulation_set_version,
                checks_required=checks_required,
            ),
            store,
        )
        return _ok({
            "compliance_stream_id": f"compliance-{application_id}",
            "application_id": application_id,
            "regulation_set_version": regulation_set_version,
            "checks_required": checks_required,
        })
    except Exception as e:
        logger.error("request_compliance_check failed: %s", e)
        return _error(e)


@mcp.tool(
    description="""
Issue compliance clearance when all required checks have passed.
This advances the application to PENDING_DECISION state.

PRECONDITIONS:
  - All required compliance checks must have PASSED status.
  - No FAILED checks may exist for this application.

RETURNS:
  clearance_issued, application_state

ERRORS:
  DomainError(rule_name=ComplianceDependency): Not all checks passed.
"""
)
async def issue_compliance_clearance(
    application_id: str,
    issuing_agent_id: str,
    regulation_set_version: str,
) -> dict[str, Any]:
    try:
        store = await get_store()
        await handle_issue_compliance_clearance(
            IssueClearanceCommand(
                application_id=application_id,
                issuing_agent_id=issuing_agent_id,
                regulation_set_version=regulation_set_version,
            ),
            store,
        )
        return _ok({
            "application_id": application_id,
            "clearance_issued": True,
            "issuing_agent_id": issuing_agent_id,
        })
    except Exception as e:
        logger.error("issue_compliance_clearance failed: %s", e)
        return _error(e)


# =============================================================================
# HELPER TOOL: request_credit_analysis
# =============================================================================

@mcp.tool(
    description="""
Request credit analysis for a loan application.
Transitions application from SUBMITTED to AWAITING_ANALYSIS state.
Must be called before record_credit_analysis.

PRECONDITIONS:
  - Application must be in SUBMITTED state.

RETURNS:
  application_id, new_state
"""
)
async def request_credit_analysis(
    application_id: str,
    assigned_agent_id: str,
    priority: str = "NORMAL",
) -> dict[str, Any]:
    try:
        store = await get_store()
        await handle_request_credit_analysis(
            RequestCreditAnalysisCommand(
                application_id=application_id,
                assigned_agent_id=assigned_agent_id,
                priority=priority,
            ),
            store,
        )
        return _ok({
            "application_id": application_id,
            "new_state": "AWAITING_ANALYSIS",
            "assigned_agent_id": assigned_agent_id,
        })
    except Exception as e:
        logger.error("request_credit_analysis failed: %s", e)
        return _error(e)
