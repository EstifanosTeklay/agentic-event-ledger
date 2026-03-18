"""
src/models/events.py
====================
Pydantic models for every event type in the Apex Financial Services domain.

Design principles:
- BaseEvent: common fields all domain events carry
- StoredEvent: BaseEvent + infrastructure fields added by the event store
- Each event class is a strict Pydantic model — unknown fields are forbidden
- Custom exceptions carry structured context for LLM-consumable error responses
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone


def _utcnow() -> datetime:
    """Timezone-aware UTC now — replaces deprecated datetime.utcnow()."""
    return datetime.now(timezone.utc)
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


# =============================================================================
# DOMAIN ENUMERATIONS
# =============================================================================

class ApplicationState(str, Enum):
    SUBMITTED               = "SUBMITTED"
    AWAITING_ANALYSIS       = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE       = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW       = "COMPLIANCE_REVIEW"
    PENDING_DECISION        = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN  = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN  = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED          = "FINAL_APPROVED"
    FINAL_DECLINED          = "FINAL_DECLINED"


class RiskTier(str, Enum):
    LOW     = "LOW"
    MEDIUM  = "MEDIUM"
    HIGH    = "HIGH"
    CRITICAL = "CRITICAL"


class Recommendation(str, Enum):
    APPROVE = "APPROVE"
    DECLINE = "DECLINE"
    REFER   = "REFER"


class ComplianceStatus(str, Enum):
    PENDING = "PENDING"
    PASSED  = "PASSED"
    FAILED  = "FAILED"


class SessionHealthStatus(str, Enum):
    HEALTHY              = "HEALTHY"
    NEEDS_RECONCILIATION = "NEEDS_RECONCILIATION"
    STALE                = "STALE"


# =============================================================================
# CUSTOM EXCEPTIONS — Structured for LLM consumption
# =============================================================================

class LedgerError(Exception):
    """Base class for all Ledger exceptions. Always carries structured context."""
    error_type: str = "LedgerError"

    def to_dict(self) -> dict[str, Any]:
        return {"error_type": self.error_type, "message": str(self)}


class OptimisticConcurrencyError(LedgerError):
    """
    Raised when an append operation encounters a version mismatch.
    Structured to enable autonomous LLM recovery via suggested_action.
    """
    error_type = "OptimisticConcurrencyError"

    def __init__(
        self,
        stream_id: str,
        expected_version: int,
        actual_version: int,
    ) -> None:
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"Stream '{stream_id}' version mismatch: "
            f"expected={expected_version}, actual={actual_version}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": str(self),
            "stream_id": self.stream_id,
            "expected_version": self.expected_version,
            "actual_version": self.actual_version,
            "suggested_action": "reload_stream_and_retry",
        }


class DomainError(LedgerError):
    """
    Raised when a business rule is violated.
    Carries rule_name so callers can route to the correct error handler.
    """
    error_type = "DomainError"

    def __init__(self, message: str, rule_name: str = "unknown") -> None:
        self.rule_name = rule_name
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": str(self),
            "rule_name": self.rule_name,
            "suggested_action": "check_business_rule_preconditions",
        }


class StreamNotFoundError(LedgerError):
    """Raised when loading a stream that does not exist."""
    error_type = "StreamNotFoundError"

    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id
        super().__init__(f"Stream '{stream_id}' not found")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": str(self),
            "stream_id": self.stream_id,
            "suggested_action": "verify_stream_id_and_aggregate_existence",
        }


class PreconditionFailedError(LedgerError):
    """
    Raised when an MCP tool call is missing a required precondition.
    e.g., record_credit_analysis called without an active agent session.
    """
    error_type = "PreconditionFailedError"

    def __init__(self, message: str, precondition: str) -> None:
        self.precondition = precondition
        super().__init__(message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": str(self),
            "precondition": self.precondition,
            "suggested_action": "satisfy_precondition_before_retry",
        }


class RetryBudgetExceededError(LedgerError):
    """Raised when optimistic concurrency retries are exhausted."""
    error_type = "RetryBudgetExceededError"

    def __init__(self, stream_id: str, attempts: int) -> None:
        self.stream_id = stream_id
        self.attempts = attempts
        super().__init__(
            f"Retry budget exhausted for stream '{stream_id}' after {attempts} attempts"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_type": self.error_type,
            "message": str(self),
            "stream_id": self.stream_id,
            "attempts": self.attempts,
            "suggested_action": "escalate_to_human_or_circuit_break",
        }


# =============================================================================
# BASE EVENT MODELS
# =============================================================================

class BaseEvent(BaseModel):
    """
    Base class for all domain events.
    Every domain event carries these fields before storage.
    """
    model_config = {"frozen": True, "extra": "forbid"}

    event_type: str = Field(description="Discriminator — set by each subclass")
    event_version: int = Field(default=1, description="Schema version for upcasting")

    def get_payload(self) -> dict[str, Any]:
        """Return payload dict excluding infrastructure fields."""
        exclude = {"event_type", "event_version"}
        return self.model_dump(exclude=exclude)

    def compute_hash(self) -> str:
        """SHA-256 hash of the canonical payload. Used in integrity chain."""
        payload_str = json.dumps(self.get_payload(), sort_keys=True, default=str)
        return hashlib.sha256(payload_str.encode()).hexdigest()


class StoredEvent(BaseModel):
    """
    An event as it exists in the event store — BaseEvent fields plus
    all infrastructure fields assigned at write time.
    """
    model_config = {"frozen": True}

    event_id:        UUID
    stream_id:       str
    stream_position: int
    global_position: int
    event_type:      str
    event_version:   int
    payload:         dict[str, Any]
    metadata:        dict[str, Any]
    recorded_at:     datetime

    def with_payload(
        self, new_payload: dict[str, Any], version: int
    ) -> "StoredEvent":
        """Return a new StoredEvent with updated payload and version (for upcasting)."""
        return StoredEvent(
            event_id=self.event_id,
            stream_id=self.stream_id,
            stream_position=self.stream_position,
            global_position=self.global_position,
            event_type=self.event_type,
            event_version=version,
            payload=new_payload,
            metadata=self.metadata,
            recorded_at=self.recorded_at,
        )

    def compute_hash(self) -> str:
        """SHA-256 hash of stored payload for integrity chain construction."""
        payload_str = json.dumps(self.payload, sort_keys=True, default=str)
        return hashlib.sha256(payload_str.encode()).hexdigest()


class StreamMetadata(BaseModel):
    """Metadata about an event stream from the event_streams table."""
    stream_id:       str
    aggregate_type:  str
    current_version: int
    created_at:      datetime
    archived_at:     datetime | None
    metadata:        dict[str, Any]

    @property
    def is_archived(self) -> bool:
        return self.archived_at is not None


# =============================================================================
# LOAN APPLICATION EVENTS
# =============================================================================

class ApplicationSubmitted(BaseEvent):
    """First event in every LoanApplication stream. Creates the aggregate."""
    event_type: str = "ApplicationSubmitted"
    event_version: int = 1

    application_id:      str
    applicant_id:        str
    requested_amount_usd: Decimal
    loan_purpose:        str
    submission_channel:  str
    submitted_at:        datetime = Field(default_factory=_utcnow)


class CreditAnalysisRequested(BaseEvent):
    """Signals that a CreditAnalysis agent has been assigned to this application."""
    event_type: str = "CreditAnalysisRequested"
    event_version: int = 1

    application_id:    str
    assigned_agent_id: str
    requested_at:      datetime = Field(default_factory=_utcnow)
    priority:          str = "NORMAL"   # NORMAL | HIGH | URGENT


class DecisionGenerated(BaseEvent):
    """
    Orchestrator agent synthesises all agent outputs into a recommendation.
    v2 adds model_versions dict. Upcaster handles v1 historical events.
    """
    event_type: str = "DecisionGenerated"
    event_version: int = 2

    application_id:           str
    orchestrator_agent_id:    str
    recommendation:           Recommendation
    confidence_score:         float = Field(ge=0.0, le=1.0)
    contributing_agent_sessions: list[str]   # list of AgentSession stream IDs
    decision_basis_summary:   str
    model_versions:           dict[str, str] = Field(default_factory=dict)
    generated_at:             datetime = Field(default_factory=_utcnow)


class HumanReviewCompleted(BaseEvent):
    """Loan officer's final review. May override the AI recommendation."""
    event_type: str = "HumanReviewCompleted"
    event_version: int = 1

    application_id:  str
    reviewer_id:     str
    override:        bool
    final_decision:  Recommendation
    override_reason: str | None = None
    reviewed_at:     datetime = Field(default_factory=_utcnow)

    @model_validator(mode="after")
    def override_requires_reason(self) -> "HumanReviewCompleted":
        if self.override and not self.override_reason:
            raise ValueError("override_reason is required when override=True")
        return self


class ApplicationApproved(BaseEvent):
    """Terminal event: application approved (pending or auto)."""
    event_type: str = "ApplicationApproved"
    event_version: int = 1

    application_id:    str
    approved_amount_usd: Decimal
    interest_rate:     float
    conditions:        list[str] = Field(default_factory=list)
    approved_by:       str       # human_id or "auto"
    effective_date:    datetime = Field(default_factory=_utcnow)


class ApplicationDeclined(BaseEvent):
    """Terminal event: application declined."""
    event_type: str = "ApplicationDeclined"
    event_version: int = 1

    application_id:              str
    decline_reasons:             list[str]
    declined_by:                 str
    adverse_action_notice_required: bool = True
    declined_at:                 datetime = Field(default_factory=_utcnow)


# =============================================================================
# AGENT SESSION EVENTS
# =============================================================================

class AgentContextLoaded(BaseEvent):
    """
    MANDATORY first event for every AgentSession stream.
    Implements the Gas Town pattern: agent declares its context source
    before any decision event. No decision may precede this event.
    """
    event_type: str = "AgentContextLoaded"
    event_version: int = 1

    agent_id:                str
    session_id:              str
    context_source:          str     # "fresh" | "replay" | "summary"
    event_replay_from_position: int = 0
    context_token_count:     int
    model_version:           str
    loaded_at:               datetime = Field(default_factory=_utcnow)


class CreditAnalysisCompleted(BaseEvent):
    """
    CreditAnalysis agent output. Version 2 — adds model_version, confidence_score,
    regulatory_basis. Version 1 historical events are upcasted at read time.
    """
    event_type: str = "CreditAnalysisCompleted"
    event_version: int = 2

    application_id:       str
    agent_id:             str
    session_id:           str
    model_version:        str
    confidence_score:     float | None = Field(default=None, ge=0.0, le=1.0)
    risk_tier:            RiskTier
    recommended_limit_usd: Decimal
    analysis_duration_ms: int
    input_data_hash:      str
    regulatory_basis:     str | None = None
    completed_at:         datetime = Field(default_factory=_utcnow)


class FraudScreeningCompleted(BaseEvent):
    """FraudDetection agent output."""
    event_type: str = "FraudScreeningCompleted"
    event_version: int = 1

    application_id:          str
    agent_id:                str
    fraud_score:             float = Field(ge=0.0, le=1.0)
    anomaly_flags:           list[str] = Field(default_factory=list)
    screening_model_version: str
    input_data_hash:         str
    screened_at:             datetime = Field(default_factory=_utcnow)


# =============================================================================
# COMPLIANCE EVENTS
# =============================================================================

class ComplianceCheckRequested(BaseEvent):
    """Initiates a compliance record for an application."""
    event_type: str = "ComplianceCheckRequested"
    event_version: int = 1

    application_id:        str
    regulation_set_version: str
    checks_required:       list[str]
    requested_at:          datetime = Field(default_factory=_utcnow)


class ComplianceRulePassed(BaseEvent):
    """A single compliance rule evaluation passed."""
    event_type: str = "ComplianceRulePassed"
    event_version: int = 1

    application_id:        str
    rule_id:               str
    rule_version:          str
    evaluation_timestamp:  datetime = Field(default_factory=_utcnow)
    evidence_hash:         str


class ComplianceRuleFailed(BaseEvent):
    """A single compliance rule evaluation failed."""
    event_type: str = "ComplianceRuleFailed"
    event_version: int = 1

    application_id:       str
    rule_id:              str
    rule_version:         str
    failure_reason:       str
    remediation_required: bool = True
    evaluation_timestamp: datetime = Field(default_factory=_utcnow)


class ComplianceClearanceIssued(BaseEvent):
    """
    All required compliance checks have passed. Prerequisite for ApplicationApproved.
    MISSING FROM ORIGINAL CATALOGUE — added to close the approval dependency gap.
    """
    event_type: str = "ComplianceClearanceIssued"
    event_version: int = 1

    application_id:        str
    regulation_set_version: str
    checks_passed:         list[str]
    issued_at:             datetime = Field(default_factory=_utcnow)
    issuing_agent_id:      str


# =============================================================================
# AUDIT LEDGER EVENTS
# =============================================================================

class AuditIntegrityCheckRun(BaseEvent):
    """
    Cryptographic integrity check result. Forms a hash chain across audit events.
    Each check records a hash of all preceding events + previous integrity hash.
    """
    event_type: str = "AuditIntegrityCheckRun"
    event_version: int = 1

    entity_id:            str
    entity_type:          str
    check_timestamp:      datetime = Field(default_factory=_utcnow)
    events_verified_count: int
    integrity_hash:       str   # sha256(previous_hash + all event hashes)
    previous_hash:        str   # hash from the previous AuditIntegrityCheckRun
    chain_valid:          bool
    tamper_detected:      bool = False


# =============================================================================
# ADDITIONAL EVENTS (identified as missing from original catalogue)
# =============================================================================

class ApplicationUnderReview(BaseEvent):
    """
    Transition event: application enters human review queue.
    MISSING FROM ORIGINAL CATALOGUE — required to complete the state machine.
    Without this, there is no event to trigger the PENDING_DECISION → human review transition.
    """
    event_type: str = "ApplicationUnderReview"
    event_version: int = 1

    application_id: str
    review_reason:  str
    queued_at:      datetime = Field(default_factory=_utcnow)


class AgentSessionClosed(BaseEvent):
    """
    Agent session completed normally or was terminated.
    MISSING FROM ORIGINAL CATALOGUE — required for session lifecycle tracking
    and to prevent orphaned sessions from blocking future analyses.
    """
    event_type: str = "AgentSessionClosed"
    event_version: int = 1

    agent_id:   str
    session_id: str
    reason:     str   # "completed" | "timeout" | "error" | "superseded"
    closed_at:  datetime = Field(default_factory=_utcnow)


class CreditAnalysisSuperseded(BaseEvent):
    """
    Marks a prior CreditAnalysisCompleted as superseded by human override.
    MISSING FROM ORIGINAL CATALOGUE — required for business rule 3 (model version locking):
    allows a second credit analysis after a HumanReviewOverride without violating the lock.
    """
    event_type: str = "CreditAnalysisSuperseded"
    event_version: int = 1

    application_id:     str
    original_session_id: str
    override_reason:    str
    superseded_at:      datetime = Field(default_factory=_utcnow)


# =============================================================================
# INTEGRITY CHECK RESULT (not stored — returned from run_integrity_check)
# =============================================================================

class IntegrityCheckResult(BaseModel):
    """Result of a cryptographic integrity check. Not stored as an event."""
    entity_id:            str
    entity_type:          str
    events_verified:      int
    chain_valid:          bool
    tamper_detected:      bool
    integrity_hash:       str
    previous_hash:        str
    check_timestamp:      datetime


# =============================================================================
# EVENT TYPE REGISTRY — maps string names to classes for deserialisation
# =============================================================================

EVENT_TYPE_REGISTRY: dict[str, type[BaseEvent]] = {
    "ApplicationSubmitted":       ApplicationSubmitted,
    "CreditAnalysisRequested":    CreditAnalysisRequested,
    "DecisionGenerated":          DecisionGenerated,
    "HumanReviewCompleted":       HumanReviewCompleted,
    "ApplicationApproved":        ApplicationApproved,
    "ApplicationDeclined":        ApplicationDeclined,
    "AgentContextLoaded":         AgentContextLoaded,
    "CreditAnalysisCompleted":    CreditAnalysisCompleted,
    "FraudScreeningCompleted":    FraudScreeningCompleted,
    "ComplianceCheckRequested":   ComplianceCheckRequested,
    "ComplianceRulePassed":       ComplianceRulePassed,
    "ComplianceRuleFailed":       ComplianceRuleFailed,
    "ComplianceClearanceIssued":  ComplianceClearanceIssued,
    "AuditIntegrityCheckRun":     AuditIntegrityCheckRun,
    "ApplicationUnderReview":     ApplicationUnderReview,
    "AgentSessionClosed":         AgentSessionClosed,
    "CreditAnalysisSuperseded":   CreditAnalysisSuperseded,
}
