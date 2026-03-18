"""
src/aggregates/compliance_record.py
=====================================
ComplianceRecordAggregate — regulatory check lifecycle for a single application.

Separated from LoanApplication to eliminate write contention:
  - ComplianceAgent writes exclusively to compliance-{id} streams
  - CreditAnalysisAgent writes exclusively to loan-{id} streams
  - No cross-stream lock contention under parallel agent execution
  See DESIGN.md §1 for full boundary justification.

Key invariants:
  - Cannot issue ComplianceClearanceIssued unless ALL required checks passed
  - Every check must reference a specific regulation rule version
  - Failed checks block clearance until remediated or waived
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from src.models.events import (
    ComplianceClearanceIssued,
    ComplianceCheckRequested,
    ComplianceRuleFailed,
    ComplianceRulePassed,
    ComplianceStatus,
    DomainError,
    StoredEvent,
)

if TYPE_CHECKING:
    from src.event_store import EventStore

logger = logging.getLogger(__name__)


class ComplianceRecordAggregate:
    """
    Tracks all compliance checks for one loan application.
    Stream ID: compliance-{application_id}
    """

    def __init__(self, application_id: str) -> None:
        self.application_id = application_id
        self.version: int = 0

        self.regulation_set_version: str | None = None
        self.checks_required: list[str] = []

        # Per-rule tracking
        self.passed_rules: dict[str, str] = {}   # rule_id → rule_version
        self.failed_rules: dict[str, str] = {}   # rule_id → failure_reason

        # Clearance state
        self.clearance_issued: bool = False
        self.clearance_issued_at: datetime | None = None
        self.initiated: bool = False

    # -------------------------------------------------------------------------
    # FACTORY
    # -------------------------------------------------------------------------

    @classmethod
    async def load(
        cls,
        store: "EventStore",
        application_id: str,
    ) -> "ComplianceRecordAggregate":
        """Reconstruct compliance record from event stream."""
        stream_id = f"compliance-{application_id}"
        events = await store.load_stream(stream_id)
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    # -------------------------------------------------------------------------
    # EVENT APPLICATION
    # -------------------------------------------------------------------------

    def _apply(self, event: StoredEvent) -> None:
        handler_name = f"_on_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self.initiated = True
        self.regulation_set_version = event.payload["regulation_set_version"]
        self.checks_required = event.payload.get("checks_required", [])

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        rule_id = event.payload["rule_id"]
        self.passed_rules[rule_id] = event.payload["rule_version"]
        # Remove from failed if it was previously failed and re-evaluated
        self.failed_rules.pop(rule_id, None)

    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        rule_id = event.payload["rule_id"]
        self.failed_rules[rule_id] = event.payload["failure_reason"]
        # If a rule fails after passing (re-evaluation), remove from passed
        self.passed_rules.pop(rule_id, None)

    def _on_ComplianceClearanceIssued(self, event: StoredEvent) -> None:
        self.clearance_issued = True
        self.clearance_issued_at = event.recorded_at

    # -------------------------------------------------------------------------
    # COMPUTED PROPERTIES
    # -------------------------------------------------------------------------

    @property
    def all_required_checks_passed(self) -> bool:
        """True if every required check has a passing result and none have failed."""
        if not self.checks_required:
            return False
        required_set = set(self.checks_required)
        passed_set = set(self.passed_rules.keys())
        failed_set = set(self.failed_rules.keys())
        return required_set.issubset(passed_set) and not (required_set & failed_set)

    @property
    def pending_checks(self) -> list[str]:
        """Rules required but not yet evaluated."""
        evaluated = set(self.passed_rules) | set(self.failed_rules)
        return [r for r in self.checks_required if r not in evaluated]

    @property
    def overall_status(self) -> ComplianceStatus:
        if self.clearance_issued:
            return ComplianceStatus.PASSED
        if self.failed_rules:
            return ComplianceStatus.FAILED
        return ComplianceStatus.PENDING

    # -------------------------------------------------------------------------
    # ASSERTION HELPERS
    # -------------------------------------------------------------------------

    def assert_initiated(self) -> None:
        """Raise DomainError if no ComplianceCheckRequested has been issued."""
        if not self.initiated:
            raise DomainError(
                f"Compliance record for application '{self.application_id}' "
                "has not been initiated. ComplianceCheckRequested must precede "
                "any rule evaluation.",
                rule_name="ComplianceNotInitiated",
            )

    def assert_rule_in_required_set(self, rule_id: str) -> None:
        """Raise DomainError if rule_id is not in the required checks list."""
        if self.checks_required and rule_id not in self.checks_required:
            raise DomainError(
                f"Rule '{rule_id}' is not in the required compliance checks "
                f"for application '{self.application_id}'. "
                f"Required: {self.checks_required}",
                rule_name="UnknownComplianceRule",
            )

    def assert_clearance_not_already_issued(self) -> None:
        """Clearance is a one-way door — cannot be re-issued."""
        if self.clearance_issued:
            raise DomainError(
                f"Compliance clearance for application '{self.application_id}' "
                "has already been issued.",
                rule_name="DuplicateClearance",
            )

    def assert_all_checks_passed(self) -> None:
        """
        Business Rule 5 (enforcement point in ComplianceRecord):
        Cannot issue clearance unless all required checks have passed.
        """
        if not self.all_required_checks_passed:
            pending = self.pending_checks
            failed = list(self.failed_rules.keys())
            raise DomainError(
                f"Cannot issue compliance clearance for application "
                f"'{self.application_id}'. "
                f"Pending checks: {pending}. Failed checks: {failed}.",
                rule_name="ComplianceDependency",
            )
