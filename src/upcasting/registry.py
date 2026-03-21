"""
src/upcasting/registry.py
==========================
UpcasterRegistry — transparent event schema migration at read time.

Core guarantee (tested in tests/test_upcasting.py):
  - v1 events stored in DB remain v1 forever (immutability)
  - When loaded via EventStore.load_stream(), they arrive as v2
  - The stored payload is NEVER touched

Design: upcasters form a chain. If v1→v2 and v2→v3 are registered,
loading a v1 event applies both automatically.

Usage:
    registry = UpcasterRegistry()

    @registry.register("CreditAnalysisCompleted", from_version=1)
    def upcast_credit_v1_to_v2(payload: dict) -> dict:
        return {**payload, "model_version": "legacy"}

    # Wire into EventStore:
    from src.event_store import set_upcaster_registry
    set_upcaster_registry(registry)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.models.events import StoredEvent

logger = logging.getLogger(__name__)


class UpcasterRegistry:
    """
    Central registry for all event version migrations.

    Thread-safe for reads (dict lookup).
    Registration happens at import time — no runtime mutation after startup.
    """

    def __init__(self) -> None:
        # Key: (event_type, from_version) → upcaster function
        self._upcasters: dict[tuple[str, int], Callable[[dict], dict]] = {}

    def register(self, event_type: str, from_version: int) -> Callable:
        """
        Decorator. Registers fn as upcaster from event_type@from_version
        to from_version+1.

        Example:
            @registry.register("CreditAnalysisCompleted", from_version=1)
            def upcast_v1_to_v2(payload: dict) -> dict:
                return {**payload, "model_version": "legacy"}
        """
        def decorator(fn: Callable[[dict], dict]) -> Callable[[dict], dict]:
            key = (event_type, from_version)
            if key in self._upcasters:
                logger.warning(
                    "Upcaster already registered for %s@v%d — overwriting",
                    event_type, from_version,
                )
            self._upcasters[key] = fn
            logger.debug(
                "Registered upcaster: %s v%d → v%d",
                event_type, from_version, from_version + 1,
            )
            return fn
        return decorator

    def upcast(self, event: "StoredEvent") -> "StoredEvent":
        """
        Apply all registered upcasters for this event type in version order.

        CRITICAL: Returns a NEW StoredEvent with updated payload and version.
        The original StoredEvent (and the DB row it came from) is NEVER mutated.
        This is the immutability guarantee tested in test_upcasting.py.
        """
        current = event
        v = event.event_version

        while (event.event_type, v) in self._upcasters:
            upcaster_fn = self._upcasters[(event.event_type, v)]
            try:
                new_payload = upcaster_fn(current.payload)
                current = current.with_payload(new_payload, version=v + 1)
                logger.debug(
                    "Upcasted %s: v%d → v%d (event_id=%s)",
                    event.event_type, v, v + 1, event.event_id,
                )
                v += 1
            except Exception as e:
                logger.error(
                    "Upcaster failed for %s@v%d (event_id=%s): %s",
                    event.event_type, v, event.event_id, e,
                )
                # Return best-effort partial upcast rather than crashing
                break

        return current

    def is_registered(self, event_type: str, from_version: int) -> bool:
        return (event_type, from_version) in self._upcasters

    def registered_upcasters(self) -> list[tuple[str, int]]:
        """Return list of (event_type, from_version) for all registered upcasters."""
        return list(self._upcasters.keys())
