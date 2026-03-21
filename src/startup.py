"""
src/startup.py
==============
Application startup — wires all infrastructure together.

Call initialise() once at application startup (before any EventStore use).
Safe to call multiple times (idempotent).
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_initialised = False


def initialise() -> None:
    """Wire upcaster registry into EventStore. Call once at startup."""
    global _initialised
    if _initialised:
        return

    from src.upcasting.upcasters import default_registry
    from src.event_store import set_upcaster_registry

    set_upcaster_registry(default_registry)
    logger.info(
        "Startup complete. Upcasters registered: %s",
        default_registry.registered_upcasters(),
    )
    _initialised = True
