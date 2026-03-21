"""
src/mcp/dependencies.py
========================
Shared database pool and EventStore for MCP tools and resources.
Lazy-initialised on first use.
"""

from __future__ import annotations

import asyncpg

from src.db import get_pool
from src.event_store import EventStore

_store: EventStore | None = None


async def get_store() -> EventStore:
    """Return the shared EventStore instance."""
    global _store
    if _store is None:
        pool = await get_pool()
        _store = EventStore(pool=pool)
    return _store


async def get_db_pool() -> asyncpg.Pool:
    """Return the shared asyncpg pool."""
    return await get_pool()
