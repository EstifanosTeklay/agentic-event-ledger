"""
src/db.py
=========
Database connection pool management.
Uses asyncpg for high-performance async PostgreSQL access.
"""

from __future__ import annotations

import asyncpg
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    database_url: str = "postgresql://postgres:123@localhost:5432/ledger"
    async_database_url: str = "postgresql://postgres:123@localhost:5432/ledger"

    application_summary_slo_ms: int = 500
    compliance_audit_slo_ms: int = 2000

    projection_poll_interval_ms: int = 100
    projection_batch_size: int = 500
    projection_max_retries: int = 3

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()


_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """Return the shared connection pool, creating it if necessary."""
    global _pool
    if _pool is None:
        settings = get_settings()
        # asyncpg uses plain postgresql:// URLs (no +asyncpg prefix)
        url = settings.async_database_url.replace("postgresql+asyncpg://", "postgresql://")
        _pool = await asyncpg.create_pool(
            dsn=url,
            min_size=2,
            max_size=20,
            command_timeout=30,
        )
    return _pool


async def close_pool() -> None:
    """Gracefully close the connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
