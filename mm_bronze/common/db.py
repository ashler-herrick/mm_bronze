from typing import Optional

import asyncpg

from mm_bronze.common.config import settings


__pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """
    Returns a singleton asyncpg pool.
    Usage:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("…")
    """
    global __pool
    if __pool is None:
        __pool = await asyncpg.create_pool(
            dsn=settings.postgres_dsn,
            min_size=settings.db_min_size,
            max_size=settings.db_max_size,
        )
    return __pool


async def close_pool() -> None:
    """
    Gracefully close the global asyncpg pool (if it exists).
    Call this on application shutdown.
    """
    global __pool
    if __pool is not None:
        await __pool.close()
        __pool = None
