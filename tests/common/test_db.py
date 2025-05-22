import pytest
import asyncio

from mm_bronze.common.db import get_pool


@pytest.mark.asyncio
async def test_postgres_insert_fetch():
    pool = await get_pool()

    async with pool.acquire() as conn:
        # create a temp table
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS test_integration(id serial PRIMARY KEY, val text);"
        )
        # insert
        await conn.execute("INSERT INTO test_integration(val) VALUES($1);", "hello")
        # fetch
        rows = await conn.fetch("SELECT val FROM test_integration;")
        assert any(r["val"] == "hello" for r in rows)

    # clean up
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE test_integration;")


async def _debug():
    pool = await get_pool()

    async with pool.acquire() as conn:
        # create a temp table
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS test_integration(id serial PRIMARY KEY, val text);"
        )
        # insert
        await conn.execute("INSERT INTO test_integration(val) VALUES($1);", "hello")
        # fetch
        rows = await conn.fetch("SELECT val FROM test_integration;")
        print(rows)
        assert any(r["val"] == "hello" for r in rows)

    # clean up
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE test_integration;")


if __name__ == "__main__":
    from mm_bronze.common.config import settings

    print(settings.postgres_dsn)

    asyncio.run(_debug(), debug=True)
