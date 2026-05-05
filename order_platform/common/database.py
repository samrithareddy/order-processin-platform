from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg

from order_platform.common.config import Settings, get_settings


class Database:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self.settings.database_url)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def execute(self, query: str, *args: object) -> str:
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)

    async def fetchrow(self, query: str, *args: object) -> asyncpg.Record | None:
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args)

    async def fetch(self, query: str, *args: object) -> list[asyncpg.Record]:
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database pool has not been initialized")
        return self._pool

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as connection:
            yield connection

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                yield connection


async def record_processed_event(
    connection: asyncpg.Connection,
    consumer_name: str,
    event_id: str,
) -> bool:
    result = await connection.execute(
        """
        INSERT INTO processed_events (consumer_name, event_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        consumer_name,
        event_id,
    )
    return result.endswith("1")
