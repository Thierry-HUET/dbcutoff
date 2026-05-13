"""
Sidecar PostgreSQL — schéma Strawberry + resolvers
Implémente le contrat défini dans sidecar/schema.graphql
"""
from __future__ import annotations

import json
from typing import Optional

import asyncpg
import strawberry
from strawberry.scalars import JSON

from .config import POSTGRES_DSN


# ---------------------------------------------------------------------------
# Pool de connexions
# ---------------------------------------------------------------------------
_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------
@strawberry.type
class Record:
    id: str
    data: JSON


@strawberry.type
class BulkResult:
    inserted: int
    errors: int


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------
@strawberry.type
class Query:

    @strawberry.field
    async def get_by_id(self, id: str) -> Optional[Record]:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id::text, row_to_json(t)::text AS data FROM benchmark_data t WHERE id = $1",
                id,
            )
        if row is None:
            return None
        return Record(id=row["id"], data=json.loads(row["data"]))

    @strawberry.field
    async def filter(self, field: str, value: str, limit: Optional[int] = 100) -> list[Record]:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id::text, row_to_json(t)::text AS data
                FROM benchmark_data t
                WHERE data->>$1 = $2
                LIMIT $3
                """,
                field, value, limit,
            )
        return [Record(id=r["id"], data=json.loads(r["data"])) for r in rows]

    @strawberry.field
    async def count(self) -> int:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT COUNT(*) FROM benchmark_data")
        return int(result)

    @strawberry.field
    async def search(self, query: str, limit: Optional[int] = 10) -> list[Record]:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id::text, row_to_json(t)::text AS data
                FROM benchmark_data t
                WHERE to_tsvector('french', data::text) @@ plainto_tsquery('french', $1)
                LIMIT $2
                """,
                query, limit,
            )
        return [Record(id=r["id"], data=json.loads(r["data"])) for r in rows]


# ---------------------------------------------------------------------------
# Mutation
# ---------------------------------------------------------------------------
@strawberry.type
class Mutation:

    @strawberry.mutation
    async def insert_one(self, data: JSON) -> bool:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO benchmark_data (data) VALUES ($1::jsonb)",
                json.dumps(data),
            )
        return True

    @strawberry.mutation
    async def insert_bulk(self, data: list[JSON]) -> BulkResult:
        pool = await get_pool()
        inserted = 0
        errors = 0
        async with pool.acquire() as conn:
            async with conn.transaction():
                for record in data:
                    try:
                        await conn.execute(
                            "INSERT INTO benchmark_data (data) VALUES ($1::jsonb)",
                            json.dumps(record),
                        )
                        inserted += 1
                    except Exception:
                        errors += 1
        return BulkResult(inserted=inserted, errors=errors)

    @strawberry.mutation
    async def truncate(self) -> bool:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE benchmark_data RESTART IDENTITY")
        return True


# ---------------------------------------------------------------------------
# Schéma exporté
# ---------------------------------------------------------------------------
schema = strawberry.Schema(query=Query, mutation=Mutation)
