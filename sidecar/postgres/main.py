"""
Sidecar PostgreSQL — point d'entrée FastAPI + Strawberry

Le schéma PostgreSQL est créé automatiquement au démarrage
et supprimé proprement à l'arrêt.

Démarrage :
    uvicorn sidecar.postgres.main:app --port 8001 --reload
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

from .schema import close_pool, get_pool, schema

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS benchmark_data (
    id   BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_benchmark_data_gin
    ON benchmark_data USING GIN (data);
"""

DROP_SQL = """
DROP TABLE IF EXISTS benchmark_data;
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SQL)
    yield
    async with pool.acquire() as conn:
        await conn.execute(DROP_SQL)
    await close_pool()


app = FastAPI(title="DB Benchmarker — Sidecar PostgreSQL", lifespan=lifespan)

graphql_app = GraphQLRouter(schema, graphql_ide="graphiql")
app.include_router(graphql_app, prefix="/graphql")


@app.get("/health", summary="Health check")
async def health():
    pool = await get_pool()
    async with pool.acquire() as conn:
        version = await conn.fetchval("SELECT version()")
        count = await conn.fetchval("SELECT COUNT(*) FROM benchmark_data")
    return {"status": "ok", "db": version, "rows": count}
