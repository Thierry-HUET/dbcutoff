"""
Tests universels — W1, W2, R1, R2, R3, R4, C1
Appliqués à toutes les bases via le sidecar GraphQL

Protocole de mesure :
- ITERATIONS = 3 exécutions par test
- La 1ère itération (warm-up) est exclue du calcul des métriques
- C1 : 4 niveaux de concurrence (10, 50, 100, 500) × 3 opérations (R1, R4, W2)
"""
from __future__ import annotations

import asyncio
import statistics
import time
from typing import Any

from gql import Client, gql
from gql.transport.httpx import HTTPXAsyncTransport

ITERATIONS       = 3
WARMUP           = 1
CONCURRENCE_LEVELS = [10, 50, 100, 500]


def _client(url: str) -> Client:
    transport = HTTPXAsyncTransport(url=url)
    return Client(transport=transport, fetch_schema_from_transport=False)


async def _timed(coro) -> tuple[Any, float]:
    t0 = time.perf_counter()
    result = await coro
    return result, time.perf_counter() - t0


def _stats(latences_ms: list[float]) -> dict:
    if not latences_ms:
        return {"latence_p50": None, "latence_p95": None, "latence_p99": None, "throughput": None, "duree_s": 0}
    s = sorted(latences_ms)
    n = len(s)
    total_s = sum(s) / 1000
    return {
        "latence_p50": statistics.median(s),
        "latence_p95": s[min(int(n * 0.95), n - 1)],
        "latence_p99": s[min(int(n * 0.99), n - 1)],
        "throughput":  n / total_s if total_s > 0 else 0,
        "duree_s":     total_s,
    }


# ---------------------------------------------------------------------------
# W1 — Insertion en masse
# ---------------------------------------------------------------------------
async def w1_bulk_insert(url: str, records: list[dict]) -> dict:
    Q = gql("""
        mutation BulkInsert($data: [JSON!]!) {
            insertBulk(data: $data) { inserted errors }
        }
    """)
    T = gql("mutation { truncate }")
    async with _client(url) as session:
        await session.execute(T)
        result, duree = await _timed(
            session.execute(Q, variable_values={"data": records})
        )
    bulk = result["insertBulk"]
    return {
        "test_code": "W1",
        "duree_s":   duree,
        "volume":    len(records),
        "erreurs":   bulk["errors"],
        "throughput": bulk["inserted"] / duree if duree > 0 else 0,
    }


# ---------------------------------------------------------------------------
# W2 — Insertion unitaire
# ---------------------------------------------------------------------------
async def w2_insert_one(url: str, record: dict) -> dict:
    Q = gql("""
        mutation InsertOne($data: JSON!) {
            insertOne(data: $data)
        }
    """)
    latences = []
    erreurs = 0
    async with _client(url) as session:
        for i in range(ITERATIONS):
            try:
                _, duree = await _timed(
                    session.execute(Q, variable_values={"data": record})
                )
                if i >= WARMUP:
                    latences.append(duree * 1000)
            except Exception:
                if i >= WARMUP:
                    erreurs += 1
    return {"test_code": "W2", "erreurs": erreurs, "volume": ITERATIONS - WARMUP, **_stats(latences)}


# ---------------------------------------------------------------------------
# R1 — Lecture par identifiant
# ---------------------------------------------------------------------------
async def r1_get_by_id(url: str, ids: list[str]) -> dict:
    Q = gql("""
        query GetById($id: String!) {
            getById(id: $id) { id data }
        }
    """)
    latences = []
    erreurs = 0
    async with _client(url) as session:
        for id_ in ids:
            for i in range(ITERATIONS):
                try:
                    _, duree = await _timed(
                        session.execute(Q, variable_values={"id": id_})
                    )
                    if i >= WARMUP:
                        latences.append(duree * 1000)
                except Exception:
                    if i >= WARMUP:
                        erreurs += 1
    return {"test_code": "R1", "erreurs": erreurs, "volume": len(ids) * (ITERATIONS - WARMUP), **_stats(latences)}


# ---------------------------------------------------------------------------
# R2 / R3 — Lecture filtrée
# ---------------------------------------------------------------------------
async def _filter_test(url: str, test_code: str, field: str, value: str, limit: int = 100) -> dict:
    Q = gql("""
        query Filter($field: String!, $value: String!, $limit: Int) {
            filter(field: $field, value: $value, limit: $limit) { id data }
        }
    """)
    latences = []
    erreurs = 0
    nb = 0
    async with _client(url) as session:
        for i in range(ITERATIONS):
            try:
                result, duree = await _timed(
                    session.execute(Q, variable_values={"field": field, "value": value, "limit": limit})
                )
                if i >= WARMUP:
                    latences.append(duree * 1000)
                    nb = len(result.get("filter", []))
            except Exception:
                if i >= WARMUP:
                    erreurs += 1
    return {"test_code": test_code, "erreurs": erreurs, "volume": nb, **_stats(latences)}


async def r2_filter_indexed(url: str, field: str, value: str) -> dict:
    return await _filter_test(url, "R2", field, value)


async def r3_filter_unindexed(url: str, field: str, value: str) -> dict:
    return await _filter_test(url, "R3", field, value)


# ---------------------------------------------------------------------------
# R4 — Comptage global
# ---------------------------------------------------------------------------
async def r4_count(url: str) -> dict:
    Q = gql("query { count }")
    latences = []
    erreurs = 0
    volume = 0
    async with _client(url) as session:
        for i in range(ITERATIONS):
            try:
                result, duree = await _timed(session.execute(Q))
                if i >= WARMUP:
                    latences.append(duree * 1000)
                    volume = result.get("count", 0)
            except Exception:
                if i >= WARMUP:
                    erreurs += 1
    return {"test_code": "R4", "erreurs": erreurs, "volume": volume, **_stats(latences)}


# ---------------------------------------------------------------------------
# Helpers concurrence
# ---------------------------------------------------------------------------
async def _concurrent_r1(url: str, ids: list[str], concurrence: int) -> dict:
    Q = gql("""
        query GetById($id: String!) { getById(id: $id) { id } }
    """)
    semaphore = asyncio.Semaphore(concurrence)
    latences  = []
    erreurs   = 0

    async def _one(id_: str):
        nonlocal erreurs
        async with semaphore:
            async with _client(url) as s:
                try:
                    _, d = await _timed(s.execute(Q, variable_values={"id": id_}))
                    latences.append(d * 1000)
                except Exception:
                    erreurs += 1

    t0 = time.perf_counter()
    await asyncio.gather(*[_one(id_) for id_ in ids])
    total_s = time.perf_counter() - t0
    latences.sort()
    n = len(latences)
    return {
        "test_code":   f"C1_R1_c{concurrence}",
        "concurrence": concurrence,
        "operation":   "R1",
        "duree_s":     total_s,
        "latence_p50": statistics.median(latences) if latences else None,
        "latence_p95": latences[min(int(n * 0.95), n - 1)] if n > 0 else None,
        "latence_p99": latences[min(int(n * 0.99), n - 1)] if n > 0 else None,
        "throughput":  n / total_s if total_s > 0 else 0,
        "volume":      len(ids),
        "erreurs":     erreurs,
    }


async def _concurrent_r4(url: str, concurrence: int) -> dict:
    Q = gql("query { count }")
    semaphore = asyncio.Semaphore(concurrence)
    latences  = []
    erreurs   = 0

    async def _one():
        nonlocal erreurs
        async with semaphore:
            async with _client(url) as s:
                try:
                    _, d = await _timed(s.execute(Q))
                    latences.append(d * 1000)
                except Exception:
                    erreurs += 1

    t0 = time.perf_counter()
    await asyncio.gather(*[_one() for _ in range(concurrence)])
    total_s = time.perf_counter() - t0
    latences.sort()
    n = len(latences)
    return {
        "test_code":   f"C1_R4_c{concurrence}",
        "concurrence": concurrence,
        "operation":   "R4",
        "duree_s":     total_s,
        "latence_p50": statistics.median(latences) if latences else None,
        "latence_p95": latences[min(int(n * 0.95), n - 1)] if n > 0 else None,
        "latence_p99": latences[min(int(n * 0.99), n - 1)] if n > 0 else None,
        "throughput":  n / total_s if total_s > 0 else 0,
        "volume":      concurrence,
        "erreurs":     erreurs,
    }


async def _concurrent_w2(url: str, record: dict, concurrence: int) -> dict:
    Q = gql("""
        mutation InsertOne($data: JSON!) { insertOne(data: $data) }
    """)
    semaphore = asyncio.Semaphore(concurrence)
    latences  = []
    erreurs   = 0

    async def _one():
        nonlocal erreurs
        async with semaphore:
            async with _client(url) as s:
                try:
                    _, d = await _timed(s.execute(Q, variable_values={"data": record}))
                    latences.append(d * 1000)
                except Exception:
                    erreurs += 1

    t0 = time.perf_counter()
    await asyncio.gather(*[_one() for _ in range(concurrence)])
    total_s = time.perf_counter() - t0
    latences.sort()
    n = len(latences)
    return {
        "test_code":   f"C1_W2_c{concurrence}",
        "concurrence": concurrence,
        "operation":   "W2",
        "duree_s":     total_s,
        "latence_p50": statistics.median(latences) if latences else None,
        "latence_p95": latences[min(int(n * 0.95), n - 1)] if n > 0 else None,
        "latence_p99": latences[min(int(n * 0.99), n - 1)] if n > 0 else None,
        "throughput":  n / total_s if total_s > 0 else 0,
        "volume":      concurrence,
        "erreurs":     erreurs,
    }


# ---------------------------------------------------------------------------
# C1 — Concurrence : 4 niveaux × 3 opérations → liste de 12 résultats
# ---------------------------------------------------------------------------
async def c1_concurrent(url: str, ids: list[str], record: dict | None = None) -> list[dict]:
    results = []
    for c in CONCURRENCE_LEVELS:
        results.append(await _concurrent_r1(url, ids, c))
        results.append(await _concurrent_r4(url, c))
        if record:
            results.append(await _concurrent_w2(url, record, c))
    return results
