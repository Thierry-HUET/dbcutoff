"""
Tests relationnels — REL1, REL2, REL3, REL4
Nécessitent des opérations GraphQL étendues côté sidecar.
"""
from __future__ import annotations

import asyncio
import statistics
import time

from gql import Client, gql
from gql.transport.httpx import HTTPXAsyncTransport


def _client(url: str) -> Client:
    transport = HTTPXAsyncTransport(url=url)
    return Client(transport=transport, fetch_schema_from_transport=False)


async def _timed(coro):
    t0 = time.perf_counter()
    result = await coro
    return result, time.perf_counter() - t0


REL1_QUERY = gql("""
    query Rel1($limit: Int) {
        joinSiren(limit: $limit) { id data }
    }
""")

REL2_QUERY = gql("""
    query Rel2 {
        joinSirenAgg { siren count }
    }
""")

REL3_QUERY = gql("""
    query Rel3($limit: Int) {
        sirenSubquery(limit: $limit) { id data }
    }
""")


async def _run(url: str, test_code: str, query, variables: dict | None = None) -> dict:
    try:
        async with _client(url) as session:
            result, duree = await _timed(
                session.execute(query, variable_values=variables or {})
            )
        first_key = next(iter(result))
        rows = result[first_key]
        volume = len(rows) if isinstance(rows, list) else 1
        return {
            "test_code": test_code,
            "duree_s": duree,
            "latence_p50": duree * 1000,
            "throughput": volume / duree if duree > 0 else 0,
            "volume": volume,
            "erreurs": 0,
        }
    except Exception as e:
        return {
            "test_code": test_code,
            "duree_s": 0,
            "erreurs": 1,
            "note": f"non supporté ou erreur : {e}",
        }


async def rel1_join_siren(url: str, limit: int = 1000) -> dict:
    return await _run(url, "REL1", REL1_QUERY, {"limit": limit})


async def rel2_join_agg(url: str) -> dict:
    return await _run(url, "REL2", REL2_QUERY)


async def rel3_subquery(url: str, limit: int = 1000) -> dict:
    return await _run(url, "REL3", REL3_QUERY, {"limit": limit})


async def rel4_join_concurrent(url: str, limit: int = 100, concurrence: int = 10) -> dict:
    semaphore = asyncio.Semaphore(concurrence)
    latences = []
    erreurs = 0

    async def one():
        nonlocal erreurs
        async with semaphore:
            r = await rel1_join_siren(url, limit)
            if r["erreurs"]:
                erreurs += 1
            else:
                latences.append(r["duree_s"] * 1000)

    t0 = time.perf_counter()
    await asyncio.gather(*[one() for _ in range(concurrence)])
    total_s = time.perf_counter() - t0

    latences.sort()
    n = len(latences)
    return {
        "test_code": "REL4",
        "duree_s": total_s,
        "latence_p50": statistics.median(latences) if latences else None,
        "latence_p95": latences[int(n * 0.95)] if n > 0 else None,
        "latence_p99": latences[int(n * 0.99)] if n > 0 else None,
        "throughput": n / total_s if total_s > 0 else 0,
        "concurrence": concurrence,
        "erreurs": erreurs,
    }
