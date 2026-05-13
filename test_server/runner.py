"""
Orchestrateur de tests
Reçoit : db_name, sidecar_url, source de données, liste de tests
Retourne : résultats + persistance SQLite

Protocole de progression géométrique (√10) :
  MAX_ROWS=10k   → [1k, 3.16k, 10k]
  MAX_ROWS=100k  → [1k, 3.16k, 10k, 31.6k, 100k]
  MAX_ROWS=100M  → [1k, 3.16k, 10k, ..., 100M]
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
from typing import Callable

from .loader import DataSource, load, sample_ids
from .storage import init_db, new_run_id, save_result
from .tests.universal import (
    c1_concurrent,
    r1_get_by_id,
    r2_filter_indexed,
    r3_filter_unindexed,
    r4_count,
    w1_bulk_insert,
    w2_insert_one,
)
from .tests.relational import (
    rel1_join_siren,
    rel2_join_agg,
    rel3_subquery,
    rel4_join_concurrent,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("gql").setLevel(logging.WARNING)
logging.getLogger("gql.transport").setLevel(logging.WARNING)
logging.getLogger("gql.transport.httpx").setLevel(logging.WARNING)

log = logging.getLogger("runner")

MAX_ROWS: int = int(os.getenv("MAX_ROWS", "10000").replace("_", ""))

# C1 retourne une liste — les autres retournent un dict
C1_TEST = "C1"

TEST_REGISTRY: dict[str, Callable] = {
    "W1":   w1_bulk_insert,
    "W2":   w2_insert_one,
    "R1":   r1_get_by_id,
    "R2":   r2_filter_indexed,
    "R3":   r3_filter_unindexed,
    "R4":   r4_count,
    "C1":   c1_concurrent,
    "REL1": rel1_join_siren,
    "REL2": rel2_join_agg,
    "REL3": rel3_subquery,
    "REL4": rel4_join_concurrent,
}


def available_tests() -> list[str]:
    return list(TEST_REGISTRY.keys())


def geometric_steps(max_rows: int, base: int = 1000) -> list[int]:
    steps = []
    n = base
    while n < max_rows:
        steps.append(round(n))
        n *= math.sqrt(10)
    steps.append(max_rows)
    return sorted(set(steps))


def _log_result(result: dict) -> None:
    err  = result.get("erreurs", 0)
    p50  = result.get("latence_p50")
    thr  = result.get("throughput")
    note = result.get("note", "")
    c    = result.get("concurrence")
    code = result.get("test_code", "?")
    msg  = f"  {'✓' if not err else '✗'} {code:<16} {result.get('duree_s', 0):.3f}s"
    if c:    msg += f"  c={c}"
    if p50:  msg += f"  p50={p50:.1f}ms"
    if thr:  msg += f"  {thr:.0f} req/s"
    if err:  msg += f"  erreurs={err}"
    if note: msg += f"  [{note}]"
    log.info(msg)


def _persist(result: dict, run_id: str, db_name: str, step: int) -> None:
    save_result(
        run_id=run_id,
        db_name=db_name,
        test_code=result["test_code"],
        duree_s=result.get("duree_s", 0),
        latence_p50=result.get("latence_p50"),
        latence_p95=result.get("latence_p95"),
        latence_p99=result.get("latence_p99"),
        throughput=result.get("throughput"),
        concurrence=result.get("concurrence"),
        volume=step,
        erreurs=result.get("erreurs", 0),
    )


def run_benchmark(
    db_name: str,
    sidecar_url: str,
    test_codes: list[str],
    source: DataSource = "insee",
    test_kwargs: dict[str, dict] | None = None,
) -> dict:
    init_db()
    run_id = new_run_id()
    test_kwargs = test_kwargs or {}
    all_results = []

    steps = geometric_steps(MAX_ROWS)
    needs_data = any(c in test_codes for c in ("W1", "W2", "R1", "C1", "R2", "R3"))

    log.info("run_id=%s  db=%s  source=%s", run_id, db_name, source)
    log.info("paliers : %s", steps)
    log.info("tests   : %s", test_codes)

    full_records: list[dict] = []
    if needs_data:
        log.info("chargement source '%s' (%d lignes max)…", source, MAX_ROWS)
        full_records = load(source, max_rows=MAX_ROWS)
        log.info("%d enregistrements chargés", len(full_records))

    async def _execute_step(step: int):
        records = full_records[:step]
        ids     = sample_ids(records, n=min(200, len(records)))
        record  = records[0] if records else {}
        log.info("─── volume=%d ───────────────────────────────", step)
        step_results = []
        total = len(test_codes)

        for idx, code in enumerate(test_codes, 1):
            log.info("[%d/%d] %s  volume=%d", idx, total, code, step)

            kwargs: dict = {"url": sidecar_url}
            kwargs.update(test_kwargs.get(code, {}))

            if code == "W1":
                kwargs.setdefault("records", records)
            elif code == "W2":
                kwargs.setdefault("record", record)
            elif code == "R1":
                kwargs.setdefault("ids", ids)
            elif code == "C1":
                kwargs.setdefault("ids", ids)
                kwargs.setdefault("record", record)
            elif code == "R2":
                kwargs.setdefault("field", "siren")
                kwargs.setdefault("value", ids[0] if ids else "")
            elif code == "R3":
                kwargs.setdefault("field", "denominationUniteLegale")
                kwargs.setdefault("value", "")

            fn = TEST_REGISTRY.get(code)
            if fn is None:
                result = {"test_code": code, "erreurs": 1, "note": "test inconnu", "duree_s": 0}
                _log_result(result)
                _persist(result, run_id, db_name, step)
                step_results.append(result)
                continue

            try:
                raw = await fn(**kwargs)
            except Exception as e:
                raw = {"test_code": code, "erreurs": 1, "note": str(e), "duree_s": 0}

            # C1 retourne une liste de résultats (un par niveau × opération)
            sub_results = raw if isinstance(raw, list) else [raw]

            for result in sub_results:
                result["db_name"] = db_name
                result["run_id"]  = run_id
                _log_result(result)
                _persist(result, run_id, db_name, step)
                step_results.append(result)

        return step_results

    async def _execute_all():
        for step in steps:
            results = await _execute_step(step)
            all_results.extend(results)
        log.info("benchmark terminé — %d résultats persistés", len(all_results))

    asyncio.run(_execute_all())
    return {"run_id": run_id, "db_name": db_name, "steps": steps, "results": all_results}
