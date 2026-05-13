"""Persistance des résultats de benchmark dans SQLite"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "results.db"


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS results (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id      TEXT    NOT NULL,
                db_name     TEXT    NOT NULL,
                test_code   TEXT    NOT NULL,
                concurrence INTEGER,
                volume      INTEGER,
                latence_p50 REAL,
                latence_p95 REAL,
                latence_p99 REAL,
                throughput  REAL,
                duree_s     REAL,
                erreurs     INTEGER,
                timestamp   TEXT    NOT NULL
            )
        """)


def save_result(
    run_id: str,
    db_name: str,
    test_code: str,
    duree_s: float,
    latence_p50: float | None = None,
    latence_p95: float | None = None,
    latence_p99: float | None = None,
    throughput: float | None = None,
    concurrence: int | None = None,
    volume: int | None = None,
    erreurs: int = 0,
) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO results
              (run_id, db_name, test_code, concurrence, volume,
               latence_p50, latence_p95, latence_p99, throughput,
               duree_s, erreurs, timestamp)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                run_id, db_name, test_code, concurrence, volume,
                latence_p50, latence_p95, latence_p99, throughput,
                duree_s, erreurs,
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def new_run_id() -> str:
    return str(uuid.uuid4())


def get_runs() -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT run_id, db_name, MIN(timestamp) AS started_at
            FROM results
            GROUP BY run_id, db_name
            ORDER BY started_at DESC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def get_results(run_id: str | None = None) -> list[dict]:
    with _connect() as conn:
        if run_id:
            rows = conn.execute(
                "SELECT * FROM results WHERE run_id = ? ORDER BY id",
                (run_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM results ORDER BY id DESC LIMIT 500"
            ).fetchall()
    return [dict(r) for r in rows]
