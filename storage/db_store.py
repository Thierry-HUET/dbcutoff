"""
storage/db_store.py — Persistance SQLite des résultats et de la configuration
"""

import sqlite3
import json
import datetime
from pathlib import Path

from config import STORAGE_DB


def _get_conn() -> sqlite3.Connection:
    STORAGE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(STORAGE_DB))
    conn.row_factory = sqlite3.Row
    return conn


def init_storage() -> None:
    """Crée les tables et ajoute les colonnes manquantes si nécessaire."""
    with _get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bench_run (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id      TEXT NOT NULL,
                db_name     TEXT NOT NULL,
                db_version  TEXT,
                started_at  TEXT NOT NULL,
                ended_at    TEXT,
                status      TEXT DEFAULT 'running',
                config_json TEXT
            );

            CREATE TABLE IF NOT EXISTS bench_result (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id      TEXT NOT NULL,
                db_name     TEXT NOT NULL,
                operation   TEXT NOT NULL,
                indexed     INTEGER NOT NULL DEFAULT 0,
                volume      INTEGER NOT NULL,
                batch_size  INTEGER,
                duration_s  REAL NOT NULL,
                repetition  INTEGER NOT NULL DEFAULT 1,
                measured_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_result_run ON bench_result(run_id);
            CREATE INDEX IF NOT EXISTS idx_result_db  ON bench_result(db_name);
        """)
        # Migration : ajouter db_version si la table existait sans cette colonne
        cols = [r[1] for r in conn.execute("PRAGMA table_info(bench_run)").fetchall()]
        if "db_version" not in cols:
            conn.execute("ALTER TABLE bench_run ADD COLUMN db_version TEXT")


def save_run(run_id: str, db_name: str, config: dict, db_version: str = "") -> None:
    with _get_conn() as conn:
        conn.execute(
            """INSERT INTO bench_run
               (run_id, db_name, db_version, started_at, config_json)
               VALUES (?,?,?,?,?)""",
            (run_id, db_name, db_version, _now(), json.dumps(config)),
        )


def close_run(run_id: str, status: str = "done") -> None:
    with _get_conn() as conn:
        conn.execute(
            "UPDATE bench_run SET ended_at=?, status=? WHERE run_id=?",
            (_now(), status, run_id),
        )


def save_result(
    run_id: str,
    db_name: str,
    operation: str,
    volume: int,
    duration_s: float,
    repetition: int = 1,
    indexed: bool = False,
    batch_size: int | None = None,
) -> None:
    with _get_conn() as conn:
        conn.execute(
            """INSERT INTO bench_result
               (run_id, db_name, operation, indexed, volume, batch_size,
                duration_s, repetition, measured_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                run_id, db_name, operation,
                int(indexed), volume, batch_size,
                duration_s, repetition, _now(),
            ),
        )


def fetch_results(db_name: str | None = None) -> list[dict]:
    sql = "SELECT * FROM bench_result"
    params: tuple = ()
    if db_name:
        sql += " WHERE db_name = ?"
        params = (db_name,)
    sql += " ORDER BY volume, operation"
    with _get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def fetch_runs() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bench_run ORDER BY started_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def fetch_versions() -> dict[str, str]:
    """
    Retourne un dict {db_name: db_version} en prenant
    la version du run le plus récent pour chaque base.
    """
    with _get_conn() as conn:
        rows = conn.execute("""
            SELECT db_name, db_version
            FROM bench_run
            WHERE db_version IS NOT NULL AND db_version != ''
            GROUP BY db_name
            HAVING started_at = MAX(started_at)
        """).fetchall()
    return {r["db_name"]: r["db_version"] for r in rows}


def _now() -> str:
    return datetime.datetime.utcnow().isoformat(timespec="seconds")