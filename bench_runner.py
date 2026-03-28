#!/usr/bin/env python3
"""
bench_runner.py — Orchestrateur du benchmark DB Cutoff

Usage
-----
    python bench_runner.py [--db postgresql] [--volumes 100,1000,10000] [--reps 3]

Variables d'environnement
-------------------------
    INSEE_FILE   : chemin vers StockEtablissementHistorique_utf8.csv
    POSTGRES_DSN : connexion string PostgreSQL
"""

import sys
import uuid
import importlib
import argparse
import logging
import traceback
from typing import Callable

from config import DATABASES, VOLUMES, REPETITIONS, INSEE_FILE, BATCH_SIZES
from loaders.insee_loader import load_sample
from storage.db_store import init_storage, save_run, close_run, save_result
from connectors.base import DBConnector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Registre des opérations
# ---------------------------------------------------------------------------

def _build_operations(connector: DBConnector, df_full) -> list[dict]:
    """
    Retourne la liste des opérations à benchmarker.
    Chaque entrée : {name, fn, indexed, batch_size, needs_data}
    """
    ops = []

    # --- Écriture en lot ---
    ops.append({
        "name": "write_bulk",
        "fn": lambda df, _v: connector.write_bulk(df),
        "indexed": False,
        "batch_size": None,
        "needs_data": True,
    })

    # --- Écriture ligne par ligne (limitée à 10 000 max pour éviter timeout) ---
    ops.append({
        "name": "write_row_by_row",
        "fn": lambda df, _v: connector.write_row_by_row(df),
        "indexed": False,
        "batch_size": None,
        "needs_data": True,
        "max_volume": 10_000,   # garde-fou
    })

    # --- Lecture complète sans index ---
    ops.append({
        "name": "read_full",
        "fn": lambda _df, v: connector.read_full(v),
        "indexed": False,
        "batch_size": None,
        "needs_data": False,
    })

    # --- Lecture filtrée sans index ---
    ops.append({
        "name": "read_filtered",
        "fn": lambda _df, v: connector.read_filtered(v),
        "indexed": False,
        "batch_size": None,
        "needs_data": False,
    })

    # --- Lecture complète avec index ---
    ops.append({
        "name": "read_full_indexed",
        "fn": lambda _df, v: connector.read_full_indexed(v),
        "indexed": True,
        "batch_size": None,
        "needs_data": False,
    })

    # --- Lecture filtrée avec index ---
    ops.append({
        "name": "read_filtered_indexed",
        "fn": lambda _df, v: connector.read_filtered_indexed(v),
        "indexed": True,
        "batch_size": None,
        "needs_data": False,
    })

    return ops


# ---------------------------------------------------------------------------
# Runner principal
# ---------------------------------------------------------------------------

def run_benchmark(
    db_cfg: dict,
    volumes: list[int],
    repetitions: int,
    run_id: str,
) -> None:
    db_name = db_cfg["name"]
    log.info("=== Démarrage benchmark : %s ===", db_name)

    # Charger le connecteur dynamiquement
    module = importlib.import_module(db_cfg["module"])
    connector_class = next(
        cls for cls in vars(module).values()
        if isinstance(cls, type) and issubclass(cls, DBConnector) and cls is not DBConnector
    )
    connector: DBConnector = connector_class(dsn=db_cfg["dsn"])

    config_snapshot = {
        "db_name": db_name,
        "dsn": db_cfg["dsn"],
        "volumes": volumes,
        "repetitions": repetitions,
        "insee_file": INSEE_FILE,
    }
    save_run(run_id, db_name, config_snapshot)

    try:
        connector.ensure_database()
        connector.connect()
        connector.setup()

        # Pré-chargement du volume max (pour les lectures)
        max_vol = max(volumes)
        log.info("Chargement du jeu de données INSEE (%d lignes)…", max_vol)
        df_max = load_sample(INSEE_FILE, max_vol)

        # Insertion initiale pour les opérations de lecture
        log.info("Insertion initiale (%d lignes) pour lectures…", max_vol)
        connector.write_bulk(df_max)

        ops = _build_operations(connector, df_max)

        for op in ops:
            op_name = op["name"]
            max_vol_op = op.get("max_volume", max(volumes))
            vols_to_test = [v for v in volumes if v <= max_vol_op]

            for vol in vols_to_test:
                df_sample = df_max.head(vol)

                for rep in range(1, repetitions + 1):
                    log.info(
                        "  %-30s | vol=%7d | rep=%d",
                        op_name, vol, rep,
                    )
                    try:
                        if op["needs_data"]:
                            duration = op["fn"](df_sample, vol)
                        else:
                            duration = op["fn"](None, vol)

                        save_result(
                            run_id=run_id,
                            db_name=db_name,
                            operation=op_name,
                            volume=vol,
                            duration_s=duration,
                            repetition=rep,
                            indexed=op["indexed"],
                            batch_size=op.get("batch_size"),
                        )
                        log.info("    → %.3f s", duration)

                    except Exception as e:
                        log.error("    ✗ ERREUR : %s", e)
                        save_result(
                            run_id=run_id,
                            db_name=db_name,
                            operation=op_name,
                            volume=vol,
                            duration_s=-1.0,
                            repetition=rep,
                            indexed=op["indexed"],
                        )

        close_run(run_id, "done")
        log.info("=== Benchmark terminé : %s ===", db_name)

    except Exception:
        log.error("Erreur critique :\n%s", traceback.format_exc())
        close_run(run_id, "error")
    finally:
        try:
            connector.teardown()
        except Exception:
            pass
        connector.disconnect()
        try:
            connector.drop_database()
        except Exception:
            log.warning("Impossible de supprimer la base : %s", traceback.format_exc())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="DB Cutoff Benchmark Runner")
    parser.add_argument(
        "--db",
        default=None,
        help="Nom de la base à tester (ex: postgresql). Défaut : toutes les bases activées.",
    )
    parser.add_argument(
        "--volumes",
        default=None,
        help="Volumes séparés par virgules (ex: 100,1000,10000). Défaut : config.py.",
    )
    parser.add_argument(
        "--reps",
        type=int,
        default=REPETITIONS,
        help=f"Nombre de répétitions par mesure (défaut: {REPETITIONS}).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    volumes = (
        [int(v) for v in args.volumes.split(",")]
        if args.volumes
        else VOLUMES
    )

    dbs = [d for d in DATABASES if d["enabled"]]
    if args.db:
        dbs = [d for d in dbs if d["name"] == args.db]
        if not dbs:
            log.error("Base '%s' introuvable ou désactivée dans config.py", args.db)
            sys.exit(1)

    init_storage()
    run_id = str(uuid.uuid4())
    log.info("Run ID : %s", run_id)

    for db_cfg in dbs:
        run_benchmark(db_cfg, volumes, args.reps, run_id)


if __name__ == "__main__":
    main()