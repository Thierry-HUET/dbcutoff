#!/usr/bin/env python3
"""
bench_runner.py — Orchestrateur du benchmark DB Cutoff

Usage
-----
    python bench_runner.py [--db postgresql] [--volumes 100,1000,10000] [--reps 3]
                           [--ops read_full,read_filtered] [--no-vector]

Variables d'environnement
-------------------------
    INSEE_FILE   : chemin vers StockEtablissementHistorique_utf8.csv
    POSTGRES_DSN : connexion string PostgreSQL
"""

import sys
import uuid
import numpy as np
import importlib
import argparse
import logging
import traceback
from typing import Callable

from config import DATABASES, VOLUMES, REPETITIONS, INSEE_FILE, BATCH_SIZES, VECTOR_VOLUMES
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

# Toutes les opérations scalaires disponibles — utilisé pour la validation de --ops
# Opérations scalaires
ALL_OPS_SCALAR = [
    "write_bulk",
    "write_row_by_row",
    "read_full",
    "read_filtered",
    "read_full_indexed",
    "read_filtered_indexed",
]

# Opérations vectorielles — sélectionnables via --ops
# Note : write_row_by_row est plafonné à 10 000 lignes (max_volume)
ALL_OPS_VECTOR = [
    "vector_insert",
    "vector_search_exact",
    "vector_search_approx",
]

ALL_OPS = ALL_OPS_SCALAR + ALL_OPS_VECTOR


def run_benchmark(
    db_cfg: dict,
    volumes: list[int],
    repetitions: int,
    run_id: str,
    ops_filter: list[str] | None = None,
    run_vector: bool = True,
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
        "db_name":    db_name,
        "dsn":        db_cfg["dsn"],
        "volumes":    volumes,
        "repetitions": repetitions,
        "insee_file": INSEE_FILE,
        "ops_filter": ops_filter or "all",
        "run_vector": run_vector,
    }
    save_run(run_id, db_name, config_snapshot)

    try:
        connector.ensure_database()
        connector.connect()
        db_version = connector.get_version()
        log.info("Version : %s", db_version or "(inconnue)")
        # Mettre à jour la version dans bench_run
        from storage.db_store import _get_conn as _gsq
        with _gsq() as _c:
            _c.execute(
                "UPDATE bench_run SET db_version=? WHERE run_id=? AND db_name=?",
                (db_version, run_id, db_name),
            )
        connector.setup()

        # Chargement du DataFrame au volume max — utilisé pour les sous-échantillons
        # Pas d'insertion initiale : chaque opération de lecture recharge
        # exactement le volume nécessaire via write_bulk(df_sample).
        max_vol = max(volumes)
        log.info("Chargement du jeu de données INSEE (%d lignes)…", max_vol)
        df_max = load_sample(INSEE_FILE, max_vol)

        ops = _build_operations(connector, df_max)

        # Filtrage par --ops si spécifié
        if ops_filter:
            # Filtrer les ops scalaires (les ops vectorielles sont gérées séparément)
            ops = [op for op in ops if op["name"] in ops_filter]
            scalar_active = [op for op in ops_filter if op in ALL_OPS_SCALAR]
            vector_active = [op for op in ops_filter if op in ALL_OPS_VECTOR]
            if not scalar_active and not vector_active:
                log.warning("Aucune opération ne correspond au filtre %s — abandon.", ops_filter)
                close_run(run_id, "done")
                return
            if not scalar_active:
                log.info("Aucune opération scalaire dans le filtre — passage direct au vectoriel.")

        for op in ops:
            op_name = op["name"]
            max_vol_op = op.get("max_volume", max(volumes))
            vols_to_test = [v for v in volumes if v <= max_vol_op]

            for vol in vols_to_test:
                df_sample = df_max.head(vol)

                # Pour les opérations de lecture, s'assurer que la table
                # contient exactement `vol` lignes avant chaque mesure.
                # Les opérations d'écriture gèrent elles-mêmes leur truncate.
                if not op["needs_data"]:
                    log.info(
                        "  [rechargement] %d lignes pour lectures…", vol
                    )
                    connector.write_bulk(df_sample)

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

        # --- Benchmark vectoriel (si supporté et non désactivé par --no-vector) ---
        if run_vector and connector.has_vector_support:
            log.info("--- Benchmark vectoriel : %s (volumes : %s) ---",
                     db_name, VECTOR_VOLUMES)
            try:
                # Déterminer quelles opérations vectorielles exécuter
                vec_ops_active = [
                    op for op in ALL_OPS_VECTOR
                    if ops_filter is None or op in ops_filter
                ]

                if not vec_ops_active:
                    log.info("Aucune opération vectorielle dans le filtre — ignoré.")
                else:
                    connector.vector_setup()
                    for vol in VECTOR_VOLUMES:
                        for rep in range(1, repetitions + 1):
                            for op_name, fn in [
                                ("vector_insert",       connector.vector_insert),
                                ("vector_search_exact", connector.vector_search_exact),
                                ("vector_search_approx",connector.vector_search_approx),
                            ]:
                                if op_name not in vec_ops_active:
                                    continue
                                log.info("  %-30s | vol=%7d | rep=%d", op_name, vol, rep)
                                try:
                                    dur = fn(vol)
                                    save_result(run_id, db_name, op_name, vol, dur, rep)
                                    log.info("    → %.3f s", dur)
                                except Exception as e:
                                    log.error("    ✗ ERREUR %s : %s", op_name, e)
                                    save_result(run_id, db_name, op_name, vol, -1.0, rep)

                    connector.vector_teardown()
            except Exception:
                log.error("Erreur benchmark vectoriel :\n%s", traceback.format_exc())
        else:
            log.info("Benchmark vectoriel non supporté par %s — ignoré.", db_name)

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
    parser.add_argument(
        "--ops",
        default=None,
        help=(
            "Opérations à exécuter, séparées par virgules. "
            f"Valeurs possibles : {', '.join(ALL_OPS)}. "
            "Défaut : toutes. Exemple : --ops read_full,read_filtered"
        ),
    )
    parser.add_argument(
        "--no-vector",
        action="store_true",
        default=False,
        help="Désactive le benchmark vectoriel même si la base le supporte.",
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

    # Validation et parsing de --ops
    ops_filter = None
    if args.ops:
        ops_filter = [op.strip() for op in args.ops.split(",")]
        invalides = [op for op in ops_filter if op not in ALL_OPS]
        if invalides:
            log.error(
                "Opération(s) inconnue(s) : %s\nValeurs acceptées : %s",
                ", ".join(invalides),
                ", ".join(ALL_OPS),
            )
            sys.exit(1)
        log.info("Opérations sélectionnées : %s", ", ".join(ops_filter))

    init_storage()
    run_id = str(uuid.uuid4())
    log.info("Run ID : %s", run_id)

    for db_cfg in dbs:
        run_benchmark(
            db_cfg, volumes, args.reps, run_id,
            ops_filter=ops_filter,
            run_vector=not args.no_vector,
        )


if __name__ == "__main__":
    main()