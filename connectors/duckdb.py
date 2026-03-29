"""
connectors/duckdb.py — Connecteur DuckDB

Dépendance : duckdb>=0.10

Spécificités DuckDB vs PostgreSQL :
    - Pas de serveur — la base est un fichier local (DSN = chemin du fichier)
    - ensure_database() : le fichier est créé automatiquement à la connexion,
      pas besoin d'une base système intermédiaire
    - drop_database()   : suppression du fichier sur disque
    - write_bulk()      : duckdb.from_df() + INSERT via relation — très rapide
      (DuckDB est optimisé pour l'ingestion de DataFrames Arrow/Pandas)
    - Pas de TRUNCATE : DELETE FROM suffit (DuckDB ne supporte pas TRUNCATE
      dans toutes les versions ; DELETE est équivalent sur table non partitionnée)
    - Les index sont moins pertinents (moteur colonnaire avec stats intégrées)
      mais sont tout de même benchmarkés pour la comparaison
    - Paramètres de requête : notation '?' (et non '%s' comme psycopg)
"""

import time
import logging
from pathlib import Path

import duckdb
import pandas as pd

from connectors.base import DBConnector

log = logging.getLogger(__name__)

TABLE      = "insee_etablissements"
INDEX_COL  = "siret"
FILTER_VAL = "A"   # etat_administratif = 'A' (actif)

COLS = (
    "siret",
    "date_debut",
    "date_fin",
    "etat_administratif",
    "enseigne1",
    "activite_principale",
    "caractere_employeur",
)

# DuckDB utilise '?' comme marqueur de paramètre
INSERT_SQL = (
    f"INSERT INTO {TABLE} ({', '.join(COLS)}) "
    f"VALUES ({', '.join(['?'] * len(COLS))})"
)


class DuckDBConnector(DBConnector):

    name = "duckdb"

    def __init__(self, dsn: str):
        """
        dsn : chemin vers le fichier DuckDB (ex: ./data/cutoff.duckdb)
              ou ':memory:' pour une base en mémoire (non persistée)
        """
        super().__init__(dsn)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._db_path = Path(dsn) if dsn != ":memory:" else None

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    def ensure_database(self) -> None:
        """
        DuckDB crée le fichier automatiquement à la connexion.
        On crée simplement le répertoire parent si nécessaire.
        """
        if self._db_path is not None:
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            log.info(
                "DuckDB : fichier '%s' %s.",
                self._db_path,
                "existant" if self._db_path.exists() else "sera créé à la connexion",
            )
        else:
            log.info("DuckDB : mode ':memory:' (pas de fichier persisté).")

    def connect(self) -> None:
        self._conn = duckdb.connect(str(self.dsn))
        log.info("DuckDB connecté : %s", self.dsn)

    def disconnect(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def setup(self) -> None:
        """Recrée la table cible (DROP + CREATE)."""
        self._conn.execute(f"DROP TABLE IF EXISTS {TABLE}")
        self._conn.execute(f"""
            CREATE TABLE {TABLE} (
                siret               VARCHAR,
                date_debut          DATE,
                date_fin            DATE,
                etat_administratif  VARCHAR,
                enseigne1           VARCHAR,
                activite_principale VARCHAR,
                caractere_employeur VARCHAR
            )
        """)

    def teardown(self) -> None:
        self._conn.execute(f"DROP TABLE IF EXISTS {TABLE}")

    def drop_database(self) -> None:
        """
        Supprime le fichier DuckDB sur disque.
        Sans effet si la base est ':memory:'.
        """
        if self._db_path is not None and self._db_path.exists():
            self._db_path.unlink()
            # Supprimer aussi le fichier WAL résiduel si présent
            wal = self._db_path.with_suffix(".duckdb.wal")
            if wal.exists():
                wal.unlink()
            log.info("DuckDB : fichier '%s' supprimé.", self._db_path)
        else:
            log.info("DuckDB : rien à supprimer (mode mémoire ou fichier absent).")

    # ------------------------------------------------------------------
    # Helpers internes
    # ------------------------------------------------------------------

    def _drop_index(self) -> None:
        self._conn.execute(f"DROP INDEX IF EXISTS idx_{TABLE}_{INDEX_COL}")

    def _create_index(self) -> None:
        self._conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{INDEX_COL}"
            f" ON {TABLE}({INDEX_COL})"
        )

    def _truncate(self) -> None:
        # DuckDB supporte DELETE FROM ; plus simple que TRUNCATE
        self._conn.execute(f"DELETE FROM {TABLE}")

    # ------------------------------------------------------------------
    # Opérations benchmarkées
    # ------------------------------------------------------------------

    def write_bulk(self, df: pd.DataFrame) -> float:
        """
        Insertion en lot via INSERT INTO ... SELECT FROM df.
        DuckDB peut scanner un DataFrame Pandas directement comme
        source SQL — c'est sa méthode d'ingestion la plus rapide.
        """
        self._truncate()
        self._drop_index()
        # Référencer le DataFrame comme table virtuelle dans la requête
        t0 = time.perf_counter()
        self._conn.execute(
            f"INSERT INTO {TABLE} SELECT * FROM df"
        )
        return time.perf_counter() - t0

    def write_row_by_row(self, df: pd.DataFrame) -> float:
        """Insertion ligne par ligne via executemany() — intentionnellement lent."""
        self._truncate()
        self._drop_index()
        records = [
            tuple(None if pd.isna(v) else v for v in row)
            for row in df.itertuples(index=False)
        ]
        t0 = time.perf_counter()
        self._conn.executemany(INSERT_SQL, records)
        return time.perf_counter() - t0

    def read_full(self, n_rows: int) -> float:
        self._drop_index()
        t0 = time.perf_counter()
        self._conn.execute(f"SELECT * FROM {TABLE} LIMIT ?", [n_rows]).fetchall()
        return time.perf_counter() - t0

    def read_filtered(self, n_rows: int) -> float:
        self._drop_index()
        t0 = time.perf_counter()
        self._conn.execute(
            f"SELECT * FROM {TABLE} WHERE etat_administratif = ? LIMIT ?",
            [FILTER_VAL, n_rows],
        ).fetchall()
        return time.perf_counter() - t0

    def read_full_indexed(self, n_rows: int) -> float:
        self._create_index()
        t0 = time.perf_counter()
        self._conn.execute(f"SELECT * FROM {TABLE} LIMIT ?", [n_rows]).fetchall()
        return time.perf_counter() - t0

    def read_filtered_indexed(self, n_rows: int) -> float:
        self._create_index()
        t0 = time.perf_counter()
        self._conn.execute(
            f"SELECT * FROM {TABLE} WHERE etat_administratif = ? LIMIT ?",
            [FILTER_VAL, n_rows],
        ).fetchall()
        return time.perf_counter() - t0