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

TABLE            = "insee_etablissements"
INDEX_COL        = "siret"               # index pour lookup par identifiant
INDEX_FILTER_COL = "etat_administratif"  # index pour accélérer les lectures filtrées
FILTER_VAL       = "F"                   # etat_administratif = 'F' (fermé)
# 'F' est la valeur MINORITAIRE — haute sélectivité, l'index est rentable
# 'A' (actif, majoritaire) déclencherait un scan complet même avec index

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

    def get_version(self) -> str:
        """Retourne la version DuckDB (ex: 'DuckDB 0.10.1')."""
        try:
            row = self._conn.execute("SELECT version()").fetchone()
            return f"DuckDB {row[0]}" if row else ""
        except Exception:
            return ""

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
        """Supprime tous les index de benchmark."""
        self._conn.execute(f"DROP INDEX IF EXISTS idx_{TABLE}_{INDEX_COL}")
        self._conn.execute(f"DROP INDEX IF EXISTS idx_{TABLE}_{INDEX_FILTER_COL}")

    def _create_index(self) -> None:
        """
        Crée deux index :
        - idx sur siret               → lookup par identifiant
        - idx sur etat_administratif  → filtre sur valeur minoritaire (F)
        Note : DuckDB est colonnaire — les index ont moins d'impact que sur
        PostgreSQL car le moteur utilise ses propres statistiques de zone.
        L'écart avec/sans index peut rester faible sur ce type de requête.
        """
        self._conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{INDEX_COL}"
            f" ON {TABLE}({INDEX_COL})"
        )
        self._conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{INDEX_FILTER_COL}"
            f" ON {TABLE}({INDEX_FILTER_COL})"
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


    # ------------------------------------------------------------------
    # Opérations vectorielles — DuckDB natif
    # ------------------------------------------------------------------
    # DuckDB supporte les vecteurs nativement via le type FLOAT[n]
    # et les fonctions array_cosine_similarity(), array_distance().
    # Pas d'extension requise.
    # ------------------------------------------------------------------

    has_vector_support = True
    VECTOR_TABLE = "insee_vecteurs"

    def vector_setup(self) -> None:
        """Crée la table vectorielle DuckDB."""
        self._conn.execute(f"DROP TABLE IF EXISTS {self.VECTOR_TABLE}")
        self._conn.execute(f"""
            CREATE TABLE {self.VECTOR_TABLE} (
                id        INTEGER,
                siret     VARCHAR,
                embedding FLOAT[{self.VECTOR_DIM}]
            )
        """)
        log.info("DuckDB vecteurs : table '%s' créée (dim=%d).", self.VECTOR_TABLE, self.VECTOR_DIM)

    def vector_teardown(self) -> None:
        self._conn.execute(f"DROP TABLE IF EXISTS {self.VECTOR_TABLE}")

    def vector_insert(self, n_rows: int) -> float:
        """
        Insère n_rows vecteurs via INSERT ... SELECT FROM df.
        DuckDB scanne le DataFrame directement — méthode la plus rapide.
        """
        import pandas as pd
        vecs = self.generate_vectors(n_rows)
        df_vec = pd.DataFrame({
            "id":        range(n_rows),
            "siret":     [f"{i:014d}" for i in range(n_rows)],
            "embedding": [vec.tolist() for vec in vecs],
        })
        t0 = time.perf_counter()
        self._conn.execute(
            f"INSERT INTO {self.VECTOR_TABLE} SELECT * FROM df_vec"
        )
        return time.perf_counter() - t0

    def vector_search_exact(self, n_rows: int, k: int = 10) -> float:
        """
        Recherche exacte par similarité cosinus (brute force).
        DuckDB utilise array_cosine_similarity() — scan séquentiel.
        """
        query_vec = self.generate_vectors(1)[0].tolist()
        t0 = time.perf_counter()
        self._conn.execute(
            f"SELECT siret, array_cosine_similarity(embedding, ?::FLOAT[{self.VECTOR_DIM}]) AS sim "
            f"FROM {self.VECTOR_TABLE} ORDER BY sim DESC LIMIT ?",
            [query_vec, k],
        ).fetchall()
        return time.perf_counter() - t0

    def vector_search_approx(self, n_rows: int, k: int = 10) -> float:
        """
        DuckDB ne dispose pas d'index ANN natif (HNSW) en v0.10.
        La recherche approximative est émulée par un scan avec filtre
        sur une partition aléatoire (10% des données) — cela mesure
        l'impact du volume réduit vs le scan complet.
        Note : DuckDB vss extension (expérimentale) apporte HNSW
        mais n'est pas encore stable en production.
        """
        query_vec = self.generate_vectors(1)[0].tolist()
        sample_pct = 10
        t0 = time.perf_counter()
        self._conn.execute(
            f"SELECT siret, array_cosine_similarity(embedding, ?::FLOAT[{self.VECTOR_DIM}]) AS sim "
            f"FROM {self.VECTOR_TABLE} USING SAMPLE {sample_pct}% "
            f"ORDER BY sim DESC LIMIT ?",
            [query_vec, k],
        ).fetchall()
        return time.perf_counter() - t0