"""
connectors/postgres.py — Connecteur PostgreSQL via psycopg v3

Dépendance : psycopg[binary]>=3.1  (PAS psycopg2)

Logique de création de base :
    ensure_database() se connecte à la base système 'postgres',
    vérifie si la base cible existe dans pg_database et la crée
    en AUTOCOMMIT si nécessaire (CREATE DATABASE interdit en transaction).
"""

import time
import logging
from urllib.parse import urlparse, urlunparse

import psycopg
import pandas as pd

from connectors.base import DBConnector

log = logging.getLogger(__name__)

TABLE      = "insee_etablissements"
INDEX_COL        = "siret"               # index pour lookup par identifiant
INDEX_FILTER_COL = "etat_administratif"  # index pour accélérer les lectures filtrées
FILTER_VAL       = "F"                   # etat_administratif = 'F' (fermé)
# 'F' est la valeur MINORITAIRE — PostgreSQL utilisera l'index (haute sélectivité)
# 'A' (actif, majoritaire) déclencherait un sequential scan même avec index

COLS = (
    "siret",
    "date_debut",
    "date_fin",
    "etat_administratif",
    "enseigne1",
    "activite_principale",
    "caractere_employeur",
)

INSERT_SQL = (
    f"INSERT INTO {TABLE} ({', '.join(COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(COLS))})"
)

COPY_SQL = f"COPY {TABLE} ({', '.join(COLS)}) FROM STDIN"


def _system_dsn(dsn: str) -> str:
    """
    Remplace la base cible par 'postgres' dans le DSN pour permettre
    la connexion au serveur avant que la base cible n'existe.
    """
    p = urlparse(dsn)
    # path = '/db_cutoff' → '/postgres'
    system = p._replace(path="/postgres")
    return urlunparse(system)


def _db_name(dsn: str) -> str:
    """Extrait le nom de la base depuis le DSN."""
    return urlparse(dsn).path.lstrip("/")


class PostgresConnector(DBConnector):

    name = "postgresql"

    def __init__(self, dsn: str):
        super().__init__(dsn)
        self._conn: psycopg.Connection | None = None

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    def ensure_database(self) -> None:
        """
        Crée la base cible si elle n'existe pas.
        Connexion temporaire à 'postgres' en autocommit
        (CREATE DATABASE est interdit dans une transaction).
        """
        db_name = _db_name(self.dsn)
        sys_dsn = _system_dsn(self.dsn)

        with psycopg.connect(sys_dsn, autocommit=True) as conn:
            row = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
            ).fetchone()

            if row is None:
                log.info("Base '%s' absente — création en cours…", db_name)
                # Les identifiants ne peuvent pas être paramétrés dans DDL
                conn.execute(f'CREATE DATABASE "{db_name}"')
                log.info("Base '%s' créée.", db_name)
            else:
                log.info("Base '%s' déjà existante.", db_name)

    def connect(self) -> None:
        self._conn = psycopg.connect(self.dsn, autocommit=False)

    def disconnect(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()

    def setup(self) -> None:
        """Recrée la table cible (DROP + CREATE) en autocommit."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            siret               VARCHAR(14),
            date_debut          DATE,
            date_fin            DATE,
            etat_administratif  VARCHAR(1),
            enseigne1           TEXT,
            activite_principale VARCHAR(10),
            caractere_employeur VARCHAR(1)
        )
        """
        self._conn.commit()
        self._conn.autocommit = True
        try:
            with self._conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
                cur.execute(ddl)
        finally:
            self._conn.autocommit = False

    def teardown(self) -> None:
        self._conn.commit()
        self._conn.autocommit = True
        try:
            with self._conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        finally:
            self._conn.autocommit = False

    # ------------------------------------------------------------------
    # Helpers internes
    # ------------------------------------------------------------------

    def _drop_index(self) -> None:
        """
        Supprime tous les index de benchmark.
        psycopg v3 ouvre une transaction implicite dès le premier SELECT —
        il faut commiter avant de passer en autocommit, sinon psycopg lève
        "can't change autocommit now: connection in transaction status INTRANS".
        """
        self._conn.commit()           # clôture toute transaction en cours
        self._conn.autocommit = True
        try:
            with self._conn.cursor() as cur:
                cur.execute(f"DROP INDEX IF EXISTS idx_{TABLE}_{INDEX_COL}")
                cur.execute(f"DROP INDEX IF EXISTS idx_{TABLE}_{INDEX_FILTER_COL}")
        finally:
            self._conn.autocommit = False

    def _create_index(self) -> None:
        """
        Crée deux index :
        - idx sur siret               → lookup par identifiant
        - idx sur etat_administratif  → filtre sur valeur minoritaire (F)
        Même contrainte que _drop_index : commit avant autocommit.
        """
        self._conn.commit()           # clôture toute transaction en cours
        self._conn.autocommit = True
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{INDEX_COL}"
                    f" ON {TABLE}({INDEX_COL})"
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_{INDEX_FILTER_COL}"
                    f" ON {TABLE}({INDEX_FILTER_COL})"
                )
        finally:
            self._conn.autocommit = False

    def _truncate(self) -> None:
        """
        DELETE FROM au lieu de TRUNCATE.
        TRUNCATE acquiert un AccessExclusiveLock qui entre en deadlock
        avec DROP INDEX dans la même transaction.
        """
        with self._conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE}")
        self._conn.commit()

    @staticmethod
    def _df_to_records(df: pd.DataFrame) -> list[tuple]:
        """Convertit un DataFrame en liste de tuples ; NaN/NaT → None."""
        return [
            tuple(None if pd.isna(v) else v for v in row)
            for row in df.itertuples(index=False)
        ]

    # ------------------------------------------------------------------
    # Opérations benchmarkées
    # ------------------------------------------------------------------

    def write_bulk(self, df: pd.DataFrame) -> float:
        """Insertion via protocole COPY (psycopg v3) — méthode la plus rapide."""
        self._truncate()
        self._drop_index()
        records = self._df_to_records(df)
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            with cur.copy(COPY_SQL) as copy:
                for record in records:
                    copy.write_row(record)
        self._conn.commit()
        return time.perf_counter() - t0

    def write_row_by_row(self, df: pd.DataFrame) -> float:
        """Insertion ligne par ligne via execute() — intentionnellement lent."""
        self._truncate()
        self._drop_index()
        records = self._df_to_records(df)
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            for record in records:
                cur.execute(INSERT_SQL, record)
        self._conn.commit()
        return time.perf_counter() - t0

    def read_full(self, n_rows: int) -> float:
        self._drop_index()
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {TABLE} LIMIT %s", (n_rows,))
            cur.fetchall()
        return time.perf_counter() - t0

    def read_filtered(self, n_rows: int) -> float:
        self._drop_index()
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {TABLE} WHERE etat_administratif = %s LIMIT %s",
                (FILTER_VAL, n_rows),
            )
            cur.fetchall()
        return time.perf_counter() - t0

    def read_full_indexed(self, n_rows: int) -> float:
        self._create_index()
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {TABLE} LIMIT %s", (n_rows,))
            cur.fetchall()
        return time.perf_counter() - t0

    def read_filtered_indexed(self, n_rows: int) -> float:
        self._create_index()
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {TABLE} WHERE etat_administratif = %s LIMIT %s",
                (FILTER_VAL, n_rows),
            )
            cur.fetchall()
        return time.perf_counter() - t0

    def drop_database(self) -> None:
        """
        Supprime la base cible après le benchmark.
        Connexion à 'postgres' en autocommit (DROP DATABASE interdit
        en transaction et impossible si connecté à la base cible).
        """
        db_name = _db_name(self.dsn)
        sys_dsn = _system_dsn(self.dsn)

        with psycopg.connect(sys_dsn, autocommit=True) as conn:
            row = conn.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
            ).fetchone()

            if row is not None:
                # Forcer la déconnexion des sessions actives avant DROP
                conn.execute(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s AND pid <> pg_backend_pid()
                    """,
                    (db_name,),
                )
                conn.execute(f'DROP DATABASE "{db_name}"')
                log.info("Base '%s' supprimée.", db_name)
            else:
                log.warning("Base '%s' introuvable — rien à supprimer.", db_name)


    # ------------------------------------------------------------------
    # Opérations vectorielles — pgvector
    # ------------------------------------------------------------------
    # Prérequis : CREATE EXTENSION vector;
    # Installation : https://github.com/pgvector/pgvector
    #   brew install pgvector          (macOS)
    #   apt install postgresql-pgvector (Linux)
    # ------------------------------------------------------------------

    has_vector_support = True
    VECTOR_TABLE = "insee_vecteurs"

    def vector_setup(self) -> None:
        """
        Active l'extension pgvector et crée la table vectorielle.
        La table contient : siret (clé), embedding (vecteur VECTOR_DIM).
        """
        self._conn.commit()
        self._conn.autocommit = True
        try:
            with self._conn.cursor() as cur:
                # Extension pgvector — silencieux si déjà installée
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
                cur.execute(f"DROP TABLE IF EXISTS {self.VECTOR_TABLE}")
                cur.execute(f"""
                    CREATE TABLE {self.VECTOR_TABLE} (
                        id      SERIAL PRIMARY KEY,
                        siret   VARCHAR(14),
                        embedding vector({self.VECTOR_DIM})
                    )
                """)
        finally:
            self._conn.autocommit = False
        log.info("pgvector : table '%s' créée (dim=%d).", self.VECTOR_TABLE, self.VECTOR_DIM)

    def vector_teardown(self) -> None:
        self._conn.commit()
        self._conn.autocommit = True
        try:
            with self._conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.VECTOR_TABLE}")
        finally:
            self._conn.autocommit = False

    def vector_insert(self, n_rows: int) -> float:
        """Insère n_rows vecteurs via COPY (le plus rapide avec pgvector)."""
        vecs = self.generate_vectors(n_rows)
        copy_sql = f"COPY {self.VECTOR_TABLE} (siret, embedding) FROM STDIN"
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for i, vec in enumerate(vecs):
                    siret = f"{i:014d}"
                    # pgvector attend le format '[0.1,0.2,...]'
                    vec_str = "[" + ",".join(f"{v:.6f}" for v in vec) + "]"
                    copy.write_row((siret, vec_str))
        self._conn.commit()
        return time.perf_counter() - t0

    def vector_search_exact(self, n_rows: int, k: int = 10) -> float:
        """
        Recherche exacte (brute force) des k plus proches voisins
        par distance L2 (<->) sans index — mesure le coût séquentiel.
        """
        query_vec = self.generate_vectors(1)[0]
        vec_str = "[" + ",".join(f"{v:.6f}" for v in query_vec) + "]"
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT siret, embedding <-> %s::vector AS dist "
                f"FROM {self.VECTOR_TABLE} ORDER BY dist LIMIT %s",
                (vec_str, k),
            )
            cur.fetchall()
        return time.perf_counter() - t0

    def vector_search_approx(self, n_rows: int, k: int = 10) -> float:
        """
        Recherche approximative (ANN) via index HNSW.
        HNSW (Hierarchical Navigable Small World) offre le meilleur
        compromis rappel/vitesse pour pgvector.
        L'index est créé avant la mesure (non inclus dans le temps).
        """
        # Création de l'index HNSW si absent
        self._conn.commit()   # clôture la transaction avant autocommit
        self._conn.autocommit = True
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.VECTOR_TABLE}_hnsw "
                    f"ON {self.VECTOR_TABLE} "
                    f"USING hnsw (embedding vector_l2_ops)"
                )
        finally:
            self._conn.autocommit = False

        query_vec = self.generate_vectors(1)[0]
        vec_str = "[" + ",".join(f"{v:.6f}" for v in query_vec) + "]"
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT siret, embedding <-> %s::vector AS dist "
                f"FROM {self.VECTOR_TABLE} ORDER BY dist LIMIT %s",
                (vec_str, k),
            )
            cur.fetchall()
        return time.perf_counter() - t0