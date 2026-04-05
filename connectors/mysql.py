"""
connectors/mysql.py — Connecteur MySQL / MariaDB via pymysql

docker run -d \
  --name mysql-myvector \
  -p 3306:3306 \
  -e MYSQL_ALLOW_EMPTY_PASSWORD=true \
  ghcr.io/askdba/myvector:mysql-8.4 \
  --loose-myvector_binlog_socket=/var/run/mysqld/mysqld.sock

Dépendance : pymysql>=1.1

Spécificités MySQL vs PostgreSQL :
    - Pas de protocole COPY — write_bulk() utilise executemany() avec
      INSERT INTO ... VALUES en lot, optimisé par pymysql
    - DDL (CREATE/DROP TABLE, CREATE/DROP INDEX) implicitement en autocommit
      MySQL valide automatiquement les DDL — pas besoin de gestion explicite
      comme avec psycopg v3
    - TRUNCATE TABLE réutilisable sans risque de deadlock (pas de conflit
      avec les index sous MySQL/InnoDB dans ce contexte)
    - Paramètres de requête : notation '%s' (identique à psycopg)
    - ensure_database() : connexion sans base cible via DSN sans path,
      puis CREATE DATABASE IF NOT EXISTS
    - drop_database()   : DROP DATABASE via connexion système
    - Moteur InnoDB par défaut (transactions, FK, index B-tree)

DSN format : mysql+pymysql://utilisateur:motdepasse@hôte:3306/ma_base
"""

import time
import logging
from urllib.parse import urlparse, urlunparse

import pymysql
import pymysql.cursors
import pandas as pd

from connectors.base import DBConnector

log = logging.getLogger(__name__)

TABLE            = "insee_etablissements"
INDEX_COL        = "siret"               # index pour lookup par identifiant
INDEX_FILTER_COL = "etat_administratif"  # index pour accélérer les lectures filtrées
FILTER_VAL       = "F"                   # etat_administratif = 'F' (fermé, minoritaire)

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


def _parse_dsn(dsn: str) -> dict:
    """
    Parse le DSN mysql+pymysql://user:pwd@host:port/dbname
    et retourne un dict de kwargs pour pymysql.connect().
    """
    # Supprimer le préfixe 'mysql+pymysql://' si présent
    raw = dsn.replace("mysql+pymysql://", "mysql://")
    p = urlparse(raw)
    kwargs = {
        "host":   p.hostname or "localhost",
        "port":   p.port or 3306,
        "user":   p.username or "root",
        "passwd": p.password or "",
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.Cursor,
    }
    db = p.path.lstrip("/")
    if db:
        kwargs["db"] = db
    return kwargs


def _db_name(dsn: str) -> str:
    """Extrait le nom de la base depuis le DSN."""
    raw = dsn.replace("mysql+pymysql://", "mysql://")
    return urlparse(raw).path.lstrip("/")


class MySQLConnector(DBConnector):

    name = "mysql"

    def __init__(self, dsn: str):
        super().__init__(dsn)
        self._conn: pymysql.Connection | None = None
        self._db_name_val = _db_name(dsn)
        MySQLConnector._db_name_val = self._db_name_val  # accès depuis vector_search_approx

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    def ensure_database(self) -> None:
        """
        Crée la base cible si elle n'existe pas.
        Connexion sans base (pas de 'db' dans les kwargs).
        """
        kwargs = _parse_dsn(self.dsn)
        kwargs.pop("db", None)          # connexion au serveur sans base cible

        conn = pymysql.connect(**kwargs)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self._db_name_val}` "
                    f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            conn.commit()
            log.info("MySQL : base '%s' prête.", self._db_name_val)
        finally:
            conn.close()

    def connect(self) -> None:
        kwargs = _parse_dsn(self.dsn)
        self._conn = pymysql.connect(**kwargs)
        self._conn.autocommit(False)
        log.info("MySQL connecté : %s", self.dsn)

    def disconnect(self) -> None:
        if self._conn and self._conn.open:
            self._conn.close()

    def setup(self) -> None:
        """Recrée la table cible (DROP + CREATE)."""
        ddl = f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            siret               VARCHAR(14),
            date_debut          DATE,
            date_fin            DATE,
            etat_administratif  VARCHAR(1),
            enseigne1           TEXT,
            activite_principale VARCHAR(10),
            caractere_employeur VARCHAR(1)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{TABLE}`")
            cur.execute(ddl)
        # DDL MySQL implicitement commité — pas de commit() nécessaire

    def teardown(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{TABLE}`")

    def get_version(self) -> str:
        """Retourne la version MySQL/MariaDB (ex: 'MySQL 8.0.35')."""
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT VERSION()")
                row = cur.fetchone()
            return f"MySQL {row[0]}" if row else ""
        except Exception:
            return ""

    # ------------------------------------------------------------------
    # Opérations vectorielles — MyVector plugin
    # ------------------------------------------------------------------
    # Prérequis : plugin MyVector installé sur le serveur MySQL 8.x
    # Image Docker : ghcr.io/askdba/myvector:mysql-8.4
    #
    # API MyVector :
    #   - Colonne VARBINARY avec COMMENT 'MYVECTOR(type=HNSW,dim=N,...)'
    #   - MYVECTOR_CONSTRUCT('[v1,v2,...]')  → bytes pour INSERT
    #   - myvector_distance(vec1, vec2)      → distance L2
    #   - CALL mysql.myvector_index_build()  → construit l'index HNSW
    #   - MYVECTOR_IS_ANN(...)               → recherche ANN
    # ------------------------------------------------------------------

    has_vector_support = True
    VECTOR_TABLE  = "insee_vecteurs"
    _db_name_val  = None   # initialisé dans __init__ via _db_name_val

    def _myvector_check(self) -> bool:
        """Vérifie que le plugin MyVector est disponible."""
        try:
            with self._conn.cursor() as cur:
                cur.execute("SELECT myvector_construct('[0.0]')")
                cur.fetchone()
            return True
        except Exception:
            log.warning(
                "MyVector non disponible — benchmark vectoriel ignoré.\n"
                "  → Utiliser l'image Docker ghcr.io/askdba/myvector:mysql-8.4"
            )
            return False

    def vector_setup(self) -> None:
        """Crée la table vectorielle MyVector."""
        if not self._myvector_check():
            raise RuntimeError("MyVector absent — benchmark vectoriel ignoré.")

        varbinary_size = self.VECTOR_DIM * 4 + 8   # 4 bytes/float + overhead
        ddl = f"""
        CREATE TABLE IF NOT EXISTS `{self.VECTOR_TABLE}` (
            id        INT AUTO_INCREMENT PRIMARY KEY,
            siret     VARCHAR(14),
            embedding VARBINARY({varbinary_size})
            COMMENT 'MYVECTOR(type=HNSW,dim={self.VECTOR_DIM},size=2000000,dist=L2)'
        ) ENGINE=InnoDB
        """
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{self.VECTOR_TABLE}`")
            cur.execute(ddl)
        log.info("MyVector : table '%s' créée (dim=%d).", self.VECTOR_TABLE, self.VECTOR_DIM)

    def vector_teardown(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{self.VECTOR_TABLE}`")

    def vector_insert(self, n_rows: int) -> float:
        """Insertion via MYVECTOR_CONSTRUCT() — convertit JSON string → bytes."""
        vecs = self.generate_vectors(n_rows)
        sql  = (
            f"INSERT INTO `{self.VECTOR_TABLE}` (siret, embedding) "
            f"VALUES (%s, MYVECTOR_CONSTRUCT(%s))"
        )
        records = [
            (f"{i:014d}", "[" + ",".join(f"{v:.6f}" for v in vec) + "]")
            for i, vec in enumerate(vecs)
        ]
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.executemany(sql, records)
        self._conn.commit()
        return time.perf_counter() - t0

    def vector_search_exact(self, n_rows: int, k: int = 10) -> float:
        """Recherche exacte via myvector_distance() — scan séquentiel."""
        query_vec = self.generate_vectors(1)[0]
        vec_str   = "[" + ",".join(f"{v:.6f}" for v in query_vec) + "]"
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT siret, myvector_distance(embedding, MYVECTOR_CONSTRUCT(%s)) AS dist "
                f"FROM `{self.VECTOR_TABLE}` ORDER BY dist LIMIT %s",
                (vec_str, k),
            )
            cur.fetchall()
        return time.perf_counter() - t0

    def vector_search_approx(self, n_rows: int, k: int = 10) -> float:
        """
        Recherche ANN via MYVECTOR_IS_ANN() après construction de l'index HNSW.
        L'index est construit avant la mesure (hors temps).
        """
        # Construire l'index HNSW (hors mesure)
        db = self._db_name_val or "db_cutoff"
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    f"CALL mysql.myvector_index_build("
                    f"'{db}.{self.VECTOR_TABLE}.embedding', 'id')"
                )
        except Exception as e:
            log.warning("myvector_index_build : %s", e)

        query_vec = self.generate_vectors(1)[0]
        vec_str   = "[" + ",".join(f"{v:.6f}" for v in query_vec) + "]"
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT siret, myvector_row_distance() AS dist "
                f"FROM `{self.VECTOR_TABLE}` "
                f"WHERE MYVECTOR_IS_ANN('{db}.{self.VECTOR_TABLE}.embedding', 'id', "
                f"MYVECTOR_CONSTRUCT(%s), %s)",
                (vec_str, k),
            )
            cur.fetchall()
        return time.perf_counter() - t0

    def drop_database(self) -> None:
        """Supprime la base entière après le benchmark."""
        kwargs = _parse_dsn(self.dsn)
        kwargs.pop("db", None)

        conn = pymysql.connect(**kwargs)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS `{self._db_name_val}`")
            conn.commit()
            log.info("MySQL : base '%s' supprimée.", self._db_name_val)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Helpers internes
    # ------------------------------------------------------------------

    def _index_exists(self, index_name: str) -> bool:
        """Vérifie l'existence d'un index via information_schema — compatible toutes versions."""
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT 1 FROM information_schema.STATISTICS
                   WHERE table_schema = DATABASE()
                   AND table_name = %s
                   AND index_name  = %s
                   LIMIT 1""",
                (TABLE, index_name),
            )
            return cur.fetchone() is not None

    def _drop_index(self) -> None:
        """
        Supprime les index de benchmark.
        Utilise information_schema pour vérifier l'existence avant le DROP —
        évite l'erreur de syntaxe sur MySQL < 8.0.16 qui ne supporte pas
        DROP INDEX IF EXISTS.
        """
        with self._conn.cursor() as cur:
            for col in [INDEX_COL, INDEX_FILTER_COL]:
                idx = f"idx_{TABLE}_{col}"
                if self._index_exists(idx):
                    cur.execute(f"DROP INDEX `{idx}` ON `{TABLE}`")

    def _create_index(self) -> None:
        """
        Crée deux index si absents :
        - idx sur siret               → lookup par identifiant
        - idx sur etat_administratif  → filtre sur valeur minoritaire (F)
        Même logique : vérification via information_schema pour compatibilité
        avec MySQL < 8.0.16 qui ne supporte pas CREATE INDEX IF NOT EXISTS.
        """
        with self._conn.cursor() as cur:
            for col in [INDEX_COL, INDEX_FILTER_COL]:
                idx = f"idx_{TABLE}_{col}"
                if not self._index_exists(idx):
                    cur.execute(
                        f"CREATE INDEX `{idx}` ON `{TABLE}` (`{col}`)"
                    )

    def _truncate(self) -> None:
        """
        TRUNCATE TABLE sous MySQL/InnoDB.
        Pas de conflit de verrous avec les index (comportement différent
        de PostgreSQL) — TRUNCATE est safe ici.
        """
        with self._conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE `{TABLE}`")

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
        """
        Insertion en lot via executemany().
        MySQL ne dispose pas de protocole COPY — executemany() est
        la méthode la plus rapide disponible avec pymysql.
        """
        self._truncate()
        self._drop_index()
        records = self._df_to_records(df)
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.executemany(INSERT_SQL, records)
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
            cur.execute(f"SELECT * FROM `{TABLE}` LIMIT %s", (n_rows,))
            cur.fetchall()
        return time.perf_counter() - t0

    def read_filtered(self, n_rows: int) -> float:
        self._drop_index()
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM `{TABLE}` "
                f"WHERE etat_administratif = %s LIMIT %s",
                (FILTER_VAL, n_rows),
            )
            cur.fetchall()
        return time.perf_counter() - t0

    def read_full_indexed(self, n_rows: int) -> float:
        self._create_index()
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{TABLE}` LIMIT %s", (n_rows,))
            cur.fetchall()
        return time.perf_counter() - t0

    def read_filtered_indexed(self, n_rows: int) -> float:
        self._create_index()
        t0 = time.perf_counter()
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM `{TABLE}` "
                f"WHERE etat_administratif = %s LIMIT %s",
                (FILTER_VAL, n_rows),
            )
            cur.fetchall()
        return time.perf_counter() - t0