"""
connectors/mongodb.py — Connecteur MongoDB via pymongo

Dépendance : pymongo>=4.6

Spécificités MongoDB vs SQL :
    - NoSQL orienté documents — pas de table mais une collection
    - ensure_database() : base et collection créées implicitement
      au premier insert (MongoDB crée à la volée)
    - drop_database()   : client.drop_database()
    - write_bulk()      : insert_many() avec ordered=False
      (désactive l'ordre pour maximiser le parallélisme interne)
    - write_row_by_row(): insert_one() en boucle
    - Pas d'index par défaut sur siret — benchmarké avec/sans
    - Filtre : find({'etat_administratif': 'F'})  # valeur minoritaire, haute sélectivité
    - DSN format : mongodb://utilisateur:mdp@host:27017
      La base cible est extraite du DSN ou définie par MONGODB_DB
"""

import time
import logging
from urllib.parse import urlparse

import pandas as pd
from pymongo import MongoClient, ASCENDING
from pymongo.errors import CollectionInvalid

from connectors.base import DBConnector

log = logging.getLogger(__name__)

COLLECTION       = "insee_etablissements"
INDEX_COL        = "siret"               # index pour lookup par identifiant
INDEX_FILTER_COL = "etat_administratif"  # index pour accélérer les lectures filtrées
FILTER_VAL       = "F"                   # etat_administratif = 'F' (fermé)
# 'F' est la valeur MINORITAIRE — haute sélectivité, l'index est rentable
# 'A' (actif, majoritaire) → MongoDB ferait un collection scan même avec index

# Nom de la base par défaut si absent du DSN
DEFAULT_DB  = "db_cutoff"


def _client_kwargs(dsn: str) -> dict:
    """
    Retourne les kwargs communs à tous les MongoClient du connecteur.

    directConnection=True : force la connexion directe au nœud sans
    passer par la découverte de topology. Nécessaire quand MongoDB tourne
    en mode Replica Set sur une instance locale (server_type: RSGhost) —
    pymongo ne peut pas élire de Primary sans ce paramètre.
    Peut être désactivé via MONGODB_DIRECT_CONNECTION=false dans .env.
    """
    import os
    direct = os.environ.get("MONGODB_DIRECT_CONNECTION", "true").lower() != "false"
    return {"directConnection": direct}


def _db_name(dsn: str) -> str:
    """
    Extrait le nom de la base depuis le DSN.
    mongodb://host:27017/ma_base → 'ma_base'
    mongodb://host:27017        → DEFAULT_DB
    """
    path = urlparse(dsn).path.lstrip("/")
    return path if path else DEFAULT_DB


def _server_dsn(dsn: str) -> str:
    """Retourne le DSN sans la partie base (pour connexion serveur)."""
    p = urlparse(dsn)
    return p._replace(path="").geturl()


class MongoDBConnector(DBConnector):

    name = "mongodb"

    def __init__(self, dsn: str):
        super().__init__(dsn)
        self._client: MongoClient | None = None
        self._db = None
        self._col = None
        self._db_name = _db_name(dsn)

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    def ensure_database(self) -> None:
        """
        MongoDB crée la base et la collection implicitement au premier insert.
        On vérifie simplement que le serveur est joignable.
        """
        client = MongoClient(
            _server_dsn(self.dsn),
            serverSelectionTimeoutMS=5_000,
            **_client_kwargs(self.dsn),
        )
        try:
            client.admin.command("ping")
            existing = client.list_database_names()
            if self._db_name in existing:
                log.info("MongoDB : base '%s' déjà existante.", self._db_name)
            else:
                log.info(
                    "MongoDB : base '%s' absente — sera créée au premier insert.",
                    self._db_name,
                )
        finally:
            client.close()

    def connect(self) -> None:
        self._client = MongoClient(
            self.dsn,
            serverSelectionTimeoutMS=10_000,
            **_client_kwargs(self.dsn),
        )
        self._db  = self._client[self._db_name]
        self._col = self._db[COLLECTION]
        log.info("MongoDB connecté : %s / %s", self.dsn, self._db_name)

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            self._db  = None
            self._col = None

    def setup(self) -> None:
        """Recrée la collection (drop si existante)."""
        if COLLECTION in self._db.list_collection_names():
            self._col.drop()
        self._db.create_collection(COLLECTION)
        self._col = self._db[COLLECTION]
        log.info("MongoDB : collection '%s' créée.", COLLECTION)

    def teardown(self) -> None:
        self._col.drop()
        log.info("MongoDB : collection '%s' supprimée.", COLLECTION)

    def drop_database(self) -> None:
        """Supprime la base entière via le client."""
        if self._client is None:
            # Ouvrir une connexion temporaire pour le DROP
            client = MongoClient(
                self.dsn,
                serverSelectionTimeoutMS=5_000,
                **_client_kwargs(self.dsn),
            )
            try:
                client.drop_database(self._db_name)
                log.info("MongoDB : base '%s' supprimée.", self._db_name)
            finally:
                client.close()
        else:
            self._client.drop_database(self._db_name)
            log.info("MongoDB : base '%s' supprimée.", self._db_name)

    # ------------------------------------------------------------------
    # Helpers internes
    # ------------------------------------------------------------------

    def _drop_index(self) -> None:
        """Supprime tous les index de benchmark."""
        for idx in [f"{INDEX_COL}_1", f"{INDEX_FILTER_COL}_1"]:
            try:
                self._col.drop_index(idx)
            except Exception:
                pass  # Index absent — pas d'erreur

    def _create_index(self) -> None:
        """
        Crée deux index :
        - idx sur siret               → lookup par identifiant
        - idx sur etat_administratif  → filtre sur valeur minoritaire (F)
        MongoDB utilise l'index si la sélectivité est suffisante ;
        sur 'F' (minoritaire), le gain sera visible dès ~50k documents.
        """
        self._col.create_index([(INDEX_COL, ASCENDING)], name=f"{INDEX_COL}_1")
        self._col.create_index(
            [(INDEX_FILTER_COL, ASCENDING)],
            name=f"{INDEX_FILTER_COL}_1",
        )

    def _truncate(self) -> None:
        self._col.delete_many({})

    @staticmethod
    def _df_to_docs(df: pd.DataFrame) -> list[dict]:
        """
        Convertit un DataFrame en liste de documents MongoDB.

        Conversions appliquées (ordre important) :
        1. None                → None
        2. pd.NaT / NaN        → None  (testé EN PREMIER — NaT est instance de
                                         datetime et planterait sur utcoffset())
        3. datetime.date seul  → datetime.datetime minuit (BSON n'accepte pas date)
        4. Autres valeurs      → inchangées
        """
        import datetime as dt
        docs = []
        for row in df.itertuples(index=False):
            doc = {}
            for field, val in zip(df.columns, row):
                # 1. None explicite
                if val is None:
                    doc[field] = None
                # 2. NaT / NaN — DOIT être testé avant isinstance(date/datetime)
                #    car pd.NaT est instance de datetime et lève ValueError sur utcoffset()
                elif pd.isna(val):
                    doc[field] = None
                # 3. datetime.date (pas datetime) → datetime.datetime
                elif isinstance(val, dt.date) and not isinstance(val, dt.datetime):
                    doc[field] = dt.datetime(val.year, val.month, val.day)
                # 4. Valeur valide — passage direct
                else:
                    doc[field] = val
            docs.append(doc)
        return docs

    # ------------------------------------------------------------------
    # Opérations benchmarkées
    # ------------------------------------------------------------------

    def write_bulk(self, df: pd.DataFrame) -> float:
        """
        Insertion en lot via insert_many(ordered=False).
        ordered=False : MongoDB n'arrête pas sur une erreur individuelle
        et peut paralléliser les writes en interne.
        """
        self._truncate()
        self._drop_index()
        docs = self._df_to_docs(df)
        t0 = time.perf_counter()
        self._col.insert_many(docs, ordered=False)
        return time.perf_counter() - t0

    def write_row_by_row(self, df: pd.DataFrame) -> float:
        """Insertion document par document via insert_one()."""
        self._truncate()
        self._drop_index()
        docs = self._df_to_docs(df)
        t0 = time.perf_counter()
        for doc in docs:
            self._col.insert_one(doc)
        return time.perf_counter() - t0

    def read_full(self, n_rows: int) -> float:
        self._drop_index()
        t0 = time.perf_counter()
        list(self._col.find({}, limit=n_rows))
        return time.perf_counter() - t0

    def read_filtered(self, n_rows: int) -> float:
        self._drop_index()
        t0 = time.perf_counter()
        list(self._col.find({"etat_administratif": FILTER_VAL}, limit=n_rows))
        return time.perf_counter() - t0

    def read_full_indexed(self, n_rows: int) -> float:
        self._create_index()
        t0 = time.perf_counter()
        list(self._col.find({}, limit=n_rows))
        return time.perf_counter() - t0

    def read_filtered_indexed(self, n_rows: int) -> float:
        self._create_index()
        t0 = time.perf_counter()
        list(self._col.find({"etat_administratif": FILTER_VAL}, limit=n_rows))
        return time.perf_counter() - t0