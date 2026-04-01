"""
connectors/couchdb.py — Connecteur CouchDB via requests (API HTTP REST)

Dépendance : requests>=2.31

Spécificités CouchDB vs SQL/MongoDB :
    - API HTTP REST — pas de driver binaire, tout passe par HTTP
    - Pas de SQL — requêtes via l'API Mango (_find) avec selector JSON
    - Index = design document (_index) créé via POST /_index
    - Base = répertoire HTTP — création par PUT /db_name
    - Pas de TRUNCATE — la base est supprimée et recrée (DELETE + PUT)
    - write_bulk() : utilise l'API _bulk_docs (lot de documents en un POST)
    - write_row_by_row() : POST /_doc par document
    - Lecture : POST /_find avec selector Mango + limit
    - Les _id CouchDB sont générés automatiquement si absents

DSN format : http://utilisateur:motdepasse@host:5984/ma_base
"""

import time
import logging
from urllib.parse import urlparse

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import datetime as dt

from connectors.base import DBConnector

log = logging.getLogger(__name__)

COLLECTION       = "insee_etablissements"   # = nom de la base CouchDB
INDEX_COL        = "siret"
INDEX_FILTER_COL = "etat_administratif"
FILTER_VAL       = "F"   # valeur minoritaire — haute sélectivité

# Taille des lots pour _bulk_docs
BULK_CHUNK_SIZE  = 10_000


def _parse_dsn(dsn: str) -> tuple[str, str, HTTPBasicAuth | None]:
    """
    Parse le DSN http://user:pwd@host:port/dbname.
    Retourne (base_url, db_name, auth).
    """
    p = urlparse(dsn)
    db_name = p.path.lstrip("/") or COLLECTION
    base_url = f"{p.scheme}://{p.hostname}:{p.port or 5984}"
    auth = HTTPBasicAuth(p.username, p.password) if p.username else None
    return base_url, db_name, auth


class CouchDBConnector(DBConnector):

    name = "couchdb"

    def __init__(self, dsn: str):
        super().__init__(dsn)
        self._base_url, self._db_name, self._auth = _parse_dsn(dsn)
        self._session: requests.Session | None = None

    def _url(self, path: str = "") -> str:
        return f"{self._base_url}/{self._db_name}{path}"

    def _server_url(self, path: str = "") -> str:
        return f"{self._base_url}{path}"

    def _check(self, resp: requests.Response, label: str) -> None:
        if not resp.ok:
            raise RuntimeError(f"CouchDB [{label}] {resp.status_code} : {resp.text}")

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    def _make_session(self) -> requests.Session:
        """Crée une session requests authentifiée — utilisée avant connect()."""
        s = requests.Session()
        if self._auth:
            s.auth = self._auth
        s.headers.update({"Content-Type": "application/json"})
        return s

    def ensure_database(self) -> None:
        """
        Crée la base si elle n'existe pas (PUT /db_name).
        Appelé AVANT connect() — ouvre une session temporaire.
        """
        s = self._session or self._make_session()
        close_after = self._session is None
        try:
            resp = s.get(self._server_url("/_up"))
            self._check(resp, "ping")

            resp = s.head(self._url())
            if resp.status_code == 404:
                log.info("CouchDB : base '%s' absente — création…", self._db_name)
                r = s.put(self._url())
                self._check(r, "create_db")
                log.info("CouchDB : base '%s' créée.", self._db_name)
            elif resp.ok:
                log.info("CouchDB : base '%s' déjà existante.", self._db_name)
            else:
                self._check(resp, "ensure_db")
        finally:
            if close_after:
                s.close()

    def connect(self) -> None:
        self._session = self._make_session()
        log.info("CouchDB connecté : %s / %s", self._base_url, self._db_name)

    def disconnect(self) -> None:
        if self._session:
            self._session.close()
            self._session = None

    def setup(self) -> None:
        """Recrée la base (DROP + PUT) — CouchDB n'a pas de TRUNCATE."""
        # Supprimer si existante
        resp = self._session.head(self._url())
        if resp.ok:
            # Récupérer le _rev de la base pour le DELETE
            info = self._session.get(self._url()).json()
            self._session.delete(self._url())
        # Recréer
        r = self._session.put(self._url())
        self._check(r, "setup")
        log.info("CouchDB : base '%s' recrée.", self._db_name)

    def teardown(self) -> None:
        """Supprime la base."""
        resp = self._session.delete(self._url())
        if not resp.ok and resp.status_code != 404:
            log.warning("CouchDB teardown : %s", resp.text)

    def drop_database(self) -> None:
        """Supprime la base après le benchmark."""
        # Ouvrir une session temporaire si déconnecté
        if self._session is None:
            s = requests.Session()
            if self._auth:
                s.auth = self._auth
        else:
            s = self._session

        resp = s.delete(self._url())
        if resp.ok or resp.status_code == 404:
            log.info("CouchDB : base '%s' supprimée.", self._db_name)
        else:
            log.warning("CouchDB drop_database : %s", resp.text)

        if self._session is None:
            s.close()

    # ------------------------------------------------------------------
    # Helpers internes
    # ------------------------------------------------------------------

    def _drop_index(self) -> None:
        """
        Supprime les design documents d'index créés par le benchmark.
        Les index Mango sont stockés dans _design/idx_<col>.
        """
        for col in [INDEX_COL, INDEX_FILTER_COL]:
            ddoc_id = f"_design/idx_{col}"
            resp = self._session.get(self._url(f"/{ddoc_id}"))
            if resp.ok:
                rev = resp.json().get("_rev")
                self._session.delete(self._url(f"/{ddoc_id}?rev={rev}"))

    def _create_index(self) -> None:
        """
        Crée des index Mango via POST /_index.
        CouchDB stocke ces index dans des design documents internes.
        """
        for col in [INDEX_COL, INDEX_FILTER_COL]:
            payload = {
                "index": {"fields": [col]},
                "name":  f"idx_{col}",
                "type":  "json",
            }
            r = self._session.post(self._url("/_index"), json=payload)
            if not r.ok:
                log.warning("CouchDB _create_index [%s] : %s", col, r.text)

    def _truncate(self) -> None:
        """
        CouchDB n'a pas de TRUNCATE — suppression + recréation de la base.
        """
        self.teardown()
        r = self._session.put(self._url())
        self._check(r, "truncate")

    @staticmethod
    def _df_to_docs(df: pd.DataFrame) -> list[dict]:
        """
        Convertit un DataFrame en liste de documents CouchDB.
        - NaN / NaT          → None
        - datetime.date      → chaîne ISO 'YYYY-MM-DD'
          (CouchDB/BSON n'a pas de type date natif — string est idiomatique)
        """
        docs = []
        for row in df.itertuples(index=False):
            doc = {}
            for field, val in zip(df.columns, row):
                if val is None or pd.isna(val):
                    doc[field] = None
                elif isinstance(val, dt.date) and not isinstance(val, dt.datetime):
                    doc[field] = val.isoformat()
                else:
                    doc[field] = val
            docs.append(doc)
        return docs

    # ------------------------------------------------------------------
    # Opérations benchmarkées
    # ------------------------------------------------------------------

    def write_bulk(self, df: pd.DataFrame) -> float:
        """
        Insertion en lot via _bulk_docs.
        Les documents sont envoyés par chunks de BULK_CHUNK_SIZE pour
        éviter les requêtes HTTP trop volumineuses (limite 16 MB).
        """
        self._truncate()
        self._drop_index()
        docs = self._df_to_docs(df)
        t0 = time.perf_counter()
        for i in range(0, len(docs), BULK_CHUNK_SIZE):
            chunk = docs[i: i + BULK_CHUNK_SIZE]
            r = self._session.post(self._url("/_bulk_docs"), json={"docs": chunk})
            self._check(r, "write_bulk")
        return time.perf_counter() - t0

    def write_row_by_row(self, df: pd.DataFrame) -> float:
        """Insertion document par document via POST /_doc — lent par design."""
        self._truncate()
        self._drop_index()
        docs = self._df_to_docs(df)
        t0 = time.perf_counter()
        for doc in docs:
            r = self._session.post(self._url(), json=doc)
            self._check(r, "write_row")
        return time.perf_counter() - t0

    def read_full(self, n_rows: int) -> float:
        """Lecture via _all_docs avec include_docs=true."""
        self._drop_index()
        t0 = time.perf_counter()
        r = self._session.get(
            self._url("/_all_docs"),
            params={"include_docs": "true", "limit": n_rows},
        )
        self._check(r, "read_full")
        _ = r.json()
        return time.perf_counter() - t0

    def read_filtered(self, n_rows: int) -> float:
        """Lecture filtrée via l'API Mango (_find) sans index."""
        self._drop_index()
        payload = {
            "selector": {"etat_administratif": {"$eq": FILTER_VAL}},
            "limit":    n_rows,
        }
        t0 = time.perf_counter()
        r = self._session.post(self._url("/_find"), json=payload)
        self._check(r, "read_filtered")
        _ = r.json()
        return time.perf_counter() - t0

    def read_full_indexed(self, n_rows: int) -> float:
        """Lecture complète après création d'index Mango."""
        self._create_index()
        t0 = time.perf_counter()
        r = self._session.get(
            self._url("/_all_docs"),
            params={"include_docs": "true", "limit": n_rows},
        )
        self._check(r, "read_full_indexed")
        _ = r.json()
        return time.perf_counter() - t0

    def read_filtered_indexed(self, n_rows: int) -> float:
        """Lecture filtrée avec index Mango sur etat_administratif."""
        self._create_index()
        payload = {
            "selector": {"etat_administratif": {"$eq": FILTER_VAL}},
            "limit":    n_rows,
            "use_index": f"idx_{INDEX_FILTER_COL}",
        }
        t0 = time.perf_counter()
        r = self._session.post(self._url("/_find"), json=payload)
        self._check(r, "read_filtered_indexed")
        _ = r.json()
        return time.perf_counter() - t0