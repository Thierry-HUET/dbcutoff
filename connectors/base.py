"""
connectors/base.py — Interface abstraite pour tous les connecteurs DB
"""

from abc import ABC, abstractmethod
import numpy as np
import pandas as pd


class DBConnector(ABC):
    """
    Contrat à implémenter pour chaque base de données cible.
    Chaque méthode retourne le temps d'exécution en secondes (float).

    Cycle de vie attendu par bench_runner :
        1. ensure_database()  — crée la base si elle n'existe pas
        2. connect()          — ouvre la connexion à la base
        3. setup()            — crée la table / collection
        4. [benchmark]
        5. teardown()         — supprime la table / collection
        6. disconnect()       — ferme la connexion
        7. drop_database()    — supprime la base entière (nettoyage final)

    Opérations vectorielles (optionnelles) :
        Les méthodes vector_* sont non abstraites et retournent None par défaut.
        Implémenter uniquement si la base supporte les vecteurs.
        Le bench_runner vérifie has_vector_support avant de les appeler.
    """

    name: str = "base"

    # Passer à True dans les connecteurs qui implémentent les méthodes vector_*
    has_vector_support: bool = False

    # Dimension des vecteurs générés pour le benchmark
    VECTOR_DIM: int = 128

    def __init__(self, dsn: str):
        self.dsn = dsn

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------

    @abstractmethod
    def ensure_database(self) -> None:
        """
        Vérifie que la base cible existe et la crée si nécessaire.
        Appelé AVANT connect() — doit se connecter au serveur sans
        spécifier la base (ex: connexion à 'postgres' pour PostgreSQL).
        """

    @abstractmethod
    def connect(self) -> None:
        """Établit la connexion à la base cible."""

    @abstractmethod
    def disconnect(self) -> None:
        """Ferme la connexion proprement."""

    @abstractmethod
    def setup(self) -> None:
        """Crée les structures (table, collection, index…) si besoin."""

    @abstractmethod
    def teardown(self) -> None:
        """Supprime les structures (tables, collections…) créées lors du benchmark."""

    @abstractmethod
    def drop_database(self) -> None:
        """
        Supprime la base entière après le benchmark.
        Appelé APRÈS disconnect() — doit se connecter à la base système,
        pas à la base cible (qui n'est plus accessible après DROP).
        """

    # ------------------------------------------------------------------
    # Opérations benchmarkées — scalaires
    # ------------------------------------------------------------------

    @abstractmethod
    def write_bulk(self, df: pd.DataFrame) -> float:
        """Insertion en lot. Retourne la durée en secondes."""

    @abstractmethod
    def write_row_by_row(self, df: pd.DataFrame) -> float:
        """Insertion ligne par ligne. Retourne la durée en secondes."""

    @abstractmethod
    def read_full(self, n_rows: int) -> float:
        """Lecture complète de n_rows lignes. Retourne la durée en secondes."""

    @abstractmethod
    def read_filtered(self, n_rows: int) -> float:
        """Lecture avec filtre (WHERE). Retourne la durée en secondes."""

    @abstractmethod
    def read_full_indexed(self, n_rows: int) -> float:
        """Lecture complète après création d'index. Retourne la durée en secondes."""

    @abstractmethod
    def read_filtered_indexed(self, n_rows: int) -> float:
        """Lecture filtrée après création d'index. Retourne la durée en secondes."""

    # ------------------------------------------------------------------
    # Opérations vectorielles (optionnelles)
    # ------------------------------------------------------------------

    def vector_setup(self) -> None:
        """
        Crée la table/collection vectorielle et les extensions nécessaires.
        Appelé avant les opérations vector_*.
        """

    def vector_teardown(self) -> None:
        """Supprime la table/collection vectorielle."""

    def vector_insert(self, n_rows: int) -> float:
        """
        Insère n_rows vecteurs aléatoires de dimension VECTOR_DIM.
        Retourne la durée en secondes.
        """
        return NotImplemented

    def vector_search_exact(self, n_rows: int, k: int = 10) -> float:
        """
        Recherche exacte des k plus proches voisins (brute force).
        Retourne la durée en secondes.
        """
        return NotImplemented

    def vector_search_approx(self, n_rows: int, k: int = 10) -> float:
        """
        Recherche approximative (ANN) via index HNSW ou IVFFlat.
        Retourne la durée en secondes.
        """
        return NotImplemented

    # ------------------------------------------------------------------
    # Utilitaire de génération de vecteurs
    # ------------------------------------------------------------------

    @classmethod
    def generate_vectors(cls, n: int) -> np.ndarray:
        """
        Génère n vecteurs aléatoires normalisés de dimension VECTOR_DIM.
        Les vecteurs sont normalisés (norme L2 = 1) pour la similarité cosinus.
        """
        rng = np.random.default_rng(seed=42)
        vecs = rng.standard_normal((n, cls.VECTOR_DIM)).astype(np.float32)
        norms = np.linalg.norm(vecs, axis=1, keepdims=True)
        return vecs / norms

    # ------------------------------------------------------------------
    # Utilitaires
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} dsn={self.dsn!r}>"