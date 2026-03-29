"""
config.py — Paramètres globaux du benchmark DB Cutoff

Chargement automatique du fichier .env à la racine du projet.
Les variables d'environnement définies dans le shell ont priorité sur .env.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Charge .env depuis la racine du projet (silencieux si absent)
load_dotenv(Path(__file__).parent / ".env")

# --- Chemins ---
BASE_DIR   = Path(__file__).parent
STORAGE_DB = BASE_DIR / "storage" / "results.db"
INSEE_FILE = os.environ.get(
    "INSEE_FILE",
    str(BASE_DIR / "data" / "StockEtablissementHistorique_utf8.csv"),
)

# --- Benchmark ---
VOLUMES         = [100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000]
BATCH_SIZES     = [100, 1_000, 10_000]
REPETITIONS     = 3
TIMEOUT_SECONDS = 300

# --- Colonnes retenues du fichier INSEE ---
INSEE_COLUMNS = [
    "siret",                              # identifiant établissement (14 car.)
    "dateDebut",                          # début de la période
    "dateFin",                            # fin de la période (vide = en cours)
    "etatAdministratifEtablissement",     # A=actif, F=fermé
    "enseigne1Etablissement",             # enseigne commerciale
    "activitePrincipaleEtablissement",    # code NAF/APE
    "caractereEmployeurEtablissement",    # O=employeur, N=non-employeur
]

# --- Bases de données disponibles ---
DATABASES = [
    {
        "name": "postgresql",
        "module": "connectors.postgres",
        "enabled": True,
        "dsn": os.environ.get(
            "POSTGRES_DSN",
            "postgresql://postgres:postgres@localhost:5432/db_cutoff",
        ),
    },
    # Futurs connecteurs :
    {
        "name": "duckdb",
        "module": "connectors.duckdb",
        "enabled": True,
        "dsn": os.environ.get("DUCKDB_DSN", str(BASE_DIR / "data" / "cutoff.duckdb")),
    },
    {
        "name": "mongodb",
        "module": "connectors.mongodb",
        "enabled": True,
        "dsn": os.environ.get(
            "MONGODB_DSN",
            "mongodb://localhost:27017/db_cutoff",
        ),
    },
    # { "name": "neo4j",     "module": "connectors.neo4j",     "enabled": False, "dsn": "bolt://localhost:7687" },
    # { "name": "cassandra", "module": "connectors.cassandra", "enabled": False, "dsn": "localhost" },
]